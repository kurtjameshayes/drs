"""
Shared tools for the LangGraph data source discovery workflow.

This module provides tools for web searching, URL fetching, and HTTP requests
that can be used by the various agents in the workflow.

IMPORTANT: Methods in this module (fetch_url, search_data_gov, etc.) use the
requests library and BeautifulSoup to parse static HTML. These methods DO NOT
execute JavaScript. If you need to analyze HTML content with XPath or perform
interactive browser operations, use BrowserAutomationService instead.

For XPath operations on HTML content:
- ALWAYS use BrowserAutomationService.get_page_content() which executes JavaScript
- NEVER use static HTML from requests library with XPath queries
"""

import logging
import time
import json
import re
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

from langchain_core.tools import tool
from langchain_community.tools.tavily_search import TavilySearchResults

from config import Config

logger = logging.getLogger(__name__)


# =============================================================================
# DATA SOURCE REGISTRIES
# =============================================================================

# Known data source registries to search
DATA_REGISTRIES = [
    {
        "name": "Data.gov",
        "url": "https://catalog.data.gov/api/3/action/package_search",
        "search_param": "q",
        "type": "government"
    },
    {
        "name": "APIs.guru",
        "url": "https://api.apis.guru/v2/list.json",
        "search_param": None,  # Full list, filter client-side
        "type": "open_source"
    },
]

# Known patterns for identifying API documentation
API_DOC_PATTERNS = [
    r"/api/?$",
    r"/api/v\d+",
    r"/docs/?$",
    r"/documentation/?$",
    r"/developers/?$",
    r"/api-docs/?$",
    r"/swagger",
    r"/openapi",
    r"/graphql",
]

# Known patterns for download sections
DOWNLOAD_PATTERNS = [
    r"/download",
    r"/data/?$",
    r"/datasets?/?$",
    r"/files/?$",
    r"/export",
    r"\.csv",
    r"\.json",
    r"\.xml",
    r"\.xlsx",
]


# =============================================================================
# WEB SEARCH TOOL
# =============================================================================

def create_tavily_search() -> TavilySearchResults:
    """Create a Tavily search tool instance."""
    return TavilySearchResults(
        max_results=Config.DISCOVERY_MAX_SEARCH_RESULTS,
        api_key=Config.TAVILY_API_KEY,
    )


@tool
def web_search(query: str) -> List[Dict[str, Any]]:
    """
    Search the web for data sources using Tavily.

    Args:
        query: Search query string

    Returns:
        List of search results with url, title, and content
    """
    try:
        tavily = create_tavily_search()
        results = tavily.invoke(query)

        # Normalize results
        normalized = []
        for result in results:
            if isinstance(result, dict):
                normalized.append({
                    "url": result.get("url", ""),
                    "title": result.get("title", ""),
                    "content": result.get("content", ""),
                })
        return normalized
    except Exception as e:
        logger.error(f"Web search failed: {e}")
        return []


@tool
def search_data_gov(query: str) -> List[Dict[str, Any]]:
    """
    Search Data.gov for government data sources.

    Args:
        query: Search query string

    Returns:
        List of data sources found
    """
    try:
        url = "https://catalog.data.gov/api/3/action/package_search"
        params = {
            "q": query,
            "rows": Config.DISCOVERY_MAX_SEARCH_RESULTS,
        }

        response = requests.get(
            url,
            params=params,
            timeout=Config.DISCOVERY_REQUEST_TIMEOUT
        )
        response.raise_for_status()

        data = response.json()
        results = []

        if data.get("success") and "result" in data:
            for package in data["result"].get("results", []):
                # Get the best resource URL
                resources = package.get("resources", [])
                api_resources = [r for r in resources if "api" in r.get("format", "").lower()]
                best_resource = api_resources[0] if api_resources else (resources[0] if resources else None)

                results.append({
                    "name": package.get("title", package.get("name", "")),
                    "url": best_resource.get("url", "") if best_resource else package.get("url", ""),
                    "description": package.get("notes", "")[:500] if package.get("notes") else "",
                    "source_type": "government",
                    "format": best_resource.get("format", "") if best_resource else "",
                    "organization": package.get("organization", {}).get("title", ""),
                })

        return results
    except Exception as e:
        logger.error(f"Data.gov search failed: {e}")
        return []


@tool
def search_apis_guru(query: str) -> List[Dict[str, Any]]:
    """
    Search APIs.guru for public APIs.

    Args:
        query: Search query string (matched against API names and descriptions)

    Returns:
        List of matching APIs
    """
    try:
        response = requests.get(
            "https://api.apis.guru/v2/list.json",
            timeout=Config.DISCOVERY_REQUEST_TIMEOUT
        )
        response.raise_for_status()

        all_apis = response.json()
        query_lower = query.lower()
        results = []

        for api_id, api_data in all_apis.items():
            # Get the preferred version
            preferred = api_data.get("preferred", "")
            versions = api_data.get("versions", {})
            version_info = versions.get(preferred, {})

            info = version_info.get("info", {})
            title = info.get("title", "")
            description = info.get("description", "")

            # Check if query matches
            if (query_lower in title.lower() or
                query_lower in description.lower() or
                query_lower in api_id.lower()):

                swagger_url = version_info.get("swaggerUrl", "")
                results.append({
                    "name": title,
                    "url": swagger_url,
                    "description": description[:500] if description else "",
                    "source_type": "open_source",
                    "api_id": api_id,
                    "version": preferred,
                })

                if len(results) >= Config.DISCOVERY_MAX_SEARCH_RESULTS:
                    break

        return results
    except Exception as e:
        logger.error(f"APIs.guru search failed: {e}")
        return []


# =============================================================================
# URL FETCHING TOOLS
# =============================================================================

@tool
def fetch_url(url: str, extract_links: bool = True) -> Dict[str, Any]:
    """
    Fetch a URL and extract its content (static HTML, NO JavaScript execution).

    This method uses requests library and returns the initial HTML only, without
    executing any JavaScript. For content that requires JavaScript execution or
    XPath-based analysis, use BrowserAutomationService.get_page_content() instead.

    Args:
        url: URL to fetch
        extract_links: Whether to extract links from the page

    Returns:
        Dictionary with page content (static HTML), title, and optionally links
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; DataDiscoveryBot/1.0)"
        }

        response = requests.get(
            url,
            headers=headers,
            timeout=Config.DISCOVERY_REQUEST_TIMEOUT,
            allow_redirects=True
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # Extract title
        title = ""
        if soup.title:
            title = soup.title.string or ""

        # Extract main text content
        # Remove script and style elements
        for element in soup(["script", "style", "nav", "footer", "header"]):
            element.decompose()

        text = soup.get_text(separator="\n", strip=True)
        # Limit text length
        text = text[:10000] if len(text) > 10000 else text

        result = {
            "url": url,
            "title": title,
            "content": text,
            "status_code": response.status_code,
        }

        if extract_links:
            links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                link_text = a.get_text(strip=True)

                # Convert relative URLs to absolute
                if href.startswith("/"):
                    href = urljoin(url, href)
                elif not href.startswith(("http://", "https://")):
                    continue

                links.append({
                    "url": href,
                    "text": link_text[:100] if link_text else "",
                })

            result["links"] = links[:50]  # Limit number of links

        return result
    except requests.exceptions.Timeout:
        return {"error": "Request timed out", "url": url}
    except requests.exceptions.RequestException as e:
        return {"error": str(e), "url": url}
    except Exception as e:
        logger.error(f"Error fetching URL {url}: {e}")
        return {"error": str(e), "url": url}


@tool
def check_api_availability(base_url: str) -> Dict[str, Any]:
    """
    Check if a URL has API documentation or endpoints available.

    Args:
        base_url: Base URL to check

    Returns:
        Dictionary with API availability information
    """
    result = {
        "has_api": False,
        "api_urls": [],
        "has_openapi": False,
        "openapi_url": None,
        "has_graphql": False,
        "graphql_url": None,
    }

    parsed = urlparse(base_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    # Common API documentation paths to check
    api_paths = [
        "/api",
        "/api/v1",
        "/api/v2",
        "/docs",
        "/documentation",
        "/developers",
        "/api-docs",
        "/swagger.json",
        "/openapi.json",
        "/graphql",
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; DataDiscoveryBot/1.0)"
    }

    for path in api_paths:
        try:
            check_url = urljoin(base, path)
            response = requests.head(
                check_url,
                headers=headers,
                timeout=5,
                allow_redirects=True
            )

            if response.status_code in (200, 301, 302):
                result["has_api"] = True
                result["api_urls"].append(check_url)

                if "swagger" in path or "openapi" in path:
                    result["has_openapi"] = True
                    result["openapi_url"] = check_url
                elif "graphql" in path:
                    result["has_graphql"] = True
                    result["graphql_url"] = check_url

        except requests.exceptions.RequestException:
            continue

    return result


# =============================================================================
# HTTP REQUEST TOOL WITH RETRY
# =============================================================================

def make_http_request(
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    json_data: Optional[Dict[str, Any]] = None,
    timeout: Optional[int] = None,
    max_retries: Optional[int] = None,
    backoff_factor: Optional[float] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Make an HTTP request with retry logic.

    Args:
        url: URL to request
        method: HTTP method (GET, POST, etc.)
        headers: Request headers
        params: Query parameters
        data: Form data
        json_data: JSON body data
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts
        backoff_factor: Backoff multiplier for retries

    Returns:
        Tuple of (success, result_dict)
    """
    timeout = timeout or Config.DISCOVERY_REQUEST_TIMEOUT
    max_retries = max_retries or Config.DISCOVERY_TEST_RETRIES
    backoff_factor = backoff_factor or Config.DISCOVERY_TEST_BACKOFF

    default_headers = {
        "User-Agent": "Mozilla/5.0 (compatible; DataDiscoveryBot/1.0)",
        "Accept": "application/json",
    }
    if headers:
        default_headers.update(headers)

    last_error = None
    attempts = 0

    for attempt in range(max_retries):
        attempts = attempt + 1
        try:
            start_time = time.time()

            response = requests.request(
                method=method.upper(),
                url=url,
                headers=default_headers,
                params=params,
                data=data,
                json=json_data,
                timeout=timeout,
                allow_redirects=True,
            )

            elapsed_ms = (time.time() - start_time) * 1000

            # Try to parse JSON response
            try:
                response_data = response.json()
            except json.JSONDecodeError:
                response_data = {"raw_text": response.text[:1000]}

            return True, {
                "success": response.status_code < 400,
                "status_code": response.status_code,
                "response_time_ms": round(elapsed_ms, 2),
                "headers": dict(response.headers),
                "data": response_data,
                "attempts": attempts,
            }

        except requests.exceptions.Timeout as e:
            last_error = f"Request timed out after {timeout}s"
            logger.warning(f"Request attempt {attempts} timed out: {url}")

        except requests.exceptions.ConnectionError as e:
            last_error = f"Connection error: {str(e)}"
            logger.warning(f"Request attempt {attempts} connection error: {url}")

        except requests.exceptions.RequestException as e:
            last_error = f"Request error: {str(e)}"
            logger.warning(f"Request attempt {attempts} failed: {url} - {e}")

        # Backoff before retry (except on last attempt)
        if attempt < max_retries - 1:
            wait_time = backoff_factor ** attempt
            logger.info(f"Retrying in {wait_time}s...")
            time.sleep(wait_time)

    return False, {
        "success": False,
        "error": last_error,
        "attempts": attempts,
    }


@tool
def test_api_endpoint(
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Test an API endpoint with retry logic.

    Args:
        url: API endpoint URL
        method: HTTP method
        headers: Request headers (including auth)
        params: Query parameters

    Returns:
        Test results dictionary
    """
    success, result = make_http_request(
        url=url,
        method=method,
        headers=headers,
        params=params,
    )

    return result


# =============================================================================
# OPENAPI PARSING TOOL
# =============================================================================

@tool
def parse_openapi_spec(url: str) -> Dict[str, Any]:
    """
    Fetch and parse an OpenAPI/Swagger specification.

    Args:
        url: URL to the OpenAPI spec (JSON or YAML)

    Returns:
        Parsed specification with endpoints and schemas
    """
    try:
        response = requests.get(
            url,
            headers={"Accept": "application/json"},
            timeout=Config.DISCOVERY_REQUEST_TIMEOUT
        )
        response.raise_for_status()

        spec = response.json()

        # Extract key information
        info = spec.get("info", {})
        servers = spec.get("servers", [])
        paths = spec.get("paths", {})

        # Parse endpoints
        endpoints = []
        for path, methods in paths.items():
            for method, details in methods.items():
                if method in ("get", "post", "put", "delete", "patch"):
                    endpoint = {
                        "path": path,
                        "method": method.upper(),
                        "summary": details.get("summary", ""),
                        "description": details.get("description", "")[:200],
                        "parameters": [],
                    }

                    # Extract parameters
                    for param in details.get("parameters", []):
                        endpoint["parameters"].append({
                            "name": param.get("name", ""),
                            "in": param.get("in", ""),
                            "required": param.get("required", False),
                            "description": param.get("description", "")[:100],
                        })

                    endpoints.append(endpoint)

        return {
            "title": info.get("title", ""),
            "description": info.get("description", "")[:500],
            "version": info.get("version", ""),
            "base_url": servers[0].get("url", "") if servers else "",
            "endpoints": endpoints[:20],  # Limit to first 20
            "auth_schemes": list(spec.get("components", {}).get("securitySchemes", {}).keys()),
        }

    except Exception as e:
        logger.error(f"Failed to parse OpenAPI spec: {e}")
        return {"error": str(e)}


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def detect_access_methods(page_content: str, links: List[Dict[str, str]]) -> Dict[str, bool]:
    """
    Detect available access methods from page content and links.

    Args:
        page_content: Text content of the page
        links: List of links found on the page

    Returns:
        Dictionary with access method flags
    """
    content_lower = page_content.lower()
    all_urls = [link.get("url", "").lower() for link in links]
    all_text = " ".join([link.get("text", "").lower() for link in links])

    has_api = any([
        "api" in content_lower and ("endpoint" in content_lower or "rest" in content_lower),
        any(re.search(pattern, url) for url in all_urls for pattern in API_DOC_PATTERNS),
        "api key" in content_lower,
        "api documentation" in content_lower,
        "swagger" in content_lower,
        "openapi" in content_lower,
        "graphql" in content_lower,
    ])

    has_web_service = any([
        "soap" in content_lower,
        "wsdl" in content_lower,
        "web service" in content_lower and "api" not in content_lower,
    ])

    has_download = any([
        any(re.search(pattern, url) for url in all_urls for pattern in DOWNLOAD_PATTERNS),
        "download" in all_text,
        "csv" in all_text or "json" in all_text or "xml" in all_text,
        "bulk data" in content_lower,
        "data files" in content_lower,
    ])

    return {
        "has_api": has_api,
        "has_web_service": has_web_service,
        "has_download": has_download,
    }


def find_documentation_url(base_url: str, links: List[Dict[str, str]]) -> Optional[str]:
    """
    Find the most likely documentation URL from a list of links.

    Args:
        base_url: Base URL of the site
        links: List of links found on the page

    Returns:
        Best documentation URL or None
    """
    doc_keywords = ["api", "docs", "documentation", "developer", "reference", "guide"]

    for link in links:
        url = link.get("url", "").lower()
        text = link.get("text", "").lower()

        for keyword in doc_keywords:
            if keyword in url or keyword in text:
                return link.get("url")

    return None
