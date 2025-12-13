"""
FBI Crime Data Explorer API Connector

This connector provides access to the FBI's Crime Data Explorer API,
which includes national and state-level crime statistics.

API Documentation: https://crime-data-explorer.fr.cloud.gov/pages/docApi
"""

import requests
import time
import re
from typing import Dict, List, Any, Optional
from urllib.parse import urlencode
from core.base_connector import BaseConnector
import logging

logger = logging.getLogger(__name__)


class FBICrimeConnector(BaseConnector):
    """
    Connector for FBI Crime Data Explorer API.
    
    Provides access to:
    - National crime estimates
    - State crime estimates
    - Agency data
    - Offense data
    - Arrest data
    
    Supports dynamic query parameters with placeholders like {param_name}
    that can be substituted at runtime.
    """
    
    # Pattern to match dynamic placeholders like {from mm-yyyy} or {to}
    DYNAMIC_PLACEHOLDER_PATTERN = re.compile(r'^\{[^}]+\}$')
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize FBI Crime Data connector.
        
        Args:
            config: Configuration dictionary containing:
                - url: Base API URL
                - api_key: FBI Crime Data API key
                - format: Response format (default: JSON)
        """
        super().__init__(config)
        default_base_url = 'https://api.usa.gov/crime/fbi/sapi'
        self.base_url = config.get('url', default_base_url).rstrip('/')
        self.api_key = config.get('api_key')
        self.format = config.get('format', 'JSON').upper()
        self.session = None
        self.max_retries = config.get('max_retries', 3)
        self.retry_delay = config.get('retry_delay', 1)

        base_url_lower = self.base_url.lower()
        self._api_key_param_explicit = 'api_key_param' in config
        self.api_key_param = config.get('api_key_param')
        if not self.api_key_param:
            self.api_key_param = 'API_KEY' if 'cde' in base_url_lower else 'api_key'

        api_namespace = config.get('api_namespace')
        if api_namespace is None:
            self.api_namespace = '' if 'cde' in base_url_lower else 'api'
        else:
            self.api_namespace = api_namespace.strip('/')

        self._year_mode_explicit = 'year_mode' in config
        year_mode = config.get('year_mode')
        if year_mode not in {'path', 'query'}:
            self.year_mode = 'query' if 'cde' in base_url_lower else 'path'
        else:
            self.year_mode = year_mode
        
    def connect(self) -> bool:
        """
        Establish connection resources without performing validation requests.
        
        Returns:
            bool: True if session initialization succeeded
        """
        try:
            if not self.session:
                self.session = requests.Session()
                self.session.headers.update({'Accept': 'application/json'})
            self.connected = True
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to initialize FBI Crime Data session: {str(e)}")
            self.connected = False
            return False
        except Exception as e:
            logger.error(f"Unexpected error initializing FBI Crime Data session: {str(e)}")
            self.connected = False
            return False
    
    def disconnect(self) -> bool:
        """
        Close connection to FBI Crime Data API.
        
        Returns:
            bool: True if disconnection successful
        """
        try:
            if self.session:
                self.session.close()
                self.session = None
            
            self.connected = False
            logger.info("Disconnected from FBI Crime Data API")
            return True
            
        except Exception as e:
            logger.error(f"Error disconnecting from FBI Crime Data API: {str(e)}")
            return False
    
    def query(self, parameters: Dict[str, Any], 
              dynamic_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a query against FBI Crime Data API.
        
        Args:
            parameters: Query parameters including:
                - endpoint: API endpoint (e.g., 'estimates/national', 'arrest/national/all')
                - Additional endpoint-specific parameters (can include placeholders like {from mm-yyyy})
            dynamic_params: Optional dictionary of values to substitute for dynamic placeholders
                Example: {"from": "01-2023", "to": "12-2023"}
        
        Returns:
            dict: Query results with metadata
            
        Example:
            # With stored query parameters containing placeholders:
            parameters = {
                "endpoint": "arrest/national/all",
                "type": "counts",
                "from": "{from mm-yyyy}",
                "to": "{to mm-yyyy}"
            }
            # Provide actual values at runtime:
            dynamic_params = {"from": "01-2023", "to": "12-2023"}
            
            result = connector.query(parameters, dynamic_params)
        """
        if not self.connected:
            if not self.connect():
                raise ConnectionError("Failed to connect to FBI Crime Data API")
        
        try:
            # Build the complete request URL with all parameters
            url, query_params = self._build_request_url(parameters, dynamic_params)
            
            response = self._execute_with_retry(url, query_params)
            
            # Parse response
            data = response.json()
            
            # Transform to standard format
            transformed_data = self.transform(data)
            
            return {
                'success': True,
                'data': transformed_data,
                'metadata': {
                    'endpoint': parameters.get('endpoint', ''),
                    'parameters': parameters,
                    'dynamic_params': dynamic_params,
                    'final_url': url,
                    'status_code': response.status_code
                }
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"FBI Crime Data API request failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error querying FBI Crime Data API: {str(e)}")
            raise
    
    def _execute_with_retry(self, url: str, params: Dict[str, Any]) -> requests.Response:
        """
        Execute request with exponential backoff retry logic.
        
        Args:
            url: Request URL
            params: Query parameters
            
        Returns:
            requests.Response: HTTP response
        """
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                full_url = self._compose_request_url(url, params)
                logger.info(
                    "Executing FBI Crime API query attempt=%s url=%s",
                    attempt + 1,
                    full_url,
                )

                response = self.session.get(url, params=params, timeout=30)
                
                # Check for rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', self.retry_delay))
                    logger.warning(f"Rate limited. Retrying after {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}). "
                                 f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Request failed after {self.max_retries} attempts")
        
        raise last_exception

    def _is_dynamic_placeholder(self, value: Any) -> bool:
        """
        Check if a value is a dynamic placeholder (e.g., {from mm-yyyy}).
        
        Args:
            value: The value to check
            
        Returns:
            bool: True if the value is a dynamic placeholder
        """
        if not isinstance(value, str):
            return False
        return bool(self.DYNAMIC_PLACEHOLDER_PATTERN.match(value.strip()))
    
    def _extract_param_name_from_placeholder(self, placeholder: str) -> str:
        """
        Extract the parameter name from a placeholder string.
        
        For example:
            "{from mm-yyyy}" -> "from"
            "{to}" -> "to"
            "{state_code}" -> "state_code"
        
        Args:
            placeholder: The placeholder string (e.g., "{from mm-yyyy}")
            
        Returns:
            str: The extracted parameter name
        """
        # Remove the braces
        inner = placeholder.strip()[1:-1]
        # Get the first word (parameter name)
        param_name = inner.split()[0]
        return param_name

    def _build_request_url(
        self,
        query: Dict[str, Any],
        dynamic_params: Optional[Dict[str, Any]] = None,
    ) -> tuple:
        """
        Construct the complete request URL with all parameters.
        
        This method processes the query parameters in the following order:
        1. Start with the base URL from connector config
        2. Append the endpoint from query parameters
        3. Append all hard-coded parameter values
        4. Substitute dynamic placeholders with provided values
        5. Add API key
        
        Args:
            query: The full query object containing:
                - endpoint: API endpoint path
                - other parameters (can be hard-coded or dynamic placeholders)
            dynamic_params: Optional dict of values to substitute for dynamic placeholders
            
        Returns:
            tuple: (url, query_params) - The final URL and query parameters dict
            
        Example:
            query = {
                "endpoint": "arrest/national/all",
                "type": "counts",
                "from": "{from mm-yyyy}",
                "to": "{to mm-yyyy}"
            }
            dynamic_params = {"from": "01-2023", "to": "12-2023"}
            
            # Returns:
            # url = "https://api.usa.gov/crime/fbi/cde/arrest/national/all"
            # query_params = {"type": "counts", "from": "01-2023", "to": "12-2023", "API_KEY": "..."}
        """
        dynamic_params = dynamic_params or {}
        
        # Step 1: Start with base URL
        print(f"\n[FBI Query Builder] Step 1 - Base URL: {self.base_url}")
        
        # Step 2: Get and process endpoint
        endpoint = (query.get('endpoint', '') or '').strip()
        print(f"[FBI Query Builder] Step 2 - Endpoint from query: {endpoint}")
        
        # Determine if this is a CDE-style endpoint
        is_cde = self._is_cde_endpoint(endpoint) or 'cde' in self.base_url.lower()
        
        # Build the URL path
        if endpoint.startswith('http://') or endpoint.startswith('https://'):
            # Full URL provided as endpoint
            url = endpoint
            print(f"[FBI Query Builder] Full URL provided as endpoint: {url}")
        else:
            clean_endpoint = endpoint.lstrip('/')
            namespace = self.api_namespace.strip('/') if self.api_namespace else ''
            
            if namespace and not is_cde:
                if clean_endpoint.startswith(f"{namespace}/"):
                    route = clean_endpoint
                elif clean_endpoint == namespace:
                    route = namespace
                else:
                    route = "/".join(part for part in [namespace, clean_endpoint] if part)
            else:
                route = clean_endpoint
            
            parts: List[str] = [self.base_url]
            if route:
                parts.append(route)
            
            # Normalize parts
            normalized_parts: List[str] = []
            for idx, part in enumerate(parts):
                if not part:
                    continue
                normalized_parts.append(part.rstrip('/') if idx == 0 else part.strip('/'))
            
            url = "/".join(normalized_parts)
        
        print(f"[FBI Query Builder] Step 3 - URL without parameters: {url}")
        
        # Step 3 & 4: Process query parameters
        query_params: Dict[str, Any] = {}
        hard_coded_params: Dict[str, Any] = {}
        dynamic_placeholders: Dict[str, str] = {}  # Maps param name -> placeholder
        
        # Keys that should not be added as query parameters
        excluded_keys = {'endpoint', 'api_key', 'API_KEY', self.api_key_param}
        
        # Categorize parameters as hard-coded or dynamic
        for key, value in query.items():
            if key in excluded_keys:
                continue
            if value is None:
                continue
                
            if self._is_dynamic_placeholder(value):
                # This is a dynamic placeholder
                param_name = self._extract_param_name_from_placeholder(value)
                dynamic_placeholders[key] = value
                print(f"[FBI Query Builder] Found dynamic placeholder: {key}={value} (param name: {param_name})")
            else:
                # This is a hard-coded value
                hard_coded_params[key] = value
                print(f"[FBI Query Builder] Found hard-coded parameter: {key}={value}")
        
        # Step 4a: Add hard-coded parameters
        print(f"[FBI Query Builder] Step 4a - Adding hard-coded parameters: {hard_coded_params}")
        query_params.update(hard_coded_params)
        
        # Step 4b: Substitute dynamic placeholders
        print(f"[FBI Query Builder] Step 4b - Processing dynamic placeholders with values: {dynamic_params}")
        for key, placeholder in dynamic_placeholders.items():
            param_name = self._extract_param_name_from_placeholder(placeholder)
            
            # Look for the value in dynamic_params using the key name first, then param_name
            if key in dynamic_params:
                query_params[key] = dynamic_params[key]
                print(f"[FBI Query Builder] Substituted {key}: {placeholder} -> {dynamic_params[key]}")
            elif param_name in dynamic_params:
                query_params[key] = dynamic_params[param_name]
                print(f"[FBI Query Builder] Substituted {key}: {placeholder} -> {dynamic_params[param_name]}")
            else:
                print(f"[FBI Query Builder] WARNING: No value provided for dynamic parameter '{key}' (placeholder: {placeholder})")
        
        # Step 5: Add API key
        api_key_param = self._resolve_api_key_param(endpoint)
        if self.api_key:
            query_params[api_key_param] = self.api_key
            print(f"[FBI Query Builder] Step 5 - Added API key parameter: {api_key_param}=***")
        
        # Build final URL for logging
        final_url = self._compose_request_url(url, query_params)
        print(f"[FBI Query Builder] Final URL (for logging): {final_url}")
        print(f"[FBI Query Builder] Query parameters: { {k: '***' if 'key' in k.lower() else v for k, v in query_params.items()} }")
        
        return url, query_params

    def _resolve_api_key_param(self, endpoint: Optional[str]) -> str:
        """
        Determine which API key parameter name to use for a request.
        
        For CDE endpoints, uses 'API_KEY' (uppercase).
        For SAPI endpoints, uses 'api_key' (lowercase).
        """
        if self._api_key_param_explicit:
            return self.api_key_param

        if self._is_cde_endpoint(endpoint) or 'cde' in self.base_url.lower():
            return 'API_KEY'

        return self.api_key_param

    @staticmethod
    def _is_cde_endpoint(endpoint: Optional[str]) -> bool:
        """Check if the endpoint is for the CDE (Crime Data Explorer) API."""
        if not endpoint:
            return False
        endpoint_lower = endpoint.strip().lower()
        if endpoint_lower.startswith('http://') or endpoint_lower.startswith('https://'):
            return '/crime/fbi/cde/' in endpoint_lower
        return False
    
    def validate(self) -> bool:
        """
        Validate FBI Crime Data API connection and credentials.
        
        Returns:
            bool: True if validation successful
        """
        try:
            if not self.api_key:
                logger.error("API key is required for FBI Crime Data API")
                return False
            
            # Test connection
            if not self.connect():
                return False
            
            # Test a simple query
            test_params = {
                'endpoint': 'estimates/national',
                'from': '2020',
                'to': '2020'
            }
            
            result = self.query(test_params)
            
            if result.get('success'):
                logger.info("FBI Crime Data API validation successful")
                return True
            else:
                logger.error("FBI Crime Data API validation failed")
                return False
                
        except Exception as e:
            logger.error(f"FBI Crime Data API validation error: {str(e)}")
            return False
        finally:
            self.disconnect()
    
    def transform(self, data: Any) -> Dict[str, Any]:
        """
        Transform FBI Crime Data API response to standard format.
        
        Args:
            data: Raw API response data
            
        Returns:
            dict: Transformed data in standard format
        """
        try:
            # FBI Crime Data API returns data in various formats
            # Most endpoints return a 'results' array
            if isinstance(data, dict):
                # Check for common response structures
                if 'results' in data:
                    records = data['results']
                elif 'data' in data:
                    records = data['data']
                else:
                    # Treat the whole response as a single record
                    records = [data]
            elif isinstance(data, list):
                records = data
            else:
                records = [{'value': data}]
            
            # Ensure records is a list
            if not isinstance(records, list):
                records = [records]
            
            return {
                'data': records,
                'metadata': {
                    'source': 'FBI Crime Data Explorer',
                    'record_count': len(records),
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S')
                }
            }
            
        except Exception as e:
            logger.error(f"Error transforming FBI Crime Data: {str(e)}")
            raise
    
    def get_capabilities(self) -> Dict[str, Any]:
        """
        Get connector capabilities.
        
        Returns:
            dict: Connector capabilities
        """
        return {
            'name': 'FBI Crime Data Explorer',
            'version': '1.0',
            'supported_endpoints': [
                'estimates/national',
                'estimates/states/{state_abbr}',
                'agencies',
                'agencies/count',
                'offenses',
                'arrests/national'
            ],
            'features': {
                'pagination': True,
                'filtering': True,
                'time_series': True,
                'geographic_queries': True
            },
            'data_types': [
                'crime_estimates',
                'agency_data',
                'offense_data',
                'arrest_data'
            ],
            'geographic_levels': [
                'national',
                'state',
                'agency'
            ]
        }
    
    def get_available_endpoints(self) -> List[str]:
        """
        Get list of available API endpoints.
        
        Returns:
            list: Available endpoints
        """
        return [
            'estimates/national',
            'estimates/states/{state_abbr}',
            'estimates/states/{state_abbr}/{offense}',
            'agencies',
            'agencies/{ori}',
            'agencies/count',
            'offenses',
            'arrests/national',
            'arrests/states/{state_abbr}'
        ]
    
    def get_state_abbreviations(self) -> List[str]:
        """
        Get list of valid state abbreviations.
        
        Returns:
            list: State abbreviations
        """
        return [
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL',
            'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME',
            'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH',
            'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI',
            'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
        ]
