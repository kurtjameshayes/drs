"""
Browser Automation Service

Service for automating web browser interactions using Playwright.
Used to navigate websites, fill forms, and extract API keys.
Includes native Shadow DOM support for extracting content from Shadow DOM elements.
"""

import logging
import time
import os
import base64
import re
from typing import Optional, Dict, Any, List
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)


class BrowserAutomationService:
    """
    Service for browser automation using Playwright.

    Provides functions for:
    - Navigating to URLs
    - Getting page content (with JavaScript execution and Shadow DOM extraction)
    - Clicking elements (including those in Shadow DOM)
    - Filling form fields (including those in Shadow DOM)
    - Submitting forms
    - Taking screenshots

    IMPORTANT: All XPath and CSS selector operations in this service operate on the
    live DOM with JavaScript fully executed. This ensures that dynamically-generated
    content is included when selecting elements. Shadow DOM content is automatically
    extracted and included in page content operations.

    Shadow DOM Support:
    - Playwright provides native Shadow DOM piercing with '>>' selectors
    - get_page_content() automatically extracts all Shadow DOM content recursively
    - Element selectors can pierce Shadow DOM boundaries using Playwright's syntax
    """

    def __init__(self, headless: bool = True, timeout: int = 30):
        """
        Initialize the browser automation service.

        Args:
            headless: Run browser in headless mode (no visible window)
            timeout: Default timeout for operations in seconds
        """
        self.headless = headless
        self.timeout = timeout
        self.browser = None
        self.context = None
        self.page = None
        self.playwright = None
        self._initialized = False

    def _ensure_driver(self):
        """Initialize the Playwright browser if not already initialized."""
        if self._initialized and self.page:
            return

        try:
            from playwright.sync_api import sync_playwright

            # Start Playwright
            self.playwright = sync_playwright().start()

            # Launch browser
            self.browser = self.playwright.chromium.launch(
                headless=self.headless,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ]
            )

            # Create context with custom user agent
            self.context = self.browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                ignore_https_errors=True,
            )

            # Set default timeout
            self.context.set_default_timeout(self.timeout * 1000)  # Convert to milliseconds

            # Create page
            self.page = self.context.new_page()

            self._initialized = True

            logger.info("Browser automation service initialized successfully with Playwright")

        except ImportError as e:
            logger.error(
                "playwright not installed. Install with: pip install playwright && playwright install chromium"
            )
            raise ImportError(
                "playwright package is required for Browser Automation Service. "
                "Install with: pip install playwright && playwright install chromium"
            ) from e
        except Exception as e:
            logger.error(f"Failed to initialize browser: {e}")
            raise

    def navigate_to_url(self, url: str) -> Dict[str, Any]:
        """
        Navigate the browser to a specific URL.

        Args:
            url: The URL to navigate to

        Returns:
            Result dict with success status and current URL
        """
        self._ensure_driver()

        try:
            logger.info(f"Navigating to: {url}")
            self.page.goto(url, wait_until="domcontentloaded")

            # Wait for page to stabilize (including JavaScript execution)
            time.sleep(2)

            return {
                "success": True,
                "current_url": self.page.url,
                "title": self.page.title(),
            }
        except Exception as e:
            logger.error(f"Failed to navigate to {url}: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    def get_page_content(self) -> Dict[str, Any]:
        """
        Get the current page's content with JavaScript-rendered HTML and Shadow DOM extraction.

        This method executes JavaScript to capture the fully-rendered DOM, including:
        - All dynamically-generated content
        - ALL Shadow DOM content (recursively extracted)
        - Nested Shadow DOM structures

        The Shadow DOM extraction process:
        1. Finds all elements with shadowRoot property
        2. Recursively extracts Shadow DOM content
        3. Merges Shadow DOM HTML into the main document structure

        Returns:
            Dict with page HTML (JavaScript-rendered + Shadow DOM), visible text, and metadata
        """
        self._ensure_driver()

        try:
            # JavaScript to recursively extract all Shadow DOM content
            shadow_dom_extraction_script = """
            function extractAllShadowDOM() {
                // Recursive function to extract shadow DOM content
                function extractShadowContent(element) {
                    if (!element || !element.shadowRoot) {
                        return null;
                    }

                    // Get the shadow root HTML
                    let shadowHTML = element.shadowRoot.innerHTML;

                    // Find nested shadow DOM elements within this shadow root
                    const shadowElements = element.shadowRoot.querySelectorAll('*');
                    shadowElements.forEach(child => {
                        if (child.shadowRoot) {
                            const nestedContent = extractShadowContent(child);
                            if (nestedContent) {
                                // Inject nested shadow content into the HTML
                                shadowHTML = shadowHTML.replace(
                                    child.outerHTML,
                                    child.outerHTML + '<!-- SHADOW-ROOT: -->' + nestedContent
                                );
                            }
                        }
                    });

                    return shadowHTML;
                }

                // Find all elements with shadow roots in the main document
                const allElements = document.querySelectorAll('*');
                const shadowHosts = [];

                allElements.forEach(element => {
                    if (element.shadowRoot) {
                        const shadowContent = extractShadowContent(element);
                        if (shadowContent) {
                            shadowHosts.push({
                                selector: element.className ? '.' + element.className.split(' ').join('.') : element.tagName,
                                html: shadowContent,
                                element: element.outerHTML
                            });
                        }
                    }
                });

                return shadowHosts;
            }

            return extractAllShadowDOM();
            """

            # Get the rendered DOM after JavaScript execution
            html_content = self.page.evaluate("document.documentElement.outerHTML")

            # Extract Shadow DOM content
            shadow_doms = self.page.evaluate(shadow_dom_extraction_script)

            # Inject Shadow DOM content into the HTML
            if shadow_doms and len(shadow_doms) > 0:
                logger.info(f"Found {len(shadow_doms)} Shadow DOM roots")
                for shadow_dom in shadow_doms:
                    # Find the shadow host element in the HTML and inject its content
                    shadow_host_html = shadow_dom.get('element', '')
                    shadow_content = shadow_dom.get('html', '')

                    if shadow_host_html and shadow_content:
                        # Insert shadow content after the shadow host element
                        # This makes it accessible for XPath and other HTML analysis
                        replacement = f"{shadow_host_html}\n<!-- SHADOW DOM CONTENT -->\n{shadow_content}\n<!-- END SHADOW DOM -->"
                        html_content = html_content.replace(shadow_host_html, replacement, 1)

            # Strip <head> element before processing
            html_content = re.sub(r'<head[^>]*>.*?</head>', '', html_content, flags=re.IGNORECASE | re.DOTALL)

            # Get visible text (excluding scripts and styles)
            visible_text = self.page.evaluate("""
                () => {
                    // Get text from main document
                    let text = document.body.innerText;

                    // Also get text from Shadow DOM
                    const shadowHosts = document.querySelectorAll('*');
                    shadowHosts.forEach(element => {
                        if (element.shadowRoot) {
                            text += '\\n' + element.shadowRoot.textContent;
                        }
                    });

                    return text;
                }
            """)

            # Get page metadata
            title = self.page.title()
            current_url = self.page.url

            return {
                "success": True,
                "html": html_content,
                "visible_text": visible_text,
                "title": title,
                "url": current_url,
            }
        except Exception as e:
            logger.error(f"Failed to get page content: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    def click_element(self, selector: str) -> Dict[str, Any]:
        """
        Click on an element identified by CSS selector, XPath, or text content.

        Supports Shadow DOM piercing:
        - Use >> syntax to pierce Shadow DOM: '.shadow-host >> #button'
        - Playwright automatically handles Shadow DOM boundaries

        Args:
            selector: CSS selector, XPath, or text content to find the element
                     These selectors operate on the live DOM with JavaScript fully executed,
                     so all dynamically-generated content is included.

        Returns:
            Result dict with success status
        """
        self._ensure_driver()

        try:
            element = None
            error_messages = []

            # Try different strategies to find the element
            strategies = []

            # If it looks like an XPath, try it first
            if selector.startswith("//") or selector.startswith("("):
                strategies.append(("xpath", selector))

            # CSS selector (may include >> for Shadow DOM piercing)
            strategies.append(("css", selector))

            # Text content strategies
            strategies.append(("text", selector))
            strategies.append(("text", f"*{selector}*"))  # Partial match

            for strategy_type, locator in strategies:
                try:
                    if strategy_type == "xpath":
                        element = self.page.locator(f"xpath={locator}").first
                    elif strategy_type == "css":
                        element = self.page.locator(locator).first
                    elif strategy_type == "text":
                        element = self.page.get_by_text(locator).first

                    # Check if element exists and is visible
                    if element and element.count() > 0:
                        # Scroll into view and click
                        element.scroll_into_view_if_needed()
                        element.click(timeout=10000)

                        # Wait for any navigation or updates
                        time.sleep(1)

                        logger.info(f"Clicked element: {selector}")
                        return {
                            "success": True,
                            "current_url": self.page.url,
                        }
                except Exception as e:
                    error_messages.append(f"{strategy_type}={locator}: {str(e)}")
                    continue

            return {
                "success": False,
                "error": f"Could not find clickable element: {selector}. Tried: {'; '.join(error_messages)}",
            }

        except Exception as e:
            logger.error(f"Failed to click element {selector}: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    def fill_form_field(self, selector: str, value: str) -> Dict[str, Any]:
        """
        Fill a form field with the provided value.

        Supports Shadow DOM piercing:
        - Use >> syntax to pierce Shadow DOM: '.shadow-host >> input[name="email"]'
        - Playwright automatically handles Shadow DOM boundaries

        Args:
            selector: CSS selector, name, or ID of the form field
                     XPath and CSS selectors operate on the live DOM with JavaScript
                     fully executed, ensuring dynamic form fields are included.
            value: Value to fill in

        Returns:
            Result dict with success status
        """
        self._ensure_driver()

        try:
            element = None
            error_messages = []

            # Try different strategies to find the input field
            strategies = [
                ("css", selector),
                ("css", f"[name='{selector}']"),
                ("css", f"#{selector}"),
                ("xpath", f"//input[@name='{selector}']"),
                ("xpath", f"//input[@id='{selector}']"),
                ("xpath", f"//input[@placeholder='{selector}']"),
                ("xpath", f"//textarea[@name='{selector}']"),
                ("xpath", f"//textarea[@id='{selector}']"),
            ]

            for strategy_type, locator in strategies:
                try:
                    if strategy_type == "xpath":
                        element = self.page.locator(f"xpath={locator}").first
                    else:
                        element = self.page.locator(locator).first

                    # Check if element exists
                    if element and element.count() > 0:
                        # Fill the field
                        element.fill(value, timeout=10000)

                        logger.info(f"Filled form field {selector}")
                        return {
                            "success": True,
                        }
                except Exception as e:
                    error_messages.append(f"{strategy_type}={locator}: {str(e)}")
                    continue

            return {
                "success": False,
                "error": f"Could not find form field: {selector}. Tried: {'; '.join(error_messages)}",
            }

        except Exception as e:
            logger.error(f"Failed to fill form field {selector}: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    def submit_form(self, selector: Optional[str] = None) -> Dict[str, Any]:
        """
        Submit a form.

        Supports Shadow DOM piercing for submit buttons inside Shadow DOM.

        Args:
            selector: Optional CSS selector for the form or submit button.
                     If not provided, attempts to find a submit button.
                     All selectors operate on the live DOM with JavaScript fully executed.

        Returns:
            Result dict with success status
        """
        self._ensure_driver()

        try:
            if selector:
                # Try to find the specified element
                result = self.click_element(selector)
                if result["success"]:
                    time.sleep(2)
                    return {
                        "success": True,
                        "current_url": self.page.url,
                    }

            # Try common submit button patterns
            submit_patterns = [
                "button[type='submit']",
                "input[type='submit']",
                "xpath=//button[@type='submit']",
                "xpath=//input[@type='submit']",
                "xpath=//button[contains(text(), 'Submit')]",
                "xpath=//button[contains(text(), 'Sign Up')]",
                "xpath=//button[contains(text(), 'Register')]",
                "xpath=//button[contains(text(), 'Get')]",
                "xpath=//button[contains(text(), 'Request')]",
                "xpath=//input[@value='Submit']",
            ]

            for pattern in submit_patterns:
                try:
                    element = self.page.locator(pattern).first
                    if element and element.count() > 0:
                        element.scroll_into_view_if_needed()
                        element.click(timeout=5000)
                        time.sleep(2)

                        logger.info(f"Submitted form using pattern: {pattern}")
                        return {
                            "success": True,
                            "current_url": self.page.url,
                        }
                except Exception:
                    continue

            return {
                "success": False,
                "error": "Could not find form submit button",
            }

        except Exception as e:
            logger.error(f"Failed to submit form: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    def get_links(self) -> Dict[str, Any]:
        """
        Get all links on the current page.

        Operates on the live DOM with JavaScript fully executed, ensuring that
        all dynamically-generated links are included. Also extracts links from
        Shadow DOM.

        Returns:
            Dict with list of links (text and URL)
        """
        self._ensure_driver()

        try:
            # Get links from main document and Shadow DOM
            links_data = self.page.evaluate("""
                () => {
                    const links = [];

                    // Get links from main document
                    document.querySelectorAll('a').forEach(a => {
                        const href = a.href;
                        const text = a.innerText.trim() || a.title || '';
                        if (href && text) {
                            links.push({ text, url: href });
                        }
                    });

                    // Get links from Shadow DOM
                    document.querySelectorAll('*').forEach(element => {
                        if (element.shadowRoot) {
                            element.shadowRoot.querySelectorAll('a').forEach(a => {
                                const href = a.href;
                                const text = a.innerText.trim() || a.title || '';
                                if (href && text) {
                                    links.push({ text, url: href });
                                }
                            });
                        }
                    });

                    return links;
                }
            """)

            logger.info(f"Found {len(links_data)} links on page")
            return {
                "success": True,
                "links": links_data,
                "count": len(links_data),
            }

        except Exception as e:
            logger.error(f"Failed to get links: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    def take_screenshot(self, filepath: Optional[str] = None) -> Dict[str, Any]:
        """
        Take a screenshot of the current page.

        Args:
            filepath: Optional path to save the screenshot.
                     If not provided, returns base64 encoded image.

        Returns:
            Dict with screenshot data or filepath
        """
        self._ensure_driver()

        try:
            if filepath:
                self.page.screenshot(path=filepath)
                logger.info(f"Screenshot saved to: {filepath}")
                return {
                    "success": True,
                    "filepath": filepath,
                }
            else:
                screenshot_bytes = self.page.screenshot()
                screenshot_base64 = base64.b64encode(screenshot_bytes).decode('utf-8')
                logger.info("Screenshot captured as base64")
                return {
                    "success": True,
                    "screenshot_base64": screenshot_base64,
                }

        except Exception as e:
            logger.error(f"Failed to take screenshot: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    def find_api_key_elements(self) -> Dict[str, Any]:
        """
        Search the current page for potential API key displays or generation buttons.

        All patterns are executed on the live DOM with JavaScript fully rendered,
        ensuring that dynamically-generated API key elements (e.g., generated after form
        submission or AJAX calls) are included in the search. Also searches Shadow DOM.

        Returns:
            Dict with found elements that might relate to API keys
        """
        self._ensure_driver()

        try:
            # Use JavaScript to search both main DOM and Shadow DOM
            results = self.page.evaluate("""
                () => {
                    const results = {
                        api_key_displays: [],
                        generate_buttons: [],
                        api_links: []
                    };

                    // Helper to search within a root (document or shadow root)
                    function searchInRoot(root) {
                        // Look for API key displays
                        const keySelectors = [
                            'code',
                            '[class*="api-key"]',
                            '[class*="apikey"]',
                            '[class*="token"]',
                            '[class*="key"]',
                            'pre',
                            'input[type="text"][readonly]',
                            '[id*="api"]',
                            '[id*="key"]',
                            '[id*="token"]'
                        ];

                        keySelectors.forEach(selector => {
                            try {
                                root.querySelectorAll(selector).forEach(el => {
                                    const text = el.innerText?.trim() || el.value?.trim() || '';
                                    if (text && text.length > 15 && text.length < 200) {
                                        results.api_key_displays.push({
                                            text: text,
                                            tag: el.tagName.toLowerCase()
                                        });
                                    }
                                });
                            } catch (e) {}
                        });

                        // Look for generate/create buttons
                        const buttonTexts = ['Generate', 'Create', 'New Key', 'Get API Key', 'Request Key'];
                        buttonTexts.forEach(buttonText => {
                            try {
                                root.querySelectorAll('button, a').forEach(el => {
                                    if (el.innerText?.includes(buttonText)) {
                                        results.generate_buttons.push({
                                            text: el.innerText.trim(),
                                            tag: el.tagName.toLowerCase()
                                        });
                                    }
                                });
                            } catch (e) {}
                        });

                        // Look for API-related links
                        const apiKeywords = ['api', 'developer', 'key', 'token', 'access', 'credentials'];
                        try {
                            root.querySelectorAll('a').forEach(link => {
                                const text = (link.innerText || '').toLowerCase();
                                const href = (link.href || '').toLowerCase();

                                for (const keyword of apiKeywords) {
                                    if (text.includes(keyword) || href.includes(keyword)) {
                                        results.api_links.push({
                                            text: link.innerText?.trim() || '',
                                            url: link.href
                                        });
                                        break;
                                    }
                                }
                            });
                        } catch (e) {}
                    }

                    // Search in main document
                    searchInRoot(document);

                    // Search in Shadow DOM
                    document.querySelectorAll('*').forEach(element => {
                        if (element.shadowRoot) {
                            searchInRoot(element.shadowRoot);
                        }
                    });

                    return results;
                }
            """)

            results["success"] = True

            logger.info(f"Found API key elements: {len(results['api_key_displays'])} displays, "
                       f"{len(results['generate_buttons'])} buttons, {len(results['api_links'])} links")

            return results

        except Exception as e:
            logger.error(f"Failed to find API key elements: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    def find_form_fields(self) -> Dict[str, Any]:
        """
        Find all form fields on the current page.

        Searches are performed on the live DOM with JavaScript fully executed,
        ensuring all dynamically-generated form fields are discovered. Also
        searches Shadow DOM for form fields.

        Returns:
            Dict with list of form fields and their properties
        """
        self._ensure_driver()

        try:
            # Use JavaScript to find form fields in both main DOM and Shadow DOM
            fields = self.page.evaluate("""
                () => {
                    const fields = [];

                    // Helper to check if element is visible
                    function isVisible(el) {
                        return el.offsetWidth > 0 && el.offsetHeight > 0;
                    }

                    // Helper to find label for element
                    function findLabel(el) {
                        if (el.id) {
                            const label = document.querySelector(`label[for="${el.id}"]`);
                            if (label) return label.innerText?.trim();
                        }
                        const parent = el.closest('label');
                        if (parent) return parent.innerText?.trim();
                        return null;
                    }

                    // Helper to search within a root (document or shadow root)
                    function searchInRoot(root) {
                        const tags = ['input', 'textarea', 'select'];
                        tags.forEach(tag => {
                            root.querySelectorAll(tag).forEach(el => {
                                if (isVisible(el)) {
                                    fields.push({
                                        tag: tag,
                                        type: el.type || tag,
                                        name: el.name || null,
                                        id: el.id || null,
                                        placeholder: el.placeholder || null,
                                        label: findLabel(el),
                                        required: el.required || false
                                    });
                                }
                            });
                        });
                    }

                    // Search in main document
                    searchInRoot(document);

                    // Search in Shadow DOM
                    document.querySelectorAll('*').forEach(element => {
                        if (element.shadowRoot) {
                            searchInRoot(element.shadowRoot);
                        }
                    });

                    return fields;
                }
            """)

            logger.info(f"Found {len(fields)} form fields")
            return {
                "success": True,
                "fields": fields,
                "count": len(fields),
            }

        except Exception as e:
            logger.error(f"Failed to find form fields: {e}")
            return {
                "success": False,
                "error": str(e),
            }


    def wait_for_element(self, selector: str, timeout: int = 10) -> Dict[str, Any]:
        """
        Wait for an element to appear on the page.

        Supports Shadow DOM piercing with >> syntax.

        Args:
            selector: CSS selector or XPath for the element
                     Selectors operate on the live DOM with JavaScript fully executed,
                     detecting elements that appear after dynamic content generation.
            timeout: Maximum time to wait in seconds

        Returns:
            Result dict with success status and element info
        """
        self._ensure_driver()

        try:
            # Try different selector strategies
            strategies = [
                ("css", selector),
                ("xpath", selector if selector.startswith("//") else None),
                ("css", f"#{selector}"),
            ]

            for strategy_type, locator in strategies:
                if not locator:
                    continue

                try:
                    if strategy_type == "xpath":
                        element = self.page.locator(f"xpath={locator}").first
                    else:
                        element = self.page.locator(locator).first

                    # Wait for element to be visible
                    element.wait_for(state="visible", timeout=timeout * 1000)

                    return {
                        "success": True,
                        "text": element.text_content(),
                        "tag": element.evaluate("el => el.tagName.toLowerCase()"),
                    }
                except Exception:
                    continue

            return {
                "success": False,
                "error": f"Element not found within {timeout} seconds: {selector}",
            }

        except Exception as e:
            logger.error(f"Failed waiting for element {selector}: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    def execute_javascript(self, script: str) -> Dict[str, Any]:
        """
        Execute JavaScript on the current page.

        Args:
            script: JavaScript code to execute

        Returns:
            Result dict with execution result
        """
        self._ensure_driver()

        try:
            result = self.page.evaluate(script)
            return {
                "success": True,
                "result": result,
            }
        except Exception as e:
            logger.error(f"Failed to execute JavaScript: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    def close(self):
        """Close the browser and clean up resources."""
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
            logger.info("Browser closed")
        except Exception as e:
            logger.error(f"Error closing browser: {e}")
        finally:
            self.page = None
            self.context = None
            self.browser = None
            self.playwright = None
            self._initialized = False

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close browser."""
        self.close()
