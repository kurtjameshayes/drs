"""
Browser Automation Service

Service for automating web browser interactions using Selenium.
Used to navigate websites, fill forms, and extract API keys.
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
    Service for browser automation using Selenium WebDriver.

    Provides functions for:
    - Navigating to URLs
    - Getting page content
    - Clicking elements
    - Filling form fields
    - Submitting forms
    - Taking screenshots
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
        self.driver = None
        self._initialized = False

    def _ensure_driver(self):
        """Initialize the WebDriver if not already initialized."""
        if self._initialized and self.driver:
            return

        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service

            chrome_options = Options()

            if self.headless:
                chrome_options.add_argument("--headless=new")

            # Common options for stability
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-popup-blocking")
            chrome_options.add_argument("--ignore-certificate-errors")

            # User agent to appear as a regular browser
            chrome_options.add_argument(
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )

            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(self.timeout)
            self.driver.implicitly_wait(10)
            self._initialized = True

            logger.info("Browser automation service initialized successfully")

        except ImportError as e:
            logger.error(
                "selenium not installed. Install with: pip install selenium"
            )
            raise ImportError(
                "selenium package is required for Browser Automation Service. "
                "Install with: pip install selenium"
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
            self.driver.get(url)

            # Wait for page to load
            time.sleep(2)

            return {
                "success": True,
                "current_url": self.driver.current_url,
                "title": self.driver.title,
            }
        except Exception as e:
            logger.error(f"Failed to navigate to {url}: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    def get_page_content(self) -> Dict[str, Any]:
        """
        Get the current page's content.

        Returns:
            Dict with page HTML, visible text, and metadata
        """
        self._ensure_driver()

        try:
            from selenium.webdriver.common.by import By

            # Get the rendered DOM after JavaScript execution
            html_content = self.driver.execute_script("return document.documentElement.outerHTML")

            # Strip <head> element before processing
            html_content = re.sub(r'<head[^>]*>.*?</head>', '', html_content, flags=re.IGNORECASE | re.DOTALL)

            # Get visible text (excluding scripts and styles)
            body = self.driver.find_element(By.TAG_NAME, "body")
            visible_text = body.text

            # Get page metadata
            title = self.driver.title
            current_url = self.driver.current_url

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

        Args:
            selector: CSS selector, XPath, or text content to find the element

        Returns:
            Result dict with success status
        """
        self._ensure_driver()

        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.common.exceptions import TimeoutException

            element = None

            # Try different strategies to find the element
            strategies = []

            # If it looks like an XPath, try it first
            if selector.startswith("//") or selector.startswith("("):
                strategies.append((By.XPATH, selector))

            # Then try other strategies
            strategies.extend([
                # CSS selector
                (By.CSS_SELECTOR, selector),
                # XPath for exact text match
                (By.XPATH, f"//*[text()='{selector}']"),
                # XPath for partial text match
                (By.XPATH, f"//*[contains(text(), '{selector}')]"),
                # Link text
                (By.LINK_TEXT, selector),
                # Partial link text
                (By.PARTIAL_LINK_TEXT, selector),
                # ID
                (By.ID, selector),
                # Name
                (By.NAME, selector),
            ])

            wait = WebDriverWait(self.driver, 10)

            for by_type, locator in strategies:
                try:
                    element = wait.until(EC.element_to_be_clickable((by_type, locator)))
                    break
                except TimeoutException:
                    continue

            if element is None:
                return {
                    "success": False,
                    "error": f"Could not find clickable element: {selector}",
                }

            # Scroll element into view
            self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
            time.sleep(0.5)

            element.click()

            # Wait for any navigation or updates
            time.sleep(1)

            logger.info(f"Clicked element: {selector}")
            return {
                "success": True,
                "current_url": self.driver.current_url,
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

        Args:
            selector: CSS selector, name, or ID of the form field
            value: Value to fill in

        Returns:
            Result dict with success status
        """
        self._ensure_driver()

        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.common.exceptions import TimeoutException

            element = None

            # Try different strategies to find the input field
            strategies = [
                (By.CSS_SELECTOR, selector),
                (By.NAME, selector),
                (By.ID, selector),
                (By.XPATH, f"//input[@name='{selector}']"),
                (By.XPATH, f"//input[@id='{selector}']"),
                (By.XPATH, f"//input[@placeholder='{selector}']"),
                (By.XPATH, f"//textarea[@name='{selector}']"),
                (By.XPATH, f"//textarea[@id='{selector}']"),
            ]

            wait = WebDriverWait(self.driver, 10)

            for by_type, locator in strategies:
                try:
                    element = wait.until(EC.presence_of_element_located((by_type, locator)))
                    break
                except TimeoutException:
                    continue

            if element is None:
                return {
                    "success": False,
                    "error": f"Could not find form field: {selector}",
                }

            # Clear existing value and type new value
            element.clear()
            element.send_keys(value)

            logger.info(f"Filled form field {selector}")
            return {
                "success": True,
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

        Args:
            selector: Optional CSS selector for the form or submit button.
                     If not provided, attempts to find a submit button.

        Returns:
            Result dict with success status
        """
        self._ensure_driver()

        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            if selector:
                # Try to find the specified element
                result = self.click_element(selector)
                if result["success"]:
                    time.sleep(2)
                    return {
                        "success": True,
                        "current_url": self.driver.current_url,
                    }

            # Try common submit button patterns
            submit_patterns = [
                "//button[@type='submit']",
                "//input[@type='submit']",
                "//button[contains(text(), 'Submit')]",
                "//button[contains(text(), 'Sign Up')]",
                "//button[contains(text(), 'Register')]",
                "//button[contains(text(), 'Get')]",
                "//button[contains(text(), 'Request')]",
                "//input[@value='Submit']",
                "//a[contains(text(), 'Submit')]",
            ]

            wait = WebDriverWait(self.driver, 5)

            for pattern in submit_patterns:
                try:
                    element = wait.until(EC.element_to_be_clickable((By.XPATH, pattern)))
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                    time.sleep(0.5)
                    element.click()
                    time.sleep(2)

                    logger.info(f"Submitted form using pattern: {pattern}")
                    return {
                        "success": True,
                        "current_url": self.driver.current_url,
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

        Returns:
            Dict with list of links (text and URL)
        """
        self._ensure_driver()

        try:
            from selenium.webdriver.common.by import By

            links = []
            elements = self.driver.find_elements(By.TAG_NAME, "a")

            for element in elements:
                try:
                    href = element.get_attribute("href")
                    text = element.text.strip() or element.get_attribute("title") or ""

                    if href and text:
                        links.append({
                            "text": text,
                            "url": href,
                        })
                except Exception:
                    continue

            logger.info(f"Found {len(links)} links on page")
            return {
                "success": True,
                "links": links,
                "count": len(links),
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
                self.driver.save_screenshot(filepath)
                logger.info(f"Screenshot saved to: {filepath}")
                return {
                    "success": True,
                    "filepath": filepath,
                }
            else:
                screenshot_base64 = self.driver.get_screenshot_as_base64()
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

        Returns:
            Dict with found elements that might relate to API keys
        """
        self._ensure_driver()

        try:
            from selenium.webdriver.common.by import By

            results = {
                "success": True,
                "api_key_displays": [],
                "generate_buttons": [],
                "api_links": [],
            }

            # Look for displayed API keys
            key_patterns = [
                "//code",
                "//*[contains(@class, 'api-key')]",
                "//*[contains(@class, 'apikey')]",
                "//*[contains(@class, 'token')]",
                "//*[contains(@class, 'key')]",
                "//pre",
                "//input[@type='text' and @readonly]",
                "//*[contains(@id, 'api')]",
                "//*[contains(@id, 'key')]",
                "//*[contains(@id, 'token')]",
            ]

            for pattern in key_patterns:
                try:
                    elements = self.driver.find_elements(By.XPATH, pattern)
                    for el in elements:
                        text = el.text.strip() or el.get_attribute("value") or ""
                        if text and len(text) > 15 and len(text) < 200:
                            # Looks like it could be an API key
                            results["api_key_displays"].append({
                                "text": text,
                                "tag": el.tag_name,
                            })
                except Exception:
                    continue

            # Look for generate/create key buttons
            button_patterns = [
                "//button[contains(text(), 'Generate')]",
                "//button[contains(text(), 'Create')]",
                "//a[contains(text(), 'Generate')]",
                "//a[contains(text(), 'Create')]",
                "//*[contains(text(), 'New Key')]",
                "//*[contains(text(), 'Get API Key')]",
                "//*[contains(text(), 'Request Key')]",
            ]

            for pattern in button_patterns:
                try:
                    elements = self.driver.find_elements(By.XPATH, pattern)
                    for el in elements:
                        text = el.text.strip()
                        if text:
                            results["generate_buttons"].append({
                                "text": text,
                                "tag": el.tag_name,
                            })
                except Exception:
                    continue

            # Look for API-related links
            try:
                all_links = self.driver.find_elements(By.TAG_NAME, "a")
                api_keywords = ["api", "developer", "key", "token", "access", "credentials"]

                for link in all_links:
                    text = (link.text or "").lower()
                    href = (link.get_attribute("href") or "").lower()

                    for keyword in api_keywords:
                        if keyword in text or keyword in href:
                            results["api_links"].append({
                                "text": link.text.strip(),
                                "url": link.get_attribute("href"),
                            })
                            break
            except Exception:
                pass

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

        Returns:
            Dict with list of form fields and their properties
        """
        self._ensure_driver()

        try:
            from selenium.webdriver.common.by import By

            fields = []

            # Find all input elements
            input_types = ["input", "textarea", "select"]

            for tag in input_types:
                elements = self.driver.find_elements(By.TAG_NAME, tag)

                for el in elements:
                    field_info = {
                        "tag": tag,
                        "type": el.get_attribute("type") or tag,
                        "name": el.get_attribute("name"),
                        "id": el.get_attribute("id"),
                        "placeholder": el.get_attribute("placeholder"),
                        "label": self._find_label_for_element(el),
                        "required": el.get_attribute("required") is not None,
                    }

                    # Only include visible fields
                    if el.is_displayed():
                        fields.append(field_info)

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

    def _find_label_for_element(self, element) -> Optional[str]:
        """Find the label text for a form element."""
        try:
            from selenium.webdriver.common.by import By

            # Check for associated label via 'for' attribute
            el_id = element.get_attribute("id")
            if el_id:
                labels = self.driver.find_elements(By.XPATH, f"//label[@for='{el_id}']")
                if labels:
                    return labels[0].text.strip()

            # Check for parent label
            parent = element.find_element(By.XPATH, "./parent::label")
            if parent:
                return parent.text.strip()

        except Exception:
            pass

        return None

    def wait_for_element(self, selector: str, timeout: int = 10) -> Dict[str, Any]:
        """
        Wait for an element to appear on the page.

        Args:
            selector: CSS selector or XPath for the element
            timeout: Maximum time to wait in seconds

        Returns:
            Result dict with success status and element info
        """
        self._ensure_driver()

        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.common.exceptions import TimeoutException

            wait = WebDriverWait(self.driver, timeout)

            strategies = [
                (By.CSS_SELECTOR, selector),
                (By.XPATH, selector),
                (By.ID, selector),
            ]

            for by_type, locator in strategies:
                try:
                    element = wait.until(EC.presence_of_element_located((by_type, locator)))
                    return {
                        "success": True,
                        "text": element.text,
                        "tag": element.tag_name,
                    }
                except TimeoutException:
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
            result = self.driver.execute_script(script)
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
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Browser closed")
            except Exception as e:
                logger.error(f"Error closing browser: {e}")
            finally:
                self.driver = None
                self._initialized = False

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close browser."""
        self.close()
