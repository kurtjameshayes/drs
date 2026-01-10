"""
API Key Agent

Agent for automatically acquiring API keys for data sources.
Orchestrates key checking, registration analysis, browser automation, and email polling.
"""

import json
import os
import secrets
import string
from typing import Optional, Dict, Any, List
import logging
import asyncio
from pymongo import MongoClient
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

from core.discovery.state import (
    DiscoveryState,
    HumanInputRequest,
    HumanInputType,
    WorkflowError,
)
from core.discovery.services.api_key_store import APIKeyStore
from core.discovery.services.arcade_email_service import ArcadeEmailService
from core.discovery.services.browser_automation_service import BrowserAutomationService
from core.discovery.llm_logger import invoke_llm_with_logging
from config import Config

logger = logging.getLogger(__name__)


# Browser automation function descriptions for the LLM
BROWSER_FUNCTIONS = """
Available browser automation functions:

- navigate_to_url(url): Navigate the browser to a specific URL
- get_page_content(): Returns the current page's HTML content and visible text
- click_element(selector): Clicks on an element identified by CSS selector or text content
- fill_form_field(selector, value): Fills a form field with the provided value
- submit_form(selector): Submits a form
- get_links(): Returns all links on the current page with their text and URLs
- take_screenshot(): Takes a screenshot of the current page (useful for debugging)
- find_api_key_elements(): Search for potential API key displays or generation buttons
- find_form_fields(): Find all form fields on the current page
"""


class APIKeyAgent:
    """
    Agent for automatically acquiring API keys for data sources.

    Orchestrates:
    1. Checking existing key storage
    2. Analyzing registration requirements
    3. Browser automation for site discovery and form filling
    4. Monitoring email for API keys (Arcade)
    5. Falling back to manual input when needed
    """

    def __init__(self, db_client: MongoClient):
        self.llm = ChatAnthropic(
            model=Config.DISCOVERY_LLM_MODEL,
            api_key=Config.ANTHROPIC_API_KEY,
            temperature=0.1,
        )
        self.key_store = APIKeyStore(db_client)
        self.browser_service = None

        # Get user email from environment
        self.user_email = os.environ.get("ARCADE_USER_ID", "") or Config.DISCOVERY_EMAIL

        # Initialize Arcade email service if configured
        if Config.ARCADE_API_KEY and Config.DISCOVERY_EMAIL:
            self.email_service = ArcadeEmailService(
                api_key=Config.ARCADE_API_KEY,
                user_id=Config.ARCADE_USER_ID or Config.DISCOVERY_EMAIL,
            )
        else:
            self.email_service = None
            logger.warning("Arcade API not configured, email polling disabled")

    def _get_browser_service(self) -> BrowserAutomationService:
        """Get or create the browser automation service."""
        if self.browser_service is None:
            headless = getattr(Config, "BROWSER_HEADLESS", True)
            timeout = getattr(Config, "BROWSER_TIMEOUT", 30)
            self.browser_service = BrowserAutomationService(
                headless=headless,
                timeout=timeout
            )
        return self.browser_service

    def _close_browser(self):
        """Close the browser if it's open."""
        if self.browser_service:
            self.browser_service.close()
            self.browser_service = None

    def run(self, state: DiscoveryState) -> DiscoveryState:
        """
        Main entry point for API key acquisition.

        Args:
            state: Current workflow state with access_documentation

        Returns:
            Updated state with API key or error
        """
        try:
            access_doc = state.get("access_documentation", {})
            source_name = access_doc.get("source_name", "Unknown")
            base_url = access_doc.get("base_url", "")
            auth_info = access_doc.get("authentication", {})
            registration_url = auth_info.get("registration_url", "")

            logger.info(f"API Key Agent: Processing {source_name}")

            # Step 1: Check if we already have a key
            existing_key = self._check_existing_key(base_url, source_name)
            if existing_key:
                logger.info(f"Using existing API key for {source_name}")
                return self._apply_key_to_state(state, existing_key, "existing")

            # Step 2: Discover the site and analyze registration process
            site_info = self.discover_site(registration_url or base_url)

            # Step 3: Try to extract key directly from site (if publicly available)
            if site_info.get("key_available_on_site"):
                extracted_key = self.extract_key_from_site(site_info)
                if extracted_key:
                    self._store_key(base_url, extracted_key, {
                        "source_name": source_name,
                        "registration_url": registration_url,
                    })
                    return self._apply_key_to_state(state, extracted_key, "automatic")

            # Step 4: Try to request key via browser automation
            if site_info.get("can_automate_registration", False):
                request_result = self.request_key(
                    site_info=site_info,
                    email=self.user_email,
                    source_name=source_name,
                )

                if request_result.get("key"):
                    # Key was immediately provided
                    self._store_key(base_url, request_result["key"], {
                        "source_name": source_name,
                        "registration_url": registration_url,
                    })
                    return self._apply_key_to_state(state, request_result["key"], "automatic")

                if request_result.get("email_verification_pending"):
                    # Need to check email for verification or key
                    return asyncio.run(self._handle_email_verification(
                        state=state,
                        site_info=site_info,
                        source_name=source_name,
                        base_url=base_url,
                        registration_url=registration_url,
                    ))

            # Step 5: Check if we can poll email for key
            if self.email_service and site_info.get("email_domain"):
                return asyncio.run(self._poll_email_only(state, {
                    "email_domain": site_info.get("email_domain"),
                    "subject_keywords": site_info.get("subject_keywords", ["API Key", "API", "Token"]),
                    "base_url": base_url,
                    "source_name": source_name,
                    "registration_url": registration_url,
                }))

            # Step 6: Fall back to manual intervention
            return self._request_manual_intervention(
                state,
                {
                    "reason": "automation_failed",
                    "message": f"Could not automatically acquire API key for {source_name}. "
                               f"Please register manually and provide the key.",
                    "registration_url": registration_url,
                    "site_info": site_info,
                },
            )

        except Exception as e:
            logger.error(f"API Key Agent failed: {e}", exc_info=True)
            state["error"] = WorkflowError(
                agent_name="APIKeyAgent",
                step="api_key_acquisition",
                issue="Failed to acquire API key",
                details=str(e),
                recoverable=True,
            ).to_dict()
            state["interrupted"] = True

        finally:
            # Clean up browser
            self._close_browser()

        return state

    def discover_site(self, url: str) -> Dict[str, Any]:
        """
        Crawl the given website to discover API key registration information.

        Uses LLM-driven browser automation to explore the site and understand
        the registration process.

        Args:
            url: The website URL to discover

        Returns:
            Site information dict with registration details
        """
        if not url:
            return {"error": "No URL provided"}

        logger.info(f"Discovering site: {url}")

        browser = self._get_browser_service()
        site_info = {
            "url": url,
            "registration_url": None,
            "key_available_on_site": False,
            "can_automate_registration": False,
            "requires_email_verification": False,
            "email_domain": None,
            "subject_keywords": ["API Key", "API", "Registration"],
            "form_fields": [],
            "navigation_steps": [],
        }

        try:
            # Navigate to the site
            nav_result = browser.navigate_to_url(url)
            if not nav_result.get("success"):
                logger.error(f"Failed to navigate to {url}")
                return site_info

            # Get initial page content
            content = browser.get_page_content()
            links = browser.get_links()
            api_elements = browser.find_api_key_elements()

            # Use LLM to analyze the page and decide next steps
            analysis = self._analyze_page_for_api_key(
                url=url,
                content=content,
                links=links,
                api_elements=api_elements,
            )

            site_info.update(analysis)

            # If we need to navigate to find API registration, use LLM-driven navigation
            if analysis.get("needs_navigation"):
                nav_steps = self._llm_driven_navigation(
                    browser=browser,
                    goal="Find the API key registration or generation page",
                    max_steps=5,
                )
                site_info["navigation_steps"] = nav_steps

                # Re-analyze after navigation
                content = browser.get_page_content()
                api_elements = browser.find_api_key_elements()
                form_fields = browser.find_form_fields()

                final_analysis = self._analyze_registration_page(
                    content=content,
                    api_elements=api_elements,
                    form_fields=form_fields,
                )
                site_info.update(final_analysis)

            # Extract email domain from URL
            from urllib.parse import urlparse
            parsed = urlparse(url)
            site_info["email_domain"] = parsed.netloc.replace("www.", "").replace("api.", "")

            logger.info(f"Site discovery complete: {site_info}")
            return site_info

        except Exception as e:
            logger.error(f"Error discovering site: {e}", exc_info=True)
            site_info["error"] = str(e)
            return site_info

    def extract_key_from_site(self, site_info: Dict[str, Any]) -> Optional[str]:
        """
        Attempt to extract an API key directly from the site.

        This is for cases where the key is immediately available without
        registration (e.g., demo/test keys, public APIs).

        Args:
            site_info: Site information from discover_site

        Returns:
            API key if found, None otherwise
        """
        browser = self._get_browser_service()

        try:
            # Check for API key elements on the current page
            api_elements = browser.find_api_key_elements()

            if api_elements.get("api_key_displays"):
                # Found potential API keys displayed on the page
                for display in api_elements["api_key_displays"]:
                    key_text = display.get("text", "")

                    # Use LLM to validate this is actually an API key
                    if self._validate_api_key(key_text):
                        logger.info("Extracted API key directly from site")
                        return key_text

            # Try clicking generate buttons if they exist
            if api_elements.get("generate_buttons"):
                for button in api_elements["generate_buttons"]:
                    button_text = button.get("text", "")
                    result = browser.click_element(button_text)

                    if result.get("success"):
                        # Wait for key to be generated
                        import time
                        time.sleep(2)

                        # Check for newly displayed key
                        new_elements = browser.find_api_key_elements()
                        if new_elements.get("api_key_displays"):
                            for display in new_elements["api_key_displays"]:
                                key_text = display.get("text", "")
                                if self._validate_api_key(key_text):
                                    logger.info("Generated and extracted API key from site")
                                    return key_text

            return None

        except Exception as e:
            logger.error(f"Error extracting key from site: {e}")
            return None

    def request_key(
        self,
        site_info: Dict[str, Any],
        email: str,
        source_name: str,
    ) -> Dict[str, Any]:
        """
        Request an API key by filling out registration forms.

        Uses Selenium-driven browser automation with LLM guidance to
        fill out forms and complete the registration process.

        Args:
            site_info: Site information from discover_site
            email: Email address to use for registration
            source_name: Name of the data source

        Returns:
            Result dict with key if immediately available, or status
        """
        browser = self._get_browser_service()
        result = {
            "success": False,
            "key": None,
            "email_verification_pending": False,
            "error": None,
        }

        try:
            # Navigate to registration URL if different from current
            reg_url = site_info.get("registration_url") or site_info.get("url")
            if reg_url:
                browser.navigate_to_url(reg_url)

            # Get form fields
            form_fields = browser.find_form_fields()

            if not form_fields.get("fields"):
                result["error"] = "No form fields found on registration page"
                return result

            # Generate a secure password if needed
            password = self._generate_secure_password()

            # Use LLM to determine how to fill the form
            fill_instructions = self._get_form_fill_instructions(
                form_fields=form_fields.get("fields", []),
                email=email,
                password=password,
                source_name=source_name,
            )

            # Fill in the form fields
            for instruction in fill_instructions:
                field_selector = instruction.get("selector")
                field_value = instruction.get("value")

                if field_selector and field_value:
                    browser.fill_form_field(field_selector, field_value)

            # Submit the form
            submit_result = browser.submit_form()

            if not submit_result.get("success"):
                result["error"] = "Failed to submit registration form"
                return result

            # Check what happened after submission
            import time
            time.sleep(3)

            content = browser.get_page_content()
            api_elements = browser.find_api_key_elements()

            # Analyze the post-submission page
            post_analysis = self._analyze_post_submission(
                content=content,
                api_elements=api_elements,
            )

            if post_analysis.get("key"):
                result["success"] = True
                result["key"] = post_analysis["key"]

            elif post_analysis.get("email_verification_required"):
                result["success"] = True
                result["email_verification_pending"] = True
                result["verification_message"] = post_analysis.get("message", "Check your email")

            elif post_analysis.get("error"):
                result["error"] = post_analysis["error"]

            else:
                # Assume email verification is needed
                result["success"] = True
                result["email_verification_pending"] = True

            return result

        except Exception as e:
            logger.error(f"Error requesting key: {e}", exc_info=True)
            result["error"] = str(e)
            return result

    def poll_email(
        self,
        sender_domain: str,
        subject_keywords: Optional[List[str]] = None,
        max_wait_minutes: int = 10,
    ) -> Optional[Dict[str, Any]]:
        """
        Poll the email inbox and wait for a message from the given site.

        Args:
            sender_domain: Domain to filter sender (e.g., "usda.gov")
            subject_keywords: Keywords to search in subject
            max_wait_minutes: Maximum time to wait for email

        Returns:
            Email content dict if found, None otherwise
        """
        if not self.email_service:
            logger.warning("Email service not configured")
            return None

        subject_keywords = subject_keywords or ["API Key", "API", "Registration", "Verification"]

        max_attempts = max_wait_minutes * 2  # Check every 30 seconds
        interval = 30

        for attempt in range(max_attempts):
            logger.info(f"Polling email (attempt {attempt + 1}/{max_attempts})")

            emails = self.email_service.search_for_api_key(
                sender_domain=sender_domain,
                subject_keywords=subject_keywords,
                since_minutes=max_wait_minutes + 5,
            )

            if emails:
                for email in emails:
                    email_id = email.get("id")
                    if email_id:
                        full_email = self.email_service.get_email_content(email_id)
                        if full_email:
                            logger.info(f"Found email from {sender_domain}")
                            return full_email

            if attempt < max_attempts - 1:
                import time
                time.sleep(interval)

        logger.warning(f"No email found from {sender_domain} after {max_wait_minutes} minutes")
        return None

    def respond_to_email(self, email_content: Dict[str, Any]) -> Dict[str, Any]:
        """
        Read an email and take the appropriate action.

        This handles verification emails by clicking links, extracting codes, etc.

        Args:
            email_content: Email content dict with body, subject, etc.

        Returns:
            Result dict with action taken and any extracted key
        """
        result = {
            "action_taken": None,
            "key": None,
            "error": None,
        }

        try:
            email_body = email_content.get("body", "") or email_content.get("snippet", "")
            email_subject = email_content.get("subject", "")

            # Use LLM to analyze the email and determine what action to take
            analysis = self._analyze_email_for_action(
                subject=email_subject,
                body=email_body,
            )

            if analysis.get("contains_api_key"):
                # Extract the key directly
                key = self.extract_key_from_email(email_body)
                if key:
                    result["action_taken"] = "extracted_key"
                    result["key"] = key
                    return result

            if analysis.get("verification_link"):
                # Click the verification link
                link = analysis["verification_link"]
                result["action_taken"] = "clicked_verification_link"

                browser = self._get_browser_service()
                nav_result = browser.navigate_to_url(link)

                if nav_result.get("success"):
                    # Check if key is now available
                    import time
                    time.sleep(3)

                    api_elements = browser.find_api_key_elements()
                    if api_elements.get("api_key_displays"):
                        for display in api_elements["api_key_displays"]:
                            key_text = display.get("text", "")
                            if self._validate_api_key(key_text):
                                result["key"] = key_text
                                return result

            if analysis.get("verification_code"):
                # May need to enter verification code somewhere
                result["action_taken"] = "found_verification_code"
                result["verification_code"] = analysis["verification_code"]

            return result

        except Exception as e:
            logger.error(f"Error responding to email: {e}")
            result["error"] = str(e)
            return result

    def extract_key_from_email(self, email_content: str) -> Optional[str]:
        """
        Extract an API key from email content.

        Args:
            email_content: Email body text

        Returns:
            Extracted API key or None
        """
        if not email_content:
            return None

        return self.email_service.extract_api_key_from_email(
            email_content=email_content,
            llm=self.llm,
        ) if self.email_service else self._llm_extract_api_key(email_content)

    # ===== Private Helper Methods =====

    def _check_existing_key(self, base_url: str, source_name: str) -> Optional[str]:
        """Check if we already have a valid key for this source."""
        if not base_url:
            return None

        if self.key_store.key_exists(base_url):
            key = self.key_store.get_key(base_url)
            if key:
                return key

        return None

    def _analyze_page_for_api_key(
        self,
        url: str,
        content: Dict[str, Any],
        links: Dict[str, Any],
        api_elements: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Use LLM to analyze a page and determine API key availability."""

        prompt = f"""Analyze this webpage to determine how to obtain an API key.

URL: {url}

Page Title: {content.get('title', 'Unknown')}

Page Content (visible text):
{content.get('visible_text', '')[:4000]}

Links found on page:
{json.dumps(links.get('links', [])[:30], indent=2)}

API-related elements found:
{json.dumps(api_elements, indent=2)}

Based on this information, determine:

1. Is an API key directly available/displayed on this page?
2. Is there a "Generate Key" or similar button that would immediately provide a key?
3. Is there a registration form that needs to be filled out?
4. What navigation would be needed to reach the API key registration page?

Return JSON:
{{
    "key_available_on_site": true/false,
    "can_automate_registration": true/false,
    "requires_email_verification": true/false,
    "needs_navigation": true/false,
    "registration_url": "url if different from current",
    "navigation_hints": ["links or buttons to click"],
    "form_present": true/false,
    "notes": "explanation"
}}
"""

        try:
            messages = [HumanMessage(content=prompt)]
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="APIKeyAgent",
                operation="analyze_page_for_api_key"
            )

            response_text = response.content
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0]
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0]

            return json.loads(response_text.strip())

        except Exception as e:
            logger.error(f"Failed to analyze page: {e}")
            return {
                "key_available_on_site": False,
                "needs_navigation": True,
            }

    def _analyze_registration_page(
        self,
        content: Dict[str, Any],
        api_elements: Dict[str, Any],
        form_fields: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Analyze a registration page to understand how to complete it."""

        prompt = f"""Analyze this API key registration page.

Page Content:
{content.get('visible_text', '')[:4000]}

API-related elements:
{json.dumps(api_elements, indent=2)}

Form fields found:
{json.dumps(form_fields.get('fields', []), indent=2)}

Determine:
1. Can this registration be automated (just email/password, no captcha)?
2. What form fields need to be filled?
3. Will this require email verification?

Return JSON:
{{
    "can_automate_registration": true/false,
    "required_fields": ["email", "password", etc.],
    "has_captcha": true/false,
    "requires_email_verification": true/false,
    "submit_button_text": "text on submit button",
    "notes": "any important observations"
}}
"""

        try:
            messages = [HumanMessage(content=prompt)]
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="APIKeyAgent",
                operation="analyze_registration_page"
            )

            response_text = response.content
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0]
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0]

            return json.loads(response_text.strip())

        except Exception as e:
            logger.error(f"Failed to analyze registration page: {e}")
            return {"can_automate_registration": False}

    def _llm_driven_navigation(
        self,
        browser: BrowserAutomationService,
        goal: str,
        max_steps: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Use LLM to navigate the browser towards a goal.

        This is the core LLM-driven browser automation loop.

        Args:
            browser: Browser automation service
            goal: What we're trying to accomplish
            max_steps: Maximum navigation steps

        Returns:
            List of steps taken
        """
        steps = []
        conversation_history = []

        system_prompt = f"""You are a browser automation agent. Your goal is to: {goal}

{BROWSER_FUNCTIONS}

Analyze the current page state and decide what action to take next.
Respond with a JSON object containing the action to take:

{{
    "action": "navigate_to_url" | "click_element" | "get_links" | "done",
    "params": {{}},  // Parameters for the action
    "reasoning": "Why you're taking this action"
}}

If the goal is achieved or you can't proceed, use action "done" with:
{{
    "action": "done",
    "success": true/false,
    "reasoning": "Explanation"
}}
"""

        conversation_history.append(SystemMessage(content=system_prompt))

        for step_num in range(max_steps):
            # Get current page state
            content = browser.get_page_content()
            links = browser.get_links()
            api_elements = browser.find_api_key_elements()

            # Build state description for LLM
            state_description = f"""
Current Page State (Step {step_num + 1}/{max_steps}):

URL: {content.get('url', 'Unknown')}
Title: {content.get('title', 'Unknown')}

Page Content:
{content.get('visible_text', '')[:3000]}

Available Links:
{json.dumps(links.get('links', [])[:20], indent=2)}

API-related elements:
{json.dumps(api_elements, indent=2)}

What action should I take next to achieve the goal?
"""

            conversation_history.append(HumanMessage(content=state_description))

            try:
                response = invoke_llm_with_logging(
                    self.llm,
                    conversation_history,
                    agent_name="APIKeyAgent",
                    operation="llm_driven_navigation"
                )

                response_text = response.content
                conversation_history.append(AIMessage(content=response_text))

                # Parse the action
                if "```json" in response_text:
                    response_text = response_text.split("```json")[1].split("```")[0]
                elif "```" in response_text:
                    response_text = response_text.split("```")[1].split("```")[0]

                action_data = json.loads(response_text.strip())

                step_record = {
                    "step": step_num + 1,
                    "action": action_data.get("action"),
                    "params": action_data.get("params", {}),
                    "reasoning": action_data.get("reasoning"),
                }

                action = action_data.get("action")

                if action == "done":
                    step_record["success"] = action_data.get("success", False)
                    steps.append(step_record)
                    break

                elif action == "navigate_to_url":
                    url = action_data.get("params", {}).get("url")
                    if url:
                        result = browser.navigate_to_url(url)
                        step_record["result"] = result

                elif action == "click_element":
                    selector = action_data.get("params", {}).get("selector")
                    if selector:
                        result = browser.click_element(selector)
                        step_record["result"] = result

                elif action == "get_links":
                    result = browser.get_links()
                    step_record["result"] = {"count": result.get("count", 0)}

                steps.append(step_record)

            except Exception as e:
                logger.error(f"Navigation step {step_num + 1} failed: {e}")
                steps.append({
                    "step": step_num + 1,
                    "error": str(e),
                })
                break

        return steps

    def _get_form_fill_instructions(
        self,
        form_fields: List[Dict[str, Any]],
        email: str,
        password: str,
        source_name: str,
    ) -> List[Dict[str, str]]:
        """Use LLM to determine how to fill form fields."""

        prompt = f"""Given these form fields, determine what values to fill in for API key registration.

Form Fields:
{json.dumps(form_fields, indent=2)}

Available information:
- Email: {email}
- Password: {password}
- Source Name: {source_name}

For each field that should be filled, provide the selector and value.
Common field mappings:
- email/Email → {email}
- password/Password → {password}
- name/Name → "API User"
- organization/Organization → "Data Integration"
- use_case/Purpose → "Data integration and analysis"

Return JSON array:
[
    {{"selector": "email", "value": "{email}"}},
    {{"selector": "password", "value": "{password}"}},
    ...
]

Only include fields that should be filled with non-empty values.
Use the field's 'name', 'id', or a CSS selector as the selector.
"""

        try:
            messages = [HumanMessage(content=prompt)]
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="APIKeyAgent",
                operation="get_form_fill_instructions"
            )

            response_text = response.content
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0]
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0]

            return json.loads(response_text.strip())

        except Exception as e:
            logger.error(f"Failed to get form fill instructions: {e}")
            # Return basic instructions
            return [
                {"selector": "email", "value": email},
                {"selector": "password", "value": password},
            ]

    def _analyze_post_submission(
        self,
        content: Dict[str, Any],
        api_elements: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Analyze the page after form submission."""

        prompt = f"""Analyze this page that appeared after submitting an API key registration form.

Page Content:
{content.get('visible_text', '')[:3000]}

API-related elements found:
{json.dumps(api_elements, indent=2)}

Determine:
1. Is an API key displayed on this page?
2. Does it say email verification is required?
3. Is there an error message?
4. What is the status of the registration?

Return JSON:
{{
    "key": "the API key if displayed, or null",
    "email_verification_required": true/false,
    "error": "error message if any, or null",
    "message": "any status message",
    "status": "success" | "pending_verification" | "error"
}}
"""

        try:
            messages = [HumanMessage(content=prompt)]
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="APIKeyAgent",
                operation="analyze_post_submission"
            )

            response_text = response.content
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0]
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0]

            return json.loads(response_text.strip())

        except Exception as e:
            logger.error(f"Failed to analyze post-submission page: {e}")
            return {"status": "unknown"}

    def _analyze_email_for_action(
        self,
        subject: str,
        body: str,
    ) -> Dict[str, Any]:
        """Analyze an email to determine what action to take."""

        prompt = f"""Analyze this email and determine what action to take.

Subject: {subject}

Body:
{body[:3000]}

Determine:
1. Does this email contain an API key?
2. Does it contain a verification link that needs to be clicked?
3. Does it contain a verification code?

Return JSON:
{{
    "contains_api_key": true/false,
    "verification_link": "URL if present, or null",
    "verification_code": "code if present, or null",
    "action_required": "description of what needs to be done"
}}
"""

        try:
            messages = [HumanMessage(content=prompt)]
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="APIKeyAgent",
                operation="analyze_email_for_action"
            )

            response_text = response.content
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0]
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0]

            return json.loads(response_text.strip())

        except Exception as e:
            logger.error(f"Failed to analyze email: {e}")
            return {}

    def _validate_api_key(self, key_text: str) -> bool:
        """Use LLM to validate if a string looks like an API key."""
        if not key_text or len(key_text) < 10 or len(key_text) > 200:
            return False

        prompt = f"""Is this text likely to be an API key? API keys are typically:
- Long alphanumeric strings (20-64 characters)
- May contain hyphens or underscores
- UUID format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
- Base64 encoded strings

Text: {key_text}

Respond with only "YES" or "NO".
"""

        try:
            messages = [HumanMessage(content=prompt)]
            response = self.llm.invoke(messages)
            return "YES" in response.content.upper()
        except Exception:
            # Fall back to simple heuristic
            import re
            # Check if it looks like an API key
            if re.match(r'^[A-Za-z0-9\-_]{16,}$', key_text):
                return True
            if re.match(r'^[A-Fa-f0-9\-]{36}$', key_text):  # UUID
                return True
            return False

    def _llm_extract_api_key(self, content: str) -> Optional[str]:
        """Extract API key from content using LLM."""
        prompt = f"""Extract the API key from this content.

Content:
{content[:3000]}

Look for:
- Explicit API key labels ("Your API key:", "API Key:", "Access Key:", "Token:", "Key:")
- Long alphanumeric strings (typically 20-64 characters)
- Keys in code blocks or highlighted text
- UUID format keys

Return ONLY the API key string if found, or "NOT_FOUND" if no key is present.
"""

        try:
            messages = [HumanMessage(content=prompt)]
            response = self.llm.invoke(messages)
            extracted = response.content.strip().replace("`", "").replace('"', "").replace("'", "")

            if extracted and extracted != "NOT_FOUND" and len(extracted) > 10:
                return extracted
            return None
        except Exception as e:
            logger.error(f"Failed to extract API key: {e}")
            return None

    def _generate_secure_password(self) -> str:
        """Generate a secure random password."""
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        return ''.join(secrets.choice(alphabet) for _ in range(16))

    async def _handle_email_verification(
        self,
        state: DiscoveryState,
        site_info: Dict[str, Any],
        source_name: str,
        base_url: str,
        registration_url: str,
    ) -> DiscoveryState:
        """Handle email verification after form submission."""

        email_domain = site_info.get("email_domain", "")

        # Poll for verification email
        email = self.poll_email(
            sender_domain=email_domain,
            max_wait_minutes=5,
        )

        if email:
            # Respond to the email (click verification link, etc.)
            response_result = self.respond_to_email(email)

            if response_result.get("key"):
                self._store_key(base_url, response_result["key"], {
                    "source_name": source_name,
                    "registration_url": registration_url,
                })
                return self._apply_key_to_state(state, response_result["key"], "automatic")

            # After clicking verification, poll for API key email
            key_email = self.poll_email(
                sender_domain=email_domain,
                subject_keywords=["API Key", "API", "Token", "Credentials"],
                max_wait_minutes=5,
            )

            if key_email:
                key = self.extract_key_from_email(
                    key_email.get("body", "") or key_email.get("snippet", "")
                )
                if key:
                    self._store_key(base_url, key, {
                        "source_name": source_name,
                        "registration_url": registration_url,
                    })
                    return self._apply_key_to_state(state, key, "automatic")

        # Fall back to manual
        return self._request_manual_intervention(
            state,
            {
                "reason": "email_verification_failed",
                "message": "Could not complete email verification automatically.",
                "registration_url": registration_url,
            },
        )

    async def _poll_email_only(
        self, state: DiscoveryState, registration_info: Dict[str, Any]
    ) -> DiscoveryState:
        """
        Just poll email for API key (assume registration already done).
        """
        email_domain = registration_info.get("email_domain", "")
        subject_keywords = registration_info.get(
            "subject_keywords", ["API Key", "API", "Token"]
        )

        api_key = await self._poll_email_for_key(
            source_domain=email_domain,
            subject_keywords=subject_keywords,
            max_attempts=Config.API_KEY_CHECK_MAX_ATTEMPTS,
            interval=Config.API_KEY_CHECK_INTERVAL,
        )

        if api_key:
            self._store_key(registration_info["base_url"], api_key, registration_info)
            return self._apply_key_to_state(state, api_key, "automatic")

        # No key found, ask user to provide
        return self._request_manual_intervention(
            state,
            {
                "reason": "email_timeout",
                "message": "No API key found in recent emails. Please register and provide the key manually.",
                "registration_url": registration_info.get("registration_url"),
            },
        )

    async def _poll_email_for_key(
        self,
        source_domain: str,
        subject_keywords: List[str],
        max_attempts: int,
        interval: int,
    ) -> Optional[str]:
        """
        Poll email looking for API key.
        """
        if not self.email_service:
            return None

        for attempt in range(max_attempts):
            logger.info(
                f"Checking email for API key (attempt {attempt + 1}/{max_attempts})"
            )

            since_minutes = max(30, (attempt + 1) * interval // 60 + 5)

            emails = self.email_service.search_for_api_key(
                sender_domain=source_domain,
                subject_keywords=subject_keywords,
                since_minutes=since_minutes,
            )

            if emails:
                for email in emails:
                    email_id = email.get("id")
                    if email_id:
                        full_email = self.email_service.get_email_content(email_id)
                        email_body = (
                            full_email.get("body", "")
                            if full_email
                            else email.get("snippet", "")
                        )
                    else:
                        email_body = email.get("snippet", "") or email.get("body", "")

                    api_key = self.email_service.extract_api_key_from_email(
                        email_content=email_body, llm=self.llm
                    )

                    if api_key:
                        logger.info("API key found in email!")
                        return api_key

            if attempt < max_attempts - 1:
                logger.info(f"No key found, waiting {interval} seconds...")
                await asyncio.sleep(interval)

        logger.warning("Email polling timeout - no API key found")
        return None

    def _store_key(
        self, base_url: str, api_key: str, registration_info: Dict[str, Any]
    ):
        """Store the acquired API key."""
        self.key_store.store_key(
            source_identifier=base_url,
            api_key=api_key,
            metadata={
                "source_name": registration_info.get("source_name"),
                "registration_url": registration_info.get("registration_url"),
                "registration_email": self.user_email or Config.DISCOVERY_EMAIL,
                "acquisition_method": "automatic",
            },
        )

    def _apply_key_to_state(
        self, state: DiscoveryState, api_key: str, source: str
    ) -> DiscoveryState:
        """Apply the acquired API key to the workflow state."""
        access_doc = state.get("access_documentation", {})
        access_doc["_provided_api_key"] = api_key
        state["access_documentation"] = access_doc
        state["api_key_acquired"] = True
        state["api_key_source"] = source
        state["api_key_acquisition_attempted"] = True

        # Clear any errors or interruptions
        state["interrupted"] = False
        if state.get("error", {}).get("agent_name") == "TestingAgent":
            state.pop("error", None)

        logger.info(f"Applied API key from {source}")
        return state

    def _request_manual_intervention(
        self, state: DiscoveryState, reason_info: Dict[str, Any]
    ) -> DiscoveryState:
        """Fall back to manual API key input."""
        state["waiting_for_human_input"] = True
        state["pause_reason"] = "needs_api_key"
        state["api_key_acquisition_attempted"] = True

        message = reason_info.get("message", "Please provide the API key manually.")
        registration_url = reason_info.get("registration_url", "")

        state["human_input_request"] = HumanInputRequest(
            input_type=HumanInputType.API_KEY,
            field_name="api_key",
            description=message,
            required=True,
            registration_url=registration_url,
            additional_info={
                "automatic_acquisition_attempted": True,
                "failure_reason": reason_info.get("reason"),
                "instructions": reason_info.get(
                    "instructions", f"Please register at: {registration_url}"
                ),
            },
        ).to_dict()

        state["interrupted"] = True
        logger.info(f"Requesting manual intervention: {reason_info.get('reason')}")
        return state
