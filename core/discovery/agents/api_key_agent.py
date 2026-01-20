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
from core.discovery.llm_logger import invoke_llm_with_logging, AgentLogger
from config import Config

logger = logging.getLogger(__name__)

# Mask sensitive data for logging
def _mask_sensitive(value: str, visible_chars: int = 4) -> str:
    """Mask sensitive values like passwords and API keys for logging."""
    if not value or len(value) <= visible_chars * 2:
        return "***"
    return value[:visible_chars] + "*" * (len(value) - visible_chars * 2) + value[-visible_chars:]


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

        # Initialize comprehensive logging
        self.agent_logger = AgentLogger("APIKeyAgent", log_level=logging.INFO)

        # Get user email from environment
        self.user_email = os.environ.get("ARCADE_USER_ID", "") or Config.DISCOVERY_EMAIL

        # Initialize Arcade email service if configured
        if Config.ARCADE_API_KEY and Config.DISCOVERY_EMAIL:
            self.email_service = ArcadeEmailService(
                api_key=Config.ARCADE_API_KEY,
                user_id=Config.ARCADE_USER_ID or Config.DISCOVERY_EMAIL,
            )
            self.agent_logger.log_info(
                "Initialized with email service",
                email_user=self.user_email,
                model=Config.DISCOVERY_LLM_MODEL,
            )
        else:
            self.email_service = None
            self.agent_logger.log_warning(
                "Arcade API not configured, email polling disabled",
                arcade_api_key_set=bool(Config.ARCADE_API_KEY),
                discovery_email_set=bool(Config.DISCOVERY_EMAIL),
            )

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
        # Reset step counter for this run
        self.agent_logger.step_counter = 0

        try:
            access_doc = state.get("access_documentation", {})
            source_name = access_doc.get("source_name", "Unknown")
            base_url = access_doc.get("base_url", "")
            auth_info = access_doc.get("authentication", {})
            registration_url = auth_info.get("registration_url", "")

            # Log workflow entry
            workflow_step = self.agent_logger.log_step_start(
                step_name="run",
                step_description="Main API key acquisition workflow entry point",
                variables={
                    "source_name": source_name,
                    "base_url": base_url,
                    "registration_url": registration_url,
                    "auth_info": auth_info,
                    "email_service_available": self.email_service is not None,
                    "user_email": self.user_email,
                },
            )

            # Step 1: Check if we already have a key
            step1 = self.agent_logger.log_step_start(
                step_name="check_existing_key",
                step_description="Check if we already have a valid API key stored for this source",
                variables={
                    "base_url": base_url,
                    "source_name": source_name,
                },
            )

            existing_key = self._check_existing_key(base_url, source_name)

            self.agent_logger.log_decision(
                step_num=step1,
                decision_point="existing_key_check",
                condition="existing_key is not None",
                result=existing_key is not None,
                action="Use existing key" if existing_key else "Proceed to discover site",
            )

            if existing_key:
                self.agent_logger.log_step_result(
                    step_num=step1,
                    step_name="check_existing_key",
                    success=True,
                    result={
                        "key_found": True,
                        "masked_key": _mask_sensitive(existing_key),
                        "source": "existing",
                    },
                )
                return self._apply_key_to_state(state, existing_key, "existing")

            self.agent_logger.log_step_result(
                step_num=step1,
                step_name="check_existing_key",
                success=True,
                result={"key_found": False, "message": "No existing key found, proceeding to discovery"},
            )

            # Step 2: Discover the site and analyze registration process
            step2 = self.agent_logger.log_step_start(
                step_name="discover_site",
                step_description="Crawl and analyze the website to discover API key registration process",
                variables={
                    "url": registration_url or base_url,
                    "using_registration_url": bool(registration_url),
                },
            )

            site_info = self.discover_site(registration_url or base_url, parent_step_num=step2)

            self.agent_logger.log_step_result(
                step_num=step2,
                step_name="discover_site",
                success="error" not in site_info,
                result={
                    "key_available_on_site": site_info.get("key_available_on_site"),
                    "can_automate_registration": site_info.get("can_automate_registration"),
                    "requires_email_verification": site_info.get("requires_email_verification"),
                    "email_domain": site_info.get("email_domain"),
                    "navigation_steps_count": len(site_info.get("navigation_steps", [])),
                    "form_fields_count": len(site_info.get("form_fields", [])),
                },
                error=site_info.get("error"),
            )

            # Step 3: Try to extract key directly from site (if publicly available)
            self.agent_logger.log_decision(
                step_num=step2,
                decision_point="key_available_on_site",
                condition="site_info.get('key_available_on_site') == True",
                result=site_info.get("key_available_on_site", False),
                action="Extract key from site" if site_info.get("key_available_on_site") else "Skip direct extraction",
            )

            if site_info.get("key_available_on_site"):
                step3 = self.agent_logger.log_step_start(
                    step_name="extract_key_from_site",
                    step_description="Attempt to extract API key directly from the site (for publicly available keys)",
                    variables={
                        "site_url": site_info.get("url"),
                        "api_elements_found": bool(site_info.get("api_elements")),
                    },
                )

                extracted_key = self.extract_key_from_site(site_info, parent_step_num=step3)

                self.agent_logger.log_step_result(
                    step_num=step3,
                    step_name="extract_key_from_site",
                    success=extracted_key is not None,
                    result={
                        "key_extracted": extracted_key is not None,
                        "masked_key": _mask_sensitive(extracted_key) if extracted_key else None,
                    },
                )

                if extracted_key:
                    self._store_key(base_url, extracted_key, {
                        "source_name": source_name,
                        "registration_url": registration_url,
                    })
                    self.agent_logger.log_info(
                        "Successfully acquired API key via direct extraction",
                        source="automatic",
                        masked_key=_mask_sensitive(extracted_key),
                    )
                    return self._apply_key_to_state(state, extracted_key, "automatic")

            # Step 4: Try to request key via browser automation
            self.agent_logger.log_decision(
                step_num=step2,
                decision_point="can_automate_registration",
                condition="site_info.get('can_automate_registration') == True",
                result=site_info.get("can_automate_registration", False),
                action="Automate registration" if site_info.get("can_automate_registration") else "Skip automation",
            )

            if site_info.get("can_automate_registration", False):
                step4 = self.agent_logger.log_step_start(
                    step_name="request_key",
                    step_description="Request API key via automated form submission",
                    variables={
                        "email": self.user_email,
                        "source_name": source_name,
                        "registration_url": site_info.get("registration_url") or site_info.get("url"),
                    },
                )

                request_result = self.request_key(
                    site_info=site_info,
                    email=self.user_email,
                    source_name=source_name,
                    parent_step_num=step4,
                )

                self.agent_logger.log_step_result(
                    step_num=step4,
                    step_name="request_key",
                    success=request_result.get("success", False),
                    result={
                        "key_received": request_result.get("key") is not None,
                        "masked_key": _mask_sensitive(request_result.get("key")) if request_result.get("key") else None,
                        "email_verification_pending": request_result.get("email_verification_pending"),
                        "verification_message": request_result.get("verification_message"),
                    },
                    error=request_result.get("error"),
                )

                if request_result.get("key"):
                    # Key was immediately provided
                    self._store_key(base_url, request_result["key"], {
                        "source_name": source_name,
                        "registration_url": registration_url,
                    })
                    self.agent_logger.log_info(
                        "Successfully acquired API key via automated registration",
                        source="automatic",
                        masked_key=_mask_sensitive(request_result["key"]),
                    )
                    return self._apply_key_to_state(state, request_result["key"], "automatic")

                if request_result.get("email_verification_pending"):
                    # Need to check email for verification or key
                    self.agent_logger.log_info(
                        "Email verification pending, proceeding to email handling",
                        verification_message=request_result.get("verification_message"),
                    )
                    return asyncio.run(self._handle_email_verification(
                        state=state,
                        site_info=site_info,
                        source_name=source_name,
                        base_url=base_url,
                        registration_url=registration_url,
                    ))

            # Step 5: Check if we can poll email for key
            self.agent_logger.log_decision(
                step_num=workflow_step,
                decision_point="email_polling",
                condition="self.email_service and site_info.get('email_domain')",
                result=bool(self.email_service and site_info.get("email_domain")),
                action="Poll email for key" if (self.email_service and site_info.get("email_domain")) else "Skip email polling",
            )

            if self.email_service and site_info.get("email_domain"):
                step5 = self.agent_logger.log_step_start(
                    step_name="poll_email_only",
                    step_description="Poll email inbox for API key (assuming registration was done elsewhere)",
                    variables={
                        "email_domain": site_info.get("email_domain"),
                        "subject_keywords": site_info.get("subject_keywords", ["API Key", "API", "Token"]),
                    },
                )

                result = asyncio.run(self._poll_email_only(state, {
                    "email_domain": site_info.get("email_domain"),
                    "subject_keywords": site_info.get("subject_keywords", ["API Key", "API", "Token"]),
                    "base_url": base_url,
                    "source_name": source_name,
                    "registration_url": registration_url,
                }))

                self.agent_logger.log_step_result(
                    step_num=step5,
                    step_name="poll_email_only",
                    success=result.get("api_key_acquired", False),
                    result={
                        "api_key_acquired": result.get("api_key_acquired", False),
                        "waiting_for_human_input": result.get("waiting_for_human_input", False),
                    },
                )

                return result

            # Step 6: Fall back to manual intervention
            step6 = self.agent_logger.log_step_start(
                step_name="manual_intervention",
                step_description="Request manual API key input as automation could not complete",
                variables={
                    "reason": "automation_failed",
                    "registration_url": registration_url,
                },
            )

            self.agent_logger.log_step_result(
                step_num=step6,
                step_name="manual_intervention",
                success=True,
                result={
                    "action": "Requesting manual intervention",
                    "message": f"Could not automatically acquire API key for {source_name}",
                },
            )

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
            self.agent_logger.log_error(
                f"API Key Agent failed with exception: {type(e).__name__}",
                error=e,
                exception_message=str(e),
            )
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
            self.agent_logger.log_info("Browser cleanup completed")

        return state

    def discover_site(self, url: str, parent_step_num: Optional[int] = None) -> Dict[str, Any]:
        """
        Crawl the given website to discover API key registration information.

        Uses LLM-driven browser automation to explore the site and understand
        the registration process.

        Args:
            url: The website URL to discover
            parent_step_num: Optional parent step number for logging (if called from another step)

        Returns:
            Site information dict with registration details
        """
        if parent_step_num is not None:
            step_num = parent_step_num
            self.agent_logger.log_substep(step_num, f"Starting site discovery for {url}", {"url": url})
        else:
            step_num = self.agent_logger.log_step_start(
                step_name="discover_site",
                step_description="Crawl website to discover API key registration process",
                variables={"url": url},
            )

        if not url:
            if parent_step_num is None:
                self.agent_logger.log_step_result(
                    step_num=step_num,
                    step_name="discover_site",
                    success=False,
                    error="No URL provided",
                )
            else:
                self.agent_logger.log_substep(step_num, "Site discovery failed", {"error": "No URL provided"})
            return {"error": "No URL provided"}

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
            self.agent_logger.log_substep(step_num, "Navigating to URL", {"url": url})
            nav_result = browser.navigate_to_url(url)

            self.agent_logger.log_substep(
                step_num,
                "Navigation result",
                {"success": nav_result.get("success"), "current_url": nav_result.get("url")},
            )

            if not nav_result.get("success"):
                if parent_step_num is None:
                    self.agent_logger.log_step_result(
                        step_num=step_num,
                        step_name="discover_site",
                        success=False,
                        error=f"Failed to navigate to {url}",
                    )
                else:
                    self.agent_logger.log_substep(step_num, "Navigation failed", {"error": f"Failed to navigate to {url}"})
                return site_info

            # Get initial page content
            self.agent_logger.log_substep(step_num, "Getting page content and API elements")
            content = browser.get_page_content()
            links = browser.get_links()
            api_elements = browser.find_api_key_elements()

            self.agent_logger.log_substep(
                step_num,
                "Page content retrieved",
                {
                    "page_title": content.get("title"),
                    "visible_text_length": len(content.get("visible_text", "")),
                    "links_count": links.get("count", 0),
                    "api_key_displays_found": len(api_elements.get("api_key_displays", [])),
                    "generate_buttons_found": len(api_elements.get("generate_buttons", [])),
                },
            )

            # Use LLM to analyze the page and decide next steps
            self.agent_logger.log_substep(step_num, "Invoking LLM to analyze page for API key")
            analysis = self._analyze_page_for_api_key(
                url=url,
                content=content,
                links=links,
                api_elements=api_elements,
                parent_step_num=step_num,
            )

            self.agent_logger.log_substep(
                step_num,
                "Page analysis complete",
                {
                    "key_available_on_site": analysis.get("key_available_on_site"),
                    "can_automate_registration": analysis.get("can_automate_registration"),
                    "needs_navigation": analysis.get("needs_navigation"),
                    "registration_url": analysis.get("registration_url"),
                    "notes": analysis.get("notes"),
                },
            )

            site_info.update(analysis)

            # If we need to navigate to find API registration, use LLM-driven navigation
            self.agent_logger.log_decision(
                step_num=step_num,
                decision_point="needs_navigation",
                condition="analysis.get('needs_navigation') == True",
                result=analysis.get("needs_navigation", False),
                action="Start LLM-driven navigation" if analysis.get("needs_navigation") else "Skip navigation",
            )

            if analysis.get("needs_navigation"):
                self.agent_logger.log_substep(
                    step_num,
                    "Starting LLM-driven navigation to find registration page",
                    {"goal": "Find the API key registration or generation page", "max_steps": 5},
                )

                nav_steps = self._llm_driven_navigation(
                    browser=browser,
                    goal="Find the API key registration or generation page",
                    parent_step_num=step_num,
                    max_steps=5,
                )
                site_info["navigation_steps"] = nav_steps

                self.agent_logger.log_substep(
                    step_num,
                    "LLM-driven navigation complete",
                    {"steps_taken": len(nav_steps), "navigation_steps": nav_steps},
                )

                # Re-analyze after navigation
                self.agent_logger.log_substep(step_num, "Re-analyzing page after navigation")
                content = browser.get_page_content()
                api_elements = browser.find_api_key_elements()
                form_fields = browser.find_form_fields()

                final_analysis = self._analyze_registration_page(
                    content=content,
                    api_elements=api_elements,
                    form_fields=form_fields,
                    parent_step_num=step_num,
                )

                self.agent_logger.log_substep(
                    step_num,
                    "Final registration page analysis",
                    {
                        "can_automate_registration": final_analysis.get("can_automate_registration"),
                        "has_captcha": final_analysis.get("has_captcha"),
                        "required_fields": final_analysis.get("required_fields"),
                        "requires_email_verification": final_analysis.get("requires_email_verification"),
                    },
                )

                site_info.update(final_analysis)

            # Extract email domain from URL
            from urllib.parse import urlparse
            parsed = urlparse(url)
            site_info["email_domain"] = parsed.netloc.replace("www.", "").replace("api.", "")

            # Only log step result if this is a standalone call (not called with parent_step_num)
            if parent_step_num is None:
                self.agent_logger.log_step_result(
                    step_num=step_num,
                    step_name="discover_site",
                    success=True,
                    result={
                        "key_available_on_site": site_info.get("key_available_on_site"),
                        "can_automate_registration": site_info.get("can_automate_registration"),
                        "requires_email_verification": site_info.get("requires_email_verification"),
                        "email_domain": site_info.get("email_domain"),
                        "navigation_steps_count": len(site_info.get("navigation_steps", [])),
                    },
                )
            else:
                self.agent_logger.log_substep(
                    step_num,
                    "Site discovery completed",
                    {
                        "key_available_on_site": site_info.get("key_available_on_site"),
                        "can_automate_registration": site_info.get("can_automate_registration"),
                        "requires_email_verification": site_info.get("requires_email_verification"),
                        "email_domain": site_info.get("email_domain"),
                        "navigation_steps_count": len(site_info.get("navigation_steps", [])),
                    },
                )

            return site_info

        except Exception as e:
            self.agent_logger.log_error(
                f"Error during site discovery: {type(e).__name__}",
                error=e,
                url=url,
            )
            site_info["error"] = str(e)
            return site_info

    def extract_key_from_site(self, site_info: Dict[str, Any], parent_step_num: Optional[int] = None) -> Optional[str]:
        """
        Attempt to extract an API key directly from the site.

        This is for cases where the key is immediately available without
        registration (e.g., demo/test keys, public APIs).

        Args:
            site_info: Site information from discover_site
            parent_step_num: Optional parent step number for logging (if called from another step)

        Returns:
            API key if found, None otherwise
        """
        if parent_step_num is not None:
            step_num = parent_step_num
            self.agent_logger.log_substep(step_num, f"Starting key extraction from site", {
                "site_url": site_info.get("url"),
                "key_available_on_site": site_info.get("key_available_on_site"),
            })
        else:
            step_num = self.agent_logger.log_step_start(
                step_name="extract_key_from_site",
                step_description="Attempt to extract API key directly from the site (for demo/public keys)",
                variables={
                    "site_url": site_info.get("url"),
                    "key_available_on_site": site_info.get("key_available_on_site"),
                },
            )

        browser = self._get_browser_service()

        try:
            # Check for API key elements on the current page
            self.agent_logger.log_substep(step_num, "Searching for API key elements on page")
            api_elements = browser.find_api_key_elements()

            self.agent_logger.log_substep(
                step_num,
                "API elements found",
                {
                    "api_key_displays_count": len(api_elements.get("api_key_displays", [])),
                    "generate_buttons_count": len(api_elements.get("generate_buttons", [])),
                },
            )

            if api_elements.get("api_key_displays"):
                # Found potential API keys displayed on the page
                self.agent_logger.log_substep(
                    step_num,
                    "Found potential API key displays, validating each",
                    {"count": len(api_elements["api_key_displays"])},
                )

                for i, display in enumerate(api_elements["api_key_displays"]):
                    key_text = display.get("text", "")

                    self.agent_logger.log_substep(
                        step_num,
                        f"Validating potential key {i+1}",
                        {"masked_key_text": _mask_sensitive(key_text), "length": len(key_text)},
                    )

                    # Use LLM to validate this is actually an API key
                    if self._validate_api_key(key_text):
                        if parent_step_num is None:
                            self.agent_logger.log_step_result(
                                step_num=step_num,
                                step_name="extract_key_from_site",
                                success=True,
                                result={
                                    "extraction_method": "direct_display",
                                    "masked_key": _mask_sensitive(key_text),
                                },
                            )
                        else:
                            self.agent_logger.log_substep(step_num, "Key extracted successfully", {
                                "extraction_method": "direct_display",
                                "masked_key": _mask_sensitive(key_text),
                            })
                        return key_text

            # Try clicking generate buttons if they exist
            if api_elements.get("generate_buttons"):
                self.agent_logger.log_substep(
                    step_num,
                    "Trying to click generate buttons",
                    {"buttons": [b.get("text") for b in api_elements["generate_buttons"]]},
                )

                for button in api_elements["generate_buttons"]:
                    button_text = button.get("text", "")

                    self.agent_logger.log_substep(
                        step_num,
                        f"Clicking generate button: '{button_text}'",
                    )

                    result = browser.click_element(button_text)

                    if result.get("success"):
                        # Wait for key to be generated
                        import time
                        self.agent_logger.log_substep(step_num, "Button clicked, waiting for key generation")
                        time.sleep(2)

                        # Check for newly displayed key
                        new_elements = browser.find_api_key_elements()
                        if new_elements.get("api_key_displays"):
                            for display in new_elements["api_key_displays"]:
                                key_text = display.get("text", "")
                                if self._validate_api_key(key_text):
                                    if parent_step_num is None:
                                        self.agent_logger.log_step_result(
                                            step_num=step_num,
                                            step_name="extract_key_from_site",
                                            success=True,
                                            result={
                                                "extraction_method": "generate_button",
                                                "button_text": button_text,
                                                "masked_key": _mask_sensitive(key_text),
                                            },
                                        )
                                    else:
                                        self.agent_logger.log_substep(step_num, "Key extracted successfully", {
                                            "extraction_method": "generate_button",
                                            "button_text": button_text,
                                            "masked_key": _mask_sensitive(key_text),
                                        })
                                    return key_text

            if parent_step_num is None:
                self.agent_logger.log_step_result(
                    step_num=step_num,
                    step_name="extract_key_from_site",
                    success=False,
                    result={"message": "No valid API key found on site"},
                )
            else:
                self.agent_logger.log_substep(step_num, "Key extraction failed", {"message": "No valid API key found on site"})
            return None

        except Exception as e:
            self.agent_logger.log_error(
                f"Error extracting key from site: {type(e).__name__}",
                error=e,
            )
            return None

    def request_key(
        self,
        site_info: Dict[str, Any],
        email: str,
        source_name: str,
        parent_step_num: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Request an API key by filling out registration forms.

        Uses Selenium-driven browser automation with LLM guidance to
        fill out forms and complete the registration process.

        Args:
            site_info: Site information from discover_site
            email: Email address to use for registration
            source_name: Name of the data source
            parent_step_num: Optional parent step number for logging (if called from another step)

        Returns:
            Result dict with key if immediately available, or status
        """
        if parent_step_num is not None:
            step_num = parent_step_num
            self.agent_logger.log_substep(step_num, f"Starting API key request via form submission", {
                "source_name": source_name,
                "email": email,
                "registration_url": site_info.get("registration_url") or site_info.get("url"),
                "form_fields_expected": site_info.get("form_fields"),
            })
        else:
            step_num = self.agent_logger.log_step_start(
                step_name="request_key",
                step_description="Request API key via automated form submission",
                variables={
                    "source_name": source_name,
                    "email": email,
                    "registration_url": site_info.get("registration_url") or site_info.get("url"),
                    "form_fields_expected": site_info.get("form_fields"),
                },
            )

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
                self.agent_logger.log_substep(step_num, "Navigating to registration URL", {"url": reg_url})
                browser.navigate_to_url(reg_url)

            # Get form fields
            self.agent_logger.log_substep(step_num, "Finding form fields on registration page")
            form_fields = browser.find_form_fields()

            self.agent_logger.log_substep(
                step_num,
                "Form fields found",
                {"fields_count": len(form_fields.get("fields", [])), "fields": form_fields.get("fields", [])},
            )

            if not form_fields.get("fields"):
                result["error"] = "No form fields found on registration page"
                if parent_step_num is None:
                    self.agent_logger.log_step_result(
                        step_num=step_num,
                        step_name="request_key",
                        success=False,
                        error=result["error"],
                    )
                else:
                    self.agent_logger.log_substep(step_num, "Key request failed", {"error": result["error"]})
                return result

            # Generate a secure password if needed
            password = self._generate_secure_password()
            self.agent_logger.log_substep(
                step_num,
                "Generated secure password for registration",
                {"password_length": len(password)},
            )

            # Use LLM to determine how to fill the form
            self.agent_logger.log_substep(step_num, "Using LLM to determine form fill instructions")
            fill_instructions = self._get_form_fill_instructions(
                form_fields=form_fields.get("fields", []),
                email=email,
                password=password,
                source_name=source_name,
                parent_step_num=step_num,
            )

            self.agent_logger.log_substep(
                step_num,
                "Form fill instructions received from LLM",
                {
                    "instructions_count": len(fill_instructions),
                    "fields_to_fill": [
                        {
                            "selector": inst.get("selector"),
                            "value_type": "password" if "password" in inst.get("selector", "").lower()
                            else "email" if "email" in inst.get("selector", "").lower()
                            else "other",
                        }
                        for inst in fill_instructions
                    ],
                },
            )

            # Fill in the form fields
            self.agent_logger.log_substep(step_num, "Filling form fields")
            for i, instruction in enumerate(fill_instructions):
                field_selector = instruction.get("selector")
                field_value = instruction.get("value")

                if field_selector and field_value:
                    # Mask sensitive values in logs
                    is_sensitive = any(s in field_selector.lower() for s in ["password", "key", "secret", "token"])
                    logged_value = _mask_sensitive(field_value) if is_sensitive else field_value

                    self.agent_logger.log_substep(
                        step_num,
                        f"Filling field {i+1}/{len(fill_instructions)}",
                        {"selector": field_selector, "value": logged_value},
                    )
                    browser.fill_form_field(field_selector, field_value)

            # Submit the form
            self.agent_logger.log_substep(step_num, "Submitting registration form")
            submit_result = browser.submit_form()

            self.agent_logger.log_substep(
                step_num,
                "Form submission result",
                {"success": submit_result.get("success"), "details": submit_result},
            )

            if not submit_result.get("success"):
                result["error"] = "Failed to submit registration form"
                if parent_step_num is None:
                    self.agent_logger.log_step_result(
                        step_num=step_num,
                        step_name="request_key",
                        success=False,
                        error=result["error"],
                    )
                else:
                    self.agent_logger.log_substep(step_num, "Form submission failed", {"error": result["error"]})
                return result

            # Check what happened after submission
            import time
            self.agent_logger.log_substep(step_num, "Waiting for page to load after submission")
            time.sleep(3)

            content = browser.get_page_content()
            api_elements = browser.find_api_key_elements()

            # Analyze the post-submission page
            self.agent_logger.log_substep(
                step_num,
                "Analyzing post-submission page",
                {
                    "page_title": content.get("title"),
                    "api_key_displays_found": len(api_elements.get("api_key_displays", [])),
                },
            )

            post_analysis = self._analyze_post_submission(
                content=content,
                api_elements=api_elements,
                parent_step_num=step_num,
            )

            self.agent_logger.log_substep(
                step_num,
                "Post-submission analysis result",
                {
                    "key_found": post_analysis.get("key") is not None,
                    "email_verification_required": post_analysis.get("email_verification_required"),
                    "status": post_analysis.get("status"),
                    "message": post_analysis.get("message"),
                },
            )

            if post_analysis.get("key"):
                result["success"] = True
                result["key"] = post_analysis["key"]
                if parent_step_num is None:
                    self.agent_logger.log_step_result(
                        step_num=step_num,
                        step_name="request_key",
                        success=True,
                        result={
                            "key_received": True,
                            "masked_key": _mask_sensitive(post_analysis["key"]),
                        },
                    )
                else:
                    self.agent_logger.log_substep(step_num, "Key received after form submission", {
                        "key_received": True,
                        "masked_key": _mask_sensitive(post_analysis["key"]),
                    })

            elif post_analysis.get("email_verification_required"):
                result["success"] = True
                result["email_verification_pending"] = True
                result["verification_message"] = post_analysis.get("message", "Check your email")
                if parent_step_num is None:
                    self.agent_logger.log_step_result(
                        step_num=step_num,
                        step_name="request_key",
                        success=True,
                        result={
                            "email_verification_pending": True,
                            "verification_message": result["verification_message"],
                        },
                    )
                else:
                    self.agent_logger.log_substep(step_num, "Email verification required", {
                        "email_verification_pending": True,
                        "verification_message": result["verification_message"],
                    })

            elif post_analysis.get("error"):
                result["error"] = post_analysis["error"]
                if parent_step_num is None:
                    self.agent_logger.log_step_result(
                        step_num=step_num,
                        step_name="request_key",
                        success=False,
                        error=result["error"],
                    )
                else:
                    self.agent_logger.log_substep(step_num, "Key request failed", {"error": result["error"]})

            else:
                # Assume email verification is needed
                result["success"] = True
                result["email_verification_pending"] = True
                if parent_step_num is None:
                    self.agent_logger.log_step_result(
                        step_num=step_num,
                        step_name="request_key",
                        success=True,
                        result={
                            "email_verification_pending": True,
                            "message": "Assuming email verification is required (no explicit confirmation)",
                        },
                    )
                else:
                    self.agent_logger.log_substep(step_num, "Email verification assumed", {
                        "email_verification_pending": True,
                        "message": "Assuming email verification is required (no explicit confirmation)",
                    })

            return result

        except Exception as e:
            self.agent_logger.log_error(
                f"Error requesting key: {type(e).__name__}",
                error=e,
                source_name=source_name,
            )
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
        subject_keywords = subject_keywords or ["API Key", "API", "Registration", "Verification"]

        step_num = self.agent_logger.log_step_start(
            step_name="poll_email",
            step_description="Poll email inbox for API key or verification email",
            variables={
                "sender_domain": sender_domain,
                "subject_keywords": subject_keywords,
                "max_wait_minutes": max_wait_minutes,
            },
        )

        if not self.email_service:
            self.agent_logger.log_step_result(
                step_num=step_num,
                step_name="poll_email",
                success=False,
                error="Email service not configured",
            )
            return None

        max_attempts = max_wait_minutes * 2  # Check every 30 seconds
        interval = 30

        self.agent_logger.log_substep(
            step_num,
            "Starting email polling loop",
            {"max_attempts": max_attempts, "interval_seconds": interval},
        )

        for attempt in range(max_attempts):
            self.agent_logger.log_substep(
                step_num,
                f"Polling email (attempt {attempt + 1}/{max_attempts})",
                {"sender_domain": sender_domain},
            )

            emails = self.email_service.search_for_api_key(
                sender_domain=sender_domain,
                subject_keywords=subject_keywords,
                since_minutes=max_wait_minutes + 5,
            )

            self.agent_logger.log_substep(
                step_num,
                "Email search result",
                {"emails_found": len(emails) if emails else 0},
            )

            if emails:
                for email in emails:
                    email_id = email.get("id")
                    email_subject = email.get("subject", "Unknown")

                    self.agent_logger.log_substep(
                        step_num,
                        "Processing found email",
                        {"email_id": email_id, "subject": email_subject},
                    )

                    if email_id:
                        full_email = self.email_service.get_email_content(email_id)
                        if full_email:
                            self.agent_logger.log_step_result(
                                step_num=step_num,
                                step_name="poll_email",
                                success=True,
                                result={
                                    "email_found": True,
                                    "sender_domain": sender_domain,
                                    "subject": full_email.get("subject"),
                                    "attempts_taken": attempt + 1,
                                },
                            )
                            return full_email

            if attempt < max_attempts - 1:
                import time
                self.agent_logger.log_substep(
                    step_num,
                    f"No email found yet, waiting {interval} seconds before next attempt",
                )
                time.sleep(interval)

        self.agent_logger.log_step_result(
            step_num=step_num,
            step_name="poll_email",
            success=False,
            result={
                "email_found": False,
                "attempts_made": max_attempts,
                "total_wait_minutes": max_wait_minutes,
            },
        )
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
        email_subject = email_content.get("subject", "")
        email_body = email_content.get("body", "") or email_content.get("snippet", "")

        step_num = self.agent_logger.log_step_start(
            step_name="respond_to_email",
            step_description="Analyze email and take appropriate action (extract key, click verification link, etc.)",
            variables={
                "email_subject": email_subject,
                "email_body_length": len(email_body),
                "email_sender": email_content.get("from"),
            },
        )

        result = {
            "action_taken": None,
            "key": None,
            "error": None,
        }

        try:
            # Use LLM to analyze the email and determine what action to take
            self.agent_logger.log_substep(step_num, "Using LLM to analyze email for action")
            analysis = self._analyze_email_for_action(
                subject=email_subject,
                body=email_body,
                parent_step_num=step_num,
            )

            self.agent_logger.log_substep(
                step_num,
                "Email analysis result",
                {
                    "contains_api_key": analysis.get("contains_api_key"),
                    "has_verification_link": analysis.get("verification_link") is not None,
                    "has_verification_code": analysis.get("verification_code") is not None,
                    "action_required": analysis.get("action_required"),
                },
            )

            if analysis.get("contains_api_key"):
                # Extract the key directly
                self.agent_logger.log_substep(step_num, "Email contains API key, attempting extraction")
                key = self.extract_key_from_email(email_body)
                if key:
                    result["action_taken"] = "extracted_key"
                    result["key"] = key
                    self.agent_logger.log_step_result(
                        step_num=step_num,
                        step_name="respond_to_email",
                        success=True,
                        result={
                            "action_taken": "extracted_key",
                            "masked_key": _mask_sensitive(key),
                        },
                    )
                    return result

            if analysis.get("verification_link"):
                # Click the verification link
                link = analysis["verification_link"]
                result["action_taken"] = "clicked_verification_link"

                self.agent_logger.log_substep(
                    step_num,
                    "Found verification link, navigating to it",
                    {"link": link},
                )

                browser = self._get_browser_service()
                nav_result = browser.navigate_to_url(link)

                self.agent_logger.log_substep(
                    step_num,
                    "Navigation to verification link result",
                    {"success": nav_result.get("success")},
                )

                if nav_result.get("success"):
                    # Check if key is now available
                    import time
                    self.agent_logger.log_substep(step_num, "Waiting for page load after verification")
                    time.sleep(3)

                    api_elements = browser.find_api_key_elements()

                    self.agent_logger.log_substep(
                        step_num,
                        "Checking for API key on verification page",
                        {"api_key_displays_found": len(api_elements.get("api_key_displays", []))},
                    )

                    if api_elements.get("api_key_displays"):
                        for display in api_elements["api_key_displays"]:
                            key_text = display.get("text", "")
                            if self._validate_api_key(key_text):
                                result["key"] = key_text
                                self.agent_logger.log_step_result(
                                    step_num=step_num,
                                    step_name="respond_to_email",
                                    success=True,
                                    result={
                                        "action_taken": "clicked_verification_link",
                                        "key_found_after_verification": True,
                                        "masked_key": _mask_sensitive(key_text),
                                    },
                                )
                                return result

            if analysis.get("verification_code"):
                # May need to enter verification code somewhere
                result["action_taken"] = "found_verification_code"
                result["verification_code"] = analysis["verification_code"]

                self.agent_logger.log_substep(
                    step_num,
                    "Found verification code in email",
                    {"verification_code": analysis["verification_code"]},
                )

            self.agent_logger.log_step_result(
                step_num=step_num,
                step_name="respond_to_email",
                success=result.get("action_taken") is not None,
                result={
                    "action_taken": result["action_taken"],
                    "key_found": result["key"] is not None,
                },
            )
            return result

        except Exception as e:
            self.agent_logger.log_error(
                f"Error responding to email: {type(e).__name__}",
                error=e,
                email_subject=email_subject,
            )
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
        parent_step_num: int,
    ) -> Dict[str, Any]:
        """Use LLM to analyze a page and determine API key availability."""

        step_num = parent_step_num
        self.agent_logger.log_substep(
            step_num,
            "Analyzing page for API key availability",
            {
                "url": url,
                "page_title": content.get("title", "Unknown"),
                "visible_text_length": len(content.get("visible_text", "")),
                "links_count": len(links.get("links", [])),
                "api_key_displays_found": len(api_elements.get("api_key_displays", [])),
                "generate_buttons_found": len(api_elements.get("generate_buttons", [])),
            },
        )

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

        # Log the prompt
        self.agent_logger.log_prompt(
            step_num=step_num,
            operation="analyze_page_for_api_key",
            prompt=prompt,
        )

        try:
            import time
            start_time = time.time()

            messages = [HumanMessage(content=prompt)]
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="APIKeyAgent",
                operation="analyze_page_for_api_key"
            )

            elapsed_ms = (time.time() - start_time) * 1000
            response_text = response.content

            # Log raw LLM response
            self.agent_logger.log_llm_response(
                step_num=step_num,
                operation="analyze_page_for_api_key",
                response_content=response_text,
                elapsed_ms=elapsed_ms,
            )

            # Parse JSON
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0]
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0]

            parsed_result = json.loads(response_text.strip())

            # Log parsed result
            self.agent_logger.log_substep(
                step_num,
                "Page analysis completed",
                {
                    "key_available_on_site": parsed_result.get("key_available_on_site"),
                    "can_automate_registration": parsed_result.get("can_automate_registration"),
                    "requires_email_verification": parsed_result.get("requires_email_verification"),
                    "needs_navigation": parsed_result.get("needs_navigation"),
                    "llm_reasoning": parsed_result.get("notes"),
                },
            )

            return parsed_result

        except Exception as e:
            self.agent_logger.log_error(
                f"Failed to analyze page: {type(e).__name__}",
                error=e,
                url=url,
            )
            return {
                "key_available_on_site": False,
                "needs_navigation": True,
            }

    def _analyze_registration_page(
        self,
        content: Dict[str, Any],
        api_elements: Dict[str, Any],
        form_fields: Dict[str, Any],
        parent_step_num: int,
    ) -> Dict[str, Any]:
        """Analyze a registration page to understand how to complete it."""

        step_num = parent_step_num
        self.agent_logger.log_substep(
            step_num,
            "Analyzing registration page for automation feasibility",
            {
                "page_title": content.get("title"),
                "visible_text_length": len(content.get("visible_text", "")),
                "form_fields_count": len(form_fields.get("fields", [])),
                "api_elements_found": bool(api_elements.get("api_key_displays") or api_elements.get("generate_buttons")),
            },
        )

        prompt = f"""Analyze this API key registration page.

Page Content:
{content.get('visible_text', '')[:4000]}

API-related elements:
{json.dumps(api_elements, indent=2)}

Form fields found:
{json.dumps(form_fields.get('fields', []), indent=2)}

When looking for form fields, recognize these common patterns:

<form-field-example-1>
<input type="text"
</form-field-example-1>

<form-field-example-2>
<input type="email"
</form-field-example-2>

<form-field-example-3>
<input type="password"
</form-field-example-3>

<form-field-example-4>
<textarea name="description"
</form-field-example-4>

When looking for submit buttons, recognize these common patterns:

<submit-button-example-1>
<button type="submit">
</submit-button-example-1>

<submit-button-example-2>
<input type="submit">
</submit-button-example-2>

<submit-button-example-3>
<input type="image" src="submit.png">
</submit-button-example-3>

<submit-button-example-4>
<button>Submit</button>
</submit-button-example-4>

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

        # Log the prompt
        self.agent_logger.log_prompt(
            step_num=step_num,
            operation="analyze_registration_page",
            prompt=prompt,
        )

        try:
            import time
            start_time = time.time()

            messages = [HumanMessage(content=prompt)]
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="APIKeyAgent",
                operation="analyze_registration_page"
            )

            elapsed_ms = (time.time() - start_time) * 1000
            response_text = response.content

            # Log raw LLM response
            self.agent_logger.log_llm_response(
                step_num=step_num,
                operation="analyze_registration_page",
                response_content=response_text,
                elapsed_ms=elapsed_ms,
            )

            # Parse JSON
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0]
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0]

            parsed_result = json.loads(response_text.strip())

            # Log parsed result
            self.agent_logger.log_substep(
                step_num,
                "Registration page analysis completed",
                {
                    "can_automate_registration": parsed_result.get("can_automate_registration"),
                    "required_fields": parsed_result.get("required_fields"),
                    "has_captcha": parsed_result.get("has_captcha"),
                    "requires_email_verification": parsed_result.get("requires_email_verification"),
                    "llm_reasoning": parsed_result.get("notes"),
                },
            )

            return parsed_result

        except Exception as e:
            self.agent_logger.log_error(
                f"Failed to analyze registration page: {type(e).__name__}",
                error=e,
            )
            return {"can_automate_registration": False}

    def _llm_driven_navigation(
        self,
        browser: BrowserAutomationService,
        goal: str,
        parent_step_num: int,
        max_steps: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Use LLM to navigate the browser towards a goal.

        This is the core LLM-driven browser automation loop.

        Args:
            browser: Browser automation service
            goal: What we're trying to accomplish
            parent_step_num: Parent step number for logging
            max_steps: Maximum navigation steps

        Returns:
            List of steps taken
        """
        nav_step_num = parent_step_num
        self.agent_logger.log_substep(
            nav_step_num,
            "Starting LLM-driven navigation",
            {
                "goal": goal,
                "max_steps": max_steps,
            },
        )

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

        self.agent_logger.log_substep(
            nav_step_num,
            "Initializing navigation with system prompt",
            {"system_prompt_length": len(system_prompt)},
        )

        conversation_history.append(SystemMessage(content=system_prompt))

        for step_num in range(max_steps):
            # Get current page state
            content = browser.get_page_content()
            links = browser.get_links()
            api_elements = browser.find_api_key_elements()

            self.agent_logger.log_substep(
                nav_step_num,
                f"Navigation iteration {step_num + 1}/{max_steps}",
                {
                    "current_url": content.get("url"),
                    "page_title": content.get("title"),
                    "links_count": links.get("count", 0),
                    "api_elements_found": bool(api_elements.get("api_key_displays") or api_elements.get("generate_buttons")),
                },
            )

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

            # Log the current state being sent to LLM
            self.agent_logger.log_prompt(
                step_num=nav_step_num,
                operation=f"llm_driven_navigation_step_{step_num + 1}",
                prompt=state_description,
                additional_context={"conversation_length": len(conversation_history)},
            )

            try:
                import time
                start_time = time.time()

                response = invoke_llm_with_logging(
                    self.llm,
                    conversation_history,
                    agent_name="APIKeyAgent",
                    operation="llm_driven_navigation"
                )

                elapsed_ms = (time.time() - start_time) * 1000
                response_text = response.content
                conversation_history.append(AIMessage(content=response_text))

                # Log LLM response
                self.agent_logger.log_llm_response(
                    step_num=nav_step_num,
                    operation=f"llm_driven_navigation_step_{step_num + 1}",
                    response_content=response_text,
                    elapsed_ms=elapsed_ms,
                )

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

                self.agent_logger.log_substep(
                    nav_step_num,
                    f"LLM decided action for step {step_num + 1}",
                    {
                        "action": action,
                        "params": action_data.get("params", {}),
                        "llm_reasoning": action_data.get("reasoning"),
                    },
                )

                if action == "done":
                    step_record["success"] = action_data.get("success", False)
                    steps.append(step_record)

                    self.agent_logger.log_substep(
                        nav_step_num,
                        "LLM-driven navigation completed",
                        {
                            "total_steps": len(steps),
                            "final_action": "done",
                            "llm_reasoning": action_data.get("reasoning"),
                        },
                    )
                    break

                elif action == "navigate_to_url":
                    url = action_data.get("params", {}).get("url")
                    if url:
                        result = browser.navigate_to_url(url)
                        step_record["result"] = result
                        self.agent_logger.log_substep(
                            nav_step_num,
                            f"Executed navigate_to_url",
                            {"url": url, "success": result.get("success")},
                        )

                elif action == "click_element":
                    selector = action_data.get("params", {}).get("selector")
                    if selector:
                        result = browser.click_element(selector)
                        step_record["result"] = result
                        self.agent_logger.log_substep(
                            nav_step_num,
                            f"Executed click_element",
                            {"selector": selector, "success": result.get("success")},
                        )

                elif action == "get_links":
                    result = browser.get_links()
                    step_record["result"] = {"count": result.get("count", 0)}
                    self.agent_logger.log_substep(
                        nav_step_num,
                        f"Executed get_links",
                        {"links_count": result.get("count", 0)},
                    )

                steps.append(step_record)

            except Exception as e:
                self.agent_logger.log_error(
                    f"Navigation step {step_num + 1} failed: {type(e).__name__}",
                    error=e,
                )
                steps.append({
                    "step": step_num + 1,
                    "error": str(e),
                })
                break

        # Final result if we didn't break early
        if not steps or steps[-1].get("action") != "done":
            self.agent_logger.log_substep(
                nav_step_num,
                "LLM-driven navigation incomplete",
                {
                    "total_steps": len(steps),
                    "reason": "Max steps reached without completing goal",
                },
            )

        return steps

    def _get_form_fill_instructions(
        self,
        form_fields: List[Dict[str, Any]],
        email: str,
        password: str,
        source_name: str,
        parent_step_num: int,
    ) -> List[Dict[str, str]]:
        """Use LLM to determine how to fill form fields."""

        step_num = parent_step_num
        self.agent_logger.log_substep(
            step_num,
            "Determining how to fill registration form fields",
            {
                "form_fields_count": len(form_fields),
                "email": email,
                "password_length": len(password),
                "source_name": source_name,
            },
        )

        prompt = f"""Given these form fields, determine what values to fill in for API key registration.

Form Fields:
{json.dumps(form_fields, indent=2)}

Available information:
- Email: {email}
- Password: {password}
- Source Name: {source_name}

When identifying form fields, look for these common patterns:

<form-field-example-1>
<input type="text" name="username">
</form-field-example-1>

<form-field-example-2>
<input type="email" name="email">
</form-field-example-2>

<form-field-example-3>
<input type="password" name="password">
</form-field-example-3>

<form-field-example-4>
<input type="text" name="organization">
</form-field-example-4>

<form-field-example-5>
<textarea name="use_case">
</form-field-example-5>

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

        # Log the prompt (with password masked)
        masked_prompt = prompt.replace(password, _mask_sensitive(password))
        self.agent_logger.log_prompt(
            step_num=step_num,
            operation="get_form_fill_instructions",
            prompt=masked_prompt,
        )

        try:
            import time
            start_time = time.time()

            messages = [HumanMessage(content=prompt)]
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="APIKeyAgent",
                operation="get_form_fill_instructions"
            )

            elapsed_ms = (time.time() - start_time) * 1000
            response_text = response.content

            # Log LLM response
            self.agent_logger.log_llm_response(
                step_num=step_num,
                operation="get_form_fill_instructions",
                response_content=response_text,
                elapsed_ms=elapsed_ms,
            )

            # Parse JSON
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0]
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0]

            parsed_result = json.loads(response_text.strip())

            # Log result (with sensitive values masked)
            masked_instructions = [
                {
                    "selector": inst.get("selector"),
                    "value_type": "password" if "password" in inst.get("selector", "").lower() else "other",
                }
                for inst in parsed_result
            ]

            self.agent_logger.log_substep(
                step_num,
                "Form fill instructions determined",
                {
                    "instructions_count": len(parsed_result),
                    "fields_mapped": masked_instructions,
                },
            )

            return parsed_result

        except Exception as e:
            self.agent_logger.log_error(
                f"Failed to get form fill instructions: {type(e).__name__}",
                error=e,
            )
            # Return basic instructions
            return [
                {"selector": "email", "value": email},
                {"selector": "password", "value": password},
            ]

    def _analyze_post_submission(
        self,
        content: Dict[str, Any],
        api_elements: Dict[str, Any],
        parent_step_num: int,
    ) -> Dict[str, Any]:
        """Analyze the page after form submission."""

        step_num = parent_step_num
        self.agent_logger.log_substep(
            step_num,
            "Analyzing page after form submission",
            {
                "page_title": content.get("title"),
                "visible_text_length": len(content.get("visible_text", "")),
                "api_key_displays_found": len(api_elements.get("api_key_displays", [])),
            },
        )

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

        # Log the prompt
        self.agent_logger.log_prompt(
            step_num=step_num,
            operation="analyze_post_submission",
            prompt=prompt,
        )

        try:
            import time
            start_time = time.time()

            messages = [HumanMessage(content=prompt)]
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="APIKeyAgent",
                operation="analyze_post_submission"
            )

            elapsed_ms = (time.time() - start_time) * 1000
            response_text = response.content

            # Log LLM response
            self.agent_logger.log_llm_response(
                step_num=step_num,
                operation="analyze_post_submission",
                response_content=response_text,
                elapsed_ms=elapsed_ms,
            )

            # Parse JSON
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0]
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0]

            parsed_result = json.loads(response_text.strip())

            # Log parsed result
            self.agent_logger.log_substep(
                step_num,
                "Post-submission analysis completed",
                {
                    "key_found": parsed_result.get("key") is not None,
                    "masked_key": _mask_sensitive(parsed_result.get("key")) if parsed_result.get("key") else None,
                    "email_verification_required": parsed_result.get("email_verification_required"),
                    "status": parsed_result.get("status"),
                    "message": parsed_result.get("message"),
                    "error": parsed_result.get("error"),
                },
            )

            return parsed_result

        except Exception as e:
            self.agent_logger.log_error(
                f"Failed to analyze post-submission page: {type(e).__name__}",
                error=e,
            )
            return {"status": "unknown"}

    def _analyze_email_for_action(
        self,
        subject: str,
        body: str,
        parent_step_num: int,
    ) -> Dict[str, Any]:
        """Analyze an email to determine what action to take."""

        step_num = parent_step_num
        self.agent_logger.log_substep(
            step_num,
            "Analyzing email to determine required action",
            {
                "email_subject": subject,
                "email_body_length": len(body),
            },
        )

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

        # Log the prompt
        self.agent_logger.log_prompt(
            step_num=step_num,
            operation="analyze_email_for_action",
            prompt=prompt,
        )

        try:
            import time
            start_time = time.time()

            messages = [HumanMessage(content=prompt)]
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="APIKeyAgent",
                operation="analyze_email_for_action"
            )

            elapsed_ms = (time.time() - start_time) * 1000
            response_text = response.content

            # Log LLM response
            self.agent_logger.log_llm_response(
                step_num=step_num,
                operation="analyze_email_for_action",
                response_content=response_text,
                elapsed_ms=elapsed_ms,
            )

            # Parse JSON
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0]
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0]

            parsed_result = json.loads(response_text.strip())

            # Log parsed result
            self.agent_logger.log_substep(
                step_num,
                "Email analysis completed",
                {
                    "contains_api_key": parsed_result.get("contains_api_key"),
                    "has_verification_link": parsed_result.get("verification_link") is not None,
                    "has_verification_code": parsed_result.get("verification_code") is not None,
                    "action_required": parsed_result.get("action_required"),
                },
            )

            return parsed_result

        except Exception as e:
            self.agent_logger.log_error(
                f"Failed to analyze email: {type(e).__name__}",
                error=e,
                email_subject=subject,
            )
            return {}

    def _validate_api_key(self, key_text: str) -> bool:
        """Use LLM to validate if a string looks like an API key."""
        self.agent_logger.log_info(
            "Validating potential API key",
            key_length=len(key_text) if key_text else 0,
            masked_key=_mask_sensitive(key_text) if key_text else None,
        )

        if not key_text or len(key_text) < 10 or len(key_text) > 200:
            self.agent_logger.log_info(
                "Key validation failed - length check",
                reason="Key too short or too long",
                length=len(key_text) if key_text else 0,
            )
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
            import time
            start_time = time.time()

            messages = [HumanMessage(content=prompt)]
            response = self.llm.invoke(messages)

            elapsed_ms = (time.time() - start_time) * 1000
            is_valid = "YES" in response.content.upper()

            self.agent_logger.log_info(
                "Key validation LLM response",
                llm_response=response.content.strip(),
                is_valid=is_valid,
                elapsed_ms=round(elapsed_ms, 0),
            )

            return is_valid
        except Exception as e:
            self.agent_logger.log_warning(
                f"LLM validation failed, falling back to heuristic: {type(e).__name__}",
            )
            # Fall back to simple heuristic
            import re
            # Check if it looks like an API key
            if re.match(r'^[A-Za-z0-9\-_]{16,}$', key_text):
                self.agent_logger.log_info("Key validation passed (heuristic: alphanumeric pattern)")
                return True
            if re.match(r'^[A-Fa-f0-9\-]{36}$', key_text):  # UUID
                self.agent_logger.log_info("Key validation passed (heuristic: UUID pattern)")
                return True
            self.agent_logger.log_info("Key validation failed (heuristic: no pattern match)")
            return False

    def _llm_extract_api_key(self, content: str) -> Optional[str]:
        """Extract API key from content using LLM."""
        step_num = self.agent_logger.log_step_start(
            step_name="_llm_extract_api_key",
            step_description="Use LLM to extract API key from content",
            variables={
                "content_length": len(content),
            },
        )

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

        # Log the prompt
        self.agent_logger.log_prompt(
            step_num=step_num,
            operation="llm_extract_api_key",
            prompt=prompt,
        )

        try:
            import time
            start_time = time.time()

            messages = [HumanMessage(content=prompt)]
            response = self.llm.invoke(messages)

            elapsed_ms = (time.time() - start_time) * 1000
            extracted = response.content.strip().replace("`", "").replace('"', "").replace("'", "")

            # Log LLM response
            self.agent_logger.log_llm_response(
                step_num=step_num,
                operation="llm_extract_api_key",
                response_content=response.content,
                elapsed_ms=elapsed_ms,
            )

            if extracted and extracted != "NOT_FOUND" and len(extracted) > 10:
                self.agent_logger.log_step_result(
                    step_num=step_num,
                    step_name="_llm_extract_api_key",
                    success=True,
                    result={
                        "key_extracted": True,
                        "masked_key": _mask_sensitive(extracted),
                    },
                )
                return extracted

            self.agent_logger.log_step_result(
                step_num=step_num,
                step_name="_llm_extract_api_key",
                success=False,
                result={"key_extracted": False, "reason": "No valid key found in content"},
            )
            return None

        except Exception as e:
            self.agent_logger.log_error(
                f"Failed to extract API key: {type(e).__name__}",
                error=e,
            )
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
