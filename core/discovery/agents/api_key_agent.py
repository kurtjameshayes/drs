"""
API Key Agent

Agent for automatically acquiring API keys for data sources.
Orchestrates key checking, registration analysis, and email polling.
"""

from typing import Optional, Dict, Any, List
import logging
import asyncio
from pymongo import MongoClient
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage

from core.discovery.state import (
    DiscoveryState,
    HumanInputRequest,
    HumanInputType,
    WorkflowError,
)
from core.discovery.services.api_key_store import APIKeyStore
from core.discovery.services.arcade_email_service import ArcadeEmailService
from core.discovery.llm_logger import invoke_llm_with_logging
from config import Config

logger = logging.getLogger(__name__)


class APIKeyAgent:
    """
    Agent for automatically acquiring API keys for data sources.

    Orchestrates:
    1. Checking existing key storage
    2. Analyzing registration requirements
    3. Monitoring email for API keys (Arcade)
    4. Falling back to manual input when needed
    """

    def __init__(self, db_client: MongoClient):
        self.llm = ChatAnthropic(
            model=Config.DISCOVERY_LLM_MODEL,
            api_key=Config.ANTHROPIC_API_KEY,
            temperature=0.1,
        )
        self.key_store = APIKeyStore(db_client)

        # Initialize Arcade email service if configured
        if Config.ARCADE_API_KEY and Config.DISCOVERY_EMAIL:
            self.email_service = ArcadeEmailService(
                api_key=Config.ARCADE_API_KEY,
                user_id=Config.ARCADE_USER_ID or Config.DISCOVERY_EMAIL,
            )
        else:
            self.email_service = None
            logger.warning("Arcade API not configured, email polling disabled")

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

            # Step 2: Determine if we can attempt automatic acquisition
            if not self.email_service:
                logger.info("Email service not configured, requesting manual input")
                return self._request_manual_intervention(
                    state,
                    {
                        "reason": "no_email_service",
                        "message": "Arcade API not configured for automatic key acquisition",
                        "registration_url": registration_url,
                    },
                )

            # Step 3: Check Gmail authorization
            auth_status = self.email_service.authorize_gmail()
            if not auth_status.get("authorized"):
                logger.warning("Gmail not authorized")
                return self._request_manual_intervention(
                    state,
                    {
                        "reason": "gmail_not_authorized",
                        "message": auth_status.get(
                            "message", "Gmail authorization required"
                        ),
                        "registration_url": registration_url,
                    },
                )

            # Step 4: Analyze registration process
            registration_info = self._analyze_registration(
                source_name=source_name,
                base_url=base_url,
                registration_url=registration_url,
                auth_info=auth_info,
            )

            # Step 5: Determine strategy
            strategy = self._determine_strategy(registration_info)
            logger.info(f"Using strategy: {strategy}")

            # Step 6: Execute based on strategy
            if strategy == "email_only":
                # Just poll email without form submission
                return asyncio.run(self._poll_email_only(state, registration_info))

            elif strategy == "manual_registration_email_check":
                # Ask user to register, then we'll check email
                return self._request_manual_registration_then_poll(
                    state, registration_info
                )

            else:  # manual_required
                return self._request_manual_intervention(state, registration_info)

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

        return state

    def _check_existing_key(self, base_url: str, source_name: str) -> Optional[str]:
        """Check if we already have a valid key for this source."""
        if not base_url:
            return None

        if self.key_store.key_exists(base_url):
            key = self.key_store.get_key(base_url)
            if key:
                return key

        return None

    def _analyze_registration(
        self,
        source_name: str,
        base_url: str,
        registration_url: str,
        auth_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Analyze the registration process using LLM.

        Returns:
            Registration analysis dict
        """
        prompt = f"""Analyze this API registration information to determine the best approach for automatic API key acquisition.

Data Source: {source_name}
Base URL: {base_url}
Registration URL: {registration_url}
Authentication Info: {auth_info}

Based on this information, determine:

1. **Registration Complexity**: simple|moderate|complex
   - Simple: Just need to check email (key already requested or publicly available)
   - Moderate: Need to fill a form with email, then check email
   - Complex: Requires manual approval, payment, phone verification, etc.

2. **Email Domain**: What domain will send the API key email? (e.g., usda.gov, data.gov)
   Extract from the base URL or registration URL. For example:
   - https://api.usda.gov → usda.gov
   - https://www.data.gov → data.gov

3. **Subject Keywords**: What keywords would appear in the API key email subject?
   Common examples: ["API Key", "API", "Registration", "Access Token", "Welcome", "Credentials"]

4. **Recommended Strategy**:
   - "email_only": Just poll email (key may already be sent)
   - "manual_registration_email_check": Ask user to register manually, then we poll email
   - "manual_required": Too complex, need full manual process

Return JSON:
{{
    "complexity": "simple|moderate|complex",
    "email_domain": "example.gov",
    "subject_keywords": ["API Key", "Registration"],
    "recommended_strategy": "...",
    "confidence": 0.0-1.0,
    "notes": "explanation"
}}
"""

        try:
            messages = [HumanMessage(content=prompt)]
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="APIKeyAgent",
                operation="analyze_registration"
            )
            import json

            content = response.content
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]

            analysis = json.loads(content.strip())
            analysis["registration_url"] = registration_url
            analysis["base_url"] = base_url
            analysis["source_name"] = source_name

            logger.info(f"Registration analysis: {analysis}")
            return analysis

        except Exception as e:
            logger.error(f"Failed to analyze registration: {e}")
            return {
                "complexity": "complex",
                "recommended_strategy": "manual_required",
                "registration_url": registration_url,
                "error": str(e),
            }

    def _determine_strategy(self, registration_info: Dict[str, Any]) -> str:
        """
        Determine the best strategy for acquiring the API key.

        Returns:
            Strategy name
        """
        recommended = registration_info.get("recommended_strategy", "manual_required")
        complexity = registration_info.get("complexity", "complex")
        confidence = registration_info.get("confidence", 0.0)

        # If LLM has high confidence, use its recommendation
        if confidence > 0.7:
            return recommended

        # Otherwise, use conservative approach based on complexity
        if complexity == "simple":
            return "email_only"
        elif complexity == "moderate":
            return "manual_registration_email_check"
        else:
            return "manual_required"

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

    def _request_manual_registration_then_poll(
        self, state: DiscoveryState, registration_info: Dict[str, Any]
    ) -> DiscoveryState:
        """
        Ask user to register manually, then we'll poll email.
        For now, just requests manual input with instructions.
        """
        return self._request_manual_intervention(
            state,
            {
                "reason": "manual_registration_needed",
                "message": f"Please register for an API key at the registration URL using email: {Config.DISCOVERY_EMAIL}",
                "registration_url": registration_info.get("registration_url"),
                "instructions": "After registering, the API key will be automatically retrieved from your email.",
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

        Args:
            source_domain: Domain to filter sender
            subject_keywords: Keywords to search in subject
            max_attempts: Maximum polling attempts
            interval: Seconds between attempts

        Returns:
            API key if found, None otherwise
        """
        if not self.email_service:
            return None

        for attempt in range(max_attempts):
            logger.info(
                f"Checking email for API key (attempt {attempt + 1}/{max_attempts})"
            )

            # Calculate time window (look further back on later attempts)
            since_minutes = max(30, (attempt + 1) * interval // 60 + 5)

            emails = self.email_service.search_for_api_key(
                sender_domain=source_domain,
                subject_keywords=subject_keywords,
                since_minutes=since_minutes,
            )

            if emails:
                for email in emails:
                    # Get full email content
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

                    # Try to extract API key
                    api_key = self.email_service.extract_api_key_from_email(
                        email_content=email_body, llm=self.llm
                    )

                    if api_key:
                        logger.info("API key found in email!")
                        return api_key

            # Wait before next attempt
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
                "registration_email": Config.DISCOVERY_EMAIL,
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
