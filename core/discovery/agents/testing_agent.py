"""
Testing Agent for the data source discovery workflow.

This agent tests access to the data source using the documented
methodology and verifies that the data matches expectations.
"""

import json
import logging
import time
from typing import Dict, Any, Optional, Tuple

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage, HumanMessage

from config import Config
from ..state import (
    DiscoveryState,
    AccessDocumentation,
    TestResults,
    AccessMethod,
    WorkflowError,
    HumanInputRequest,
    HumanInputType,
)
from ..prompts import TESTING_AGENT_SYSTEM, TESTING_AGENT_TASK
from ..tools import make_http_request
from ..llm_logger import invoke_llm_with_logging
from ..skill_registry import get_skill_registry

logger = logging.getLogger(__name__)


class TestingAgent:
    """
    Agent 5: Test access to the data source.

    Implements a test query using the documented methodology and
    verifies that the response matches expectations.
    """

    def __init__(self):
        self.llm = ChatAnthropic(
            model=Config.DISCOVERY_LLM_MODEL,
            api_key=Config.ANTHROPIC_API_KEY,
            temperature=0.1,
        )
        self.max_retries = Config.DISCOVERY_TEST_RETRIES
        self.backoff_factor = Config.DISCOVERY_TEST_BACKOFF
        self.skill_registry = get_skill_registry()

    def _build_system_prompt_with_skills(self, base_prompt: str, task_description: str) -> str:
        """Build system prompt with task-specific skills."""
        skills = self.skill_registry.get_skills_for_task('testing', task_description)

        if skills:
            skills_text = self.skill_registry.format_skills_for_prompt(skills)
            return f"{base_prompt}\n\n{skills_text}"

        return base_prompt

    def _build_test_request(
        self, access_doc: Dict[str, Any], user_description: str
    ) -> Tuple[Dict[str, Any], bool]:
        """
        Build a test request based on the documentation.

        Returns:
            Tuple of (request_dict, has_real_credentials)
        """
        endpoints = access_doc.get("endpoints", [])
        auth = access_doc.get("authentication", {})
        base_url = access_doc.get("base_url", "")
        mapped_connector_type = access_doc.get("mapped_connector_type", "")

        # Check if we have a provided API key from human input
        provided_api_key = access_doc.get("_provided_api_key")

        # Find a suitable endpoint for testing
        test_endpoint = None
        for ep in endpoints:
            # Prefer GET endpoints with minimal required params
            if ep.get("method", "GET").upper() == "GET":
                if not test_endpoint or len(ep.get("required_params", [])) < len(test_endpoint.get("required_params", [])):
                    test_endpoint = ep

        if not test_endpoint and endpoints:
            test_endpoint = endpoints[0]

        if not test_endpoint:
            # No endpoints defined, try the base URL
            # Handle USDA NASS connector - requires /api_GET endpoint
            test_url = base_url
            test_params = {}

            if mapped_connector_type == "usda_nass" or "quickstats.nass.usda.gov" in base_url:
                if not test_url.endswith("/api_GET"):
                    if test_url.endswith("/"):
                        test_url = test_url + "api_GET"
                    else:
                        test_url = test_url + "/api_GET"
                # Add minimal test parameters for NASS
                test_params = {
                    "commodity_desc": "CORN",
                    "year": "2020",
                    "state_alpha": "IA",
                    "statisticcat_desc": "PRODUCTION",
                }
                # Add API key if provided
                if provided_api_key:
                    test_params["key"] = provided_api_key
                    logger.info(f"Testing Agent: Using NASS-specific endpoint (no endpoints defined): {test_url}")
                    return {
                        "url": test_url,
                        "method": "GET",
                        "headers": {},
                        "params": test_params,
                    }, True

                logger.info(f"Testing Agent: Using NASS-specific endpoint (no endpoints defined): {test_url}")

            return {
                "url": test_url,
                "method": "GET",
                "headers": {},
                "params": test_params,
            }, False

        # Build params - use example values if available
        params = {}
        ep_params = test_endpoint.get("parameters", {})
        for param_name in test_endpoint.get("required_params", []):
            # Skip the 'key' param for NASS API - we'll add it separately with actual key
            if param_name.lower() == "key" and provided_api_key:
                continue
            if param_name in ep_params:
                params[param_name] = ep_params[param_name]
            else:
                params[param_name] = "test"

        # Build headers
        headers = {}
        has_real_credentials = False

        if auth.get("required") and provided_api_key:
            auth_type = auth.get("auth_type", "")
            auth_header = auth.get("auth_header", "Authorization")
            auth_format = auth.get("auth_format", "{key}")
            auth_notes = auth.get("notes", "")

            has_real_credentials = True

            # Check if API key should be passed as query parameter
            # Look for hints in notes or auth_header
            is_query_param = (
                "query parameter" in auth_notes.lower() or
                "query param" in auth_notes.lower() or
                auth_header.lower() in ("key", "api_key", "apikey") or
                (auth_type == "api_key" and auth_header.lower() not in ("authorization", "x-api-key"))
            )

            if is_query_param:
                # Add API key as query parameter
                param_name = auth_header if auth_header else "key"
                params[param_name] = provided_api_key
                logger.info(f"Testing Agent: Using provided API key as query parameter '{param_name}'")
            elif auth_type == "bearer" or "bearer" in auth_format.lower():
                headers[auth_header] = f"Bearer {provided_api_key}"
                logger.info(f"Testing Agent: Using provided API key as Bearer token in header")
            elif auth_type == "api_key":
                formatted_key = auth_format.replace("{key}", provided_api_key).replace("{token}", provided_api_key)
                headers[auth_header] = formatted_key
                logger.info(f"Testing Agent: Using provided API key in header '{auth_header}'")
            else:
                # Default: use the key directly in the header
                headers[auth_header] = provided_api_key
                logger.info(f"Testing Agent: Using provided API key in header '{auth_header}'")

        elif auth.get("required"):
            # No API key provided, use placeholder
            auth_type = auth.get("auth_type", "")
            auth_header = auth.get("auth_header", "Authorization")
            auth_format = auth.get("auth_format", "{key}")

            if auth_type == "api_key":
                headers[auth_header] = auth_format.replace("{key}", "PLACEHOLDER_API_KEY")
            elif auth_type == "bearer":
                headers[auth_header] = f"Bearer PLACEHOLDER_TOKEN"

        # Determine the test URL
        test_url = test_endpoint.get("url", base_url) if test_endpoint else base_url

        # Handle USDA NASS connector - requires /api_GET endpoint
        if mapped_connector_type == "usda_nass" or "quickstats.nass.usda.gov" in base_url:
            # Ensure we're using the correct endpoint
            if not test_url.endswith("/api_GET"):
                if test_url.endswith("/"):
                    test_url = test_url + "api_GET"
                else:
                    test_url = test_url + "/api_GET"

            # NASS requires at least some query parameters for a valid request
            # Add minimal test parameters if not already present
            if "commodity_desc" not in params:
                params["commodity_desc"] = "CORN"
            if "year" not in params:
                params["year"] = "2020"
            if "state_alpha" not in params:
                params["state_alpha"] = "IA"
            if "statisticcat_desc" not in params:
                params["statisticcat_desc"] = "PRODUCTION"

            logger.info(f"Testing Agent: Using NASS-specific endpoint: {test_url}")

        return {
            "url": test_url,
            "method": test_endpoint.get("method", "GET") if test_endpoint else "GET",
            "headers": headers,
            "params": params,
        }, has_real_credentials

    def _execute_test(
        self, request: Dict[str, Any]
    ) -> Tuple[bool, Dict[str, Any]]:
        """Execute the test request with retry logic."""
        return make_http_request(
            url=request["url"],
            method=request["method"],
            headers=request.get("headers"),
            params=request.get("params"),
            max_retries=self.max_retries,
            backoff_factor=self.backoff_factor,
        )

    def _validate_response(
        self, response_data: Dict[str, Any], user_description: str, access_doc: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """Validate that the response contains relevant data."""
        # Check for common error indicators
        data = response_data.get("data", {})

        if isinstance(data, dict):
            # Check for error fields
            if data.get("error") or data.get("errors"):
                error_msg = data.get("error") or data.get("errors")
                return False, f"API returned error: {error_msg}"

        # Get system prompt with task-specific skills
        system_prompt = self._build_system_prompt_with_skills(
            """You are a data validation specialist. Analyze the API response
to determine if it contains data relevant to the user's needs.

Return a JSON object:
{
    "is_relevant": true/false,
    "confidence": 0.0-1.0,
    "reasoning": "explanation"
}""",
            f"Validate API response data for {user_description}"
        )

        # Use LLM to validate data relevance
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"""User is looking for: {user_description}

API Response (sample):
{json.dumps(data, indent=2)[:2000]}

Does this response contain or indicate access to relevant data?"""),
        ]

        try:
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="TestingAgent",
                operation="validate_response"
            )
            content = response.content

            start = content.find("{")
            end = content.rfind("}") + 1
            if start != -1 and end > start:
                validation = json.loads(content[start:end])
                is_relevant = validation.get("is_relevant", False)
                reasoning = validation.get("reasoning", "")

                return is_relevant, reasoning

        except Exception as e:
            logger.warning(f"Validation LLM call failed: {e}")

        # Fallback: assume relevant if we got any data
        return bool(data), "Could not determine relevance automatically"

    def run(self, state: DiscoveryState) -> DiscoveryState:
        """
        Execute the testing agent.

        Args:
            state: Current workflow state

        Returns:
            Updated state with test results
        """
        logger.info("Testing Agent: Starting access test")

        try:
            access_doc = state.get("access_documentation")
            user_description = state["user_description"]

            if not access_doc:
                state["error"] = WorkflowError(
                    agent_name="TestingAgent",
                    step="testing",
                    issue="No access documentation available for testing",
                    details="The documentation agent did not provide access documentation",
                    recoverable=False,
                ).to_dict()
                state["interrupted"] = True
                return state

            # Build test request
            request, has_real_credentials = self._build_test_request(access_doc, user_description)
            logger.info(f"Testing Agent: Testing {request['method']} {request['url']}")

            # Check if authentication is required but we don't have credentials
            auth = access_doc.get("authentication", {})
            if auth.get("required") and not has_real_credentials:
                logger.warning(
                    "Testing Agent: API requires authentication but no credentials provided. "
                    "Will attempt test anyway, then invoke api_key_agent if authentication fails."
                )
                # Still try the request - some APIs return useful info even without auth

            # Execute test
            success, response = self._execute_test(request)

            if not success:
                test_results = TestResults(
                    success=False,
                    error_message=response.get("error", "Unknown error"),
                    attempts=response.get("attempts", 1),
                )

                state["test_results"] = test_results.to_dict()
                state["test_passed"] = False
                state["testing_completed"] = True

                # Check if this might be an auth issue
                if auth.get("required"):
                    state["error"] = WorkflowError(
                        agent_name="TestingAgent",
                        step="testing",
                        issue="Test failed - authentication may be required",
                        details=f"Error: {response.get('error')}. API requires {auth.get('auth_type', 'authentication')}. Registration URL: {auth.get('registration_url', 'N/A')}",
                        recoverable=True,
                    ).to_dict()
                else:
                    state["error"] = WorkflowError(
                        agent_name="TestingAgent",
                        step="testing",
                        issue="Test request failed",
                        details=str(response.get("error", "Unknown error")),
                        recoverable=False,
                    ).to_dict()

                state["interrupted"] = True
                return state

            # Check response status
            status_code = response.get("status_code", 0)

            if status_code >= 400:
                # Handle auth errors specifically
                if status_code in (401, 403):
                    source_name = access_doc.get("source_name", "the data source")
                    auth_type = auth.get("auth_type", "unknown")
                    registration_url = auth.get("registration_url")

                    test_results = TestResults(
                        success=False,
                        status_code=status_code,
                        response_time_ms=response.get("response_time_ms"),
                        data_received=False,
                        data_matches_description=False,
                        error_message=f"Authentication required (HTTP {status_code})",
                        attempts=response.get("attempts", 1),
                    )

                    state["test_results"] = test_results.to_dict()
                    state["test_passed"] = False
                    state["testing_completed"] = True

                    # Check if we already had a provided API key that failed
                    provided_api_key = access_doc.get("_provided_api_key")
                    retry_count = state.get("_api_key_retry_count", 0)

                    if provided_api_key:
                        # API key was provided but still failed - key is likely invalid
                        retry_count += 1
                        state["_api_key_retry_count"] = retry_count

                        if retry_count >= 2:
                            # Too many failed attempts - mark as non-recoverable
                            state["error"] = WorkflowError(
                                agent_name="TestingAgent",
                                step="testing",
                                issue=f"API key authentication failed after {retry_count} attempts",
                                details=f"The provided API key was rejected by the server (HTTP {status_code}). "
                                       f"Please verify: 1) The API key is correct, 2) The key has not expired, "
                                       f"3) The key has the required permissions. Registration: {registration_url or 'Check documentation'}",
                                recoverable=False,
                            ).to_dict()
                            state["interrupted"] = True
                            logger.warning(f"Testing Agent: API key failed {retry_count} times. Marking as non-recoverable.")
                            return state

                        # First retry - ask for API key again with better message
                        state["error"] = WorkflowError(
                            agent_name="TestingAgent",
                            step="testing",
                            issue=f"Provided API key was rejected (HTTP {status_code})",
                            details=f"The API key you provided was not accepted. Please verify the key is correct and try again. "
                                   f"Registration: {registration_url or 'Check documentation'}",
                            recoverable=True,
                        ).to_dict()
                        state["interrupted"] = True

                        # Clear the old invalid key
                        access_doc["_provided_api_key"] = None
                        state["access_documentation"] = access_doc

                        state["waiting_for_human_input"] = True
                        state["pause_reason"] = "needs_api_key"
                        state["human_input_request"] = HumanInputRequest(
                            input_type=HumanInputType.API_KEY,
                            field_name="api_key",
                            description=f"The API key provided was invalid. Please provide a valid API key for {source_name}",
                            required=True,
                            registration_url=registration_url,
                            additional_info={
                                "auth_type": auth_type,
                                "auth_header": auth.get("auth_header"),
                                "previous_key_failed": True,
                                "retry_count": retry_count,
                            },
                        ).to_dict()

                        logger.info(f"Testing Agent: Provided API key was rejected ({status_code}). Asking for new key (attempt {retry_count + 1}).")
                        return state

                    # No API key was provided yet - trigger automatic key acquisition
                    state["error"] = WorkflowError(
                        agent_name="TestingAgent",
                        step="testing",
                        issue=f"Authentication required (HTTP {status_code})",
                        details=f"The API requires authentication. Type: {auth_type}. Registration: {registration_url or 'Check documentation'}",
                        recoverable=True,
                    ).to_dict()
                    state["interrupted"] = True

                    # Don't set waiting_for_human_input - let workflow route to acquire_api_key step
                    logger.info(f"Testing Agent: Auth failed ({status_code}). Routing to API key acquisition.")
                    return state

                test_results = TestResults(
                    success=False,
                    status_code=status_code,
                    response_time_ms=response.get("response_time_ms"),
                    error_message=f"HTTP error: {status_code}",
                    attempts=response.get("attempts", 1),
                )

                state["test_results"] = test_results.to_dict()
                state["test_passed"] = False
                state["testing_completed"] = True
                state["error"] = WorkflowError(
                    agent_name="TestingAgent",
                    step="testing",
                    issue=f"API returned error status {status_code}",
                    details=json.dumps(response.get("data", {}))[:500],
                    recoverable=False,
                ).to_dict()
                state["interrupted"] = True
                return state

            # Validate the response data
            is_relevant, validation_reason = self._validate_response(
                response, user_description, access_doc
            )

            # Create sample data (truncated)
            sample_data = response.get("data", {})
            if isinstance(sample_data, dict):
                sample_data = {k: v for k, v in list(sample_data.items())[:5]}
            elif isinstance(sample_data, list):
                sample_data = sample_data[:3]

            test_results = TestResults(
                success=True,
                status_code=status_code,
                response_time_ms=response.get("response_time_ms"),
                data_received=True,
                data_matches_description=is_relevant,
                sample_data=sample_data,
                error_message="" if is_relevant else validation_reason,
                attempts=response.get("attempts", 1),
            )

            state["test_results"] = test_results.to_dict()
            state["test_passed"] = True
            state["testing_completed"] = True

            logger.info(f"Testing Agent: Test passed. Status: {status_code}, Time: {response.get('response_time_ms')}ms")

            return state

        except Exception as e:
            logger.error(f"Testing Agent failed: {e}", exc_info=True)
            state["error"] = WorkflowError(
                agent_name="TestingAgent",
                step="testing",
                issue="Testing agent encountered an error",
                details=str(e),
                recoverable=False,
            ).to_dict()
            state["interrupted"] = True
            return state
