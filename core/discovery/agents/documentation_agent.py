"""
Documentation Agent for the data source discovery workflow.

This agent thoroughly examines the technical documentation of the
selected data source to determine the exact access methodology.
"""

import json
import logging
from typing import Dict, Any, List, Optional

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage, HumanMessage

from config import Config
from ..state import (
    DiscoveryState,
    AccessDocumentation,
    AuthenticationDetails,
    EndpointDetails,
    AccessMethod,
    ConnectorType,
    WorkflowError,
)
from ..prompts import DOCUMENTATION_AGENT_SYSTEM, DOCUMENTATION_AGENT_TASK, CONNECTOR_TYPE_MAPPING
from ..tools import fetch_url, parse_openapi_spec, check_api_availability
from ..llm_logger import invoke_llm_with_logging
from ..utils import extract_first_json_object, sanitize_for_format_string
from ..skill_registry import get_skill_registry

logger = logging.getLogger(__name__)


class DocumentationAgent:
    """
    Agent 4: Document the complete access methodology for the selected source.

    Examines API documentation, OpenAPI specs, and developer guides to
    determine exactly how to access the data source.
    """

    def __init__(self):
        self.llm = ChatAnthropic(
            model=Config.DISCOVERY_LLM_MODEL,
            api_key=Config.ANTHROPIC_API_KEY,
            temperature=0.1,
            max_tokens=4096,
        )
        self.skill_registry = get_skill_registry()

    def _build_system_prompt_with_skills(self, base_prompt: str, task_description: str) -> str:
        """Build system prompt with task-specific skills."""
        skills = self.skill_registry.get_skills_for_task('documentation', task_description)

        if skills:
            skills_text = self.skill_registry.format_skills_for_prompt(skills)
            return f"{base_prompt}\n\n{skills_text}"

        return base_prompt

    def _determine_connector_type(
        self, source_name: str, source_url: str, description: str
    ) -> ConnectorType:
        """Determine which existing connector type this source maps to."""
        # Sanitize inputs that may contain curly braces from web content
        safe_source_name = sanitize_for_format_string(source_name)
        safe_source_url = sanitize_for_format_string(source_url)
        safe_description = sanitize_for_format_string(description)

        messages = [
            SystemMessage(content="You are a data source classifier."),
            HumanMessage(content=CONNECTOR_TYPE_MAPPING.format(
                source_name=safe_source_name,
                source_url=safe_source_url,
                source_description=safe_description,
            )),
        ]

        try:
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="DocumentationAgent",
                operation="determine_connector_type"
            )
            content = response.content

            result = extract_first_json_object(content)
            if result:
                connector_type = result.get("connector_type", "discovered")

                try:
                    return ConnectorType(connector_type)
                except ValueError:
                    pass

        except Exception as e:
            logger.warning(f"Failed to determine connector type: {e}")

        return ConnectorType.DISCOVERED

    def _fetch_documentation_content(
        self, doc_url: Optional[str], api_url: Optional[str], base_url: str
    ) -> Dict[str, Any]:
        """Fetch all relevant documentation content."""
        content = {
            "main_docs": "",
            "api_docs": "",
            "openapi_spec": None,
            "links": [],
        }

        # Fetch main documentation
        if doc_url:
            page = fetch_url.invoke(doc_url)
            if "content" in page:
                content["main_docs"] = page["content"][:8000]
                content["links"].extend(page.get("links", []))

        # Fetch API-specific docs
        if api_url and api_url != doc_url:
            page = fetch_url.invoke(api_url)
            if "content" in page:
                content["api_docs"] = page["content"][:8000]
                content["links"].extend(page.get("links", []))

        # Try to find and parse OpenAPI spec
        api_check = check_api_availability.invoke(base_url)
        if api_check.get("has_openapi") and api_check.get("openapi_url"):
            spec = parse_openapi_spec.invoke(api_check["openapi_url"])
            if "error" not in spec:
                content["openapi_spec"] = spec

        return content

    def _safe_access_method(self, access_method_str: str) -> AccessMethod:
        """
        Safely convert a string to an AccessMethod enum.

        Args:
            access_method_str: String representation of access method

        Returns:
            AccessMethod enum value, defaults to UNKNOWN if invalid
        """
        if not access_method_str:
            return AccessMethod.UNKNOWN

        # Normalize the string to lowercase and handle potential variations
        normalized = access_method_str.lower().strip()

        # Try direct mapping
        try:
            return AccessMethod(normalized)
        except ValueError:
            logger.warning(f"Invalid access method '{access_method_str}', defaulting to UNKNOWN")
            return AccessMethod.UNKNOWN

    def _extract_documentation(
        self,
        selected_source: Dict[str, Any],
        user_description: str,
        doc_content: Dict[str, Any],
    ) -> AccessDocumentation:
        """Use LLM to extract structured documentation."""
        candidate = selected_source.get("candidate", {})
        access_method_str = selected_source.get("best_access_method", "api")

        # Build context for LLM
        context_parts = []

        if doc_content.get("main_docs"):
            context_parts.append(f"Main Documentation:\n{doc_content['main_docs']}")

        if doc_content.get("api_docs"):
            context_parts.append(f"API Documentation:\n{doc_content['api_docs']}")

        if doc_content.get("openapi_spec"):
            context_parts.append(f"OpenAPI Specification:\n{json.dumps(doc_content['openapi_spec'], indent=2)}")

        if doc_content.get("links"):
            context_parts.append(f"Relevant Links:\n{json.dumps(doc_content['links'][:30], indent=2)}")

        documentation_context = "\n\n---\n\n".join(context_parts)

        # Sanitize inputs that may contain curly braces from web content
        safe_source_name = sanitize_for_format_string(candidate.get("name", "Unknown"))
        safe_source_url = sanitize_for_format_string(candidate.get("url", ""))
        safe_documentation_url = sanitize_for_format_string(
            selected_source.get("documentation_url") or selected_source.get("api_url") or candidate.get("url", "")
        )
        safe_access_method = sanitize_for_format_string(access_method_str)
        safe_user_description = sanitize_for_format_string(user_description)

        # Get system prompt with task-specific skills
        system_prompt = self._build_system_prompt_with_skills(
            DOCUMENTATION_AGENT_SYSTEM,
            f"Document API for {safe_source_name} with {safe_access_method} access method"
        )

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=DOCUMENTATION_AGENT_TASK.format(
                source_name=safe_source_name,
                source_url=safe_source_url,
                documentation_url=safe_documentation_url,
                access_method=safe_access_method,
                user_description=safe_user_description,
            )),
            HumanMessage(content=f"Here is the documentation content I found:\n\n{documentation_context}"),
        ]

        try:
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="DocumentationAgent",
                operation="extract_documentation"
            )
            content = response.content

            # Extract JSON from response
            doc_data = extract_first_json_object(content)
            if doc_data:

                # Determine connector type
                connector_type = self._determine_connector_type(
                    candidate.get("name", ""),
                    candidate.get("url", ""),
                    candidate.get("description", ""),
                )

                # Build authentication details
                auth_data = doc_data.get("authentication", {})
                authentication = AuthenticationDetails(
                    required=auth_data.get("required", False),
                    auth_type=auth_data.get("auth_type", "none"),
                    auth_header=auth_data.get("auth_header", ""),
                    auth_format=auth_data.get("auth_format", ""),
                    registration_url=auth_data.get("registration_url"),
                    notes=auth_data.get("notes", ""),
                )

                # Build endpoint details
                endpoints = []
                for ep_data in doc_data.get("endpoints", []):
                    endpoints.append(EndpointDetails(
                        url=ep_data.get("url", ""),
                        method=ep_data.get("method", "GET"),
                        parameters=ep_data.get("parameters", {}),
                        required_params=ep_data.get("required_params", []),
                        optional_params=ep_data.get("optional_params", []),
                        response_format=ep_data.get("response_format", "json"),
                        example_request=ep_data.get("example_request", ""),
                        example_response=ep_data.get("example_response", ""),
                    ))

                # Safely convert access_method string to enum
                access_method = self._safe_access_method(
                    doc_data.get("access_method", access_method_str)
                )

                return AccessDocumentation(
                    source_name=candidate.get("name", "Unknown"),
                    base_url=doc_data.get("base_url", candidate.get("url", "")),
                    access_method=access_method,
                    authentication=authentication,
                    endpoints=endpoints,
                    rate_limits=doc_data.get("rate_limits", {}),
                    data_format=doc_data.get("data_format", "json"),
                    update_frequency=doc_data.get("update_frequency", ""),
                    terms_of_use_url=doc_data.get("terms_of_use_url"),
                    notes=doc_data.get("notes", ""),
                    mapped_connector_type=connector_type,
                )
            else:
                # Print the raw LLM response for debugging
                print(f"\n{'='*60}")
                print("DOCUMENTATION AGENT: JSON PARSING FAILED")
                print(f"{'='*60}")
                print(f"LLM Response (first 2000 chars):\n{content[:2000]}")
                print(f"{'='*60}\n")
                raise ValueError(f"No valid JSON object found in LLM response. Response started with: {content[:100]}...")

        except Exception as e:
            logger.error(f"Failed to extract documentation: {e}")
            raise

    def run(self, state: DiscoveryState) -> DiscoveryState:
        """
        Execute the documentation agent.

        Args:
            state: Current workflow state

        Returns:
            Updated state with access documentation
        """
        logger.info("Documentation Agent: Starting documentation analysis")

        try:
            selected_source = state.get("selected_source")
            user_description = state["user_description"]

            if not selected_source:
                state["error"] = WorkflowError(
                    agent_name="DocumentationAgent",
                    step="documentation",
                    issue="No source selected for documentation",
                    details="The selection agent did not provide a selected source",
                    recoverable=False,
                ).to_dict()
                state["interrupted"] = True
                return state

            candidate = selected_source.get("candidate", {})
            logger.info(f"Documentation Agent: Documenting '{candidate.get('name')}'")

            # Fetch documentation content
            doc_content = self._fetch_documentation_content(
                doc_url=selected_source.get("documentation_url"),
                api_url=selected_source.get("api_url"),
                base_url=candidate.get("url", ""),
            )

            # Extract structured documentation
            access_doc = self._extract_documentation(
                selected_source, user_description, doc_content
            )

            state["access_documentation"] = access_doc.to_dict()
            state["documentation_completed"] = True

            logger.info(f"Documentation Agent: Completed. Found {len(access_doc.endpoints)} endpoints")
            logger.info(f"Documentation Agent: Mapped to connector type: {access_doc.mapped_connector_type.value}")

            return state

        except Exception as e:
            logger.error(f"Documentation Agent failed: {e}", exc_info=True)
            state["error"] = WorkflowError(
                agent_name="DocumentationAgent",
                step="documentation",
                issue="Documentation agent encountered an error",
                details=str(e),
                recoverable=False,
            ).to_dict()
            state["interrupted"] = True
            return state
