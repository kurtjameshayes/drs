"""
Examination Agent for the data source discovery workflow.

This agent examines each search result to determine access methods
and data availability.
"""

import json
import logging
from typing import Dict, Any

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage, HumanMessage

from config import Config
from ..state import (
    DiscoveryState,
    DataSourceCandidate,
    ExaminedSource,
    WorkflowError,
)
from ..prompts import EXAMINATION_AGENT_SYSTEM, EXAMINATION_AGENT_TASK
from ..tools import fetch_url, check_api_availability, detect_access_methods, find_documentation_url
from ..llm_logger import invoke_llm_with_logging
from ..utils import extract_first_json_object, sanitize_for_format_string, check_relevance_from_metadata

logger = logging.getLogger(__name__)


class ExaminationAgent:
    """
    Agent 2: Examine each search result to determine access methods.

    Visits each data source to determine:
    - API availability
    - Web service availability
    - Download availability
    - Whether it provides the desired data
    """

    def __init__(self):
        self.llm = ChatAnthropic(
            model=Config.DISCOVERY_LLM_MODEL,
            api_key=Config.ANTHROPIC_API_KEY,
            temperature=0.2,
        )

    def _examine_source(
        self, candidate: DataSourceCandidate, user_description: str
    ) -> ExaminedSource:
        """
        Examine a single data source.

        This method separates two concerns:
        1. RELEVANCE: Determined from metadata (name, description) - never fails
        2. ACCESS METHODS: Determined by fetching the page - may fail

        If page fetch fails, we still return the source as relevant based on
        metadata, but mark access methods as unknown.
        """
        logger.info(f"Examination Agent: Examining {candidate.name} at {candidate.url}")

        # STEP 1: Determine relevance from metadata FIRST (this never fails)
        # Search engines already filtered for the user's query, so be generous
        metadata_relevant, relevance_reason = check_relevance_from_metadata(
            source_name=candidate.name,
            source_url=candidate.url,
            source_description=candidate.description,
            user_description=user_description,
        )
        logger.info(f"Examination Agent: Metadata relevance for {candidate.name}: {metadata_relevant} ({relevance_reason})")

        # STEP 2: Try to fetch the page for access method detection
        page_data = fetch_url.invoke(candidate.url)

        if "error" in page_data:
            # Page fetch failed - but we can still determine relevance from metadata
            logger.warning(f"Failed to fetch {candidate.url}: {page_data['error']} - using metadata-based relevance")
            return ExaminedSource(
                candidate=candidate,
                has_api=False,
                has_web_service=False,
                has_download=False,
                has_contact_required=False,
                provides_desired_data=metadata_relevant,  # Use metadata relevance
                access_notes=f"Page fetch failed ({page_data['error']}), but metadata suggests relevance: {relevance_reason}",
            )

        # STEP 3: Detect access methods from page content
        content = page_data.get("content", "")
        links = page_data.get("links", [])
        access_methods = detect_access_methods(content, links)

        # Check for API availability
        api_check = check_api_availability.invoke(candidate.url)

        # Find documentation URL
        doc_url = find_documentation_url(candidate.url, links)

        # STEP 4: Try LLM analysis for more accurate relevance determination
        # Sanitize all inputs that may contain curly braces from web content
        safe_source_name = sanitize_for_format_string(candidate.name)
        safe_source_url = sanitize_for_format_string(candidate.url)
        safe_source_description = sanitize_for_format_string(candidate.description)
        safe_user_description = sanitize_for_format_string(user_description)

        # Use LLM to analyze whether this source provides the desired data
        messages = [
            SystemMessage(content=EXAMINATION_AGENT_SYSTEM),
            HumanMessage(content=EXAMINATION_AGENT_TASK.format(
                source_name=safe_source_name,
                source_url=safe_source_url,
                source_description=safe_source_description,
                user_description=safe_user_description,
            )),
            HumanMessage(content=f"""Here is the content from the source's main page:

{content[:5000]}

And here are some links found on the page:
{json.dumps(links[:20], indent=2)}

API check results: {json.dumps(api_check)}

Based on this information, provide your analysis as JSON."""),
        ]

        try:
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="ExaminationAgent",
                operation="analyze_source"
            )
            response_content = response.content

            # Extract JSON from response using safe extraction
            analysis = extract_first_json_object(response_content)
            if analysis:
                # LLM analysis succeeded - use its relevance determination
                # but fall back to metadata relevance if LLM says false
                llm_relevant = analysis.get("provides_desired_data", False)
                # Be generous: if metadata says relevant, trust it even if LLM disagrees
                final_relevant = llm_relevant or metadata_relevant

                return ExaminedSource(
                    candidate=candidate,
                    has_api=analysis.get("has_api", access_methods["has_api"]) or api_check.get("has_api", False),
                    has_web_service=analysis.get("has_web_service", access_methods["has_web_service"]),
                    has_download=analysis.get("has_download", access_methods["has_download"]),
                    has_contact_required=analysis.get("has_contact_required", False),
                    provides_desired_data=final_relevant,
                    api_url=analysis.get("api_url") or api_check.get("api_urls", [None])[0] if api_check.get("api_urls") else None,
                    documentation_url=analysis.get("documentation_url") or doc_url,
                    access_notes=analysis.get("access_notes", ""),
                )

        except Exception as e:
            logger.warning(f"LLM analysis failed for {candidate.name}: {e} - using metadata-based relevance")

        # Fallback: use detected methods + metadata relevance
        return ExaminedSource(
            candidate=candidate,
            has_api=access_methods["has_api"] or api_check.get("has_api", False),
            has_web_service=access_methods["has_web_service"],
            has_download=access_methods["has_download"],
            has_contact_required=False,  # Default to false in fallback case
            provides_desired_data=metadata_relevant,  # Use metadata relevance
            api_url=api_check.get("api_urls", [None])[0] if api_check.get("api_urls") else None,
            documentation_url=doc_url,
            access_notes=f"Automated detection with metadata relevance: {relevance_reason}",
        )

    def run(self, state: DiscoveryState) -> DiscoveryState:
        """
        Execute the examination agent.

        Examines sources one at a time, updating the state after each.

        Args:
            state: Current workflow state

        Returns:
            Updated state with examination results
        """
        logger.info("Examination Agent: Starting source examination")

        try:
            search_results = state.get("search_results", [])
            user_description = state["user_description"]
            examined_sources = state.get("examined_sources", [])
            current_index = state.get("current_examination_index", 0)

            if not search_results:
                state["error"] = WorkflowError(
                    agent_name="ExaminationAgent",
                    step="examination",
                    issue="No search results to examine",
                    details="The search agent did not return any results",
                    recoverable=False,
                ).to_dict()
                state["interrupted"] = True
                return state

            # Examine all sources - with per-source exception handling
            for i, result_dict in enumerate(search_results[current_index:], start=current_index):
                logger.info(f"Examination Agent: Examining source {i + 1}/{len(search_results)}")

                try:
                    candidate = DataSourceCandidate.from_dict(result_dict)
                    examined = self._examine_source(candidate, user_description)
                    examined_sources.append(examined.to_dict())
                except Exception as source_error:
                    # Log the error and skip this source, continue with others
                    source_url = result_dict.get("url", "unknown")
                    source_name = result_dict.get("name", "unknown")
                    source_description = result_dict.get("description", "")[:500] if result_dict.get("description") else ""
                    logger.warning(
                        f"Examination Agent: Failed to examine source '{source_name}' ({source_url}): {source_error}. "
                        f"Creating fallback with metadata-based relevance."
                    )
                    # Create a fallback examined source using metadata-based relevance
                    try:
                        # Determine relevance from metadata even when examination fails
                        metadata_relevant, relevance_reason = check_relevance_from_metadata(
                            source_name=source_name,
                            source_url=source_url,
                            source_description=source_description,
                            user_description=user_description,
                        )
                        logger.info(f"Examination Agent: Fallback relevance for {source_name}: {metadata_relevant} ({relevance_reason})")

                        fallback_candidate = DataSourceCandidate(
                            name=source_name,
                            url=source_url,
                            description=source_description,
                            source_type=result_dict.get("source_type", "unknown"),
                            relevance_score=result_dict.get("relevance_score", 0.0),
                        )
                        fallback_examined = ExaminedSource(
                            candidate=fallback_candidate,
                            has_api=False,
                            has_web_service=False,
                            has_download=False,
                            has_contact_required=False,
                            provides_desired_data=metadata_relevant,  # Use metadata-based relevance
                            access_notes=f"Examination failed ({str(source_error)[:100]}), metadata relevance: {relevance_reason}",
                        )
                        examined_sources.append(fallback_examined.to_dict())
                    except Exception as fallback_error:
                        logger.error(f"Examination Agent: Could not create fallback for source: {fallback_error}")

                # Update state
                state["examined_sources"] = examined_sources
                state["current_examination_index"] = i + 1

            state["examination_completed"] = True

            # Check if any sources provide the desired data
            viable_sources = [
                s for s in examined_sources
                if s.get("provides_desired_data", False)
            ]

            if not viable_sources:
                state["error"] = WorkflowError(
                    agent_name="ExaminationAgent",
                    step="examination",
                    issue="No examined sources provide the desired data",
                    details=f"Examined {len(examined_sources)} sources, none matched requirements",
                    recoverable=False,
                ).to_dict()
                state["interrupted"] = True

            logger.info(f"Examination Agent: Completed. {len(viable_sources)} viable sources found")
            return state

        except Exception as e:
            logger.error(f"Examination Agent failed: {e}", exc_info=True)
            state["error"] = WorkflowError(
                agent_name="ExaminationAgent",
                step="examination",
                issue="Examination agent encountered an error",
                details=str(e),
                recoverable=False,
            ).to_dict()
            state["interrupted"] = True
            return state
