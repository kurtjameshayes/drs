"""
Search Agent for the data source discovery workflow.

This agent searches for potential data sources that match the user's description
using both web search (Tavily) and known data source registries.

Also checks for existing configured data sources in the database before
running external searches.
"""

import json
import logging
from typing import Dict, Any, List, Optional

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage, HumanMessage
from pymongo import MongoClient

from config import Config
from models.connector_config import ConnectorConfig
from ..state import DiscoveryState, DataSourceCandidate, WorkflowError, HumanInputRequest, HumanInputType
from ..prompts import SEARCH_AGENT_SYSTEM, SEARCH_AGENT_TASK
from ..tools import web_search, search_data_gov, search_apis_guru
from ..llm_logger import invoke_llm_with_logging

logger = logging.getLogger(__name__)


class SearchAgent:
    """
    Agent 1: Search for data sources matching the user's description.

    Uses web search (Tavily) and data source registries (Data.gov, APIs.guru)
    to find potential data sources.

    Also checks for existing configured data sources in the database first.
    """

    # Known connector type keywords for matching
    CONNECTOR_TYPE_KEYWORDS = {
        "usda_nass": ["usda", "nass", "agricultural", "agriculture", "crop", "farm", "quickstats"],
        "census": ["census", "population", "demographic", "american community survey", "acs"],
        "fbi_crime": ["fbi", "crime", "criminal", "ucr", "uniform crime"],
    }

    def __init__(self, db_client: MongoClient = None):
        self.llm = ChatAnthropic(
            model=Config.DISCOVERY_LLM_MODEL,
            api_key=Config.ANTHROPIC_API_KEY,
            temperature=0.3,
        )
        self.max_results = Config.DISCOVERY_MAX_SEARCH_RESULTS

        if db_client is None:
            db_client = MongoClient(Config.MONGO_URI)
        self.connector_config = ConnectorConfig(db_client)

    def _find_existing_sources(self, user_description: str) -> List[Dict[str, Any]]:
        """
        Check if any existing configured sources match the user's description.

        Args:
            user_description: User's description of the desired data source

        Returns:
            List of matching connector configurations
        """
        try:
            # Handle None or empty description
            if not user_description:
                logger.warning("Empty user description provided to _find_existing_sources")
                return []

            description_lower = user_description.lower()
            matching_sources = []

            # Get all active connectors
            all_connectors = self.connector_config.get_all(active_only=True)

            for connector in all_connectors:
                try:
                    # Handle None values gracefully
                    connector_type = connector.get("connector_type") or ""
                    source_name = connector.get("source_name") or ""

                    if not isinstance(connector_type, str):
                        connector_type = str(connector_type)
                    if not isinstance(source_name, str):
                        source_name = str(source_name)

                    connector_type = connector_type.lower()
                    source_name = source_name.lower()

                    # Check if connector type has matching keywords
                    keywords = self.CONNECTOR_TYPE_KEYWORDS.get(connector_type, [])

                    # Check if any keyword is in the user's description
                    match_score = 0
                    for keyword in keywords:
                        if keyword in description_lower:
                            match_score += 1

                    # Also check source name
                    if source_name:
                        for word in source_name.split():
                            if len(word) > 3 and word in description_lower:
                                match_score += 1

                    if match_score > 0:
                        connector["_match_score"] = match_score
                        matching_sources.append(connector)
                except Exception as e:
                    logger.warning(f"Failed to process connector during matching: {e}, connector: {connector}")
                    continue

            # Sort by match score
            matching_sources.sort(key=lambda x: x.get("_match_score", 0), reverse=True)

            return matching_sources
        except Exception as e:
            logger.error(f"Failed to find existing sources: {e}", exc_info=True)
            return []

    def _format_existing_source_options(self, sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format existing sources as selection options."""
        options = []
        for i, source in enumerate(sources):
            options.append({
                "index": i,
                "source_id": source.get("source_id"),
                "source_name": source.get("source_name", "Unknown"),
                "connector_type": source.get("connector_type"),
                "url": source.get("url", ""),
                "description": source.get("discovery_metadata", {}).get("user_description", "")
                              or source.get("notes", "No description available"),
                "is_existing": True,
            })
        return options

    def _generate_search_queries(self, user_description: str) -> List[str]:
        """Generate optimized search queries from the user's description."""
        # Ask LLM to generate search queries
        messages = [
            SystemMessage(content="""You are a search query optimizer. Given a user's description
of the data they need, generate 3-5 effective search queries to find relevant data sources.

Return the queries as a JSON array of strings. Focus on:
- Official data APIs and services
- Government data portals
- Academic and research data repositories
- Well-known data providers

Example output: ["query 1", "query 2", "query 3"]"""),
            HumanMessage(content=f"Generate search queries for: {user_description}")
        ]

        try:
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="SearchAgent",
                operation="generate_search_queries"
            )
            content = response.content

            # Extract JSON array from response
            start = content.find("[")
            end = content.rfind("]") + 1
            if start != -1 and end > start:
                queries = json.loads(content[start:end])
                return queries[:5]
        except Exception as e:
            logger.warning(f"Failed to generate search queries: {e}")

        # Fallback: use the description directly
        return [
            f"{user_description} API",
            f"{user_description} data source",
            f"{user_description} public data",
        ]

    def _deduplicate_results(
        self, results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Remove duplicate results based on URL."""
        seen_urls = set()
        unique_results = []

        for result in results:
            try:
                # Handle None values gracefully
                url = result.get("url") or ""
                if not isinstance(url, str):
                    url = str(url) if url is not None else ""
                url = url.lower().rstrip("/")

                if url and url not in seen_urls:
                    seen_urls.add(url)
                    unique_results.append(result)
            except Exception as e:
                logger.warning(f"Failed to process result for deduplication: {e}, result: {result}")
                # Still include the result even if URL processing failed
                unique_results.append(result)

        return unique_results

    def _score_and_rank_results(
        self, results: List[Dict[str, Any]], user_description: str
    ) -> List[DataSourceCandidate]:
        """Score and rank results by relevance."""

        def _safe_get_string(result: Dict[str, Any], *keys: str, default: str = "") -> str:
            """Safely get a string value from result, handling None and non-string types."""
            for key in keys:
                value = result.get(key)
                if value is not None:
                    if not isinstance(value, str):
                        try:
                            return str(value)
                        except Exception:
                            continue
                    return value
            return default

        def _create_candidate(result: Dict[str, Any], score: float = 0.5, source_type: str = "other") -> Optional[DataSourceCandidate]:
            """Safely create a DataSourceCandidate from a result dict."""
            try:
                name = _safe_get_string(result, "name", "title", default="Unknown")
                url = _safe_get_string(result, "url", default="")
                description = _safe_get_string(result, "description", "content", default="")

                # Safely truncate description
                if description and len(description) > 500:
                    description = description[:500]

                return DataSourceCandidate(
                    name=name,
                    url=url,
                    description=description,
                    source_type=source_type,
                    relevance_score=float(score),
                )
            except Exception as e:
                logger.warning(f"Failed to create candidate from result: {e}, result: {result}")
                return None

        # Ask LLM to score results
        messages = [
            SystemMessage(content="""You are a data source evaluator. Given a list of potential
data sources and a user's data needs, score each source's relevance from 0.0 to 1.0.

Return a JSON array of objects with the format:
[{"index": 0, "score": 0.85, "source_type": "government"}, ...]

source_type should be one of: government, commercial, academic, open_source, other"""),
            HumanMessage(content=f"""User needs: {user_description}

Data sources to evaluate:
{json.dumps(results, indent=2)}

Score each source by relevance to the user's needs.""")
        ]

        try:
            response = invoke_llm_with_logging(
                self.llm,
                messages,
                agent_name="SearchAgent",
                operation="score_and_rank_results"
            )
            content = response.content

            # Extract JSON array
            start = content.find("[")
            end = content.rfind("]") + 1
            if start != -1 and end > start:
                scores = json.loads(content[start:end])

                # Create scored candidates
                candidates = []
                for score_data in scores:
                    try:
                        idx = score_data.get("index", 0)
                        if idx < len(results):
                            result = results[idx]
                            candidate = _create_candidate(
                                result,
                                score=score_data.get("score", 0.5),
                                source_type=score_data.get("source_type", "other")
                            )
                            if candidate:
                                candidates.append(candidate)
                    except Exception as e:
                        logger.warning(f"Failed to process scored result at index {idx}: {e}")
                        continue

                # Sort by score
                candidates.sort(key=lambda x: x.relevance_score, reverse=True)
                return candidates[:self.max_results]

        except Exception as e:
            logger.warning(f"Failed to score results: {e}", exc_info=True)

        # Fallback: return results without scoring
        candidates = []
        for r in results[:self.max_results]:
            candidate = _create_candidate(r)
            if candidate:
                candidates.append(candidate)

        return candidates

    def run(self, state: DiscoveryState) -> DiscoveryState:
        """
        Execute the search agent.

        Args:
            state: Current workflow state

        Returns:
            Updated state with search results
        """
        logger.info("Search Agent: Starting data source search")

        try:
            user_description = state.get("user_description")

            # Validate user_description
            if not user_description or not isinstance(user_description, str) or not user_description.strip():
                logger.error("Invalid or missing user_description in state")
                state["error"] = WorkflowError(
                    agent_name="SearchAgent",
                    step="search",
                    issue="Missing or invalid user description",
                    details="user_description is required and must be a non-empty string",
                    recoverable=False,
                ).to_dict()
                state["interrupted"] = True
                return state

            user_description = user_description.strip()

            # Check if user already decided about existing sources
            # Note: use_existing can be True/False (boolean), 1/0 (integer), or None
            # We use explicit None check to distinguish "not set" from "set to falsy value"
            use_existing = state.get("use_existing_source")

            if use_existing is not None and use_existing:
                # User wants to use an existing source - skip search and complete
                # This handles both True and truthy values like 1
                selected_source_id = state.get("selected_existing_source_id")
                if selected_source_id:
                    logger.info(f"Search Agent: User chose to use existing source: {selected_source_id}")
                    # Store the existing source info in the state for later use
                    existing_sources = state.get("existing_sources_found", [])
                    for source in existing_sources:
                        if source.get("source_id") == selected_source_id:
                            state["_using_existing_source"] = source
                            state["source_id"] = selected_source_id
                            state["config_id"] = source.get("_id")
                            break

                    state["search_completed"] = True
                    state["examination_completed"] = True
                    state["selection_completed"] = True
                    state["documentation_completed"] = True
                    state["testing_completed"] = True
                    state["configuration_completed"] = True
                    return state
                else:
                    # User indicated use_existing but no source_id selected
                    # Try to use the first available existing source
                    existing_sources = state.get("existing_sources_found", [])
                    if existing_sources:
                        first_source = existing_sources[0]
                        selected_source_id = first_source.get("source_id")
                        if selected_source_id:
                            logger.info(f"Search Agent: No source_id specified, using first existing source: {selected_source_id}")
                            state["_using_existing_source"] = first_source
                            state["source_id"] = selected_source_id
                            state["selected_existing_source_id"] = selected_source_id
                            state["config_id"] = first_source.get("_id")

                            state["search_completed"] = True
                            state["examination_completed"] = True
                            state["selection_completed"] = True
                            state["documentation_completed"] = True
                            state["testing_completed"] = True
                            state["configuration_completed"] = True
                            return state
                    # Fall through to search if no existing sources available
                    logger.warning("Search Agent: use_existing_source set but no existing sources found, proceeding with search")

            elif use_existing is not None and not use_existing:
                # User explicitly wants a new source - continue with search
                logger.info("Search Agent: User requested new source discovery, skipping existing source check")
            else:
                # First time - check for existing sources
                existing_sources = self._find_existing_sources(user_description)

                if existing_sources:
                    logger.info(f"Search Agent: Found {len(existing_sources)} existing sources that may match")

                    # Store the existing sources for later reference
                    state["existing_sources_found"] = existing_sources

                    # Format options for user
                    options = self._format_existing_source_options(existing_sources)

                    # Pause and ask user if they want to use an existing source
                    state["waiting_for_human_input"] = True
                    state["pause_reason"] = "existing_source_found"
                    state["human_input_request"] = HumanInputRequest(
                        input_type=HumanInputType.EXISTING_SOURCE_FOUND,
                        field_name="use_existing_source",
                        description=f"Found {len(existing_sources)} existing data source(s) that may match your request. "
                                   f"Would you like to use one of these, or discover a new source?",
                        required=True,
                        options=options,
                        recommended_option=0 if options else None,
                        additional_info={
                            "existing_source_count": len(existing_sources),
                            "user_description": user_description,
                        },
                    ).to_dict()

                    logger.info(f"Search Agent: Pausing to ask about existing sources")
                    return state

            all_results = []

            # Generate search queries
            queries = self._generate_search_queries(user_description)
            if not queries:
                logger.warning("No search queries generated, using fallback")
                queries = [user_description]

            logger.info(f"Search Agent: Generated {len(queries)} search queries")

            # Search using Tavily
            for query in queries:
                try:
                    if not query or not isinstance(query, str):
                        logger.warning(f"Invalid query skipped: {query}")
                        continue

                    web_results = web_search.invoke(query)
                    if web_results and isinstance(web_results, list):
                        for result in web_results:
                            if isinstance(result, dict):
                                result["source"] = "web_search"
                        all_results.extend(web_results)
                        logger.info(f"Search Agent: Found {len(web_results)} web results for '{query}'")
                except Exception as e:
                    logger.warning(f"Web search failed for '{query}': {e}")

            # Search Data.gov
            try:
                gov_results = search_data_gov.invoke(user_description)
                if gov_results and isinstance(gov_results, list):
                    for result in gov_results:
                        if isinstance(result, dict):
                            result["source"] = "data_gov"
                    all_results.extend(gov_results)
                    logger.info(f"Search Agent: Found {len(gov_results)} Data.gov results")
            except Exception as e:
                logger.warning(f"Data.gov search failed: {e}")

            # Search APIs.guru
            try:
                # Extract key terms for API search - handle empty descriptions
                search_term = user_description
                if user_description:
                    words = user_description.split()
                    if words:
                        search_term = words[0]

                if search_term:
                    api_results = search_apis_guru.invoke(search_term)
                    if api_results and isinstance(api_results, list):
                        for result in api_results:
                            if isinstance(result, dict):
                                result["source"] = "apis_guru"
                        all_results.extend(api_results)
                        logger.info(f"Search Agent: Found {len(api_results)} APIs.guru results")
            except Exception as e:
                logger.warning(f"APIs.guru search failed: {e}")

            # Deduplicate and score results
            unique_results = self._deduplicate_results(all_results)
            logger.info(f"Search Agent: {len(unique_results)} unique results after deduplication")

            candidates = self._score_and_rank_results(unique_results, user_description)
            logger.info(f"Search Agent: Returning {len(candidates)} scored candidates")

            # Update state
            state["search_results"] = [c.to_dict() for c in candidates]
            state["search_completed"] = True

            if not candidates:
                state["error"] = WorkflowError(
                    agent_name="SearchAgent",
                    step="search",
                    issue="No data sources found matching the description",
                    details=f"Searched with queries: {queries}",
                    recoverable=False,
                ).to_dict()
                state["interrupted"] = True

            return state

        except Exception as e:
            logger.error(f"Search Agent failed: {e}", exc_info=True)
            state["error"] = WorkflowError(
                agent_name="SearchAgent",
                step="search",
                issue="Search agent encountered an error",
                details=str(e),
                recoverable=False,
            ).to_dict()
            state["interrupted"] = True
            return state
