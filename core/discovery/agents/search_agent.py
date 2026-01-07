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
        description_lower = user_description.lower()
        matching_sources = []

        # Get all active connectors
        all_connectors = self.connector_config.get_all(active_only=True)

        for connector in all_connectors:
            connector_type = connector.get("connector_type", "").lower()
            source_name = connector.get("source_name", "").lower()

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

        # Sort by match score
        matching_sources.sort(key=lambda x: x.get("_match_score", 0), reverse=True)

        return matching_sources

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
            url = result.get("url", "").lower().rstrip("/")
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_results.append(result)

        return unique_results

    def _score_and_rank_results(
        self, results: List[Dict[str, Any]], user_description: str
    ) -> List[DataSourceCandidate]:
        """Score and rank results by relevance."""
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
                    idx = score_data.get("index", 0)
                    if idx < len(results):
                        result = results[idx]
                        candidates.append(DataSourceCandidate(
                            name=result.get("name", result.get("title", "Unknown")),
                            url=result.get("url", ""),
                            description=result.get("description", result.get("content", ""))[:500],
                            source_type=score_data.get("source_type", "other"),
                            relevance_score=float(score_data.get("score", 0.5)),
                        ))

                # Sort by score
                candidates.sort(key=lambda x: x.relevance_score, reverse=True)
                return candidates[:self.max_results]

        except Exception as e:
            logger.warning(f"Failed to score results: {e}")

        # Fallback: return results without scoring
        return [
            DataSourceCandidate(
                name=r.get("name", r.get("title", "Unknown")),
                url=r.get("url", ""),
                description=r.get("description", r.get("content", ""))[:500],
                source_type=r.get("source_type", "other"),
                relevance_score=0.5,
            )
            for r in results[:self.max_results]
        ]

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
            user_description = state["user_description"]

            # Check if user already decided about existing sources
            use_existing = state.get("use_existing_source")

            if use_existing is True:
                # User wants to use an existing source - skip search and complete
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

            if use_existing is False:
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
            logger.info(f"Search Agent: Generated {len(queries)} search queries")

            # Search using Tavily
            for query in queries:
                try:
                    web_results = web_search.invoke(query)
                    if web_results:
                        for result in web_results:
                            result["source"] = "web_search"
                        all_results.extend(web_results)
                        logger.info(f"Search Agent: Found {len(web_results)} web results for '{query}'")
                except Exception as e:
                    logger.warning(f"Web search failed for '{query}': {e}")

            # Search Data.gov
            try:
                gov_results = search_data_gov.invoke(user_description)
                if gov_results:
                    for result in gov_results:
                        result["source"] = "data_gov"
                    all_results.extend(gov_results)
                    logger.info(f"Search Agent: Found {len(gov_results)} Data.gov results")
            except Exception as e:
                logger.warning(f"Data.gov search failed: {e}")

            # Search APIs.guru
            try:
                # Extract key terms for API search
                api_results = search_apis_guru.invoke(user_description.split()[0])
                if api_results:
                    for result in api_results:
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
