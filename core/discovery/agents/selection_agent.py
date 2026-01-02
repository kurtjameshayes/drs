"""
Selection Agent for the data source discovery workflow.

This agent selects the best data source from the examined options
based on priority rules.
"""

import json
import logging
from typing import Dict, Any, List, Optional

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage, HumanMessage

from config import Config
from ..state import (
    DiscoveryState,
    ExaminedSource,
    AccessMethod,
    WorkflowError,
)
from ..prompts import SELECTION_AGENT_SYSTEM, SELECTION_AGENT_TASK

logger = logging.getLogger(__name__)


class SelectionAgent:
    """
    Agent 3: Select the best data source from examined options.

    Selection criteria:
    1. Source must provide the desired data
    2. Prefer API access over web service
    3. Prefer web service over downloads
    4. Among equal access methods, prefer official/government sources
    """

    def __init__(self):
        self.llm = ChatAnthropic(
            model=Config.DISCOVERY_LLM_MODEL,
            api_key=Config.ANTHROPIC_API_KEY,
            temperature=0.1,
        )

    def _filter_viable_sources(
        self, examined_sources: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Filter to only sources that provide the desired data."""
        return [
            source for source in examined_sources
            if source.get("provides_desired_data", False)
        ]

    def _score_source(self, source: Dict[str, Any]) -> float:
        """
        Score a source based on selection criteria.

        Higher score = better source.
        """
        score = 0.0

        # Access method scoring (API > Web Service > Download)
        if source.get("has_api"):
            score += 100
        elif source.get("has_web_service"):
            score += 50
        elif source.get("has_download"):
            score += 25

        # Source type scoring
        candidate = source.get("candidate", {})
        source_type = candidate.get("source_type", "other").lower()

        if source_type == "government":
            score += 20
        elif source_type == "academic":
            score += 15
        elif source_type == "open_source":
            score += 10

        # Relevance score from search
        relevance = candidate.get("relevance_score", 0.5)
        score += relevance * 10

        # Bonus for having documentation
        if source.get("documentation_url"):
            score += 5

        return score

    def _rank_sources(
        self, viable_sources: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Rank sources by score."""
        scored = [(source, self._score_source(source)) for source in viable_sources]
        scored.sort(key=lambda x: x[1], reverse=True)
        return [source for source, score in scored]

    def _select_best(
        self, viable_sources: List[Dict[str, Any]], user_description: str
    ) -> Optional[Dict[str, Any]]:
        """Select the best source using LLM analysis."""
        # First, rank by score
        ranked_sources = self._rank_sources(viable_sources)

        if not ranked_sources:
            return None

        if len(ranked_sources) == 1:
            return ranked_sources[0]

        # Use LLM to make final selection among top candidates
        top_candidates = ranked_sources[:5]  # Consider top 5

        messages = [
            SystemMessage(content=SELECTION_AGENT_SYSTEM),
            HumanMessage(content=SELECTION_AGENT_TASK.format(
                user_description=user_description,
                examined_sources_json=json.dumps(top_candidates, indent=2),
            )),
        ]

        try:
            response = self.llm.invoke(messages)
            content = response.content

            # Extract JSON from response
            start = content.find("{")
            end = content.rfind("}") + 1
            if start != -1 and end > start:
                selection = json.loads(content[start:end])
                selected_index = selection.get("selected_index", 0)

                if 0 <= selected_index < len(top_candidates):
                    logger.info(f"Selection Agent: LLM selected index {selected_index}")
                    logger.info(f"Selection Agent: Reasoning: {selection.get('reasoning', 'N/A')}")
                    return top_candidates[selected_index]

        except Exception as e:
            logger.warning(f"LLM selection failed: {e}")

        # Fallback: return highest scored
        return ranked_sources[0]

    def run(self, state: DiscoveryState) -> DiscoveryState:
        """
        Execute the selection agent.

        Args:
            state: Current workflow state

        Returns:
            Updated state with selected source
        """
        logger.info("Selection Agent: Starting source selection")

        try:
            examined_sources = state.get("examined_sources", [])
            user_description = state["user_description"]

            if not examined_sources:
                state["error"] = WorkflowError(
                    agent_name="SelectionAgent",
                    step="selection",
                    issue="No examined sources available for selection",
                    details="The examination agent did not produce any results",
                    recoverable=False,
                ).to_dict()
                state["interrupted"] = True
                return state

            # Filter to viable sources
            viable_sources = self._filter_viable_sources(examined_sources)
            logger.info(f"Selection Agent: {len(viable_sources)} viable sources from {len(examined_sources)} examined")

            if not viable_sources:
                state["error"] = WorkflowError(
                    agent_name="SelectionAgent",
                    step="selection",
                    issue="No sources provide the desired data",
                    details=f"All {len(examined_sources)} examined sources were filtered out",
                    recoverable=False,
                ).to_dict()
                state["interrupted"] = True
                return state

            # Select the best source
            selected = self._select_best(viable_sources, user_description)

            if not selected:
                state["error"] = WorkflowError(
                    agent_name="SelectionAgent",
                    step="selection",
                    issue="Failed to select a best source",
                    details="Selection algorithm returned no result",
                    recoverable=False,
                ).to_dict()
                state["interrupted"] = True
                return state

            state["selected_source"] = selected
            state["selection_completed"] = True

            candidate = selected.get("candidate", {})
            logger.info(f"Selection Agent: Selected '{candidate.get('name')}' ({selected.get('best_access_method', 'unknown')})")

            return state

        except Exception as e:
            logger.error(f"Selection Agent failed: {e}", exc_info=True)
            state["error"] = WorkflowError(
                agent_name="SelectionAgent",
                step="selection",
                issue="Selection agent encountered an error",
                details=str(e),
                recoverable=False,
            ).to_dict()
            state["interrupted"] = True
            return state
