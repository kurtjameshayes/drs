"""
LangGraph workflow for data source discovery.

This module defines the main workflow that orchestrates all agents
to discover, evaluate, and configure a new data source based on
a user's description.
"""

import logging
from typing import Dict, Any, Literal, Optional
from datetime import datetime

from langgraph.graph import StateGraph, END
from pymongo import MongoClient

from config import Config
from .state import DiscoveryState, create_initial_state, WorkflowError
from .agents import (
    SearchAgent,
    ExaminationAgent,
    SelectionAgent,
    DocumentationAgent,
    TestingAgent,
    ConfigurationAgent,
)

logger = logging.getLogger(__name__)


class DataSourceDiscoveryWorkflow:
    """
    LangGraph workflow for discovering and configuring data sources.

    This workflow orchestrates 6 agents:
    1. Search Agent - Find potential data sources
    2. Examination Agent - Evaluate access methods for each source
    3. Selection Agent - Select the best source
    4. Documentation Agent - Document the access methodology
    5. Testing Agent - Test the data source access
    6. Configuration Agent - Store configuration in MongoDB
    """

    def __init__(self, db_client: MongoClient = None):
        """
        Initialize the workflow.

        Args:
            db_client: Optional MongoDB client for dependency injection
        """
        self.db_client = db_client

        # Initialize agents
        self.search_agent = SearchAgent()
        self.examination_agent = ExaminationAgent()
        self.selection_agent = SelectionAgent()
        self.documentation_agent = DocumentationAgent()
        self.testing_agent = TestingAgent()
        self.configuration_agent = ConfigurationAgent(db_client)

        # Build the graph
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        """Build the LangGraph state graph."""
        # Create the graph with our state type
        workflow = StateGraph(DiscoveryState)

        # Add nodes for each agent
        workflow.add_node("search", self._search_node)
        workflow.add_node("examine", self._examine_node)
        workflow.add_node("select", self._select_node)
        workflow.add_node("document", self._document_node)
        workflow.add_node("test", self._test_node)
        workflow.add_node("configure", self._configure_node)

        # Set the entry point
        workflow.set_entry_point("search")

        # Add conditional edges based on interruption status
        workflow.add_conditional_edges(
            "search",
            self._check_interrupted,
            {
                "continue": "examine",
                "stop": END,
            }
        )

        workflow.add_conditional_edges(
            "examine",
            self._check_interrupted,
            {
                "continue": "select",
                "stop": END,
            }
        )

        workflow.add_conditional_edges(
            "select",
            self._check_interrupted,
            {
                "continue": "document",
                "stop": END,
            }
        )

        workflow.add_conditional_edges(
            "document",
            self._check_interrupted,
            {
                "continue": "test",
                "stop": END,
            }
        )

        workflow.add_conditional_edges(
            "test",
            self._check_interrupted,
            {
                "continue": "configure",
                "stop": END,
            }
        )

        # Configuration always ends the workflow
        workflow.add_edge("configure", END)

        return workflow.compile()

    def _check_interrupted(self, state: DiscoveryState) -> Literal["continue", "stop"]:
        """Check if the workflow should continue or stop."""
        if state.get("interrupted", False):
            return "stop"
        return "continue"

    def _search_node(self, state: DiscoveryState) -> DiscoveryState:
        """Execute the search agent."""
        logger.info("Workflow: Entering search node")
        return self.search_agent.run(state)

    def _examine_node(self, state: DiscoveryState) -> DiscoveryState:
        """Execute the examination agent."""
        logger.info("Workflow: Entering examination node")
        return self.examination_agent.run(state)

    def _select_node(self, state: DiscoveryState) -> DiscoveryState:
        """Execute the selection agent."""
        logger.info("Workflow: Entering selection node")
        return self.selection_agent.run(state)

    def _document_node(self, state: DiscoveryState) -> DiscoveryState:
        """Execute the documentation agent."""
        logger.info("Workflow: Entering documentation node")
        return self.documentation_agent.run(state)

    def _test_node(self, state: DiscoveryState) -> DiscoveryState:
        """Execute the testing agent."""
        logger.info("Workflow: Entering testing node")
        return self.testing_agent.run(state)

    def _configure_node(self, state: DiscoveryState) -> DiscoveryState:
        """Execute the configuration agent."""
        logger.info("Workflow: Entering configuration node")
        return self.configuration_agent.run(state)

    def run(self, user_description: str) -> Dict[str, Any]:
        """
        Run the complete discovery workflow.

        Args:
            user_description: User's description of the desired data source

        Returns:
            Dictionary containing:
            - success: Whether the workflow completed successfully
            - source_id: ID of the configured source (if successful)
            - config_id: MongoDB ID of the config (if successful)
            - error: Error details (if failed)
            - state: Full final state
        """
        logger.info(f"Starting data source discovery workflow for: {user_description[:100]}...")

        # Create initial state
        initial_state = create_initial_state(user_description)

        try:
            # Run the graph
            final_state = self.graph.invoke(initial_state)

            # Prepare result
            result = {
                "success": not final_state.get("interrupted", False),
                "source_id": final_state.get("source_id"),
                "config_id": final_state.get("config_id"),
                "error": final_state.get("error"),
                "state": dict(final_state),
            }

            if result["success"]:
                logger.info(f"Workflow completed successfully. Source ID: {result['source_id']}")
            else:
                error = final_state.get("error", {})
                logger.warning(f"Workflow interrupted: {error.get('issue', 'Unknown error')}")

            return result

        except Exception as e:
            logger.error(f"Workflow failed with exception: {e}", exc_info=True)
            return {
                "success": False,
                "source_id": None,
                "config_id": None,
                "error": WorkflowError(
                    agent_name="Workflow",
                    step="execution",
                    issue="Workflow execution failed",
                    details=str(e),
                    recoverable=False,
                ).to_dict(),
                "state": dict(initial_state),
            }

    async def run_async(self, user_description: str) -> Dict[str, Any]:
        """
        Run the workflow asynchronously.

        Args:
            user_description: User's description of the desired data source

        Returns:
            Same as run()
        """
        # LangGraph supports async execution
        logger.info(f"Starting async data source discovery workflow for: {user_description[:100]}...")

        initial_state = create_initial_state(user_description)

        try:
            final_state = await self.graph.ainvoke(initial_state)

            result = {
                "success": not final_state.get("interrupted", False),
                "source_id": final_state.get("source_id"),
                "config_id": final_state.get("config_id"),
                "error": final_state.get("error"),
                "state": dict(final_state),
            }

            return result

        except Exception as e:
            logger.error(f"Async workflow failed: {e}", exc_info=True)
            return {
                "success": False,
                "source_id": None,
                "config_id": None,
                "error": WorkflowError(
                    agent_name="Workflow",
                    step="async_execution",
                    issue="Async workflow execution failed",
                    details=str(e),
                    recoverable=False,
                ).to_dict(),
                "state": dict(initial_state),
            }


def discover_data_source(
    user_description: str,
    db_client: MongoClient = None,
) -> Dict[str, Any]:
    """
    Convenience function to run the discovery workflow.

    Args:
        user_description: Description of the desired data source
        db_client: Optional MongoDB client

    Returns:
        Workflow result dictionary
    """
    workflow = DataSourceDiscoveryWorkflow(db_client)
    return workflow.run(user_description)


# Example usage and testing
if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    if len(sys.argv) < 2:
        print("Usage: python workflow.py '<data source description>'")
        print("Example: python workflow.py 'US agricultural commodity prices and production statistics'")
        sys.exit(1)

    description = sys.argv[1]
    print(f"\nDiscovering data source for: {description}\n")

    result = discover_data_source(description)

    print("\n" + "=" * 50)
    print("WORKFLOW RESULT")
    print("=" * 50)

    if result["success"]:
        print(f"SUCCESS!")
        print(f"Source ID: {result['source_id']}")
        print(f"Config ID: {result['config_id']}")
    else:
        print(f"FAILED")
        error = result.get("error", {})
        print(f"Agent: {error.get('agent_name', 'Unknown')}")
        print(f"Step: {error.get('step', 'Unknown')}")
        print(f"Issue: {error.get('issue', 'Unknown')}")
        print(f"Details: {error.get('details', 'N/A')}")
