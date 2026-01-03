"""
LangGraph workflow for data source discovery.

This module defines the main workflow that orchestrates all agents
to discover, evaluate, and configure a new data source based on
a user's description.

Supports:
- State persistence after each step
- Pausing for human input (e.g., API keys)
- Resuming from paused state
"""

import logging
import uuid
from typing import Dict, Any, Literal, Optional
from datetime import datetime

from langgraph.graph import StateGraph, END
from pymongo import MongoClient

from config import Config
from .state import (
    DiscoveryState,
    create_initial_state,
    WorkflowError,
    HumanInputRequest,
    HumanInputType,
)
from .agents import (
    SearchAgent,
    ExaminationAgent,
    SelectionAgent,
    DocumentationAgent,
    TestingAgent,
    ConfigurationAgent,
)
from models.workflow_state import WorkflowState, WorkflowStatus, PauseReason

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

    Features:
    - Persists state after each step to MongoDB
    - Can pause for human input (e.g., API keys)
    - Can resume from where it left off
    """

    # Maps step names to the next step
    STEP_ORDER = [
        "search",
        "examine",
        "select",
        "document",
        "test",
        "configure",
    ]

    def __init__(self, db_client: MongoClient = None):
        """
        Initialize the workflow.

        Args:
            db_client: Optional MongoDB client for dependency injection
        """
        if db_client is None:
            db_client = MongoClient(Config.MONGO_URI)

        self.db_client = db_client
        self.workflow_state_model = WorkflowState(db_client)

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
        if state.get("waiting_for_human_input", False):
            return "stop"
        return "continue"

    def _persist_state(self, state: DiscoveryState, step_name: str) -> None:
        """Persist workflow state after a step completes."""
        workflow_id = state.get("workflow_id")
        if not workflow_id:
            return

        state["current_step"] = step_name

        # Check if this is a pause for human input
        if state.get("waiting_for_human_input"):
            error = state.get("error", {})
            auth_info = state.get("access_documentation", {}).get("authentication", {})

            pause_reason = PauseReason.NEEDS_API_KEY
            if error.get("issue", "").lower().find("oauth") >= 0:
                pause_reason = PauseReason.NEEDS_AUTHENTICATION

            human_input_required = {
                "type": "api_key",
                "field_name": "api_key",
                "description": error.get("details", "API key required"),
                "registration_url": auth_info.get("registration_url"),
            }

            self.workflow_state_model.pause_for_input(
                workflow_id=workflow_id,
                state=dict(state),
                current_step=step_name,
                pause_reason=pause_reason,
                pause_details=error.get("issue", "Human input required"),
                human_input_required=human_input_required,
            )
        elif state.get("interrupted"):
            # Failed with error
            self.workflow_state_model.mark_failed(
                workflow_id=workflow_id,
                state=dict(state),
                error=state.get("error"),
            )
        else:
            # Normal step completion
            self.workflow_state_model.update_state(
                workflow_id=workflow_id,
                state=dict(state),
                current_step=step_name,
                status=WorkflowStatus.RUNNING,
            )

    def _search_node(self, state: DiscoveryState) -> DiscoveryState:
        """Execute the search agent."""
        logger.info("Workflow: Entering search node")
        state = self.search_agent.run(state)
        self._persist_state(state, "search")
        return state

    def _examine_node(self, state: DiscoveryState) -> DiscoveryState:
        """Execute the examination agent."""
        logger.info("Workflow: Entering examination node")
        state = self.examination_agent.run(state)
        self._persist_state(state, "examine")
        return state

    def _select_node(self, state: DiscoveryState) -> DiscoveryState:
        """Execute the selection agent."""
        logger.info("Workflow: Entering selection node")
        state = self.selection_agent.run(state)
        self._persist_state(state, "select")
        return state

    def _document_node(self, state: DiscoveryState) -> DiscoveryState:
        """Execute the documentation agent."""
        logger.info("Workflow: Entering documentation node")
        state = self.documentation_agent.run(state)
        self._persist_state(state, "document")
        return state

    def _test_node(self, state: DiscoveryState) -> DiscoveryState:
        """Execute the testing agent."""
        logger.info("Workflow: Entering testing node")

        # Check if we have human input to apply (e.g., API key)
        human_input = state.get("human_input_received")
        if human_input:
            # Apply the API key to the access documentation
            if "api_key" in human_input:
                access_doc = state.get("access_documentation", {})
                if access_doc:
                    # Store the API key for use in testing
                    access_doc["_provided_api_key"] = human_input["api_key"]
                    state["access_documentation"] = access_doc

        state = self.testing_agent.run(state)

        # Check if test failed due to auth requirement
        error = state.get("error", {})
        if error.get("recoverable") and "authentication" in error.get("issue", "").lower():
            # This needs human input - mark as waiting
            state["waiting_for_human_input"] = True

            # Create the human input request
            auth_info = state.get("access_documentation", {}).get("authentication", {})
            state["human_input_request"] = HumanInputRequest(
                input_type=HumanInputType.API_KEY,
                field_name="api_key",
                description=f"API key required for {state.get('access_documentation', {}).get('source_name', 'data source')}",
                required=True,
                registration_url=auth_info.get("registration_url"),
                additional_info={
                    "auth_type": auth_info.get("auth_type"),
                    "auth_header": auth_info.get("auth_header"),
                }
            ).to_dict()

        self._persist_state(state, "test")
        return state

    def _configure_node(self, state: DiscoveryState) -> DiscoveryState:
        """Execute the configuration agent."""
        logger.info("Workflow: Entering configuration node")
        state = self.configuration_agent.run(state)

        # Mark workflow as completed if successful
        workflow_id = state.get("workflow_id")
        if workflow_id and not state.get("interrupted"):
            self.workflow_state_model.mark_completed(
                workflow_id=workflow_id,
                state=dict(state),
                source_id=state.get("source_id", ""),
                config_id=state.get("config_id", ""),
            )
        else:
            self._persist_state(state, "configure")

        return state

    def run(self, user_description: str, workflow_id: str = None) -> Dict[str, Any]:
        """
        Run the complete discovery workflow.

        Args:
            user_description: User's description of the desired data source
            workflow_id: Optional workflow ID for tracking

        Returns:
            Dictionary containing:
            - success: Whether the workflow completed successfully
            - workflow_id: ID of this workflow execution
            - source_id: ID of the configured source (if successful)
            - config_id: MongoDB ID of the config (if successful)
            - paused: Whether the workflow is paused waiting for input
            - human_input_request: Details about required input (if paused)
            - error: Error details (if failed)
            - state: Full final state
        """
        logger.info(f"Starting data source discovery workflow for: {user_description[:100]}...")

        # Generate workflow ID if not provided
        if workflow_id is None:
            workflow_id = f"wf_{uuid.uuid4().hex[:12]}"

        # Create initial state
        initial_state = create_initial_state(user_description, workflow_id)

        # Create workflow record in MongoDB
        self.workflow_state_model.create(
            workflow_id=workflow_id,
            user_description=user_description,
            initial_state=dict(initial_state),
        )

        # Update status to running
        self.workflow_state_model.update_state(
            workflow_id=workflow_id,
            state=dict(initial_state),
            current_step="started",
            status=WorkflowStatus.RUNNING,
        )

        try:
            # Run the graph
            final_state = self.graph.invoke(initial_state)

            # Check if paused for human input
            if final_state.get("waiting_for_human_input"):
                logger.info(f"Workflow {workflow_id} paused for human input")
                return {
                    "success": False,
                    "workflow_id": workflow_id,
                    "source_id": None,
                    "config_id": None,
                    "paused": True,
                    "human_input_request": final_state.get("human_input_request"),
                    "current_step": final_state.get("current_step"),
                    "error": final_state.get("error"),
                    "state": dict(final_state),
                }

            # Prepare result
            result = {
                "success": not final_state.get("interrupted", False),
                "workflow_id": workflow_id,
                "source_id": final_state.get("source_id"),
                "config_id": final_state.get("config_id"),
                "paused": False,
                "human_input_request": None,
                "error": final_state.get("error"),
                "state": dict(final_state),
            }

            if result["success"]:
                logger.info(f"Workflow {workflow_id} completed successfully. Source ID: {result['source_id']}")
            else:
                error = final_state.get("error", {})
                logger.warning(f"Workflow {workflow_id} interrupted: {error.get('issue', 'Unknown error')}")

            return result

        except Exception as e:
            logger.error(f"Workflow {workflow_id} failed with exception: {e}", exc_info=True)

            error = WorkflowError(
                agent_name="Workflow",
                step="execution",
                issue="Workflow execution failed",
                details=str(e),
                recoverable=False,
            ).to_dict()

            self.workflow_state_model.mark_failed(
                workflow_id=workflow_id,
                state=dict(initial_state),
                error=error,
            )

            return {
                "success": False,
                "workflow_id": workflow_id,
                "source_id": None,
                "config_id": None,
                "paused": False,
                "human_input_request": None,
                "error": error,
                "state": dict(initial_state),
            }

    def resume(self, workflow_id: str, human_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resume a paused workflow with human input.

        Args:
            workflow_id: ID of the paused workflow
            human_input: Input provided by the user (e.g., {"api_key": "abc123"})

        Returns:
            Same as run() - workflow result dictionary
        """
        logger.info(f"Resuming workflow {workflow_id} with human input")

        # Get the saved workflow state
        workflow_doc = self.workflow_state_model.get_by_workflow_id(workflow_id)
        if not workflow_doc:
            return {
                "success": False,
                "workflow_id": workflow_id,
                "error": {"issue": "Workflow not found", "details": f"No workflow with ID {workflow_id}"},
                "paused": False,
            }

        if workflow_doc["status"] != WorkflowStatus.PAUSED.value:
            return {
                "success": False,
                "workflow_id": workflow_id,
                "error": {"issue": "Workflow not paused", "details": f"Workflow status is {workflow_doc['status']}"},
                "paused": False,
            }

        # Get the saved state
        saved_state = workflow_doc["state"]
        current_step = workflow_doc["current_step"]

        # Mark as resumed
        self.workflow_state_model.resume_with_input(workflow_id, human_input)

        # Apply the human input to the state
        saved_state["human_input_received"] = human_input
        saved_state["waiting_for_human_input"] = False
        saved_state["human_input_request"] = None
        saved_state["interrupted"] = False
        saved_state["error"] = None

        # If we have an API key, apply it to the access documentation
        if "api_key" in human_input:
            access_doc = saved_state.get("access_documentation", {})
            if access_doc:
                access_doc["_provided_api_key"] = human_input["api_key"]
                saved_state["access_documentation"] = access_doc

        # Determine where to resume from
        # We need to re-run from the step that was paused
        logger.info(f"Resuming from step: {current_step}")

        try:
            # Create a new graph that starts from the paused step
            resume_state = DiscoveryState(**saved_state)

            # Run the remaining steps manually based on current_step
            step_index = self.STEP_ORDER.index(current_step) if current_step in self.STEP_ORDER else -1

            # Re-run from the current step
            if current_step == "test" or step_index < self.STEP_ORDER.index("test"):
                # Re-run test with the API key
                resume_state = self._test_node(resume_state)

                if resume_state.get("waiting_for_human_input") or resume_state.get("interrupted"):
                    return {
                        "success": False,
                        "workflow_id": workflow_id,
                        "source_id": None,
                        "config_id": None,
                        "paused": resume_state.get("waiting_for_human_input", False),
                        "human_input_request": resume_state.get("human_input_request"),
                        "error": resume_state.get("error"),
                        "state": dict(resume_state),
                    }

            # Continue with configuration if test passed
            if not resume_state.get("interrupted") and not resume_state.get("waiting_for_human_input"):
                resume_state = self._configure_node(resume_state)

            # Prepare final result
            result = {
                "success": not resume_state.get("interrupted", False) and not resume_state.get("waiting_for_human_input", False),
                "workflow_id": workflow_id,
                "source_id": resume_state.get("source_id"),
                "config_id": resume_state.get("config_id"),
                "paused": resume_state.get("waiting_for_human_input", False),
                "human_input_request": resume_state.get("human_input_request"),
                "error": resume_state.get("error"),
                "state": dict(resume_state),
            }

            if result["success"]:
                logger.info(f"Resumed workflow {workflow_id} completed successfully. Source ID: {result['source_id']}")

            return result

        except Exception as e:
            logger.error(f"Resume workflow {workflow_id} failed: {e}", exc_info=True)

            error = WorkflowError(
                agent_name="Workflow",
                step="resume",
                issue="Workflow resume failed",
                details=str(e),
                recoverable=False,
            ).to_dict()

            self.workflow_state_model.mark_failed(
                workflow_id=workflow_id,
                state=saved_state,
                error=error,
            )

            return {
                "success": False,
                "workflow_id": workflow_id,
                "error": error,
                "paused": False,
            }

    def get_workflow_status(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the current status of a workflow.

        Args:
            workflow_id: ID of the workflow

        Returns:
            Workflow status document or None if not found
        """
        return self.workflow_state_model.get_by_workflow_id(workflow_id)

    def get_paused_workflows(self) -> list:
        """
        Get all workflows that are paused waiting for input.

        Returns:
            List of paused workflow documents
        """
        return self.workflow_state_model.get_paused_workflows()

    def cancel_workflow(self, workflow_id: str) -> bool:
        """
        Cancel a workflow.

        Args:
            workflow_id: ID of the workflow to cancel

        Returns:
            True if cancelled successfully
        """
        return self.workflow_state_model.cancel(workflow_id)

    async def run_async(self, user_description: str, workflow_id: str = None) -> Dict[str, Any]:
        """
        Run the workflow asynchronously.

        Args:
            user_description: User's description of the desired data source
            workflow_id: Optional workflow ID for tracking

        Returns:
            Same as run()
        """
        logger.info(f"Starting async data source discovery workflow for: {user_description[:100]}...")

        if workflow_id is None:
            workflow_id = f"wf_{uuid.uuid4().hex[:12]}"

        initial_state = create_initial_state(user_description, workflow_id)

        # Create workflow record
        self.workflow_state_model.create(
            workflow_id=workflow_id,
            user_description=user_description,
            initial_state=dict(initial_state),
        )

        self.workflow_state_model.update_state(
            workflow_id=workflow_id,
            state=dict(initial_state),
            current_step="started",
            status=WorkflowStatus.RUNNING,
        )

        try:
            final_state = await self.graph.ainvoke(initial_state)

            if final_state.get("waiting_for_human_input"):
                return {
                    "success": False,
                    "workflow_id": workflow_id,
                    "source_id": None,
                    "config_id": None,
                    "paused": True,
                    "human_input_request": final_state.get("human_input_request"),
                    "error": final_state.get("error"),
                    "state": dict(final_state),
                }

            result = {
                "success": not final_state.get("interrupted", False),
                "workflow_id": workflow_id,
                "source_id": final_state.get("source_id"),
                "config_id": final_state.get("config_id"),
                "paused": False,
                "error": final_state.get("error"),
                "state": dict(final_state),
            }

            return result

        except Exception as e:
            logger.error(f"Async workflow {workflow_id} failed: {e}", exc_info=True)

            error = WorkflowError(
                agent_name="Workflow",
                step="async_execution",
                issue="Async workflow execution failed",
                details=str(e),
                recoverable=False,
            ).to_dict()

            self.workflow_state_model.mark_failed(
                workflow_id=workflow_id,
                state=dict(initial_state),
                error=error,
            )

            return {
                "success": False,
                "workflow_id": workflow_id,
                "error": error,
                "paused": False,
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
        print(f"Workflow ID: {result['workflow_id']}")
        print(f"Source ID: {result['source_id']}")
        print(f"Config ID: {result['config_id']}")
    elif result.get("paused"):
        print(f"PAUSED - Waiting for human input")
        print(f"Workflow ID: {result['workflow_id']}")
        print(f"Input Required: {result.get('human_input_request')}")
        print(f"\nTo resume, provide the required input via the API:")
        print(f"POST /api/v1/discovery/{result['workflow_id']}/resume")
    else:
        print(f"FAILED")
        error = result.get("error", {})
        print(f"Workflow ID: {result['workflow_id']}")
        print(f"Agent: {error.get('agent_name', 'Unknown')}")
        print(f"Step: {error.get('step', 'Unknown')}")
        print(f"Issue: {error.get('issue', 'Unknown')}")
        print(f"Details: {error.get('details', 'N/A')}")
