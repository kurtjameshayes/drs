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
    APIKeyAgent,
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

    def __init__(self, db_client: MongoClient = None, require_selection_confirmation: bool = True):
        """
        Initialize the workflow.

        Args:
            db_client: Optional MongoDB client for dependency injection
            require_selection_confirmation: If True, pause for user confirmation
                                            when multiple data sources are found
        """
        if db_client is None:
            db_client = MongoClient(Config.MONGO_URI)

        self.db_client = db_client
        self.workflow_state_model = WorkflowState(db_client)
        self.require_selection_confirmation = require_selection_confirmation

        # Initialize agents
        self.search_agent = SearchAgent(db_client)
        self.examination_agent = ExaminationAgent()
        self.selection_agent = SelectionAgent(require_confirmation=require_selection_confirmation)
        self.documentation_agent = DocumentationAgent()
        self.testing_agent = TestingAgent()
        self.api_key_agent = APIKeyAgent(db_client)
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
        workflow.add_node("acquire_api_key", self._acquire_api_key_node)
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
            self._check_after_test,
            {
                "needs_key": "acquire_api_key",
                "configure": "configure",
                "stop": END,
            }
        )

        workflow.add_conditional_edges(
            "acquire_api_key",
            self._check_after_key_acquisition,
            {
                "retry_test": "test",
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
        # If using an existing source, skip to end
        if state.get("_using_existing_source"):
            return "stop"
        return "continue"

    def _determine_pause_reason(self, state: DiscoveryState) -> PauseReason:
        """Determine the appropriate PauseReason from state."""
        # Check if pause_reason is explicitly set in state
        explicit_reason = state.get("pause_reason")
        if explicit_reason:
            try:
                return PauseReason(explicit_reason)
            except ValueError:
                pass

        # Infer from human_input_request
        human_input_request = state.get("human_input_request", {})
        input_type = human_input_request.get("input_type", "")

        if input_type == "selection_confirmation":
            return PauseReason.NEEDS_SELECTION_CONFIRMATION
        elif input_type == "api_key":
            return PauseReason.NEEDS_API_KEY
        elif input_type in ("oauth_token", "username_password"):
            return PauseReason.NEEDS_AUTHENTICATION
        elif input_type == "confirmation":
            return PauseReason.NEEDS_CONFIRMATION
        elif input_type == "selection":
            return PauseReason.NEEDS_SELECTION
        elif input_type == "error_guidance":
            return PauseReason.ERROR_RECOVERABLE
        elif input_type == "missing_info":
            return PauseReason.MISSING_INFORMATION
        elif input_type == "guidance":
            return PauseReason.NEEDS_GUIDANCE
        elif input_type == "existing_source_found":
            return PauseReason.EXISTING_SOURCE_FOUND

        # Fallback: check error for clues
        error = state.get("error", {})
        if error.get("issue", "").lower().find("oauth") >= 0:
            return PauseReason.NEEDS_AUTHENTICATION
        elif error.get("recoverable"):
            return PauseReason.ERROR_RECOVERABLE

        return PauseReason.OTHER

    def _get_pause_details(self, state: DiscoveryState, pause_reason: PauseReason) -> str:
        """Generate human-readable pause details."""
        human_input_request = state.get("human_input_request", {})

        if human_input_request.get("description"):
            return human_input_request["description"]

        error = state.get("error", {})
        if error.get("issue"):
            return error["issue"]

        # Default messages based on pause reason
        defaults = {
            PauseReason.NEEDS_API_KEY: "API key required to access the data source",
            PauseReason.NEEDS_AUTHENTICATION: "Authentication credentials required",
            PauseReason.NEEDS_CONFIRMATION: "User confirmation required",
            PauseReason.NEEDS_SELECTION: "User selection required",
            PauseReason.NEEDS_SELECTION_CONFIRMATION: "Please confirm or modify the data source selection",
            PauseReason.NEEDS_GUIDANCE: "User guidance required to proceed",
            PauseReason.ERROR_RECOVERABLE: "A recoverable error occurred - user input may help",
            PauseReason.ERROR_REQUIRES_INPUT: "An error occurred that requires user input to resolve",
            PauseReason.RATE_LIMITED: "Rate limited - waiting before retry",
            PauseReason.MISSING_INFORMATION: "Additional information required",
            PauseReason.EXISTING_SOURCE_FOUND: "Existing data source found - choose to use it or discover a new one",
            PauseReason.OTHER: "Human input required",
        }
        return defaults.get(pause_reason, "Human input required")

    def _persist_state(self, state: DiscoveryState, step_name: str) -> None:
        """Persist workflow state after a step completes."""
        workflow_id = state.get("workflow_id")
        if not workflow_id:
            return

        state["current_step"] = step_name

        # Check if this is a pause for human input
        if state.get("waiting_for_human_input"):
            pause_reason = self._determine_pause_reason(state)
            pause_details = self._get_pause_details(state, pause_reason)

            # Use the human_input_request from state if available
            human_input_required = state.get("human_input_request")
            if not human_input_required:
                # Fallback for legacy behavior (testing agent API key flow)
                error = state.get("error", {})
                auth_info = state.get("access_documentation", {}).get("authentication", {})
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
                pause_details=pause_details,
                human_input_required=human_input_required,
            )
        elif state.get("interrupted") and not state.get("waiting_for_human_input"):
            # Failed with non-recoverable error
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

    def _acquire_api_key_node(self, state: DiscoveryState) -> DiscoveryState:
        """Execute the API key acquisition agent."""
        logger.info("Workflow: Entering API key acquisition node")
        state["current_step"] = "acquire_api_key"
        state = self.api_key_agent.run(state)
        self._persist_state(state, "acquire_api_key")
        return state

    def _check_after_test(self, state: DiscoveryState) -> Literal["needs_key", "configure", "stop"]:
        """Check what to do after testing."""
        # If test succeeded, go to configure
        test_results = state.get("test_results", {})
        if test_results.get("success"):
            return "configure"

        # If interrupted with auth error and haven't tried key acquisition
        if state.get("interrupted"):
            error = state.get("error", {})
            error_issue = error.get("issue", "").lower()

            # Check if it's an auth error
            if any(
                keyword in error_issue
                for keyword in ["authentication", "authorization", "401", "403", "api key"]
            ):
                # Only attempt key acquisition once
                if not state.get("api_key_acquisition_attempted"):
                    logger.info("Auth error detected, attempting API key acquisition")
                    return "needs_key"

        # Check if waiting for human input (may be set by testing agent)
        if state.get("waiting_for_human_input"):
            return "stop"

        # Otherwise stop (will pause for human input or end with error)
        return "stop"

    def _check_after_key_acquisition(
        self, state: DiscoveryState
    ) -> Literal["retry_test", "stop"]:
        """Check what to do after key acquisition attempt."""
        if state.get("api_key_acquired"):
            # Successfully acquired key, retry test
            logger.info("API key acquired, retrying test")
            return "retry_test"
        else:
            # Failed to acquire, stop (will pause for manual input)
            logger.info("API key acquisition failed, pausing for manual input")
            return "stop"

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

            # Check if using an existing source (immediate success)
            if final_state.get("_using_existing_source"):
                existing_source = final_state.get("_using_existing_source", {})
                source_id = final_state.get("source_id") or existing_source.get("source_id")
                config_id = final_state.get("config_id") or existing_source.get("_id")

                # Mark as completed in database
                self.workflow_state_model.mark_completed(
                    workflow_id=workflow_id,
                    state=dict(final_state),
                    source_id=source_id,
                    config_id=config_id,
                )

                logger.info(f"Workflow {workflow_id} completed using existing source: {source_id}")
                return {
                    "success": True,
                    "workflow_id": workflow_id,
                    "source_id": source_id,
                    "config_id": config_id,
                    "paused": False,
                    "human_input_request": None,
                    "error": None,
                    "state": dict(final_state),
                    "used_existing_source": True,
                }

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

    def _apply_human_input(self, state: Dict[str, Any], human_input: Dict[str, Any], pause_reason: str) -> Dict[str, Any]:
        """
        Apply human input to state based on the pause reason.

        Args:
            state: Current workflow state
            human_input: User-provided input
            pause_reason: Reason workflow was paused

        Returns:
            Updated state with human input applied
        """
        state["human_input_received"] = human_input
        state["waiting_for_human_input"] = False
        state["human_input_request"] = None
        state["pause_reason"] = None

        # Handle different input types
        if pause_reason == PauseReason.NEEDS_API_KEY.value:
            if "api_key" in human_input:
                access_doc = state.get("access_documentation", {})
                if access_doc:
                    access_doc["_provided_api_key"] = human_input["api_key"]
                    state["access_documentation"] = access_doc
            state["interrupted"] = False
            state["error"] = None

        elif pause_reason == PauseReason.NEEDS_AUTHENTICATION.value:
            # Handle OAuth or username/password
            if "oauth_token" in human_input:
                access_doc = state.get("access_documentation", {})
                if access_doc:
                    access_doc["_provided_oauth_token"] = human_input["oauth_token"]
                    state["access_documentation"] = access_doc
            elif "username" in human_input and "password" in human_input:
                access_doc = state.get("access_documentation", {})
                if access_doc:
                    access_doc["_provided_username"] = human_input["username"]
                    access_doc["_provided_password"] = human_input["password"]
                    state["access_documentation"] = access_doc
            state["interrupted"] = False
            state["error"] = None

        elif pause_reason == PauseReason.NEEDS_SELECTION_CONFIRMATION.value:
            # User confirmed or selected a different option
            if human_input.get("confirmed", True):
                state["selection_confirmed"] = True
                # Check if user selected a different option
                if "selected_index" in human_input:
                    state["user_selected_index"] = human_input["selected_index"]
                else:
                    state["user_selected_index"] = None  # Use recommended
            state["interrupted"] = False
            state["error"] = None

        elif pause_reason == PauseReason.ERROR_RECOVERABLE.value:
            # Handle error guidance
            action = human_input.get("action", "retry")
            if action == "retry":
                state["interrupted"] = False
                state["error"] = None
            elif action == "cancel":
                state["interrupted"] = True
                # Keep the error
            elif action == "skip":
                state["interrupted"] = False
                state["error"] = None
                # Mark step as skipped - allow workflow to try next step

        elif pause_reason in (PauseReason.NEEDS_CONFIRMATION.value, PauseReason.NEEDS_GUIDANCE.value):
            # General confirmation or guidance
            if human_input.get("confirmed", True):
                state["interrupted"] = False
                state["error"] = None
            # Apply any additional data from human_input
            for key, value in human_input.items():
                if key not in ("confirmed", "action"):
                    state[f"_user_provided_{key}"] = value

        elif pause_reason == PauseReason.EXISTING_SOURCE_FOUND.value:
            # User choosing whether to use existing source or discover new
            if human_input.get("use_existing", False):
                # User wants to use an existing source
                state["use_existing_source"] = True
                selected_index = human_input.get("selected_index", 0)
                existing_sources = state.get("existing_sources_found", [])
                if existing_sources and 0 <= selected_index < len(existing_sources):
                    state["selected_existing_source_id"] = existing_sources[selected_index].get("source_id")
            else:
                # User wants to discover a new source
                state["use_existing_source"] = False
            state["interrupted"] = False
            state["error"] = None

        else:
            # Default handling - clear error state and continue
            state["interrupted"] = False
            state["error"] = None

        return state

    def _run_remaining_steps(self, state: DiscoveryState, current_step: str) -> DiscoveryState:
        """
        Run workflow steps from current_step onwards.

        Args:
            state: Current workflow state
            current_step: Step to resume from

        Returns:
            Updated state after running remaining steps
        """
        step_methods = {
            "search": self._search_node,
            "examine": self._examine_node,
            "select": self._select_node,
            "document": self._document_node,
            "test": self._test_node,
            "configure": self._configure_node,
        }

        # Find the index of current step
        try:
            start_index = self.STEP_ORDER.index(current_step)
        except ValueError:
            # Unknown step, start from beginning
            start_index = 0

        # Run from current step onwards
        for i in range(start_index, len(self.STEP_ORDER)):
            step_name = self.STEP_ORDER[i]
            step_method = step_methods.get(step_name)

            if not step_method:
                continue

            logger.info(f"Resume: Running step '{step_name}'")
            state = step_method(state)

            # Check if we need to stop
            if state.get("waiting_for_human_input") or state.get("interrupted"):
                logger.info(f"Resume: Stopping at step '{step_name}' - waiting_for_human_input={state.get('waiting_for_human_input')}, interrupted={state.get('interrupted')}")
                break

            # Check if using existing source - skip remaining steps
            if state.get("_using_existing_source"):
                logger.info(f"Resume: Using existing source, skipping remaining steps")
                break

        return state

    def resume(self, workflow_id: str, human_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resume a paused workflow with human input.

        Args:
            workflow_id: ID of the paused workflow
            human_input: Input provided by the user. Format depends on pause reason:
                - For API key: {"api_key": "your-key"}
                - For selection confirmation: {"confirmed": true} or {"confirmed": true, "selected_index": 1}
                - For error guidance: {"action": "retry"} or {"action": "cancel"} or {"action": "skip"}
                - For confirmation: {"confirmed": true}

        Returns:
            Same as run() - workflow result dictionary
        """
        logger.info(f"Resuming workflow {workflow_id} with human input: {list(human_input.keys())}")

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

        # Get the saved state and pause reason
        saved_state = workflow_doc["state"]
        current_step = workflow_doc["current_step"]
        pause_reason = workflow_doc.get("pause_reason", PauseReason.OTHER.value)

        logger.info(f"Resuming from step: {current_step}, pause_reason: {pause_reason}")

        # Mark as resumed in database
        self.workflow_state_model.resume_with_input(workflow_id, human_input)

        try:
            # Apply the human input to the state
            saved_state = self._apply_human_input(saved_state, human_input, pause_reason)

            # Check if user requested cancellation
            if human_input.get("action") == "cancel":
                self.workflow_state_model.cancel(workflow_id)
                return {
                    "success": False,
                    "workflow_id": workflow_id,
                    "error": {"issue": "Workflow cancelled", "details": "User requested cancellation"},
                    "paused": False,
                    "cancelled": True,
                }

            # Create typed state object
            resume_state = DiscoveryState(**saved_state)

            # Run remaining steps from current step
            resume_state = self._run_remaining_steps(resume_state, current_step)

            # Check if using an existing source (immediate success)
            if resume_state.get("_using_existing_source"):
                existing_source = resume_state.get("_using_existing_source", {})
                source_id = resume_state.get("source_id") or existing_source.get("source_id")
                config_id = resume_state.get("config_id") or existing_source.get("_id")

                # Mark as completed in database
                self.workflow_state_model.mark_completed(
                    workflow_id=workflow_id,
                    state=dict(resume_state),
                    source_id=source_id,
                    config_id=config_id,
                )

                logger.info(f"Resumed workflow {workflow_id} completed using existing source: {source_id}")
                return {
                    "success": True,
                    "workflow_id": workflow_id,
                    "source_id": source_id,
                    "config_id": config_id,
                    "paused": False,
                    "human_input_request": None,
                    "error": None,
                    "state": dict(resume_state),
                    "used_existing_source": True,
                }

            # Prepare result
            result = {
                "success": not resume_state.get("interrupted", False) and not resume_state.get("waiting_for_human_input", False),
                "workflow_id": workflow_id,
                "source_id": resume_state.get("source_id"),
                "config_id": resume_state.get("config_id"),
                "paused": resume_state.get("waiting_for_human_input", False),
                "current_step": resume_state.get("current_step"),
                "human_input_request": resume_state.get("human_input_request"),
                "error": resume_state.get("error"),
                "state": dict(resume_state),
            }

            if result["success"]:
                logger.info(f"Resumed workflow {workflow_id} completed successfully. Source ID: {result['source_id']}")
            elif result["paused"]:
                logger.info(f"Resumed workflow {workflow_id} paused again at step: {result['current_step']}")

            return result

        except Exception as e:
            logger.error(f"Resume workflow {workflow_id} failed: {e}", exc_info=True)

            error = WorkflowError(
                agent_name="Workflow",
                step="resume",
                issue="Workflow resume failed",
                details=str(e),
                recoverable=True,
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
