"""
Model for persisting workflow execution states to MongoDB.

This enables:
- Tracking workflow progress through each agent step
- Pausing workflows for human input (e.g., API keys)
- Resuming workflows from where they left off
- Auditing workflow execution history
"""

from pymongo import MongoClient, DESCENDING
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum
from config import Config


class WorkflowStatus(str, Enum):
    """Status of a workflow execution."""
    PENDING = "pending"           # Created but not started
    RUNNING = "running"           # Currently executing
    PAUSED = "paused"             # Paused, waiting for human input
    COMPLETED = "completed"       # Successfully completed
    FAILED = "failed"             # Failed with non-recoverable error
    CANCELLED = "cancelled"       # Cancelled by user


class PauseReason(str, Enum):
    """Reasons why a workflow might pause for human input."""
    NEEDS_API_KEY = "needs_api_key"
    NEEDS_AUTHENTICATION = "needs_authentication"
    NEEDS_CONFIRMATION = "needs_confirmation"
    NEEDS_SELECTION = "needs_selection"
    RATE_LIMITED = "rate_limited"
    OTHER = "other"


class WorkflowState:
    """
    Model for storing and retrieving workflow execution states from MongoDB.

    Each workflow execution gets a unique workflow_id and its state is persisted
    after each agent step. This enables pause/resume functionality.
    """

    def __init__(self, db_client: MongoClient = None):
        if db_client is None:
            db_client = MongoClient(Config.MONGO_URI)
        self.db = db_client[Config.DATABASE_NAME]
        self.collection = self.db.workflow_states
        self._create_indexes()

    def _create_indexes(self):
        """Create indexes for efficient querying."""
        self.collection.create_index("workflow_id", unique=True)
        self.collection.create_index("status")
        self.collection.create_index([("created_at", DESCENDING)])
        self.collection.create_index([("updated_at", DESCENDING)])
        self.collection.create_index("user_description")

    def create(self, workflow_id: str, user_description: str, initial_state: Dict[str, Any]) -> str:
        """
        Create a new workflow execution record.

        Args:
            workflow_id: Unique identifier for this workflow execution
            user_description: The user's data source description
            initial_state: Initial DiscoveryState dictionary

        Returns:
            str: MongoDB document ID
        """
        now = datetime.utcnow()

        doc = {
            "workflow_id": workflow_id,
            "user_description": user_description,
            "status": WorkflowStatus.PENDING.value,
            "current_step": "initialized",
            "steps_completed": [],
            "state": initial_state,

            # Pause/resume fields
            "pause_reason": None,
            "pause_details": None,
            "human_input_required": None,
            "human_input_received": None,

            # Results
            "source_id": None,
            "config_id": None,
            "error": None,

            # Timestamps
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "completed_at": None,
            "paused_at": None,
            "resumed_at": None,
        }

        result = self.collection.insert_one(doc)
        return str(result.inserted_id)

    def get_by_workflow_id(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """
        Get workflow state by workflow ID.

        Args:
            workflow_id: Unique workflow identifier

        Returns:
            Dict containing workflow state or None if not found
        """
        doc = self.collection.find_one({"workflow_id": workflow_id})
        if doc:
            doc["_id"] = str(doc["_id"])
        return doc

    def update_state(
        self,
        workflow_id: str,
        state: Dict[str, Any],
        current_step: str,
        status: Optional[WorkflowStatus] = None,
    ) -> bool:
        """
        Update the workflow state after an agent step completes.

        Args:
            workflow_id: Unique workflow identifier
            state: Updated DiscoveryState dictionary
            current_step: Name of the step just completed
            status: Optional new status

        Returns:
            bool: True if update successful
        """
        update_data = {
            "state": state,
            "current_step": current_step,
            "updated_at": datetime.utcnow(),
        }

        if status:
            update_data["status"] = status.value
            if status == WorkflowStatus.RUNNING:
                update_data["started_at"] = datetime.utcnow()
            elif status == WorkflowStatus.COMPLETED:
                update_data["completed_at"] = datetime.utcnow()

        result = self.collection.update_one(
            {"workflow_id": workflow_id},
            {
                "$set": update_data,
                "$addToSet": {"steps_completed": current_step}
            }
        )
        return result.modified_count > 0

    def pause_for_input(
        self,
        workflow_id: str,
        state: Dict[str, Any],
        current_step: str,
        pause_reason: PauseReason,
        pause_details: str,
        human_input_required: Dict[str, Any],
    ) -> bool:
        """
        Pause a workflow and request human input.

        Args:
            workflow_id: Unique workflow identifier
            state: Current DiscoveryState dictionary
            current_step: Step where pause occurred
            pause_reason: Why the workflow is pausing
            pause_details: Human-readable explanation
            human_input_required: Schema describing what input is needed
                Example: {
                    "type": "api_key",
                    "field_name": "api_key",
                    "description": "API key for USDA QuickStats",
                    "registration_url": "https://quickstats.nass.usda.gov/api"
                }

        Returns:
            bool: True if update successful
        """
        update_data = {
            "state": state,
            "current_step": current_step,
            "status": WorkflowStatus.PAUSED.value,
            "pause_reason": pause_reason.value,
            "pause_details": pause_details,
            "human_input_required": human_input_required,
            "paused_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }

        result = self.collection.update_one(
            {"workflow_id": workflow_id},
            {"$set": update_data}
        )
        return result.modified_count > 0

    def resume_with_input(
        self,
        workflow_id: str,
        human_input: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Resume a paused workflow with human input.

        Args:
            workflow_id: Unique workflow identifier
            human_input: The input provided by the user
                Example: {"api_key": "abc123"}

        Returns:
            Dict: Updated workflow document, or None if not found/not paused
        """
        # Get current state
        doc = self.get_by_workflow_id(workflow_id)
        if not doc:
            return None

        if doc["status"] != WorkflowStatus.PAUSED.value:
            return None

        update_data = {
            "status": WorkflowStatus.RUNNING.value,
            "human_input_received": human_input,
            "resumed_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            # Clear pause fields
            "pause_reason": None,
            "pause_details": None,
        }

        self.collection.update_one(
            {"workflow_id": workflow_id},
            {"$set": update_data}
        )

        return self.get_by_workflow_id(workflow_id)

    def mark_completed(
        self,
        workflow_id: str,
        state: Dict[str, Any],
        source_id: str,
        config_id: str,
    ) -> bool:
        """
        Mark a workflow as successfully completed.

        Args:
            workflow_id: Unique workflow identifier
            state: Final DiscoveryState dictionary
            source_id: ID of the configured data source
            config_id: MongoDB ID of the connector config

        Returns:
            bool: True if update successful
        """
        update_data = {
            "state": state,
            "status": WorkflowStatus.COMPLETED.value,
            "current_step": "completed",
            "source_id": source_id,
            "config_id": config_id,
            "completed_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }

        result = self.collection.update_one(
            {"workflow_id": workflow_id},
            {"$set": update_data}
        )
        return result.modified_count > 0

    def mark_failed(
        self,
        workflow_id: str,
        state: Dict[str, Any],
        error: Dict[str, Any],
    ) -> bool:
        """
        Mark a workflow as failed.

        Args:
            workflow_id: Unique workflow identifier
            state: Final DiscoveryState dictionary
            error: WorkflowError dictionary

        Returns:
            bool: True if update successful
        """
        update_data = {
            "state": state,
            "status": WorkflowStatus.FAILED.value,
            "error": error,
            "completed_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }

        result = self.collection.update_one(
            {"workflow_id": workflow_id},
            {"$set": update_data}
        )
        return result.modified_count > 0

    def cancel(self, workflow_id: str) -> bool:
        """
        Cancel a workflow.

        Args:
            workflow_id: Unique workflow identifier

        Returns:
            bool: True if update successful
        """
        result = self.collection.update_one(
            {"workflow_id": workflow_id},
            {"$set": {
                "status": WorkflowStatus.CANCELLED.value,
                "completed_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            }}
        )
        return result.modified_count > 0

    def get_paused_workflows(self) -> List[Dict[str, Any]]:
        """
        Get all workflows that are paused and waiting for input.

        Returns:
            List of paused workflow documents
        """
        docs = list(self.collection.find(
            {"status": WorkflowStatus.PAUSED.value}
        ).sort("paused_at", DESCENDING))

        for doc in docs:
            doc["_id"] = str(doc["_id"])
        return docs

    def get_recent_workflows(
        self,
        limit: int = 20,
        status: Optional[WorkflowStatus] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get recent workflow executions.

        Args:
            limit: Maximum number of workflows to return
            status: Optional status filter

        Returns:
            List of workflow documents
        """
        query = {}
        if status:
            query["status"] = status.value

        docs = list(self.collection.find(query)
                    .sort("created_at", DESCENDING)
                    .limit(limit))

        for doc in docs:
            doc["_id"] = str(doc["_id"])
        return docs

    def delete(self, workflow_id: str) -> bool:
        """
        Delete a workflow record.

        Args:
            workflow_id: Unique workflow identifier

        Returns:
            bool: True if deletion successful
        """
        result = self.collection.delete_one({"workflow_id": workflow_id})
        return result.deleted_count > 0
