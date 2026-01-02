"""
Configuration Agent for the data source discovery workflow.

This agent creates a MongoDB configuration record for the discovered
data source, following the existing connector_configs schema.
"""

import json
import logging
import re
from datetime import datetime
from typing import Dict, Any
import uuid

from pymongo import MongoClient

from config import Config
from models.connector_config import ConnectorConfig
from ..state import (
    DiscoveryState,
    WorkflowError,
)

logger = logging.getLogger(__name__)


class ConfigurationAgent:
    """
    Agent 6: Store the data source configuration in MongoDB.

    Creates a record in the connector_configs collection with all
    necessary information to access the data source.
    """

    def __init__(self, db_client: MongoClient = None):
        if db_client is None:
            db_client = MongoClient(Config.MONGO_URI)
        self.connector_config = ConnectorConfig(db_client)

    def _generate_source_id(self, source_name: str) -> str:
        """Generate a URL-safe source ID from the source name."""
        # Convert to lowercase and replace spaces/special chars with hyphens
        slug = source_name.lower()
        slug = re.sub(r"[^a-z0-9]+", "-", slug)
        slug = slug.strip("-")

        # Add a short unique suffix to avoid collisions
        suffix = str(uuid.uuid4())[:8]
        return f"{slug}-{suffix}"

    def _build_config(
        self,
        access_doc: Dict[str, Any],
        test_results: Dict[str, Any],
        user_description: str,
        selected_source: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build the configuration record for MongoDB."""
        source_name = access_doc.get("source_name", "Unknown Source")
        source_id = self._generate_source_id(source_name)
        connector_type = access_doc.get("mapped_connector_type", "discovered")

        # Build the query methodology
        endpoints = access_doc.get("endpoints", [])
        query_methodology = {
            "access_method": access_doc.get("access_method", "api"),
            "base_url": access_doc.get("base_url", ""),
            "endpoints": endpoints,
            "data_format": access_doc.get("data_format", "json"),
            "rate_limits": access_doc.get("rate_limits", {}),
        }

        # Get primary endpoint URL
        primary_url = access_doc.get("base_url", "")
        if endpoints:
            primary_url = endpoints[0].get("url", primary_url)

        # Build authentication config
        auth = access_doc.get("authentication", {})
        auth_config = {
            "required": auth.get("required", False),
            "type": auth.get("auth_type", "none"),
            "header": auth.get("auth_header", ""),
            "format": auth.get("auth_format", ""),
            "registration_url": auth.get("registration_url", ""),
        }

        # Build the full config
        config = {
            "source_id": source_id,
            "source_name": source_name,
            "connector_type": connector_type,
            "url": primary_url,
            "api_key": "",  # Placeholder - user needs to provide
            "active": True,

            # Discovery-specific fields
            "discovery_metadata": {
                "discovered_at": datetime.utcnow().isoformat(),
                "user_description": user_description,
                "original_url": selected_source.get("candidate", {}).get("url", ""),
                "discovery_source": selected_source.get("candidate", {}).get("source_type", ""),
            },

            # Access configuration
            "authentication": auth_config,
            "query_methodology": query_methodology,

            # Documentation
            "documentation_url": access_doc.get("terms_of_use_url", "") or selected_source.get("documentation_url", ""),
            "update_frequency": access_doc.get("update_frequency", ""),
            "notes": access_doc.get("notes", ""),

            # Test results
            "test_results": {
                "last_tested": test_results.get("test_timestamp", datetime.utcnow().isoformat()),
                "success": test_results.get("success", False),
                "status_code": test_results.get("status_code"),
                "response_time_ms": test_results.get("response_time_ms"),
            },
        }

        return config

    def run(self, state: DiscoveryState) -> DiscoveryState:
        """
        Execute the configuration agent.

        Args:
            state: Current workflow state

        Returns:
            Updated state with configuration ID
        """
        logger.info("Configuration Agent: Starting configuration creation")

        try:
            access_doc = state.get("access_documentation")
            test_results = state.get("test_results", {})
            user_description = state["user_description"]
            selected_source = state.get("selected_source", {})

            if not access_doc:
                state["error"] = WorkflowError(
                    agent_name="ConfigurationAgent",
                    step="configuration",
                    issue="No access documentation available for configuration",
                    details="The documentation agent did not provide access documentation",
                    recoverable=False,
                ).to_dict()
                state["interrupted"] = True
                return state

            # Build the configuration
            config = self._build_config(
                access_doc, test_results, user_description, selected_source
            )

            logger.info(f"Configuration Agent: Creating config for '{config['source_name']}' (ID: {config['source_id']})")

            # Store in MongoDB
            try:
                config_id = self.connector_config.create(config)
                logger.info(f"Configuration Agent: Created config with MongoDB ID: {config_id}")

                state["config_id"] = config_id
                state["source_id"] = config["source_id"]
                state["configuration_completed"] = True
                state["workflow_end_time"] = datetime.utcnow().isoformat()

                return state

            except Exception as db_error:
                logger.error(f"Failed to store config in MongoDB: {db_error}")
                state["error"] = WorkflowError(
                    agent_name="ConfigurationAgent",
                    step="mongodb_insert",
                    issue="Failed to store configuration in MongoDB",
                    details=str(db_error),
                    recoverable=True,
                ).to_dict()
                state["interrupted"] = True
                return state

        except Exception as e:
            logger.error(f"Configuration Agent failed: {e}", exc_info=True)
            state["error"] = WorkflowError(
                agent_name="ConfigurationAgent",
                step="configuration",
                issue="Configuration agent encountered an error",
                details=str(e),
                recoverable=False,
            ).to_dict()
            state["interrupted"] = True
            return state
