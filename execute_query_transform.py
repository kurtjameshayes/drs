#!/usr/bin/env python3
"""
Execute a stored query by ID using the query engine.

Fetches the query and connector configuration from MongoDB,
then executes the query through the query engine.

Examples:
    python execute_query_transform.py my_query_id
"""

import argparse
import json
import logging
import os
import sys
from typing import Any, Dict

# Ensure repository root is on sys.path so local modules can be imported.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.query_engine import QueryEngine
from models.stored_query import StoredQuery
from models.connector_config import ConnectorConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Execute a stored query by ID, fetching query and connector info from MongoDB.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("query_id", help="Stored query identifier to execute.")
    return parser.parse_args()


def execute_query_transform(query_id: str) -> Dict[str, Any]:
    """
    Execute a stored query by fetching query and connector info from MongoDB.

    Args:
        query_id: The identifier of the stored query to execute.

    Returns:
        Dict containing query results.
    """
    # Fetch the query from MongoDB
    stored_query_model = StoredQuery()
    query_data = stored_query_model.get_by_id(query_id)

    if not query_data:
        return {
            "success": False,
            "error": f"Stored query not found: {query_id}",
            "query_id": query_id,
        }

    # Check if query is active
    if not query_data.get("active", True):
        return {
            "success": False,
            "error": f"Stored query is inactive: {query_id}",
            "query_id": query_id,
        }

    # Extract connector_id from the query
    connector_id = query_data.get("connector_id")
    if not connector_id:
        return {
            "success": False,
            "error": f"Query missing connector_id: {query_id}",
            "query_id": query_id,
        }

    # Fetch connector configuration from MongoDB
    connector_config_model = ConnectorConfig()
    connector_config = connector_config_model.get_by_source_id(connector_id)

    if not connector_config:
        return {
            "success": False,
            "error": f"Connector configuration not found: {connector_id}",
            "query_id": query_id,
            "connector_id": connector_id,
        }

    # Check if connector is active
    if not connector_config.get("active", True):
        return {
            "success": False,
            "error": f"Connector is inactive: {connector_id}",
            "query_id": query_id,
            "connector_id": connector_id,
        }

    # Get query parameters
    parameters = query_data.get("parameters", {})

    # Execute the query using the query engine
    query_engine = QueryEngine()
    result = query_engine.execute_query(
        source_id=connector_id,
        parameters=parameters,
        query_id=query_id,
    )

    # Add query metadata to result
    if result.get("success"):
        result["query_name"] = query_data.get("query_name")
        result["query_description"] = query_data.get("description")
        result["connector_type"] = connector_config.get("connector_type")

    return result


def print_summary(result: Dict[str, Any]) -> None:
    """Print a human-friendly summary of the execution outcome."""
    print("=" * 70)
    print("QUERY TRANSFORM EXECUTION")
    print("=" * 70)

    if not result.get("success"):
        print(f"\n✗ Execution failed: {result.get('error', 'Unknown error')}")
        return

    query_id = result.get("query_id", "N/A")
    query_name = result.get("query_name", "N/A")
    connector_type = result.get("connector_type", "N/A")
    source = result.get("source", "unknown")
    data = result.get("data", {})
    metadata = data.get("metadata", {}) if isinstance(data, dict) else {}
    record_count = metadata.get("record_count", "N/A")

    print(f"\n✓ Query executed successfully")
    print(f"  Query ID: {query_id}")
    print(f"  Query Name: {query_name}")
    print(f"  Connector Type: {connector_type}")
    print(f"  Source: {source}")
    print(f"  Records: {record_count}")


def main() -> int:
    """Main entry point."""
    args = parse_args()

    result = execute_query_transform(args.query_id)

    print_summary(result)
    print("\nFull Result:\n")
    print(json.dumps(result, indent=2, default=str))

    return 0 if result.get("success") else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
