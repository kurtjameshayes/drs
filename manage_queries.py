#!/usr/bin/env python3
"""
Manage Stored Queries

This script provides command-line utilities for managing stored queries in MongoDB.

Usage:
    python manage_queries.py --list                    # List all queries
    python manage_queries.py --create <json_file>      # Create query from JSON file
    python manage_queries.py --create-json '<json>'    # Create query from JSON string
    python manage_queries.py --create-interactive      # Create query interactively
    python manage_queries.py --get <query_id>          # Get query details
    python manage_queries.py --execute <query_id>      # Execute a stored query
    python manage_queries.py --delete <query_id>       # Delete a query
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.stored_query import StoredQuery
from core.query_engine import QueryEngine
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def list_queries(connector_id=None, active_only=False):
    """List stored queries."""
    print("=" * 70)
    print("STORED QUERIES")
    print("=" * 70 + "\n")

    stored_query = StoredQuery()
    queries = stored_query.get_all(connector_id=connector_id, active_only=active_only)

    if not queries:
        print("No queries found.\n")
        return

    for query in queries:
        print(f"ID: {query['query_id']}")
        print(f"  Name: {query['query_name']}")
        print(f"  Connector: {query['connector_id']}")
        print(f"  Active: {query.get('active', True)}")

        if 'description' in query:
            print(f"  Description: {query['description']}")

        if 'tags' in query and query['tags']:
            print(f"  Tags: {', '.join(query['tags'])}")

        print(f"  Created: {query.get('created_at', 'N/A')}")
        print()

    print(f"Total: {len(queries)} queries\n")


def create_query_from_json(json_file):
    """Create a stored query from JSON file."""
    print("=" * 70)
    print("CREATE STORED QUERY")
    print("=" * 70 + "\n")

    try:
        with open(json_file, 'r') as f:
            query_data = json.load(f)

        stored_query = StoredQuery()
        stored_query.create(query_data)

        print(f"✓ Query created successfully")
        print(f"  ID: {query_data['query_id']}")
        print(f"  Name: {query_data['query_name']}")
        print()

    except FileNotFoundError:
        print(f"✗ File not found: {json_file}\n")
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON: {str(e)}\n")
    except Exception as e:
        print(f"✗ Error creating query: {str(e)}\n")


def create_query_from_json_string(json_string):
    """Create a stored query from JSON string."""
    print("=" * 70)
    print("CREATE STORED QUERY FROM JSON STRING")
    print("=" * 70 + "\n")

    try:
        query_data = json.loads(json_string)

        stored_query = StoredQuery()
        stored_query.create(query_data)

        print(f"✓ Query created successfully")
        print(f"  ID: {query_data['query_id']}")
        print(f"  Name: {query_data['query_name']}")
        print()

    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON: {str(e)}\n")
    except Exception as e:
        print(f"✗ Error creating query: {str(e)}\n")


def create_query_interactive():
    """Create a query interactively."""
    print("=" * 70)
    print("CREATE STORED QUERY - INTERACTIVE")
    print("=" * 70 + "\n")

    try:
        query_id = input("Query ID: ").strip()
        query_name = input("Query Name: ").strip()
        connector_id = input("Connector ID: ").strip()
        description = input("Description (optional): ").strip()

        print("\nEnter query parameters as JSON:")
        print("Example: {\"endpoint\": \"estimates/national\", \"from\": \"2020\", \"to\": \"2021\"}")
        parameters_str = input("Parameters: ").strip()

        parameters = json.loads(parameters_str)

        tags_str = input("Tags (comma-separated, optional): ").strip()
        tags = [t.strip() for t in tags_str.split(',')] if tags_str else []

        query_data = {
            "query_id": query_id,
            "query_name": query_name,
            "connector_id": connector_id,
            "parameters": parameters
        }

        if description:
            query_data["description"] = description

        if tags:
            query_data["tags"] = tags

        stored_query = StoredQuery()
        stored_query.create(query_data)

        print(f"\n✓ Query created successfully")
        print(f"  ID: {query_id}")
        print()

    except KeyboardInterrupt:
        print("\n\nCancelled.\n")
    except json.JSONDecodeError as e:
        print(f"\n✗ Invalid JSON in parameters: {str(e)}\n")
    except Exception as e:
        print(f"\n✗ Error creating query: {str(e)}\n")


def get_query(query_id):
    """Get query details."""
    print("=" * 70)
    print(f"QUERY: {query_id}")
    print("=" * 70 + "\n")

    stored_query = StoredQuery()
    query = stored_query.get_by_id(query_id)

    if not query:
        print(f"✗ Query not found: {query_id}\n")
        return

    print(f"ID: {query['query_id']}")
    print(f"Name: {query['query_name']}")
    print(f"Connector: {query['connector_id']}")
    print(f"Active: {query.get('active', True)}")

    if 'description' in query:
        print(f"Description: {query['description']}")

    if 'tags' in query and query['tags']:
        print(f"Tags: {', '.join(query['tags'])}")

    print(f"\nParameters:")
    print(json.dumps(query['parameters'], indent=2))

    print(f"\nCreated: {query.get('created_at', 'N/A')}")
    print(f"Updated: {query.get('updated_at', 'N/A')}")
    print()


def execute_query(query_id):
    """Execute a stored query."""
    print("=" * 70)
    print(f"EXECUTING QUERY: {query_id}")
    print("=" * 70 + "\n")

    query_engine = QueryEngine()
    result = query_engine.execute_stored_query(query_id)

    if not result.get("success"):
        print(f"✗ Query execution failed: {result.get('error')}\n")
        return

    print(f"✓ Query executed successfully")
    print(f"  Source: {result.get('source', 'N/A')}")
    print(f"  Query Name: {result.get('query_name', 'N/A')}")

    data = result.get('data', {})
    if isinstance(data, dict):
        record_count = data.get('metadata', {}).get('record_count', 'N/A')
        print(f"  Records: {record_count}")

    print(f"\nResult:")
    print(json.dumps(result, indent=2, default=str))
    print()


def delete_query(query_id):
    """Delete a stored query."""
    print("=" * 70)
    print(f"DELETE QUERY: {query_id}")
    print("=" * 70 + "\n")

    # Confirm deletion
    confirm = input(f"Are you sure you want to delete query '{query_id}'? (yes/no): ").strip().lower()

    if confirm not in ['yes', 'y']:
        print("Cancelled.\n")
        return

    stored_query = StoredQuery()
    success = stored_query.delete(query_id)

    if success:
        print(f"✓ Query deleted successfully\n")
    else:
        print(f"✗ Query not found: {query_id}\n")


def search_queries(search_term):
    """Search queries."""
    print("=" * 70)
    print(f"SEARCH: {search_term}")
    print("=" * 70 + "\n")

    stored_query = StoredQuery()
    queries = stored_query.search(search_term)

    if not queries:
        print("No queries found.\n")
        return

    for query in queries:
        print(f"ID: {query['query_id']}")
        print(f"  Name: {query['query_name']}")
        print(f"  Connector: {query['connector_id']}")

        if 'description' in query:
            print(f"  Description: {query['description']}")

        print()

    print(f"Total: {len(queries)} queries\n")


def show_usage():
    """Show usage information."""
    print("""
Manage Stored Queries

Usage:
    python manage_queries.py --list [--connector <id>] [--active]
    python manage_queries.py --create <json_file>
    python manage_queries.py --create-json '<json_string>'
    python manage_queries.py --create-interactive
    python manage_queries.py --get <query_id>
    python manage_queries.py --execute <query_id>
    python manage_queries.py --delete <query_id>
    python manage_queries.py --search <term>

Examples:
    # List all queries
    python manage_queries.py --list

    # List queries for specific connector
    python manage_queries.py --list --connector fbi_crime

    # Create query from JSON file
    python manage_queries.py --create my_query.json

    # Create query from JSON string
    python manage_queries.py --create-json '{"query_id":"test","query_name":"Test","connector_id":"fbi_crime","parameters":{"endpoint":"estimates/national"}}'

    # Create query interactively
    python manage_queries.py --create-interactive

    # Get query details
    python manage_queries.py --get my_query_id

    # Execute a stored query
    python manage_queries.py --execute my_query_id

    # Delete a query
    python manage_queries.py --delete my_query_id

    # Search queries
    python manage_queries.py --search crime

JSON Format for --create and --create-json:
{
    "query_id": "my_query",
    "query_name": "My Query Name",
    "connector_id": "fbi_crime",
    "description": "Optional description",
    "parameters": {
        "endpoint": "estimates/national",
        "from": "2020",
        "to": "2021"
    },
    "tags": ["crime", "national"]
}
""")


def main():
    """Main entry point."""
    if len(sys.argv) < 2 or sys.argv[1] in ['-h', '--help', 'help']:
        show_usage()
        return

    command = sys.argv[1]

    if command in ['--list', 'list']:
        connector_id = None
        active_only = False

        # Parse options
        if '--connector' in sys.argv:
            idx = sys.argv.index('--connector')
            if idx + 1 < len(sys.argv):
                connector_id = sys.argv[idx + 1]

        if '--active' in sys.argv:
            active_only = True

        list_queries(connector_id, active_only)

    elif command in ['--create', 'create']:
        if len(sys.argv) < 3:
            print("Error: JSON file required")
            print("Usage: python manage_queries.py --create <json_file>")
            return

        create_query_from_json(sys.argv[2])

    elif command in ['--create-json', 'create-json']:
        if len(sys.argv) < 3:
            print("Error: JSON string required")
            print("Usage: python manage_queries.py --create-json '<json_string>'")
            return

        create_query_from_json_string(sys.argv[2])

    elif command in ['--create-interactive', 'create-interactive']:
        create_query_interactive()

    elif command in ['--get', 'get']:
        if len(sys.argv) < 3:
            print("Error: Query ID required")
            print("Usage: python manage_queries.py --get <query_id>")
            return

        get_query(sys.argv[2])

    elif command in ['--execute', 'execute']:
        if len(sys.argv) < 3:
            print("Error: Query ID required")
            print("Usage: python manage_queries.py --execute <query_id>")
            return

        execute_query(sys.argv[2])

    elif command in ['--delete', 'delete']:
        if len(sys.argv) < 3:
            print("Error: Query ID required")
            print("Usage: python manage_queries.py --delete <query_id>")
            return

        delete_query(sys.argv[2])

    elif command in ['--search', 'search']:
        if len(sys.argv) < 3:
            print("Error: Search term required")
            print("Usage: python manage_queries.py --search <term>")
            return

        search_queries(sys.argv[2])

    else:
        print(f"Unknown command: {command}")
        show_usage()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        sys.exit(0)