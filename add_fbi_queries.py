#!/usr/bin/env python3
"""
Add FBI Crime Data stored queries to MongoDB.

This script preloads curated FBI Crime Data Explorer (CDE) queries so they can
be executed by query ID via `manage_queries.py`, the REST API, or
`execute_query.py`.

Usage:
    python add_fbi_queries.py              # Add or update all queries
    python add_fbi_queries.py --list       # List the queries that will be added
    python add_fbi_queries.py --show <id>  # Show query details before adding
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.stored_query import StoredQuery

FBI_QUERIES = [
    {
        "query_id": "fbi_national_arrests_all_offenses",
        "query_name": "FBI National Arrest Counts (All Offenses)",
        "connector_id": "fbi_crime",
        "description": (
            "National arrest counts for all offense categories, aggregated monthly "
            "for 2023 using the FBI Crime Data Explorer (CDE) arrests endpoint."
        ),
        "parameters": {
            "endpoint": "https://api.usa.gov/crime/fbi/cde/arrest/national/all",
            "type": "counts",
            "from": "01-2023",
            "to": "12-2023",
        },
        "tags": ["fbi", "crime", "arrests", "national", "counts"],
        "notes": {
            "endpoint": "arrest/national/all",
            "documentation": "https://crime-data-explorer.fr.cloud.gov/api",
            "expected_url": (
                "https://api.usa.gov/crime/fbi/cde/arrest/national/all"
                "?type=counts&from=01-2023&to=12-2023&API_KEY=<YOUR_KEY>"
            ),
        },
    },
]


def list_queries() -> None:
    """List the queries that will be created or updated."""
    print("=" * 70)
    print("FBI CRIME DATA QUERIES")
    print("=" * 70 + "\n")

    for query in FBI_QUERIES:
        print(f"ID: {query['query_id']}")
        print(f"  Name: {query['query_name']}")
        print(f"  Connector: {query['connector_id']}")
        print(f"  Description: {query['description']}")
        print(f"  Tags: {', '.join(query.get('tags', []))}")
        print()

    print(f"Total queries: {len(FBI_QUERIES)}\n")


def show_query_details(query_id: str) -> None:
    """Print the complete definition for a specific query."""
    query = next((q for q in FBI_QUERIES if q["query_id"] == query_id), None)
    if not query:
        print(f"Query '{query_id}' not found.\n")
        return

    print("=" * 70)
    print(f"QUERY DETAILS: {query_id}")
    print("=" * 70 + "\n")
    print(f"Name: {query['query_name']}")
    print(f"Connector: {query['connector_id']}")
    print(f"Description: {query['description']}\n")
    print("Parameters:")
    print(json.dumps(query["parameters"], indent=2))
    print()

    if query.get("notes"):
        print("Notes:")
        for key, value in query["notes"].items():
            print(f"  {key}: {value}")
        print()

    if query.get("tags"):
        print("Tags:", ", ".join(query["tags"]))
        print()


def add_queries() -> Dict[str, int]:
    """
    Create or update the predefined queries in MongoDB.

    Returns:
        Dict[str, int]: counts of added/updated/failed queries.
    """
    stored_query = StoredQuery()
    results = {"added": 0, "updated": 0, "failed": 0}

    for query in FBI_QUERIES:
        query_id = query["query_id"]
        try:
            existing = stored_query.get_by_id(query_id)
            if existing:
                stored_query.update(query_id, query)
                results["updated"] += 1
                action = "Updated"
            else:
                stored_query.create(query)
                results["added"] += 1
                action = "Added"

            print(f"{action}: {query_id} - {query['query_name']}")

        except Exception as exc:  # pragma: no cover - defensive logging
            print(f"Failed: {query_id} -> {exc}")
            results["failed"] += 1

    print("\nSummary:")
    print(f"  Added:   {results['added']}")
    print(f"  Updated: {results['updated']}")
    print(f"  Failed:  {results['failed']}\n")

    if results["added"] or results["updated"]:
        print("Next steps:")
        print("  python manage_queries.py --list --connector fbi_crime")
        print("  python manage_queries.py --execute fbi_national_arrests_all_offenses\n")

    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add FBI Crime Data stored queries to MongoDB.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--list", action="store_true", help="List built-in queries.")
    parser.add_argument(
        "--show",
        metavar="QUERY_ID",
        help="Show the definition for a specific query.",
    )
    parser.add_argument(
        "--add",
        action="store_true",
        help="Add/update queries (default action if no flags supplied).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.list:
        list_queries()
        return

    if args.show:
        show_query_details(args.show)
        return

    add_queries()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
