#!/usr/bin/env python3
"""
Execute a stored query by ID using the query engine.

Examples:
    python execute_query.py my_query_id
    python execute_query.py my_query_id --no-cache
    python execute_query.py my_query_id --overrides '{"from": "2020", "to": "2021"}'
    python execute_query.py my_query_id --overrides-file overrides.json --output result.json
"""

import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, Optional

# Ensure repository root is on sys.path so local modules can be imported.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.query_engine import QueryEngine  # pylint: disable=wrong-import-position

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Execute a stored query by ID.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("query_id", help="Stored query identifier to execute.")
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable cache usage for this execution.",
    )
    parser.add_argument(
        "--overrides",
        help="JSON string with parameter overrides for the stored query.",
    )
    parser.add_argument(
        "--overrides-file",
        help="Path to a JSON file containing parameter overrides.",
    )
    parser.add_argument(
        "--output",
        help="Optional path to write the raw JSON result.",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="Indent level for printed JSON output.",
    )
    return parser.parse_args()


def load_overrides(args: argparse.Namespace) -> Optional[Dict[str, Any]]:
    """
    Load parameter overrides from CLI arguments.

    Raises:
        ValueError: If overrides cannot be parsed.
    """
    if args.overrides and args.overrides_file:
        raise ValueError("Use either --overrides or --overrides-file, not both.")

    if args.overrides:
        try:
            return json.loads(args.overrides)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in --overrides: {exc}") from exc

    if args.overrides_file:
        try:
            with open(args.overrides_file, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except FileNotFoundError as exc:
            raise ValueError(f"Overrides file not found: {args.overrides_file}") from exc
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in overrides file: {exc}") from exc

    return None


def print_summary(result: Dict[str, Any]) -> None:
    """Print a human-friendly summary of the execution outcome."""
    print("=" * 70)
    print("STORED QUERY EXECUTION")
    print("=" * 70)

    if not result.get("success"):
        print(f"\n✗ Execution failed: {result.get('error', 'Unknown error')}")
        return

    query_id = result.get("query_id", "N/A")
    query_name = result.get("query_name", "N/A")
    source = result.get("source", "unknown")
    data = result.get("data", {})
    metadata = data.get("metadata", {}) if isinstance(data, dict) else {}
    record_count = metadata.get("record_count", "N/A")

    print(f"\n✓ Query executed successfully")
    print(f"  Query ID: {query_id}")
    print(f"  Query Name: {query_name}")
    print(f"  Source: {source}")
    print(f"  Records: {record_count}")


def write_output(path: str, payload: Dict[str, Any]) -> None:
    """Write execution result to disk."""
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
    logger.info("Result written to %s", path)


def main() -> int:
    args = parse_args()

    try:
        overrides = load_overrides(args)
    except ValueError as exc:
        logger.error(str(exc))
        return 1

    query_engine = QueryEngine()

    use_cache = False if args.no_cache else None
    result = query_engine.execute_stored_query(
        args.query_id,
        use_cache=use_cache,
        parameter_overrides=overrides,
    )

    print_summary(result)
    print("\nFull Result:\n")
    print(json.dumps(result, indent=args.indent, default=str))

    if args.output:
        try:
            write_output(args.output, result)
        except OSError as exc:
            logger.error("Failed to write output file: %s", exc)
            return 1

    return 0 if result.get("success") else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
