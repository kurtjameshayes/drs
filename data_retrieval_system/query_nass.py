#!/usr/bin/env python3
"""
USDA NASS QuickStats Query Examples

This script demonstrates how to execute queries against the USDA NASS QuickStats
API using the data retrieval system's query engine.

Prerequisites:
1. MongoDB must be running
2. USDA NASS connector must be configured with valid API key
3. Connector must be active

Usage:
    python query_nass.py                    # Run all example queries
    python query_nass.py --example <num>    # Run specific example
    python query_nass.py --custom           # Run custom query
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.query_engine import QueryEngine
from models.connector_config import ConnectorConfig
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# EXAMPLE QUERIES
# Modify these or add your own query examples
# ============================================================================

EXAMPLE_QUERIES = {
    1: {
        "name": "Corn Production in Iowa - 2020",
        "description": "Get corn production statistics for Iowa in 2020",
        "parameters": {
            "commodity_desc": "CORN",
            "year": "2020",
            "state_alpha": "IA",
            "statisticcat_desc": "PRODUCTION"
        }
    },
    
    2: {
        "name": "Wheat Yield by State - 2021",
        "description": "Get wheat yield data for all states in 2021",
        "parameters": {
            "commodity_desc": "WHEAT",
            "year": "2021",
            "statisticcat_desc": "YIELD",
            "unit_desc": "BU / ACRE"
        }
    },
    
    3: {
        "name": "Cattle Inventory in Texas",
        "description": "Get cattle inventory data for Texas",
        "parameters": {
            "commodity_desc": "CATTLE",
            "state_alpha": "TX",
            "statisticcat_desc": "INVENTORY",
            "year": "2022"
        }
    },
    
    4: {
        "name": "Soybean Acres Planted in Illinois",
        "description": "Get soybean planted acres for Illinois",
        "parameters": {
            "commodity_desc": "SOYBEANS",
            "statisticcat_desc": "AREA PLANTED",
            "state_alpha": "IL",
            "year": "2022"
        }
    },
    
    5: {
        "name": "Barley Production in Montana",
        "description": "Get barley production data for Montana",
        "parameters": {
            "commodity_desc": "BARLEY",
            "year": "2022",
            "state_alpha": "MT",
            "statisticcat_desc": "PRODUCTION"
        }
    },
    
    6: {
        "name": "Milk Production in Wisconsin",
        "description": "Get milk production data for Wisconsin",
        "parameters": {
            "commodity_desc": "MILK",
            "state_alpha": "WI",
            "statisticcat_desc": "PRODUCTION",
            "year": "2022"
        }
    },
    
    7: {
        "name": "Corn Price Received",
        "description": "Get price received for corn",
        "parameters": {
            "commodity_desc": "CORN",
            "statisticcat_desc": "PRICE RECEIVED",
            "year": "2022",
            "state_alpha": "IA",
            "agg_level_desc": "STATE"
        }
    },
    
    8: {
        "name": "Cotton Acres Harvested - All States",
        "description": "Get cotton harvested acres for all states in recent year",
        "parameters": {
            "commodity_desc": "COTTON",
            "statisticcat_desc": "AREA HARVESTED",
            "year": "2022",
            "agg_level_desc": "STATE"
        }
    }
}

# NOTE: The examples above use basic, reliable parameter combinations.
# Advanced queries (organic, county-level, price data) may require additional
# parameters or may not have data available for all commodities/years/states.
# 
# To add advanced queries:
# 1. Test them first on https://quickstats.nass.usda.gov/
# 2. Verify data exists for your parameter combination
# 3. Add to EXAMPLE_QUERIES above
#
# For troubleshooting, see NASS_TROUBLESHOOTING.md


def check_connector_status():
    """
    Check if USDA NASS connector is configured and active.
    
    Returns:
        tuple: (is_ready: bool, message: str)
    """
    try:
        config_model = ConnectorConfig()
        config = config_model.get_by_source_id("usda_quickstats")
        
        if not config:
            return False, "USDA NASS connector not found. Run: python add_connectors.py usda_quickstats"
        
        if not config.get("active"):
            return False, "USDA NASS connector is inactive. Update configuration to set active=true"
        
        if not config.get("api_key"):
            return False, "USDA NASS connector has no API key. Add API key to configuration"
        
        return True, "USDA NASS connector is ready"
        
    except Exception as e:
        return False, f"Error checking connector: {str(e)}"


def execute_query(parameters, use_cache=True, show_details=True):
    """
    Execute a query against USDA NASS QuickStats.
    
    Args:
        parameters: Query parameters dictionary
        use_cache: Whether to use cached results
        show_details: Whether to show detailed output
        
    Returns:
        dict: Query results
    """
    query_engine = QueryEngine()
    
    if show_details:
        print("\n" + "="*70)
        print("EXECUTING NASS QUERY")
        print("="*70)
        print(f"\nParameters:")
        for key, value in parameters.items():
            print(f"  {key}: {value}")
        print(f"\nCache: {'Enabled' if use_cache else 'Disabled'}")
        print("\nQuerying...", end=" ", flush=True)
    
    try:
        result = query_engine.execute_query(
            "usda_quickstats",
            parameters,
            use_cache=use_cache
        )
        
        if show_details:
            print("Done!")
        
        return result
        
    except Exception as e:
        if show_details:
            print(f"Error!")
            print(f"\nError: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }


def display_results(result, max_records=10, query_name=None):
    """
    Display query results in a formatted way.
    
    Args:
        result: Query result dictionary
        max_records: Maximum number of records to display
        query_name: Optional name of the query being displayed
    """
    print("\n" + "="*70)
    print("QUERY RESULTS")
    print("="*70)
    
    if not result.get("success"):
        print(f"\n✗ Query failed: {result.get('error', 'Unknown error')}")
        return
    
    data = result.get("data", {})
    metadata = data.get("metadata", {})
    records = data.get("data", [])
    
    # Show metadata
    if query_name:
        print(f"\nQuery: {query_name}")
    print(f"Source: {result.get('source', 'unknown')}")
    print(f"Timestamp: {metadata.get('timestamp', 'N/A')}")
    print(f"Total Records: {metadata.get('record_count', 0)}")
    
    if not records:
        print("\nNo data returned.")
        return
    
    # Show sample records
    print(f"\nShowing first {min(max_records, len(records))} record(s):")
    print("-"*70)
    
    for i, record in enumerate(records[:max_records], 1):
        print(f"\nRecord {i}:")
        for key, value in record.items():
            # Show key fields prominently
            if key in ['commodity_desc', 'state_alpha', 'year', 'Value', 'unit_desc']:
                print(f"  {key}: {value}")
        
        # Show a few other fields
        other_fields = [k for k in record.keys() 
                       if k not in ['commodity_desc', 'state_alpha', 'year', 'Value', 'unit_desc']]
        if other_fields[:3]:
            print(f"  ... and {len(other_fields)} more fields")
    
    if len(records) > max_records:
        print(f"\n... and {len(records) - max_records} more record(s)")
    
    print("="*70 + "\n")


def run_example(example_num):
    """
    Run a specific example query.
    
    Args:
        example_num: Example number to run
        
    Returns:
        bool: True if successful
    """
    if example_num not in EXAMPLE_QUERIES:
        print(f"Example {example_num} not found.")
        print(f"Available examples: {list(EXAMPLE_QUERIES.keys())}")
        return False
    
    example = EXAMPLE_QUERIES[example_num]
    
    print("\n" + "="*70)
    print(f"EXAMPLE {example_num}: {example['name']}")
    print("="*70)
    print(f"\n{example['description']}\n")
    
    result = execute_query(example['parameters'])
    display_results(result, query_name=example['name'])
    
    return result.get("success", False)


def run_all_examples():
    """Run all example queries."""
    print("\n" + "="*70)
    print("RUNNING ALL NASS QUERY EXAMPLES")
    print("="*70)
    
    results = []
    
    for num in sorted(EXAMPLE_QUERIES.keys()):
        success = run_example(num)
        results.append((num, success))
        
        # Pause between queries
        if num < max(EXAMPLE_QUERIES.keys()):
            input("\nPress Enter to continue to next example...")
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    successful = sum(1 for _, success in results if success)
    print(f"\nTotal Examples: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {len(results) - successful}")
    print("="*70 + "\n")


def run_custom_query():
    """Run a custom user-defined query."""
    print("\n" + "="*70)
    print("CUSTOM NASS QUERY")
    print("="*70)
    
    print("""
Common NASS Query Parameters:
  - commodity_desc: Commodity name (e.g., CORN, WHEAT, CATTLE)
  - year: Year (e.g., 2022)
  - state_alpha: State abbreviation (e.g., IA, IL, TX)
  - statisticcat_desc: Statistic category (e.g., PRODUCTION, YIELD, AREA PLANTED)
  - unit_desc: Unit of measurement (e.g., BU, ACRES, LB)
  - county_name: County name (optional)
  - agg_level_desc: Aggregation level (COUNTY, STATE, NATIONAL)

Example query structure:
{
    "commodity_desc": "CORN",
    "year": "2022",
    "state_alpha": "IA",
    "statisticcat_desc": "PRODUCTION"
}
""")
    
    print("\nEnter your query parameters (JSON format):")
    print("(or press Ctrl+C to cancel)")
    
    try:
        lines = []
        print("\n{")
        while True:
            line = input()
            if line.strip() == "}":
                lines.append(line)
                break
            lines.append(line)
        
        json_str = "\n".join(lines)
        parameters = json.loads(json_str)
        
        result = execute_query(parameters)
        display_results(result)
        
    except KeyboardInterrupt:
        print("\n\nCancelled.")
    except json.JSONDecodeError as e:
        print(f"\nInvalid JSON: {str(e)}")
    except Exception as e:
        print(f"\nError: {str(e)}")


def export_results(result, filename=None):
    """
    Export query results to a JSON file.
    
    Args:
        result: Query result dictionary
        filename: Output filename (auto-generated if None)
    """
    if not result.get("success"):
        print("Cannot export failed query.")
        return
    
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"nass_query_{timestamp}.json"
    
    try:
        with open(filename, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\n✓ Results exported to: {filename}")
    except Exception as e:
        print(f"\n✗ Export failed: {str(e)}")


def list_examples():
    """List all available example queries."""
    print("\n" + "="*70)
    print("AVAILABLE NASS QUERY EXAMPLES")
    print("="*70 + "\n")
    
    for num, example in sorted(EXAMPLE_QUERIES.items()):
        print(f"{num}. {example['name']}")
        print(f"   {example['description']}")
        print()


def show_help():
    """Show usage help."""
    print("""
USDA NASS QuickStats Query Examples

Usage:
    python query_nass.py                    # Run all examples
    python query_nass.py --example <num>    # Run specific example
    python query_nass.py --list             # List all examples
    python query_nass.py --custom           # Run custom query
    python query_nass.py --help             # Show this help

Examples:
    python query_nass.py --example 1
    python query_nass.py --example 5

Prerequisites:
    1. MongoDB must be running
    2. Run: python add_connectors.py usda_quickstats
    3. Add API key to connector configuration
    4. Set active=true in connector configuration
    
Get USDA NASS API Key:
    https://quickstats.nass.usda.gov/api

Documentation:
    https://quickstats.nass.usda.gov/api
""")


def main():
    """Main entry point."""
    print("\n" + "="*70)
    print("USDA NASS QUICKSTATS QUERY TOOL")
    print("="*70 + "\n")
    
    # Check connector status
    is_ready, message = check_connector_status()
    print(f"Connector Status: {message}")
    
    if not is_ready:
        print("\nPlease configure the USDA NASS connector before running queries.")
        print("\nSteps:")
        print("1. Get API key from: https://quickstats.nass.usda.gov/api")
        print("2. Run: python add_connectors.py usda_quickstats")
        print("3. Update with API key:")
        print("   curl -X PUT http://localhost:5000/api/v1/sources/usda_quickstats \\")
        print("     -H 'Content-Type: application/json' \\")
        print("     -d '{\"api_key\": \"YOUR_KEY\", \"active\": true}'")
        print()
        sys.exit(1)
    
    print()
    
    # Parse command line arguments
    if len(sys.argv) == 1:
        # No arguments - run all examples
        run_all_examples()
    
    elif sys.argv[1] in ["-h", "--help", "help"]:
        show_help()
    
    elif sys.argv[1] in ["-l", "--list", "list"]:
        list_examples()
    
    elif sys.argv[1] in ["-c", "--custom", "custom"]:
        run_custom_query()
    
    elif sys.argv[1] in ["-e", "--example", "example"]:
        if len(sys.argv) < 3:
            print("Error: Example number required")
            print("Usage: python query_nass.py --example <num>")
            sys.exit(1)
        
        try:
            example_num = int(sys.argv[2])
            run_example(example_num)
        except ValueError:
            print(f"Error: Invalid example number: {sys.argv[2]}")
            sys.exit(1)
    
    else:
        print(f"Unknown option: {sys.argv[1]}")
        print("Run 'python query_nass.py --help' for usage information")
        sys.exit(1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        sys.exit(0)
