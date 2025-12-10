#!/usr/bin/env python3
"""
FBI Crime Data Explorer API Query Examples

This script demonstrates how to execute queries against the FBI Crime Data Explorer API
using the data retrieval system's query engine.

Prerequisites:
1. MongoDB must be running
2. FBI Crime Data connector must be configured with valid API key
3. Connector must be active

Usage:
    python query_fbi.py                  # Run all example queries
    python query_fbi.py --example <num>  # Run specific example
    python query_fbi.py --custom         # Run custom query
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
        "name": "National Crime Estimates",
        "description": "Get national crime estimates for recent years",
        "parameters": {
            "endpoint": "estimates/national",
            "from": "2015",
            "to": "2021"
        },
        "notes": "Returns violent crime, property crime, and other statistics at national level"
    },
    
    2: {
        "name": "California Crime Estimates",
        "description": "Get crime estimates for California",
        "parameters": {
            "endpoint": "estimates/states/CA",
            "from": "2018",
            "to": "2021"
        },
        "notes": "State-level crime statistics for California"
    },
    
    3: {
        "name": "Texas Violent Crime Estimates",
        "description": "Get violent crime estimates for Texas",
        "parameters": {
            "endpoint": "estimates/states/TX/violent-crime",
            "from": "2018",
            "to": "2021"
        },
        "notes": "Violent crime data specifically for Texas"
    },
    
    4: {
        "name": "New York Property Crime",
        "description": "Get property crime estimates for New York",
        "parameters": {
            "endpoint": "estimates/states/NY/property-crime",
            "from": "2018",
            "to": "2021"
        },
        "notes": "Property crime statistics for New York state"
    },
    
    5: {
        "name": "Illinois Crime - Recent Year",
        "description": "Get all crime estimates for Illinois for recent year",
        "parameters": {
            "endpoint": "estimates/states/IL",
            "from": "2020",
            "to": "2020"
        },
        "notes": "Complete crime statistics for Illinois for 2020"
    },
    
    6: {
        "name": "Florida Violent Crime - Multi-Year",
        "description": "Get violent crime trend for Florida",
        "parameters": {
            "endpoint": "estimates/states/FL/violent-crime",
            "from": "2017",
            "to": "2021"
        },
        "notes": "5-year trend of violent crime in Florida"
    },
    
    7: {
        "name": "Washington State Crime Estimates",
        "description": "Get crime estimates for Washington state",
        "parameters": {
            "endpoint": "estimates/states/WA",
            "from": "2019",
            "to": "2021"
        },
        "notes": "Crime statistics for Washington state"
    },
    
    8: {
        "name": "Pennsylvania Property Crime Trend",
        "description": "Get property crime trend for Pennsylvania",
        "parameters": {
            "endpoint": "estimates/states/PA/property-crime",
            "from": "2016",
            "to": "2021"
        },
        "notes": "6-year property crime trend for Pennsylvania"
    }
}

# NOTE: The examples above use basic, reliable parameter combinations.
# The FBI Crime Data API provides national and state-level crime statistics.
# 
# Available endpoints:
# - estimates/national - National estimates
# - estimates/states/{state_abbr} - State estimates
# - estimates/states/{state_abbr}/violent-crime - State violent crime
# - estimates/states/{state_abbr}/property-crime - State property crime
#
# For more information: https://crime-data-explorer.fr.cloud.gov/pages/docApi


# Common State Abbreviations for reference
STATE_ABBR = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming"
}


def check_connector_status():
    """
    Check if FBI Crime Data connector is configured and active.
    
    Returns:
        tuple: (is_ready: bool, message: str)
    """
    try:
        config_model = ConnectorConfig()
        config = config_model.get_by_source_id("fbi_crime")
        
        if not config:
            return False, "FBI Crime Data connector not found. Run: python add_connectors.py fbi_crime"
        
        if not config.get("active"):
            return False, "FBI Crime Data connector is inactive. Update configuration to set active=true"
        
        if not config.get("api_key"):
            return False, "FBI Crime Data connector has no API key. Add API key to configuration"
        
        return True, "FBI Crime Data connector is ready"
        
    except Exception as e:
        return False, f"Error checking connector: {str(e)}"


def execute_query(parameters, use_cache=True, show_details=True):
    """
    Execute a query against FBI Crime Data Explorer API.
    
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
        print("EXECUTING FBI CRIME DATA QUERY")
        print("="*70)
        print(f"\nEndpoint: {parameters.get('endpoint', 'N/A')}")
        if 'from' in parameters:
            print(f"Year Range: {parameters.get('from')} to {parameters.get('to')}")
        print(f"\nCache: {'Enabled' if use_cache else 'Disabled'}")
        print("\nQuerying...", end=" ", flush=True)
    
    try:
        result = query_engine.execute_query(
            "fbi_crime",
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


def display_results(result, max_records=10, example_notes=None, query_name=None):
    """
    Display query results in a formatted way.
    
    Args:
        result: Query result dictionary
        max_records: Maximum number of records to display
        example_notes: Optional notes about the query
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
    
    if example_notes:
        print(f"\nNote: {example_notes}")
    
    if not records:
        print("\nNo data returned.")
        return
    
    # Show sample records
    print(f"\nShowing first {min(max_records, len(records))} record(s):")
    print("-"*70)
    
    for i, record in enumerate(records[:max_records], 1):
        print(f"\nRecord {i}:")
        
        # Try to show most relevant fields first
        priority_fields = ['year', 'state_abbr', 'state_name', 'population']
        
        # Show priority fields
        for field in priority_fields:
            if field in record:
                value = record[field]
                # Format large numbers with commas
                if isinstance(value, (int, float)) and field == 'population':
                    value = f"{value:,}"
                print(f"  {field}: {value}")
        
        # Show crime statistics
        crime_fields = ['violent_crime', 'homicide', 'rape', 'robbery', 'aggravated_assault',
                       'property_crime', 'burglary', 'larceny', 'motor_vehicle_theft', 'arson']
        
        for field in crime_fields:
            if field in record:
                value = record[field]
                if isinstance(value, (int, float)):
                    print(f"  {field}: {value:,}")
        
        # Show other fields (limited)
        other_fields = {k: v for k, v in record.items() 
                       if k not in priority_fields + crime_fields}
        
        shown_other = 0
        for key, value in other_fields.items():
            if shown_other < 3:
                if isinstance(value, (int, float)):
                    print(f"  {key}: {value:,}")
                else:
                    print(f"  {key}: {value}")
                shown_other += 1
        
        if len(other_fields) > 3:
            print(f"  ... and {len(other_fields) - 3} more field(s)")
    
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
    display_results(result, example_notes=example.get('notes'), query_name=example['name'])
    
    return result.get("success", False)


def run_all_examples():
    """Run all example queries."""
    print("\n" + "="*70)
    print("RUNNING ALL FBI CRIME DATA QUERY EXAMPLES")
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
    print("CUSTOM FBI CRIME DATA QUERY")
    print("="*70)
    
    print("""
Common FBI Crime Data Query Parameters:

  endpoint: API endpoint path
    Examples:
    - "estimates/national" - National estimates
    - "estimates/states/CA" - California state estimates
    - "estimates/states/TX/violent-crime" - Texas violent crime
    - "estimates/states/NY/property-crime" - New York property crime
  
  from: Start year (e.g., "2018")
  to: End year (e.g., "2021")

Available State Abbreviations:
  AL, AK, AZ, AR, CA, CO, CT, DE, DC, FL, GA, HI, ID, IL, IN, IA, KS, KY,
  LA, ME, MD, MA, MI, MN, MS, MO, MT, NE, NV, NH, NJ, NM, NY, NC, ND, OH,
  OK, OR, PA, RI, SC, SD, TN, TX, UT, VT, VA, WA, WV, WI, WY

Example query structure:
{
    "endpoint": "estimates/states/CA",
    "from": "2019",
    "to": "2021"
}

API Documentation: https://crime-data-explorer.fr.cloud.gov/pages/docApi
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


def list_examples():
    """List all available example queries."""
    print("\n" + "="*70)
    print("AVAILABLE FBI CRIME DATA QUERY EXAMPLES")
    print("="*70 + "\n")
    
    for num, example in sorted(EXAMPLE_QUERIES.items()):
        print(f"{num}. {example['name']}")
        print(f"   {example['description']}")
        if 'notes' in example:
            print(f"   Note: {example['notes']}")
        print()


def show_help():
    """Show usage help."""
    print("""
FBI Crime Data Explorer API Query Examples

Usage:
    python query_fbi.py                  # Run all examples
    python query_fbi.py --example <num>  # Run specific example
    python query_fbi.py --list           # List all examples
    python query_fbi.py --custom         # Run custom query
    python query_fbi.py --help           # Show this help

Examples:
    python query_fbi.py --example 1
    python query_fbi.py --example 2

Prerequisites:
    1. MongoDB must be running
    2. Run: python add_connectors.py fbi_crime
    3. Ensure API key is configured
    4. Set active=true in connector configuration

Get FBI Crime Data API Key:
    Sign up at: https://api.data.gov/signup

Resources:
    - API Documentation: https://crime-data-explorer.fr.cloud.gov/pages/docApi
    - Crime Data Explorer: https://crime-data-explorer.fr.cloud.gov/
    - API Home: https://api.data.gov/
""")


def show_states():
    """Display state abbreviations reference."""
    print("\n" + "="*70)
    print("STATE ABBREVIATIONS REFERENCE")
    print("="*70 + "\n")
    
    for abbr, name in sorted(STATE_ABBR.items(), key=lambda x: x[1]):
        print(f"{abbr}: {name}")
    
    print("\n" + "="*70)
    print("Use these abbreviations in endpoint parameters")
    print("Example: 'endpoint': 'estimates/states/CA'")
    print("="*70 + "\n")


def main():
    """Main entry point."""
    print("\n" + "="*70)
    print("FBI CRIME DATA EXPLORER API QUERY TOOL")
    print("="*70 + "\n")
    
    # Check connector status
    is_ready, message = check_connector_status()
    print(f"Connector Status: {message}")
    
    if not is_ready:
        print("\nPlease configure the FBI Crime Data connector before running queries.")
        print("\nSteps:")
        print("1. Get API key from: https://api.data.gov/signup")
        print("2. Run: python add_connectors.py fbi_crime")
        print("3. Update with API key:")
        print("   curl -X PUT http://localhost:5000/api/v1/sources/fbi_crime \\")
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
    
    elif sys.argv[1] in ["-s", "--states", "states"]:
        show_states()
    
    elif sys.argv[1] in ["-e", "--example", "example"]:
        if len(sys.argv) < 3:
            print("Error: Example number required")
            print("Usage: python query_fbi.py --example <num>")
            sys.exit(1)
        
        try:
            example_num = int(sys.argv[2])
            run_example(example_num)
        except ValueError:
            print(f"Error: Invalid example number: {sys.argv[2]}")
            sys.exit(1)
    
    else:
        print(f"Unknown option: {sys.argv[1]}")
        print("Run 'python query_fbi.py --help' for usage information")
        sys.exit(1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        sys.exit(0)
