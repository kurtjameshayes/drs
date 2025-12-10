#!/usr/bin/env python3
"""
US Census Bureau API Query Examples

This script demonstrates how to execute queries against the US Census Bureau API
using the data retrieval system's query engine.

Prerequisites:
1. MongoDB must be running
2. Census API connector must be configured (API key optional but recommended)
3. Connector must be active

Usage:
    python query_census.py                  # Run all example queries
    python query_census.py --example <num>  # Run specific example
    python query_census.py --custom         # Run custom query
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
        "name": "State Population - 2020 Decennial Census",
        "description": "Get total population for all states from 2020 Census",
        "parameters": {
            "dataset": "2020/dec/pl",
            "get": "NAME,P1_001N",
            "for": "state:*"
        },
        "notes": "P1_001N is total population"
    },
    
    2: {
        "name": "County Population - Specific State",
        "description": "Get population for all counties in California",
        "parameters": {
            "dataset": "2020/dec/pl",
            "get": "NAME,P1_001N",
            "for": "county:*",
            "in": "state:06"
        },
        "notes": "State code 06 is California"
    },
    
    3: {
        "name": "ACS 5-Year Data - Median Household Income",
        "description": "Get median household income for all states",
        "parameters": {
            "dataset": "2021/acs/acs5",
            "get": "NAME,B19013_001E",
            "for": "state:*"
        },
        "notes": "B19013_001E is median household income in past 12 months"
    },
    
    4: {
        "name": "Poverty Estimates by State",
        "description": "Get poverty estimates for all states",
        "parameters": {
            "dataset": "timeseries/poverty/saipe",
            "get": "NAME,SAEPOVRTALL_PT,YEAR",
            "for": "state:*",
            "time": "2021"
        },
        "notes": "SAEPOVRTALL_PT is poverty rate for all ages"
    },
    
    5: {
        "name": "Educational Attainment - Bachelor's Degree or Higher",
        "description": "Get percentage with bachelor's degree or higher by state",
        "parameters": {
            "dataset": "2021/acs/acs5",
            "get": "NAME,B15003_022E,B15003_001E",
            "for": "state:*"
        },
        "notes": "B15003_022E is bachelor's degree, B15003_001E is total"
    },
    
    6: {
        "name": "Population by Age and Sex - Specific State",
        "description": "Get age/sex demographics for Texas",
        "parameters": {
            "dataset": "2021/acs/acs5",
            "get": "NAME,B01001_001E,B01001_002E,B01001_026E",
            "for": "state:48"
        },
        "notes": "B01001_001E total, B01001_002E male, B01001_026E female. State 48 is Texas"
    },
    
    7: {
        "name": "Housing Units by State",
        "description": "Get total housing units for all states",
        "parameters": {
            "dataset": "2020/dec/pl",
            "get": "NAME,H1_001N",
            "for": "state:*"
        },
        "notes": "H1_001N is total housing units"
    },
    
    8: {
        "name": "Median Age by County - Specific State",
        "description": "Get median age for counties in New York",
        "parameters": {
            "dataset": "2021/acs/acs5",
            "get": "NAME,B01002_001E",
            "for": "county:*",
            "in": "state:36"
        },
        "notes": "B01002_001E is median age. State 36 is New York"
    }
}

# NOTE: The examples above use reliable Census API parameter combinations.
# The Census API has many datasets and variables. Always verify:
# 1. Dataset exists and is accessible
# 2. Variables (get parameters) are valid for that dataset
# 3. Geographic levels (for/in) are compatible
#
# To find variables and datasets:
# - Browse: https://api.census.gov/data.html
# - Variables: https://api.census.gov/data/<year>/<dataset>/variables.html
#
# For troubleshooting, see CENSUS_TROUBLESHOOTING.md (if available)


# Common State FIPS Codes for reference
STATE_FIPS = {
    "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
    "06": "California", "08": "Colorado", "09": "Connecticut", "10": "Delaware",
    "11": "District of Columbia", "12": "Florida", "13": "Georgia", "15": "Hawaii",
    "16": "Idaho", "17": "Illinois", "18": "Indiana", "19": "Iowa",
    "20": "Kansas", "21": "Kentucky", "22": "Louisiana", "23": "Maine",
    "24": "Maryland", "25": "Massachusetts", "26": "Michigan", "27": "Minnesota",
    "28": "Mississippi", "29": "Missouri", "30": "Montana", "31": "Nebraska",
    "32": "Nevada", "33": "New Hampshire", "34": "New Jersey", "35": "New Mexico",
    "36": "New York", "37": "North Carolina", "38": "North Dakota", "39": "Ohio",
    "40": "Oklahoma", "41": "Oregon", "42": "Pennsylvania", "44": "Rhode Island",
    "45": "South Carolina", "46": "South Dakota", "47": "Tennessee", "48": "Texas",
    "49": "Utah", "50": "Vermont", "51": "Virginia", "53": "Washington",
    "54": "West Virginia", "55": "Wisconsin", "56": "Wyoming", "72": "Puerto Rico"
}


def check_connector_status():
    """
    Check if Census API connector is configured and active.
    
    Returns:
        tuple: (is_ready: bool, message: str)
    """
    try:
        config_model = Connector Config()
        config = config_model.get_by_source_id("census_api")
        
        if not config:
            return False, "Census API connector not found. Run: python add_connectors.py census_api"
        
        if not config.get("active"):
            return False, "Census API connector is inactive. Update configuration to set active=true"
        
        api_key = config.get("api_key", "")
        if api_key:
            return True, "Census API connector is ready (with API key)"
        else:
            return True, "Census API connector is ready (without API key - rate limited)"
        
    except Exception as e:
        return False, f"Error checking connector: {str(e)}"


def execute_query(parameters, use_cache=True, show_details=True):
    """
    Execute a query against US Census Bureau API.
    
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
        print("EXECUTING CENSUS QUERY")
        print("="*70)
        print(f"\nDataset: {parameters.get('dataset', 'N/A')}")
        print(f"Variables: {parameters.get('get', 'N/A')}")
        print(f"Geography: {parameters.get('for', 'N/A')}")
        if 'in' in parameters:
            print(f"Within: {parameters.get('in')}")
        print(f"\nCache: {'Enabled' if use_cache else 'Disabled'}")
        print("\nQuerying...", end=" ", flush=True)
    
    try:
        result = query_engine.execute_query(
            "census_api",
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
        priority_fields = ['NAME', 'name', 'state', 'county']
        
        # Show priority fields
        for field in priority_fields:
            if field in record:
                print(f"  {field}: {record[field]}")
        
        # Show other fields
        other_fields = {k: v for k, v in record.items() if k not in priority_fields}
        for key, value in list(other_fields.items())[:5]:
            print(f"  {key}: {value}")
        
        if len(other_fields) > 5:
            print(f"  ... and {len(other_fields) - 5} more field(s)")
    
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
    print("RUNNING ALL CENSUS QUERY EXAMPLES")
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
    print("CUSTOM CENSUS QUERY")
    print("="*70)
    
    print("""
Common Census Query Parameters:

  dataset: Census dataset identifier
    Examples:
    - "2021/acs/acs5" - American Community Survey 5-Year
    - "2020/dec/pl" - 2020 Decennial Census
    - "timeseries/poverty/saipe" - Poverty estimates
  
  get: Variables to retrieve (comma-separated)
    Examples:
    - "NAME,B01001_001E" - Name and total population
    - "NAME,B19013_001E" - Name and median household income
  
  for: Geographic level
    Examples:
    - "state:*" - All states
    - "state:06" - California
    - "county:*" - All counties (use with 'in')
  
  in: Parent geography (optional)
    Examples:
    - "state:06" - Within California
    - "state:*" - Within all states

Example query structure:
{
    "dataset": "2021/acs/acs5",
    "get": "NAME,B01001_001E",
    "for": "state:*"
}

Browse datasets: https://api.census.gov/data.html
Find variables: https://api.census.gov/data/<year>/<dataset>/variables.html
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
    print("AVAILABLE CENSUS QUERY EXAMPLES")
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
US Census Bureau API Query Examples

Usage:
    python query_census.py                  # Run all examples
    python query_census.py --example <num>  # Run specific example
    python query_census.py --list           # List all examples
    python query_census.py --custom         # Run custom query
    python query_census.py --help           # Show this help

Examples:
    python query_census.py --example 1
    python query_census.py --example 3

Prerequisites:
    1. MongoDB must be running
    2. Run: python add_connectors.py census_api
    3. (Optional) Add API key for higher rate limits
    4. Set active=true in connector configuration

Get Census API Key (Optional but Recommended):
    https://api.census.gov/data/key_signup.html

Resources:
    - Browse Datasets: https://api.census.gov/data.html
    - API Documentation: https://www.census.gov/data/developers/guidance.html
    - Variable Discovery: Use /variables.html endpoint for each dataset
""")


def show_state_codes():
    """Display common state FIPS codes."""
    print("\n" + "="*70)
    print("STATE FIPS CODES REFERENCE")
    print("="*70 + "\n")
    
    for code, name in sorted(STATE_FIPS.items(), key=lambda x: x[1]):
        print(f"{code}: {name}")
    
    print("\n" + "="*70)
    print("Use these codes in 'for' or 'in' parameters")
    print("Example: 'for': 'state:06' for California")
    print("="*70 + "\n")


def main():
    """Main entry point."""
    print("\n" + "="*70)
    print("US CENSUS BUREAU API QUERY TOOL")
    print("="*70 + "\n")
    
    # Check connector status
    is_ready, message = check_connector_status()
    print(f"Connector Status: {message}")
    
    if not is_ready:
        print("\nPlease configure the Census API connector before running queries.")
        print("\nSteps:")
        print("1. Run: python add_connectors.py census_api")
        print("2. (Optional) Get API key from: https://api.census.gov/data/key_signup.html")
        print("3. (Optional) Update with API key:")
        print("   curl -X PUT http://localhost:5000/api/v1/sources/census_api \\")
        print("     -H 'Content-Type: application/json' \\")
        print("     -d '{\"api_key\": \"YOUR_KEY\", \"active\": true}'")
        print("\nNote: API key is optional but provides higher rate limits")
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
        show_state_codes()
    
    elif sys.argv[1] in ["-e", "--example", "example"]:
        if len(sys.argv) < 3:
            print("Error: Example number required")
            print("Usage: python query_census.py --example <num>")
            sys.exit(1)
        
        try:
            example_num = int(sys.argv[2])
            run_example(example_num)
        except ValueError:
            print(f"Error: Invalid example number: {sys.argv[2]}")
            sys.exit(1)
    
    else:
        print(f"Unknown option: {sys.argv[1]}")
        print("Run 'python query_census.py --help' for usage information")
        sys.exit(1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        sys.exit(0)
