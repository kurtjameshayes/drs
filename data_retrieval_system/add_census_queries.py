#!/usr/bin/env python3
"""
Add Census Queries to MongoDB

This script adds pre-configured Census API queries for:
- SNAP (Supplemental Nutrition Assistance Program) attributes by ZIP code
- Education levels by ZIP code
- Household types by ZIP code

MongoDB URI: mongodb+srv://kurtjhayes_db_user:Rvw6cndMQjWOilXj@cluster0.ngyd1r7.mongodb.net/?appName=Cluster0
Database: data_retrieval_system

Usage:
    python add_census_queries.py              # Add all queries
    python add_census_queries.py --list       # List queries to be added
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.stored_query import StoredQuery
from pymongo import MongoClient
import json

# MongoDB Configuration
MONGO_URI = "mongodb+srv://kurtjhayes_db_user:Rvw6cndMQjWOilXj@cluster0.ngyd1r7.mongodb.net/?appName=Cluster0"
DATABASE_NAME = "data_retrieval_system"

# Census API Queries
CENSUS_QUERIES = [
    # ========================================================================
    # SNAP (Food Assistance) Queries
    # ========================================================================
    {
        "query_id": "snap_all_attributes_by_zip",
        "query_name": "SNAP - All Attributes by ZIP Code",
        "connector_id": "census_api",
        "description": "Retrieve all SNAP/food assistance attributes for a specific ZIP code. Includes total households, households receiving SNAP, households with and without children, income brackets, and demographic breakdowns.",
        "parameters": {
            "dataset": "2022/acs/acs5",
            "get": "NAME,B22010_001E,B22010_002E,B22010_003E,B22010_004E,B22010_005E,B22010_006E",
            "for": "zip code tabulation area:*"
        },
        "tags": ["census", "snap", "food-assistance", "zip-code", "social-services"],
        "notes": {
            "variables": {
                "B22010_001E": "Total households",
                "B22010_002E": "Households receiving SNAP in past 12 months",
                "B22010_003E": "Households receiving SNAP with children under 18",
                "B22010_004E": "Households receiving SNAP with no children",
                "B22010_005E": "Households NOT receiving SNAP",
                "B22010_006E": "Households NOT receiving SNAP with children under 18"
            },
            "usage": "To query specific ZIP code, add parameter: 'for': 'zip code tabulation area:12345'",
            "data_year": "ACS 5-Year estimates (most recent: 2022)",
            "source": "American Community Survey Table B22010"
        }
    },

    {
        "query_id": "snap_by_income_by_zip",
        "query_name": "SNAP - Participation by Income Level by ZIP Code",
        "connector_id": "census_api",
        "description": "SNAP participation broken down by household income brackets for ZIP codes. Shows which income levels receive food assistance.",
        "parameters": {
            "dataset": "2022/acs/acs5",
            "get": "NAME,B22003_001E,B22003_002E,B22003_003E,B22003_004E,B22003_005E",
            "for": "zip code tabulation area:*"
        },
        "tags": ["census", "snap", "income", "zip-code", "poverty"],
        "notes": {
            "variables": {
                "B22003_001E": "Total households",
                "B22003_002E": "Households with income below poverty level receiving SNAP",
                "B22003_003E": "Households with income below poverty level NOT receiving SNAP",
                "B22003_004E": "Households with income at/above poverty receiving SNAP",
                "B22003_005E": "Households with income at/above poverty NOT receiving SNAP"
            },
            "source": "American Community Survey Table B22003"
        }
    },

    # ========================================================================
    # Education Level Queries
    # ========================================================================
    {
        "query_id": "education_all_levels_by_zip",
        "query_name": "Education - All Levels by ZIP Code",
        "connector_id": "census_api",
        "description": "Complete educational attainment breakdown for population 25 years and over by ZIP code. Includes all education levels from no schooling through doctorate degrees.",
        "parameters": {
            "dataset": "2022/acs/acs5",
            "get": "NAME,B15003_001E,B15003_002E,B15003_017E,B15003_018E,B15003_019E,B15003_020E,B15003_021E,B15003_022E,B15003_023E,B15003_024E,B15003_025E",
            "for": "zip code tabulation area:*"
        },
        "tags": ["census", "education", "attainment", "zip-code", "demographics"],
        "notes": {
            "variables": {
                "B15003_001E": "Total population 25 years and over",
                "B15003_002E": "No schooling completed",
                "B15003_017E": "Regular high school diploma",
                "B15003_018E": "GED or alternative credential",
                "B15003_019E": "Some college, less than 1 year",
                "B15003_020E": "Some college, 1 or more years, no degree",
                "B15003_021E": "Associate's degree",
                "B15003_022E": "Bachelor's degree",
                "B15003_023E": "Master's degree",
                "B15003_024E": "Professional school degree",
                "B15003_025E": "Doctorate degree"
            },
            "population": "Population 25 years and over",
            "source": "American Community Survey Table B15003"
        }
    },

    {
        "query_id": "education_summary_by_zip",
        "query_name": "Education - Summary Categories by ZIP Code",
        "connector_id": "census_api",
        "description": "Simplified education level summary: less than high school, high school graduate, some college, bachelor's degree, and graduate degree.",
        "parameters": {
            "dataset": "2022/acs/acs5",
            "get": "NAME,B15002_001E,B15002_003E,B15002_011E,B15002_015E,B15002_016E,B15002_017E,B15002_018E",
            "for": "zip code tabulation area:*"
        },
        "tags": ["census", "education", "summary", "zip-code"],
        "notes": {
            "variables": {
                "B15002_001E": "Total population 25 years and over",
                "B15002_003E": "Male: No schooling completed",
                "B15002_011E": "Male: High school graduate (includes equivalency)",
                "B15002_015E": "Male: Bachelor's degree",
                "B15002_016E": "Male: Master's degree",
                "B15002_017E": "Male: Professional school degree",
                "B15002_018E": "Male: Doctorate degree"
            },
            "note": "Table B15002 separates by sex - combine male/female for totals",
            "source": "American Community Survey Table B15002"
        }
    },

    # ========================================================================
    # Household Type Queries
    # ========================================================================
    {
        "query_id": "household_all_types_by_zip",
        "query_name": "Household Types - All Categories by ZIP Code",
        "connector_id": "census_api",
        "description": "Complete household type breakdown by ZIP code including family households, married couples, single parents, non-family households, and living alone statistics.",
        "parameters": {
            "dataset": "2022/acs/acs5",
            "get": "NAME,B11001_001E,B11001_002E,B11001_003E,B11001_005E,B11001_006E,B11001_007E,B11001_008E,B11001_009E",
            "for": "zip code tabulation area:*"
        },
        "tags": ["census", "household", "family", "zip-code", "demographics"],
        "notes": {
            "variables": {
                "B11001_001E": "Total households",
                "B11001_002E": "Family households",
                "B11001_003E": "Family households: Married-couple family",
                "B11001_005E": "Family households: Male householder, no spouse present",
                "B11001_006E": "Family households: Female householder, no spouse present",
                "B11001_007E": "Nonfamily households",
                "B11001_008E": "Nonfamily households: Householder living alone",
                "B11001_009E": "Nonfamily households: Householder not living alone"
            },
            "source": "American Community Survey Table B11001"
        }
    },

    {
        "query_id": "household_single_parent_by_zip",
        "query_name": "Household Types - Single Parent Families by ZIP Code",
        "connector_id": "census_api",
        "description": "Single parent household statistics by ZIP code, broken down by male and female householders with children present.",
        "parameters": {
            "dataset": "2022/acs/acs5",
            "get": "NAME,B11003_001E,B11003_010E,B11003_011E,B11003_012E,B11003_016E,B11003_017E,B11003_018E",
            "for": "zip code tabulation area:*"
        },
        "tags": ["census", "household", "single-parent", "family", "zip-code"],
        "notes": {
            "variables": {
                "B11003_001E": "Total families",
                "B11003_010E": "Male householder, no spouse present",
                "B11003_011E": "Male householder, no spouse present: With own children under 18",
                "B11003_012E": "Male householder, no spouse present: With own children under 6 only",
                "B11003_016E": "Female householder, no spouse present",
                "B11003_017E": "Female householder, no spouse present: With own children under 18",
                "B11003_018E": "Female householder, no spouse present: With own children under 6 only"
            },
            "source": "American Community Survey Table B11003"
        }
    },

    {
        "query_id": "household_children_presence_by_zip",
        "query_name": "Household Types - Presence of Children by ZIP Code",
        "connector_id": "census_api",
        "description": "Households categorized by presence and age of children, including households with no children, children under 6, children 6-17, etc.",
        "parameters": {
            "dataset": "2022/acs/acs5",
            "get": "NAME,B11005_001E,B11005_002E,B11005_003E,B11005_011E",
            "for": "zip code tabulation area:*"
        },
        "tags": ["census", "household", "children", "family", "zip-code"],
        "notes": {
            "variables": {
                "B11005_001E": "Total households",
                "B11005_002E": "Households with one or more people under 18 years",
                "B11005_003E": "Households with no people under 18 years",
                "B11005_011E": "Nonfamily households"
            },
            "source": "American Community Survey Table B11005"
        }
    },

    {
        "query_id": "household_single_adult_no_children_by_zip",
        "query_name": "Household Types - Single Adult No Children by ZIP Code",
        "connector_id": "census_api",
        "description": "Single adult households with no children present, broken down by age groups and living arrangements.",
        "parameters": {
            "dataset": "2022/acs/acs5",
            "get": "NAME,B11001_001E,B11001_007E,B11001_008E,B11005_003E,B11005_011E",
            "for": "zip code tabulation area:*"
        },
        "tags": ["census", "household", "single-adult", "no-children", "zip-code"],
        "notes": {
            "variables": {
                "B11001_001E": "Total households",
                "B11001_007E": "Nonfamily households",
                "B11001_008E": "Nonfamily households: Householder living alone",
                "B11005_003E": "Households with no people under 18 years",
                "B11005_011E": "Nonfamily households (from children presence table)"
            },
            "calculation": "Single adult no children = Nonfamily living alone AND no children under 18",
            "source": "American Community Survey Tables B11001 and B11005"
        }
    }
]


def check_mongodb():
    """Check if MongoDB is accessible."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        print("✓ MongoDB connection successful\n")
        return True
    except Exception as e:
        print(f"✗ MongoDB connection failed: {str(e)}")
        print(f"  URI: {MONGO_URI}\n")
        return False


def list_queries():
    """List all queries that will be added."""
    print("=" * 70)
    print("CENSUS QUERIES TO BE ADDED")
    print("=" * 70 + "\n")

    print(f"Total queries: {len(CENSUS_QUERIES)}\n")

    categories = {}
    for query in CENSUS_QUERIES:
        category = query['tags'][1] if len(query['tags']) > 1 else 'other'
        if category not in categories:
            categories[category] = []
        categories[category].append(query)

    for category, queries in categories.items():
        print(f"\n{category.upper()} Queries ({len(queries)}):")
        print("-" * 70)
        for query in queries:
            print(f"\n  ID: {query['query_id']}")
            print(f"  Name: {query['query_name']}")
            print(f"  Description: {query['description'][:80]}...")
            if 'notes' in query and 'variables' in query['notes']:
                print(f"  Variables: {len(query['notes']['variables'])} Census variables")

    print("\n" + "=" * 70)
    print(f"Total: {len(CENSUS_QUERIES)} queries")
    print("=" * 70 + "\n")


def add_queries():
    """Add all Census queries to MongoDB."""
    print("=" * 70)
    print("ADDING CENSUS QUERIES TO MONGODB")
    print("=" * 70 + "\n")

    stored_query = StoredQuery()

    results = {
        "added": 0,
        "updated": 0,
        "failed": 0,
        "skipped": 0
    }

    for query_data in CENSUS_QUERIES:
        query_id = query_data['query_id']

        try:
            # Check if query exists
            existing = stored_query.get_by_id(query_id)

            if existing:
                # Update existing query
                stored_query.update(query_id, query_data)
                print(f"⟳ Updated: {query_id}")
                print(f"  Name: {query_data['query_name']}")
                results["updated"] += 1
            else:
                # Create new query
                stored_query.create(query_data)
                print(f"✓ Added: {query_id}")
                print(f"  Name: {query_data['query_name']}")
                results["added"] += 1

            print()

        except Exception as e:
            print(f"✗ Failed: {query_id}")
            print(f"  Error: {str(e)}\n")
            results["failed"] += 1

    # Print summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"✓ Added: {results['added']}")
    print(f"⟳ Updated: {results['updated']}")
    print(f"✗ Failed: {results['failed']}")
    print("=" * 70 + "\n")

    if results["added"] > 0 or results["updated"] > 0:
        print("Next Steps:")
        print("1. List all queries:")
        print("   python manage_queries.py --list")
        print()
        print("2. View specific query:")
        print("   python manage_queries.py --get snap_all_attributes_by_zip")
        print()
        print("3. Execute a query:")
        print("   python manage_queries.py --execute snap_all_attributes_by_zip")
        print()
        print("4. Customize for specific ZIP:")
        print("   Edit query parameters: 'for': 'zip code tabulation area:12345'")
        print()


def show_query_details(query_id):
    """Show detailed information about a specific query."""
    query = next((q for q in CENSUS_QUERIES if q['query_id'] == query_id), None)

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
    print(json.dumps(query['parameters'], indent=2))
    print()

    if 'notes' in query:
        print("Notes:")
        if 'variables' in query['notes']:
            print("\nVariables:")
            for var, desc in query['notes']['variables'].items():
                print(f"  {var}: {desc}")

        for key, value in query['notes'].items():
            if key != 'variables' and isinstance(value, str):
                print(f"\n{key.title()}: {value}")

    print("\nTags:", ", ".join(query['tags']))
    print()


def show_usage():
    """Show usage information."""
    print("""
Add Census Queries to MongoDB

This script adds pre-configured Census API queries for demographic data by ZIP code.

Queries included:
  - SNAP (food assistance) attributes
  - Education levels and attainment
  - Household types (single parent, single adult, etc.)

Usage:
    python add_census_queries.py              # Add all queries
    python add_census_queries.py --list       # List queries to be added
    python add_census_queries.py --show <id>  # Show query details

Examples:
    # Add all queries to MongoDB
    python add_census_queries.py

    # List what will be added
    python add_census_queries.py --list

    # Show details for specific query
    python add_census_queries.py --show snap_all_attributes_by_zip

    # After adding, execute queries with manage_queries.py:
    python manage_queries.py --execute snap_all_attributes_by_zip
    python manage_queries.py --execute education_all_levels_by_zip
    python manage_queries.py --execute household_single_parent_by_zip

Query Categories:
    SNAP/Food Assistance:
      - snap_all_attributes_by_zip
      - snap_by_income_by_zip

    Education:
      - education_all_levels_by_zip
      - education_summary_by_zip

    Household Types:
      - household_all_types_by_zip
      - household_single_parent_by_zip
      - household_children_presence_by_zip
      - household_single_adult_no_children_by_zip

Database:
    URI: mongodb+srv://kurtjhayes_db_user:...@cluster0.ngyd1r7.mongodb.net/
    Database: data_retrieval_system
    Collection: stored_queries
""")


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command in ['-h', '--help', 'help']:
            show_usage()
            return

        elif command in ['-l', '--list', 'list']:
            list_queries()
            return

        elif command in ['-s', '--show', 'show']:
            if len(sys.argv) < 3:
                print("Error: Query ID required")
                print("Usage: python add_census_queries.py --show <query_id>")
                return
            show_query_details(sys.argv[2])
            return

        else:
            print(f"Unknown command: {command}")
            show_usage()
            return

    # Default action: add queries
    print("\n" + "=" * 70)
    print("ADD CENSUS QUERIES TO MONGODB")
    print("=" * 70 + "\n")

    # Check MongoDB connection
    if not check_mongodb():
        sys.exit(1)

    # Add queries
    add_queries()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        sys.exit(0)