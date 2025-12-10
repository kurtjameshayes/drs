#!/usr/bin/env python3
"""
Add/Update Connector Configurations

This script adds or updates connector configurations in MongoDB.
If a connector already exists, it will be updated with the new configuration.
Modify the CONNECTOR_CONFIGS list below to add new connectors or update existing ones.

Usage:
    python add_connectors.py           # Add/update all connectors
    python add_connectors.py <id>      # Add/update specific connector by source_id
    python add_connectors.py --list    # List available connectors
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.connector_config import ConnectorConfig
from pymongo import MongoClient
from config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# CONNECTOR CONFIGURATIONS
# Add new connector configurations to this list
# ============================================================================

CONNECTOR_CONFIGS = [
    # USDA NASS QuickStats Connector
    {
        "source_id": "usda_quickstats",
        "source_name": "USDA NASS QuickStats",
        "connector_type": "usda_nass",
        "url": "https://quickstats.nass.usda.gov/api",
        "api_key": "3C820E9E-DF04-3474-A1F7-A31014391B71",  # API key configured
        "format": "JSON",
        "max_retries": 3,
        "retry_delay": 1,
        "active": True,  # Set to True once API key is added
        "description": "USDA National Agricultural Statistics Service QuickStats API",
        "documentation": "https://quickstats.nass.usda.gov/api",
        "notes": "Get API key from: https://quickstats.nass.usda.gov/api"
    },
    
    # US Census Bureau API Connector
    {
        "source_id": "census_api",
        "source_name": "US Census Bureau API",
        "connector_type": "census",
        "url": "https://api.census.gov/data",
        "api_key": "520d25bd9288bc2a6cec5806e715d8ffc29c6812",  # API key configured
        "max_retries": 3,
        "retry_delay": 1,
        "active": True,  # Set to True when ready to use
        "description": "US Census Bureau Data API - Access to demographic and economic data",
        "documentation": "https://www.census.gov/data/developers/guidance.html",
        "notes": "API key is optional but provides higher rate limits. Get key from: https://api.census.gov/data/key_signup.html",
        "common_datasets": [
            "2020/acs/acs5 - American Community Survey 5-Year Data",
            "2020/dec/pl - Decennial Census Redistricting Data",
            "timeseries/poverty/saipe - Small Area Income and Poverty Estimates"
        ]
    },
    
    # ========================================================================
    # ADD NEW CONNECTORS BELOW THIS LINE
    # ========================================================================
    
    # Example: Local File Connector (uncomment and modify as needed)
    # {
    #     "source_id": "my_local_data",
    #     "source_name": "My Local Data File",
    #     "connector_type": "local_file",
    #     "file_path": "/path/to/your/data.csv",
    #     "file_type": "csv",
    #     "encoding": "utf-8",
    #     "delimiter": ",",
    #     "active": False,
    #     "description": "Local CSV file containing custom data",
    #     "notes": "Update file_path before activating"
    # },
    
    # Example: Another USDA NASS instance for different dataset
    # {
    #     "source_id": "usda_quickstats_livestock",
    #     "source_name": "USDA NASS QuickStats - Livestock",
    #     "connector_type": "usda_nass",
    #     "url": "https://quickstats.nass.usda.gov/api",
    #     "api_key": "",
    #     "format": "JSON",
    #     "max_retries": 3,
    #     "retry_delay": 1,
    #     "active": False,
    #     "description": "USDA QuickStats focused on livestock data",
    #     "documentation": "https://quickstats.nass.usda.gov/api"
    # },
    
    # Example: Census ACS 1-Year Data
    # {
    #     "source_id": "census_acs1",
    #     "source_name": "US Census ACS 1-Year Data",
    #     "connector_type": "census",
    #     "url": "https://api.census.gov/data",
    #     "api_key": "",
    #     "max_retries": 3,
    #     "retry_delay": 1,
    #     "active": False,
    #     "description": "US Census American Community Survey 1-Year Estimates"
    # },
    
    # FBI Crime Data Explorer
    {
        "source_id": "fbi_crime",
        "source_name": "FBI Crime Data Explorer",
        "connector_type": "fbi_crime",
        "url": "https://api.usa.gov/crime/fbi/sapi",
        "api_key": "iSNiIUIPpFPIanf4l9DdCDPZZK7yppVF0tlviXy3",
        "format": "JSON",
        "active": True,
        "description": "FBI Crime Data Explorer API - National and state crime statistics",
        "documentation": "https://crime-data-explorer.fr.cloud.gov/pages/docApi",
        "notes": "Provides access to national and state-level crime statistics, agency data, and offense data"
    }
]


def check_mongodb():
    """Check if MongoDB is accessible."""
    try:
        client = MongoClient(Config.MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        logger.info("✓ MongoDB connection successful")
        return True
    except Exception as e:
        logger.error(f"✗ MongoDB connection failed: {str(e)}")
        logger.error(f"  Ensure MongoDB is running at: {Config.MONGO_URI}")
        return False


def add_connector(config_data, config_model):
    """
    Add or update a connector configuration.
    
    Args:
        config_data: Dictionary containing connector configuration
        config_model: ConnectorConfig model instance
        
    Returns:
        tuple: (success: bool, action: str, message: str)
            action: 'added', 'updated', or 'failed'
    """
    source_id = config_data.get("source_id")
    
    if not source_id:
        return False, 'failed', "Missing source_id"
    
    # Check if already exists
    existing = config_model.get_by_source_id(source_id)
    
    try:
        if existing:
            # Update existing connector
            config_model.update(source_id, config_data)
            return True, 'updated', "Updated successfully"
        else:
            # Create new connector
            config_model.create(config_data)
            return True, 'added', "Added successfully"
    except Exception as e:
        return False, 'failed', f"Error: {str(e)}"


def add_all_connectors():
    """Add or update all connectors from CONNECTOR_CONFIGS list."""
    print("="*70)
    print("ADDING/UPDATING CONNECTOR CONFIGURATIONS")
    print("="*70)
    print(f"Database: {Config.DATABASE_NAME}")
    print(f"Total connectors to process: {len(CONNECTOR_CONFIGS)}")
    print("="*70 + "\n")
    
    config_model = ConnectorConfig()
    
    results = {
        "added": 0,
        "updated": 0,
        "failed": 0
    }
    
    for config_data in CONNECTOR_CONFIGS:
        source_id = config_data.get("source_id", "unknown")
        source_name = config_data.get("source_name", "Unknown")
        connector_type = config_data.get("connector_type", "unknown")
        
        print(f"Processing: {source_id}")
        print(f"  Name: {source_name}")
        print(f"  Type: {connector_type}")
        
        success, action, message = add_connector(config_data, config_model)
        
        if success:
            if action == 'added':
                print(f"  Status: ✓ {message}")
                results["added"] += 1
            elif action == 'updated':
                print(f"  Status: ⟳ {message}")
                results["updated"] += 1
            
            # Show important notes
            if "notes" in config_data:
                print(f"  Note: {config_data['notes']}")
        else:
            print(f"  Status: ✗ {message}")
            results["failed"] += 1
        
        print()
    
    # Print summary
    print("="*70)
    print("SUMMARY")
    print("="*70)
    print(f"✓ Added: {results['added']}")
    print(f"⟳ Updated: {results['updated']}")
    print(f"✗ Failed: {results['failed']}")
    print("="*70 + "\n")
    
    if results["added"] > 0 or results["updated"] > 0:
        print("Next Steps:")
        print("1. Validate connectors:")
        print("   python validate_connectors.py")
        print()
        print("2. Run example queries:")
        print("   python query_nass.py --example 1")
        print("   python query_census.py --example 1")
        print("   python query_fbi.py --example 1")
        print()
        print("3. Start the API server:")
        print("   python main.py")
        print()


def add_specific_connector(source_id):
    """
    Add or update a specific connector by source_id.
    
    Args:
        source_id: Source ID of the connector to add
        
    Returns:
        bool: True if successful, False otherwise
    """
    print("="*70)
    print(f"ADDING/UPDATING CONNECTOR: {source_id}")
    print("="*70 + "\n")
    
    # Find the connector in our configs
    config_data = None
    for config in CONNECTOR_CONFIGS:
        if config.get("source_id") == source_id:
            config_data = config
            break
    
    if not config_data:
        print(f"✗ Connector '{source_id}' not found in configuration list")
        print("\nAvailable connectors in this file:")
        for config in CONNECTOR_CONFIGS:
            print(f"  - {config.get('source_id')} ({config.get('connector_type')})")
        print()
        return False
    
    config_model = ConnectorConfig()
    
    # Display details
    print(f"Name: {config_data.get('source_name')}")
    print(f"Type: {config_data.get('connector_type')}")
    print(f"Description: {config_data.get('description', 'N/A')}")
    print()
    
    # Add or update the connector
    success, action, message = add_connector(config_data, config_model)
    
    if success:
        if action == 'added':
            print(f"✓ {message}\n")
        elif action == 'updated':
            print(f"⟳ {message}\n")
        
        if "notes" in config_data:
            print(f"Important: {config_data['notes']}\n")
        
        if "documentation" in config_data:
            print(f"Documentation: {config_data['documentation']}\n")
        
        print("Next Steps:")
        print(f"1. Validate: python validate_connectors.py {source_id}")
        print(f"2. Run queries (if applicable)")
        print()
        return True
    else:
        print(f"✗ {message}")
        print()
        return False


def list_available_connectors():
    """List all connectors available in this file."""
    print("="*70)
    print("AVAILABLE CONNECTORS")
    print("="*70 + "\n")
    
    for config in CONNECTOR_CONFIGS:
        source_id = config.get("source_id")
        source_name = config.get("source_name")
        connector_type = config.get("connector_type")
        description = config.get("description", "No description")
        
        print(f"{source_id}")
        print(f"  Name: {source_name}")
        print(f"  Type: {connector_type}")
        print(f"  Description: {description}")
        print()


def show_usage():
    """Show usage instructions."""
    print("="*70)
    print("ADD/UPDATE CONNECTORS - Usage")
    print("="*70)
    print()
    print("Add or update all connectors:")
    print("  python add_connectors.py")
    print()
    print("Add or update specific connector:")
    print("  python add_connectors.py <source_id>")
    print()
    print("List available connectors:")
    print("  python add_connectors.py --list")
    print()
    print("Examples:")
    print("  python add_connectors.py usda_quickstats")
    print("  python add_connectors.py census_api")
    print("  python add_connectors.py fbi_crime")
    print()
    print("Note: If a connector already exists, it will be updated with")
    print("      the configuration from this file.")
    print()


def main():
    """Main entry point."""
    # Check for help flag
    if len(sys.argv) > 1 and sys.argv[1] in ["-h", "--help", "help"]:
        show_usage()
        return
    
    # Check for list flag
    if len(sys.argv) > 1 and sys.argv[1] in ["-l", "--list", "list"]:
        list_available_connectors()
        return
    
    print("\n" + "="*70)
    print("DATA RETRIEVAL SYSTEM - ADD/UPDATE CONNECTORS")
    print("="*70 + "\n")
    
    # Check MongoDB
    if not check_mongodb():
        sys.exit(1)
    
    print()
    
    # Parse arguments
    if len(sys.argv) > 1:
        # Add specific connector
        source_id = sys.argv[1]
        success = add_specific_connector(source_id)
        sys.exit(0 if success else 1)
    else:
        # Add all connectors
        add_all_connectors()


if __name__ == '__main__':
    main()
