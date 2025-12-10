#!/usr/bin/env python3
"""
Connector Validation Script

This script validates all configured connectors by testing their
connection and authentication credentials.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.connector_config import ConnectorConfig
from core.connector_manager import ConnectorManager
from pymongo import MongoClient
from config import Config
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_mongodb():
    """Check if MongoDB is accessible."""
    try:
        client = MongoClient(Config.MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        print("✓ MongoDB connection successful\n")
        return True
    except Exception as e:
        print(f"✗ MongoDB connection failed: {str(e)}")
        print(f"  Please ensure MongoDB is running at: {Config.MONGO_URI}\n")
        return False


def validate_all_connectors():
    """Validate all configured connectors."""
    print("="*70)
    print("CONNECTOR VALIDATION REPORT")
    print("="*70)
    print(f"Timestamp: {datetime.utcnow().isoformat()}")
    print(f"Database: {Config.DATABASE_NAME}")
    print("="*70 + "\n")
    
    config_model = ConnectorConfig()
    connector_manager = ConnectorManager(config_model)
    
    # Get all connectors (active and inactive)
    all_configs = config_model.get_all(active_only=False)
    
    if not all_configs:
        print("No connectors found in database.")
        print("Run 'python init_db.py' to create sample configurations.\n")
        return
    
    print(f"Found {len(all_configs)} connector(s) in database\n")
    
    results = {
        "total": len(all_configs),
        "active": 0,
        "inactive": 0,
        "valid": 0,
        "invalid": 0,
        "error": 0
    }
    
    for config in all_configs:
        source_id = config.get("source_id")
        source_name = config.get("source_name")
        connector_type = config.get("connector_type")
        is_active = config.get("active", False)
        
        print("-"*70)
        print(f"Source ID: {source_id}")
        print(f"Name: {source_name}")
        print(f"Type: {connector_type}")
        print(f"Active: {is_active}")
        
        if is_active:
            results["active"] += 1
        else:
            results["inactive"] += 1
            print(f"Status: SKIPPED (inactive)")
            print("-"*70 + "\n")
            continue
        
        # Attempt to validate the connector
        try:
            # Load the connector class
            connector_class = connector_manager._load_connector_class(connector_type)
            
            if not connector_class:
                print(f"Status: ERROR - Unknown connector type")
                results["error"] += 1
                print("-"*70 + "\n")
                continue
            
            # Create connector instance
            connector = connector_class(config)
            
            # Test connection
            print("Testing connection...", end=" ")
            can_connect = connector.connect()
            
            if not can_connect:
                print("✗ FAILED")
                print(f"Status: INVALID - Cannot establish connection")
                results["invalid"] += 1
                print("-"*70 + "\n")
                continue
            
            print("✓ SUCCESS")
            
            # Test validation
            print("Validating credentials...", end=" ")
            is_valid = connector.validate()
            
            if is_valid:
                print("✓ SUCCESS")
                print(f"Status: VALID")
                results["valid"] += 1
                
                # Get capabilities
                capabilities = connector.get_capabilities()
                print(f"\nCapabilities:")
                print(f"  - Pagination: {capabilities.get('supports_pagination', False)}")
                print(f"  - Filtering: {capabilities.get('supports_filtering', False)}")
                print(f"  - Sorting: {capabilities.get('supports_sorting', False)}")
            else:
                print("✗ FAILED")
                print(f"Status: INVALID - Validation failed")
                results["invalid"] += 1
            
            # Cleanup
            connector.disconnect()
            
        except ValueError as e:
            print(f"Status: ERROR - Configuration issue")
            print(f"Details: {str(e)}")
            results["error"] += 1
        except Exception as e:
            print(f"Status: ERROR - {type(e).__name__}")
            print(f"Details: {str(e)}")
            results["error"] += 1
        
        print("-"*70 + "\n")
    
    # Print summary
    print("="*70)
    print("VALIDATION SUMMARY")
    print("="*70)
    print(f"Total Connectors: {results['total']}")
    print(f"Active: {results['active']}")
    print(f"Inactive: {results['inactive']}")
    print(f"\nValidation Results (Active Only):")
    print(f"  ✓ Valid: {results['valid']}")
    print(f"  ✗ Invalid: {results['invalid']}")
    print(f"  ⚠ Errors: {results['error']}")
    print("="*70 + "\n")
    
    # Provide recommendations
    if results["invalid"] > 0 or results["error"] > 0:
        print("RECOMMENDATIONS:")
        print("-"*70)
        if results["invalid"] > 0:
            print("• Check API keys and credentials for invalid connectors")
            print("• Verify URLs and endpoints are accessible")
            print("• For local files, ensure paths are correct and readable")
        if results["error"] > 0:
            print("• Review error messages above for configuration issues")
            print("• Ensure all required fields are present in configurations")
        print("-"*70 + "\n")


def validate_specific_connector(source_id):
    """Validate a specific connector by source ID."""
    print("="*70)
    print(f"VALIDATING CONNECTOR: {source_id}")
    print("="*70 + "\n")
    
    config_model = ConnectorConfig()
    connector_manager = ConnectorManager(config_model)
    
    # Get the configuration
    config = config_model.get_by_source_id(source_id)
    
    if not config:
        print(f"✗ Connector '{source_id}' not found in database")
        print("\nAvailable connectors:")
        all_configs = config_model.get_all(active_only=False)
        for cfg in all_configs:
            print(f"  - {cfg['source_id']} ({cfg['connector_type']})")
        print()
        return False
    
    source_name = config.get("source_name")
    connector_type = config.get("connector_type")
    is_active = config.get("active", False)
    
    print(f"Name: {source_name}")
    print(f"Type: {connector_type}")
    print(f"Active: {is_active}\n")
    
    if not is_active:
        print("⚠ Warning: Connector is marked as inactive")
        print("  Set 'active: true' in the configuration to enable it\n")
    
    try:
        # Load the connector
        connector_class = connector_manager._load_connector_class(connector_type)
        
        if not connector_class:
            print(f"✗ Unknown connector type: {connector_type}")
            return False
        
        # Create and test
        connector = connector_class(config)
        
        print("Step 1: Testing connection...", end=" ")
        if not connector.connect():
            print("✗ FAILED")
            print("\nConnection could not be established.")
            print("Check configuration parameters.\n")
            return False
        print("✓ SUCCESS")
        
        print("Step 2: Validating credentials...", end=" ")
        if not connector.validate():
            print("✗ FAILED")
            print("\nValidation failed. This could mean:")
            print("  - Invalid API key or credentials")
            print("  - Service is unavailable")
            print("  - Incorrect endpoint URL")
            print("  - For local files: file not found or not readable\n")
            connector.disconnect()
            return False
        print("✓ SUCCESS")
        
        print("\n✓ Connector is fully validated and ready to use!\n")
        
        # Show capabilities
        capabilities = connector.get_capabilities()
        print("Capabilities:")
        for key, value in capabilities.items():
            if isinstance(value, bool):
                status = "✓" if value else "✗"
                print(f"  {status} {key.replace('_', ' ').title()}: {value}")
            else:
                print(f"  • {key.replace('_', ' ').title()}: {value}")
        print()
        
        # Cleanup
        connector.disconnect()
        return True
        
    except Exception as e:
        print(f"✗ ERROR: {type(e).__name__}")
        print(f"\nDetails: {str(e)}\n")
        return False


def main():
    """Main entry point."""
    print("\n" + "="*70)
    print("DATA RETRIEVAL SYSTEM - CONNECTOR VALIDATOR")
    print("="*70 + "\n")
    
    # Check MongoDB
    if not check_mongodb():
        sys.exit(1)
    
    # Parse arguments
    if len(sys.argv) > 1:
        # Validate specific connector
        source_id = sys.argv[1]
        success = validate_specific_connector(source_id)
        sys.exit(0 if success else 1)
    else:
        # Validate all connectors
        validate_all_connectors()


if __name__ == '__main__':
    main()
