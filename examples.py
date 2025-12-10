#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.connector_config import ConnectorConfig
from core.query_engine import QueryEngine
import json

def setup_example_connectors():
    config_model = ConnectorConfig()
    
    print("Setting up example connectors...")
    
    usda_config = {
        "source_id": "usda_quickstats",
        "source_name": "USDA NASS QuickStats",
        "connector_type": "usda_nass",
        "url": "https://quickstats.nass.usda.gov/api",
        "api_key": "YOUR_API_KEY_HERE",
        "format": "JSON",
        "active": False
    }
    
    existing = config_model.get_by_source_id("usda_quickstats")
    if not existing:
        config_model.create(usda_config)
        print("✓ Created USDA NASS QuickStats connector")
    else:
        print("✓ USDA NASS QuickStats connector already configured")

def example_local_file_query():
    print("\n" + "="*60)
    print("Example: Local File Query")
    print("="*60)
    
    query_engine = QueryEngine()
    
    parameters = {
        "columns": ["product_name", "price", "quantity"],
        "filters": {"price": {"$gt": 50.0}},
        "limit": 10
    }
    
    print(f"\nQuery Parameters:")
    print(json.dumps(parameters, indent=2))
    
    print("\nNote: Update connector configuration with actual file path to run this query")

def list_all_sources():
    print("\n" + "="*60)
    print("Configured Data Sources")
    print("="*60)
    
    from core.connector_manager import ConnectorManager
    connector_manager = ConnectorManager()
    connector_manager.load_connectors()
    
    sources = connector_manager.list_sources()
    print(f"\nTotal sources: {len(sources)}")
    
    for source in sources:
        print(f"\n{source['source_id']}")
        print(f"  Connected: {source['connected']}")

def main():
    print("\n" + "="*60)
    print("Data Retrieval System - Examples")
    print("="*60)
    
    print("\n1. Setting up example connectors...")
    setup_example_connectors()
    
    print("\n2. Listing all configured sources...")
    list_all_sources()
    
    print("\n" + "="*60)
    print("Note: Query examples require:")
    print("1. Valid API keys for USDA NASS and Census.gov")
    print("2. Actual data files for local file connectors")
    print("="*60)

if __name__ == '__main__':
    main()
