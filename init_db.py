#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.connector_config import ConnectorConfig
from pymongo import MongoClient
from config import Config
import json

def check_mongodb_connection():
    try:
        client = MongoClient(Config.MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        print("✓ MongoDB connection successful")
        return True
    except Exception as e:
        print(f"✗ MongoDB connection failed: {str(e)}")
        print("\nPlease ensure MongoDB is running at:", Config.MONGO_URI)
        return False

def initialize_connectors():
    print("\n" + "="*60)
    print("Initializing Connector Configurations")
    print("="*60)
    
    config_model = ConnectorConfig()
    
    connectors = [
        {
            "source_id": "usda_quickstats",
            "source_name": "USDA NASS QuickStats",
            "connector_type": "usda_nass",
            "url": "https://quickstats.nass.usda.gov/api",
            "api_key": "YOUR_USDA_API_KEY_HERE",
            "format": "JSON",
            "max_retries": 3,
            "retry_delay": 1,
            "active": False,
            "description": "USDA National Agricultural Statistics Service QuickStats API"
        },
        {
            "source_id": "census_acs5",
            "source_name": "US Census ACS 5-Year Data",
            "connector_type": "census",
            "url": "https://api.census.gov/data",
            "api_key": "YOUR_CENSUS_API_KEY_HERE",
            "max_retries": 3,
            "retry_delay": 1,
            "active": False,
            "description": "US Census Bureau American Community Survey 5-Year Data"
        },
        {
            "source_id": "sample_csv",
            "source_name": "Sample CSV Data",
            "connector_type": "local_file",
            "file_path": "/path/to/sample_data.csv",
            "file_type": "csv",
            "encoding": "utf-8",
            "delimiter": ",",
            "active": False,
            "description": "Sample local CSV file connector"
        }
    ]
    
    created = 0
    updated = 0
    
    for connector in connectors:
        source_id = connector["source_id"]
        existing = config_model.get_by_source_id(source_id)
        
        if existing:
            print(f"\n{source_id}: Already exists (skipping)")
            updated += 1
        else:
            try:
                config_model.create(connector)
                print(f"\n{source_id}: Created")
                created += 1
            except Exception as e:
                print(f"\n{source_id}: Failed - {str(e)}")
    
    print("\n" + "="*60)
    print(f"Summary: {created} created, {updated} already existed")
    print("="*60)

def create_sample_data_file():
    print("\n" + "="*60)
    print("Creating Sample Data Files")
    print("="*60)
    
    sample_csv = """product_name,category,price,quantity,date
Laptop,Electronics,999.99,5,2024-01-15
Mouse,Electronics,24.99,50,2024-01-15
Keyboard,Electronics,79.99,30,2024-01-15
Monitor,Electronics,299.99,15,2024-01-15
Desk,Furniture,399.99,10,2024-01-16
Chair,Furniture,249.99,20,2024-01-16"""
    
    try:
        data_dir = os.path.join(os.path.dirname(__file__), "sample_data")
        os.makedirs(data_dir, exist_ok=True)
        
        csv_path = os.path.join(data_dir, "sample_products.csv")
        with open(csv_path, "w") as f:
            f.write(sample_csv)
        print(f"\n✓ Created sample CSV: {csv_path}")
        
    except Exception as e:
        print(f"\n✗ Failed to create sample files: {str(e)}")

def main():
    print("="*60)
    print("Data Retrieval System - Database Initialization")
    print("="*60)
    
    if not check_mongodb_connection():
        return
    
    initialize_connectors()
    create_sample_data_file()
    
    print("\nNext Steps:")
    print("1. Update connector configurations with API keys")
    print("2. Set file_path for local file connectors")
    print("3. Set active=True for connectors you want to use")
    print("4. Run: python main.py")

if __name__ == '__main__':
    main()
