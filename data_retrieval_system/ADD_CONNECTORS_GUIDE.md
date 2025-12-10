# Add Connectors Script

## Overview

The `add_connectors.py` file is the central location for managing connector configurations. This file is designed to be easily modified to add new connectors to the system.

## Purpose

- **Single Source of Truth**: All connector configurations are defined in one place
- **Idempotent**: Safe to run multiple times - only adds connectors that don't exist
- **Easy to Modify**: Simple list structure for adding new connectors
- **Version Controlled**: Track connector configurations in your repository

## Usage

### Add All Connectors

```bash
python add_connectors.py
```

Adds all connectors defined in the `CONNECTOR_CONFIGS` list.

### Add Specific Connector

```bash
python add_connectors.py <source_id>
```

Examples:
```bash
python add_connectors.py usda_quickstats
python add_connectors.py census_api
```

### List Available Connectors

```bash
python add_connectors.py --list
```

Shows all connectors available in the file.

## Pre-Configured Connectors

The file comes with two connectors ready to use:

### 1. USDA NASS QuickStats
- **source_id**: `usda_quickstats`
- **Type**: `usda_nass`
- **API Key Required**: Yes
- **Get API Key**: https://quickstats.nass.usda.gov/api

### 2. US Census Bureau API
- **source_id**: `census_api`
- **Type**: `census`
- **API Key Required**: Optional (but recommended)
- **Get API Key**: https://api.census.gov/data/key_signup.html

## Adding New Connectors

### Step 1: Edit the File

Open `add_connectors.py` and add your connector to the `CONNECTOR_CONFIGS` list:

```python
CONNECTOR_CONFIGS = [
    # ... existing connectors ...
    
    # Your new connector
    {
        "source_id": "my_data_source",
        "source_name": "My Data Source",
        "connector_type": "local_file",  # or "usda_nass", "census"
        "file_path": "/path/to/data.csv",
        "file_type": "csv",
        "encoding": "utf-8",
        "active": False,
        "description": "My custom data source",
        "notes": "Update file_path before activating"
    },
]
```

### Step 2: Run the Script

```bash
python add_connectors.py
```

Or add just your new connector:
```bash
python add_connectors.py my_data_source
```

### Step 3: Update Configuration

Update API keys or other settings:

**Option A: Edit the file directly**
- Update the `api_key` field in the configuration
- Run `python add_connectors.py` again (safe, won't duplicate)

**Option B: Use the API**
```bash
curl -X PUT http://localhost:5000/api/v1/sources/my_data_source \
  -H "Content-Type: application/json" \
  -d '{"api_key": "your_key", "active": true}'
```

### Step 4: Validate

```bash
python validate_connectors.py my_data_source
```

## Connector Configuration Fields

### Required Fields

- **source_id**: Unique identifier (string)
- **source_name**: Human-readable name (string)
- **connector_type**: Type of connector (string)
  - `"usda_nass"` - USDA NASS QuickStats
  - `"census"` - US Census Bureau
  - `"local_file"` - Local files (CSV, JSON, Excel, Parquet)

### Type-Specific Fields

#### USDA NASS (`usda_nass`)
```python
{
    "source_id": "usda_example",
    "connector_type": "usda_nass",
    "url": "https://quickstats.nass.usda.gov/api",
    "api_key": "",
    "format": "JSON",  # or "CSV", "XML"
    "max_retries": 3,
    "retry_delay": 1
}
```

#### Census (`census`)
```python
{
    "source_id": "census_example",
    "connector_type": "census",
    "url": "https://api.census.gov/data",
    "api_key": "",  # Optional
    "max_retries": 3,
    "retry_delay": 1
}
```

#### Local File (`local_file`)
```python
{
    "source_id": "local_example",
    "connector_type": "local_file",
    "file_path": "/path/to/data.csv",
    "file_type": "csv",  # or "json", "excel", "parquet"
    "encoding": "utf-8",
    "delimiter": ","  # For CSV files
}
```

### Optional Fields

- **active**: Enable/disable connector (boolean, default: False)
- **description**: Detailed description (string)
- **documentation**: URL to API documentation (string)
- **notes**: Important notes or instructions (string)

## Example Workflows

### Workflow 1: Add USDA NASS with API Key

1. **Get API Key** from https://quickstats.nass.usda.gov/api

2. **Edit add_connectors.py**:
```python
{
    "source_id": "usda_quickstats",
    "api_key": "YOUR_API_KEY_HERE",  # Add your key
    "active": True,  # Enable it
    # ... rest of config
}
```

3. **Add to database**:
```bash
python add_connectors.py usda_quickstats
```

4. **Validate**:
```bash
python validate_connectors.py usda_quickstats
```

### Workflow 2: Add Local CSV File

1. **Edit add_connectors.py**:
```python
{
    "source_id": "sales_data",
    "source_name": "Sales Data CSV",
    "connector_type": "local_file",
    "file_path": "/data/sales_2024.csv",
    "file_type": "csv",
    "encoding": "utf-8",
    "delimiter": ",",
    "active": True
}
```

2. **Add to database**:
```bash
python add_connectors.py sales_data
```

3. **Validate**:
```bash
python validate_connectors.py sales_data
```

### Workflow 3: Multiple Instances of Same Type

You can have multiple connectors of the same type:

```python
CONNECTOR_CONFIGS = [
    {
        "source_id": "usda_crops",
        "source_name": "USDA QuickStats - Crops",
        "connector_type": "usda_nass",
        "api_key": "KEY1",
        # ... config
    },
    {
        "source_id": "usda_livestock",
        "source_name": "USDA QuickStats - Livestock",
        "connector_type": "usda_nass",
        "api_key": "KEY1",  # Can reuse same key
        # ... config
    },
]
```

## Best Practices

### 1. Use Descriptive IDs
```python
# Good
"source_id": "usda_quickstats_crops_iowa"

# Avoid
"source_id": "data1"
```

### 2. Add Documentation URLs
```python
{
    "source_id": "my_api",
    "documentation": "https://api.example.com/docs",
    "notes": "Rate limit: 1000 requests/hour"
}
```

### 3. Start with active=False
```python
{
    "active": False,  # Test first before enabling
}
```

### 4. Include Useful Notes
```python
{
    "notes": "Get API key from: https://example.com/signup"
}
```

### 5. Version Control
Commit this file to your repository to track configuration changes.

## Security Considerations

### Don't Commit API Keys

**Option 1: Environment Variables**
```python
import os

{
    "source_id": "usda_quickstats",
    "api_key": os.getenv("USDA_API_KEY", ""),
}
```

**Option 2: Separate Config File**
```python
from config_secrets import API_KEYS

{
    "source_id": "usda_quickstats",
    "api_key": API_KEYS.get("usda", ""),
}
```

Then add `config_secrets.py` to `.gitignore`.

**Option 3: Update After Adding**
```bash
# Add without key
python add_connectors.py usda_quickstats

# Update with key via API
curl -X PUT http://localhost:5000/api/v1/sources/usda_quickstats \
  -H "Content-Type: application/json" \
  -d '{"api_key": "SECRET_KEY"}'
```

## Troubleshooting

### "Already exists (skipped)"
The connector is already in the database. To update:
```bash
# Use the API
curl -X PUT http://localhost:5000/api/v1/sources/<source_id> \
  -H "Content-Type: application/json" \
  -d '{"field": "new_value"}'

# Or delete and re-add
curl -X DELETE http://localhost:5000/api/v1/sources/<source_id>
python add_connectors.py <source_id>
```

### "Connector not found in configuration list"
Check that the `source_id` matches exactly what's in the file:
```bash
python add_connectors.py --list
```

### MongoDB Connection Failed
Ensure MongoDB is running:
```bash
# Check if MongoDB is running
systemctl status mongodb

# Or
brew services list | grep mongodb
```

## Integration

### CI/CD Pipeline
```yaml
# .github/workflows/deploy.yml
- name: Add Connectors
  run: python add_connectors.py
  
- name: Validate Connectors
  run: python validate_connectors.py
```

### Docker Entrypoint
```bash
#!/bin/bash
python add_connectors.py
python main.py
```

### Deployment Script
```bash
#!/bin/bash
echo "Adding connectors..."
python add_connectors.py

echo "Validating connectors..."
python validate_connectors.py

if [ $? -eq 0 ]; then
    echo "Starting API server..."
    python main.py
else
    echo "Validation failed. Fix issues before deploying."
    exit 1
fi
```

## Summary

The `add_connectors.py` file is your central configuration hub:

✅ **Easy to Use** - Simple list structure  
✅ **Safe** - Idempotent, won't duplicate  
✅ **Flexible** - Add any connector type  
✅ **Documented** - Self-documenting with comments  
✅ **Version Controlled** - Track changes in git  
✅ **Validated** - Works with validation script  

Modify the `CONNECTOR_CONFIGS` list to add new connectors as your needs grow!
