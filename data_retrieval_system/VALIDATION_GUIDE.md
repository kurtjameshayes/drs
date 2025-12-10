# Connector Validation Script

## Overview

The `validate_connectors.py` script tests all configured connectors by running their `validate()` method. This helps you verify that:
- Connectors can establish connections
- API keys and credentials are valid
- File paths are correct and accessible
- Services are reachable

## Usage

### Validate All Connectors

```bash
python validate_connectors.py
```

**Output Example:**
```
======================================================================
CONNECTOR VALIDATION REPORT
======================================================================
Timestamp: 2024-11-28T03:45:00.000000
Database: data_retrieval_system
======================================================================

Found 3 connector(s) in database

----------------------------------------------------------------------
Source ID: usda_quickstats
Name: USDA NASS QuickStats
Type: usda_nass
Active: True
Testing connection... ✓ SUCCESS
Validating credentials... ✓ SUCCESS
Status: VALID

Capabilities:
  - Pagination: True
  - Filtering: True
  - Sorting: False
----------------------------------------------------------------------

----------------------------------------------------------------------
Source ID: census_acs5
Name: US Census ACS 5-Year Data
Type: census
Active: True
Testing connection... ✓ SUCCESS
Validating credentials... ✓ SUCCESS
Status: VALID

Capabilities:
  - Pagination: False
  - Filtering: True
  - Sorting: False
----------------------------------------------------------------------

----------------------------------------------------------------------
Source ID: sample_csv
Name: Sample CSV Data
Type: local_file
Active: False
Status: SKIPPED (inactive)
----------------------------------------------------------------------

======================================================================
VALIDATION SUMMARY
======================================================================
Total Connectors: 3
Active: 2
Inactive: 1

Validation Results (Active Only):
  ✓ Valid: 2
  ✗ Invalid: 0
  ⚠ Errors: 0
======================================================================
```

### Validate Specific Connector

```bash
python validate_connectors.py <source_id>
```

**Example:**
```bash
python validate_connectors.py usda_quickstats
```

**Output Example:**
```
======================================================================
VALIDATING CONNECTOR: usda_quickstats
======================================================================

Name: USDA NASS QuickStats
Type: usda_nass
Active: True

Step 1: Testing connection... ✓ SUCCESS
Step 2: Validating credentials... ✓ SUCCESS

✓ Connector is fully validated and ready to use!

Capabilities:
  ✓ Supports Pagination: True
  ✓ Supports Filtering: True
  ✗ Supports Sorting: False
  • Source Id: usda_quickstats
  • Source Name: USDA NASS QuickStats
  • Connected: True
  • Data Formats: ['JSON', 'CSV', 'XML']
  • Api Documentation: https://quickstats.nass.usda.gov/api
```

## What Gets Validated

### For API Connectors (USDA NASS, Census.gov)
- ✓ API endpoint is reachable
- ✓ API key is valid
- ✓ Authentication succeeds
- ✓ Test query returns valid data

### For Local File Connectors
- ✓ File exists at specified path
- ✓ File is readable
- ✓ File format can be parsed
- ✓ Data can be loaded

## Common Issues and Solutions

### Issue: "Invalid API key"
**Solution:** Update the connector configuration with a valid API key:
```bash
curl -X PUT http://localhost:5000/api/v1/sources/usda_quickstats \
  -H "Content-Type: application/json" \
  -d '{"api_key": "YOUR_VALID_API_KEY"}'
```

### Issue: "File not found"
**Solution:** Update the file path in the connector configuration:
```bash
curl -X PUT http://localhost:5000/api/v1/sources/sample_csv \
  -H "Content-Type: application/json" \
  -d '{"file_path": "/correct/path/to/file.csv"}'
```

### Issue: "Connector is marked as inactive"
**Solution:** Activate the connector:
```bash
curl -X PUT http://localhost:5000/api/v1/sources/sample_csv \
  -H "Content-Type: application/json" \
  -d '{"active": true}'
```

### Issue: "Unknown connector type"
**Solution:** Check that the connector type is one of:
- `usda_nass`
- `census`
- `local_file`

## Integration with Workflow

### Recommended Workflow

1. **Initialize Database**
   ```bash
   python init_db.py
   ```

2. **Update Configurations**
   - Add API keys
   - Set correct file paths
   - Activate connectors

3. **Validate Connectors**
   ```bash
   python validate_connectors.py
   ```

4. **Fix Any Issues**
   - Update configurations as needed
   - Re-run validation

5. **Start API Server**
   ```bash
   python main.py
   ```

## Automation

### Pre-Deployment Check
Add to your deployment script:
```bash
#!/bin/bash
echo "Validating connectors before deployment..."
python validate_connectors.py

if [ $? -eq 0 ]; then
    echo "All connectors validated successfully!"
    python main.py
else
    echo "Connector validation failed. Please fix issues before deploying."
    exit 1
fi
```

### Cron Job for Monitoring
Check connector health periodically:
```bash
# Run validation daily at 6 AM
0 6 * * * /path/to/validate_connectors.py >> /var/log/connector_validation.log 2>&1
```

## Exit Codes

- `0` - All validations passed
- `1` - One or more validations failed or errors occurred

Use exit codes in scripts:
```bash
python validate_connectors.py usda_quickstats
if [ $? -eq 0 ]; then
    echo "Connector is ready!"
else
    echo "Connector validation failed!"
fi
```

## Tips

1. **Run after configuration changes** - Always validate after updating connector settings
2. **Check before production** - Validate all connectors before deploying to production
3. **Monitor regularly** - Set up periodic validation to catch service disruptions
4. **Keep credentials secure** - Use environment variables or secure vaults for API keys
5. **Test locally first** - Validate file connectors work with local paths before deployment
