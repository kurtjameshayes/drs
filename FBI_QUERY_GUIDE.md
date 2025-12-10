# FBI Crime Data Explorer Query Guide

## Overview

The `query_fbi.py` script provides easy access to the FBI Crime Data Explorer API using the data retrieval system's query engine.

## Features

✅ **8 Pre-Built Example Queries** - Ready to run  
✅ **Custom Query Support** - Enter your own parameters  
✅ **Formatted Output** - Clean, readable crime statistics  
✅ **State Reference** - Built-in state abbreviation lookup  
✅ **Caching Support** - Faster repeat queries  
✅ **Error Handling** - Clear error messages  
✅ **API Key Required** - Configured and ready to use  

## Prerequisites

1. **MongoDB** must be running
2. **FBI Crime Data connector** must be configured with API key
3. **Connector must be active**

### Setup (Already Configured!)

The FBI Crime Data connector is already configured with your API key:

```python
# In add_connectors.py
{
    "source_id": "fbi_crime",
    "api_key": "iSNiIUIPpFPIanf4l9DdCDPZZK7yppVF0tlviXy3",
    "active": True
}
```

## Usage

### Run All Example Queries

```bash
python query_fbi.py
```

### Run Specific Example

```bash
python query_fbi.py --example 1
python query_fbi.py --example 2
```

### List Available Examples

```bash
python query_fbi.py --list
```

### Run Custom Query

```bash
python query_fbi.py --custom
```

### Show State Abbreviations

```bash
python query_fbi.py --states
```

### Show Help

```bash
python query_fbi.py --help
```

## Pre-Built Example Queries

### Example 1: National Crime Estimates
Get national crime estimates for recent years
```python
{
    "endpoint": "estimates/national",
    "from": "2015",
    "to": "2021"
}
```
**Returns:** National violent crime, property crime, and other statistics

### Example 2: California Crime Estimates
Get crime estimates for California
```python
{
    "endpoint": "estimates/states/CA",
    "from": "2018",
    "to": "2021"
}
```
**Returns:** State-level crime statistics for California

### Example 3: Texas Violent Crime Estimates
Get violent crime estimates for Texas
```python
{
    "endpoint": "estimates/states/TX/violent-crime",
    "from": "2018",
    "to": "2021"
}
```
**Returns:** Violent crime data specifically for Texas

### Example 4: New York Property Crime
Get property crime estimates for New York
```python
{
    "endpoint": "estimates/states/NY/property-crime",
    "from": "2018",
    "to": "2021"
}
```
**Returns:** Property crime statistics for New York state

### Example 5: Illinois Crime - Recent Year
Get all crime estimates for Illinois for recent year
```python
{
    "endpoint": "estimates/states/IL",
    "from": "2020",
    "to": "2020"
}
```
**Returns:** Complete crime statistics for Illinois for 2020

### Example 6: Florida Violent Crime - Multi-Year
Get violent crime trend for Florida
```python
{
    "endpoint": "estimates/states/FL/violent-crime",
    "from": "2017",
    "to": "2021"
}
```
**Returns:** 5-year trend of violent crime in Florida

### Example 7: Washington State Crime Estimates
Get crime estimates for Washington state
```python
{
    "endpoint": "estimates/states/WA",
    "from": "2019",
    "to": "2021"
}
```
**Returns:** Crime statistics for Washington state

### Example 8: Pennsylvania Property Crime Trend
Get property crime trend for Pennsylvania
```python
{
    "endpoint": "estimates/states/PA/property-crime",
    "from": "2016",
    "to": "2021"
}
```
**Returns:** 6-year property crime trend for Pennsylvania

## Sample Output

```
======================================================================
EXAMPLE 1: National Crime Estimates
======================================================================

Get national crime estimates for recent years

======================================================================
EXECUTING FBI CRIME DATA QUERY
======================================================================

Endpoint: estimates/national
Year Range: 2015 to 2021

Cache: Enabled

Querying... Done!

======================================================================
QUERY RESULTS
======================================================================

Query: National Crime Estimates
Source: connector
Total Records: 7

Note: Returns violent crime, property crime, and other statistics at national level

Showing first 7 record(s):
----------------------------------------------------------------------

Record 1:
  year: 2015
  population: 320,896,618
  violent_crime: 1,199,310
  homicide: 15,696
  rape: 90,185
  robbery: 327,374
  aggravated_assault: 764,449
  property_crime: 7,993,631
  burglary: 1,579,527
  larceny: 5,706,346
  motor_vehicle_theft: 707,758

Record 2:
  year: 2016
  population: 323,405,935
  violent_crime: 1,250,162
  ...
```

## FBI Crime Data API Parameters

### Required Parameters

**endpoint** - API endpoint path
- Format: `"estimates/national"` or `"estimates/states/{state_abbr}"`
- Examples:
  - `"estimates/national"` - National estimates
  - `"estimates/states/CA"` - California estimates
  - `"estimates/states/TX/violent-crime"` - Texas violent crime
  - `"estimates/states/NY/property-crime"` - New York property crime

### Optional Parameters

**from** - Start year
- Format: `"2018"` (4-digit year as string)
- Determines the beginning of the time range

**to** - End year
- Format: `"2021"` (4-digit year as string)
- Determines the end of the time range

## Available Endpoints

### National Data
```python
"endpoint": "estimates/national"
```
Returns national crime estimates for all crime types

### State-Level Data
```python
"endpoint": "estimates/states/{STATE_ABBR}"
```
Returns all crime data for specified state

### State Violent Crime
```python
"endpoint": "estimates/states/{STATE_ABBR}/violent-crime"
```
Returns only violent crime data for specified state

### State Property Crime
```python
"endpoint": "estimates/states/{STATE_ABBR}/property-crime"
```
Returns only property crime data for specified state

## Crime Categories

### Violent Crime
- Homicide (murder and nonnegligent manslaughter)
- Rape
- Robbery
- Aggravated assault

### Property Crime
- Burglary
- Larceny-theft
- Motor vehicle theft
- Arson (when reported)

## Common State Abbreviations

Use `python query_fbi.py --states` to see the complete list. Common examples:

| Code | State | Code | State |
|------|-------|------|-------|
| CA | California | FL | Florida |
| TX | Texas | NY | New York |
| IL | Illinois | PA | Pennsylvania |
| OH | Ohio | GA | Georgia |
| NC | North Carolina | MI | Michigan |
| WA | Washington | AZ | Arizona |

## Common Query Patterns

### National Trends
```python
{
    "endpoint": "estimates/national",
    "from": "2010",
    "to": "2021"
}
```

### Single State - All Crime Types
```python
{
    "endpoint": "estimates/states/CA",
    "from": "2019",
    "to": "2021"
}
```

### Multiple States - Violent Crime
Run separate queries for each state:
```python
# California
{
    "endpoint": "estimates/states/CA/violent-crime",
    "from": "2018",
    "to": "2021"
}

# Texas
{
    "endpoint": "estimates/states/TX/violent-crime",
    "from": "2018",
    "to": "2021"
}
```

### Year-by-Year Comparison
```python
{
    "endpoint": "estimates/states/NY",
    "from": "2020",
    "to": "2021"
}
```

### Long-Term Trends
```python
{
    "endpoint": "estimates/national",
    "from": "2000",
    "to": "2021"
}
```

## Data Fields Returned

Common fields in API responses:

- **year** - Year of the data
- **state_abbr** - State abbreviation (state queries only)
- **state_name** - Full state name (state queries only)
- **population** - Total population
- **violent_crime** - Total violent crimes
- **homicide** - Murder and nonnegligent manslaughter
- **rape** - Rape (revised definition)
- **robbery** - Robbery
- **aggravated_assault** - Aggravated assault
- **property_crime** - Total property crimes
- **burglary** - Burglary
- **larceny** - Larceny-theft
- **motor_vehicle_theft** - Motor vehicle theft
- **arson** - Arson (when reported)

## Custom Query Example

Run interactive custom query:

```bash
python query_fbi.py --custom
```

Then enter your parameters in JSON format:

```json
{
    "endpoint": "estimates/states/CA/violent-crime",
    "from": "2015",
    "to": "2021"
}
```

This queries for violent crime trends in California from 2015-2021.

## Troubleshooting

### "Connector not found"
```bash
# Add the connector
python add_connectors.py fbi_crime
```

### "Connector is inactive"
```bash
# Already active in configuration!
# If needed, update via API:
curl -X PUT http://localhost:5000/api/v1/sources/fbi_crime \
  -H "Content-Type: application/json" \
  -d '{"active": true}'
```

### "Invalid API key"
```bash
# API key is already configured in add_connectors.py
# If needed, update:
curl -X PUT http://localhost:5000/api/v1/sources/fbi_crime \
  -H "Content-Type: application/json" \
  -d '{"api_key": "iSNiIUIPpFPIanf4l9DdCDPZZK7yppVF0tlviXy3"}'
```

### "Invalid endpoint"
- Verify endpoint format: `estimates/national` or `estimates/states/CA`
- Check state abbreviation is valid (2 letters, uppercase)
- Ensure endpoint exists in FBI Crime Data API

### "No data returned"
- Check year range is valid (typically 1960-2021)
- Some states may not have data for all years
- Verify state abbreviation is correct

### Rate Limiting
- FBI Crime Data API has generous rate limits
- Use caching (enabled by default) for repeated queries
- Space out large batch queries

## Tips

1. **Start Broad** - Query national data first, then drill down to states
2. **Use Year Ranges** - Get multi-year trends with from/to parameters
3. **Check Data Availability** - Not all states have data for all years
4. **Enable Caching** - Default caching speeds up repeat queries
5. **Compare States** - Run separate queries for state comparisons

## Use Cases

1. **Crime Trend Analysis** - Track crime over time nationally or by state
2. **State Comparisons** - Compare crime rates across states
3. **Policy Research** - Analyze impact of policy changes on crime
4. **Demographics Study** - Correlate crime with population data
5. **Reporting** - Generate crime statistics for reports

## Resources

- **FBI Crime Data Explorer**: https://crime-data-explorer.fr.cloud.gov/
- **API Documentation**: https://crime-data-explorer.fr.cloud.gov/pages/docApi
- **Get API Key**: https://api.data.gov/signup
- **About UCR Program**: https://www.fbi.gov/services/cjis/ucr

## Integration

### Use in Scripts

```python
from query_fbi import execute_query

result = execute_query({
    "endpoint": "estimates/states/CA",
    "from": "2020",
    "to": "2021"
}, use_cache=True, show_details=False)

if result["success"]:
    data = result["data"]["data"]
    for record in data:
        print(f"{record['year']}: {record['violent_crime']:,} violent crimes")
```

### Batch Processing

```bash
#!/bin/bash
# Query multiple states

for state in CA TX NY FL IL; do
    python query_fbi.py --custom << EOF
{
    "endpoint": "estimates/states/$state",
    "from": "2020",
    "to": "2020"
}
EOF
done
```

## Summary

The `query_fbi.py` script provides:
- Quick access to FBI crime data
- Pre-built example queries
- Custom query capability
- Clean, formatted output
- Built-in caching
- Error handling
- State abbreviation reference

Perfect for:
- Crime trend analysis
- Policy research
- State comparisons
- Academic studies
- Reporting and visualization
- Data-driven journalism
