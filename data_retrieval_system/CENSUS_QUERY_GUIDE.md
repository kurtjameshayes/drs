# Census Query Script Documentation

## Overview

The `query_census.py` script provides an easy way to execute queries against the US Census Bureau API using the data retrieval system's query engine.

## Features

✅ **8 Pre-Built Example Queries** - Ready to run  
✅ **Custom Query Support** - Enter your own parameters  
✅ **Formatted Output** - Clean, readable results  
✅ **State FIPS Code Reference** - Built-in lookup table  
✅ **Caching Support** - Faster repeat queries  
✅ **No API Key Required** - Works without key (with rate limits)  
✅ **Error Handling** - Clear error messages  

## Prerequisites

1. **MongoDB** must be running
2. **Census API connector** must be configured
3. **API key optional** but recommended for higher rate limits

### Setup Steps

```bash
# 1. Add the connector
python add_connectors.py census_api

# 2. (Optional) Add your API key for higher rate limits
curl -X PUT http://localhost:5000/api/v1/sources/census_api \
  -H "Content-Type: application/json" \
  -d '{"api_key": "YOUR_API_KEY_HERE", "active": true}'

# 3. Activate without API key (if preferred)
curl -X PUT http://localhost:5000/api/v1/sources/census_api \
  -H "Content-Type: application/json" \
  -d '{"active": true}'
```

Get your API key from: https://api.census.gov/data/key_signup.html

**Note:** API key is optional. Without it, you're subject to lower rate limits but queries still work.

## Usage

### Run All Example Queries

```bash
python query_census.py
```

### Run Specific Example

```bash
python query_census.py --example 1
python query_census.py --example 3
```

### List Available Examples

```bash
python query_census.py --list
```

### Run Custom Query

```bash
python query_census.py --custom
```

### Show State FIPS Codes

```bash
python query_census.py --states
```

### Show Help

```bash
python query_census.py --help
```

## Pre-Built Example Queries

### Example 1: State Population - 2020 Decennial Census
Get total population for all states from 2020 Census
```python
{
    "dataset": "2020/dec/pl",
    "get": "NAME,P1_001N",
    "for": "state:*"
}
```
**Variable:** P1_001N = Total population

### Example 2: County Population - Specific State
Get population for all counties in California
```python
{
    "dataset": "2020/dec/pl",
    "get": "NAME,P1_001N",
    "for": "county:*",
    "in": "state:06"
}
```
**State Code:** 06 = California

### Example 3: ACS 5-Year Data - Median Household Income
Get median household income for all states
```python
{
    "dataset": "2021/acs/acs5",
    "get": "NAME,B19013_001E",
    "for": "state:*"
}
```
**Variable:** B19013_001E = Median household income in past 12 months

### Example 4: Poverty Estimates by State
Get poverty estimates for all states
```python
{
    "dataset": "timeseries/poverty/saipe",
    "get": "NAME,SAEPOVRTALL_PT,YEAR",
    "for": "state:*",
    "time": "2021"
}
```
**Variable:** SAEPOVRTALL_PT = Poverty rate for all ages

### Example 5: Educational Attainment - Bachelor's Degree or Higher
Get percentage with bachelor's degree or higher by state
```python
{
    "dataset": "2021/acs/acs5",
    "get": "NAME,B15003_022E,B15003_001E",
    "for": "state:*"
}
```
**Variables:** 
- B15003_022E = Bachelor's degree count
- B15003_001E = Total population

### Example 6: Population by Age and Sex - Specific State
Get age/sex demographics for Texas
```python
{
    "dataset": "2021/acs/acs5",
    "get": "NAME,B01001_001E,B01001_002E,B01001_026E",
    "for": "state:48"
}
```
**Variables:** 
- B01001_001E = Total population
- B01001_002E = Male population
- B01001_026E = Female population
**State Code:** 48 = Texas

### Example 7: Housing Units by State
Get total housing units for all states
```python
{
    "dataset": "2020/dec/pl",
    "get": "NAME,H1_001N",
    "for": "state:*"
}
```
**Variable:** H1_001N = Total housing units

### Example 8: Median Age by County - Specific State
Get median age for counties in New York
```python
{
    "dataset": "2021/acs/acs5",
    "get": "NAME,B01002_001E",
    "for": "county:*",
    "in": "state:36"
}
```
**Variable:** B01002_001E = Median age  
**State Code:** 36 = New York

## Sample Output

```
======================================================================
EXAMPLE 1: State Population - 2020 Decennial Census
======================================================================

Get total population for all states from 2020 Census

======================================================================
EXECUTING CENSUS QUERY
======================================================================

Dataset: 2020/dec/pl
Variables: NAME,P1_001N
Geography: state:*

Cache: Enabled

Querying... Done!

======================================================================
QUERY RESULTS
======================================================================

Source: connector
Total Records: 52

Note: P1_001N is total population

Showing first 10 record(s):
----------------------------------------------------------------------

Record 1:
  NAME: Alabama
  P1_001N: 5024279
  state: 01

Record 2:
  NAME: Alaska
  P1_001N: 733391
  state: 02
...
```

## Census API Parameters

### Required Parameters

**dataset** - Census dataset identifier
- Format: `"year/program/dataset"`
- Examples:
  - `"2021/acs/acs5"` - American Community Survey 5-Year
  - `"2020/dec/pl"` - 2020 Decennial Census (PL 94-171)
  - `"timeseries/poverty/saipe"` - Small Area Income and Poverty Estimates

**get** - Variables to retrieve (comma-separated)
- Format: `"NAME,VAR1,VAR2"`
- `NAME` returns geographic name
- Examples:
  - `"NAME,B01001_001E"` - Name and total population
  - `"NAME,B19013_001E"` - Name and median household income

**for** - Geographic level
- Format: `"geography:code"`
- Examples:
  - `"state:*"` - All states
  - `"state:06"` - California only
  - `"county:*"` - All counties (requires `in` parameter)
  - `"county:001"` - Specific county (requires `in` parameter)

### Optional Parameters

**in** - Parent geography
- Used with county or lower-level geographies
- Examples:
  - `"state:06"` - Within California
  - `"state:*"` - Within all states

**time** - Time period (for time series datasets)
- Example: `"2021"`

## Common Census Datasets

### Decennial Census (2020)
- **Dataset:** `"2020/dec/pl"`
- **Description:** Redistricting data (PL 94-171)
- **Key Variables:**
  - `P1_001N` - Total population
  - `H1_001N` - Total housing units

### American Community Survey (ACS) 5-Year
- **Dataset:** `"2021/acs/acs5"` (or 2019, 2020, etc.)
- **Description:** Detailed demographic, social, economic data
- **Common Variable Prefixes:**
  - `B01001` - Sex by age
  - `B19013` - Median household income
  - `B15003` - Educational attainment
  - `B25001` - Housing units
  - `B01002` - Median age

### Small Area Income and Poverty Estimates (SAIPE)
- **Dataset:** `"timeseries/poverty/saipe"`
- **Description:** Poverty and median household income estimates
- **Key Variables:**
  - `SAEPOVRTALL_PT` - Poverty rate (all ages)
  - `SAEMHI_PT` - Median household income

## Common State FIPS Codes

| Code | State | Code | State |
|------|-------|------|-------|
| 01 | Alabama | 30 | Montana |
| 02 | Alaska | 31 | Nebraska |
| 04 | Arizona | 32 | Nevada |
| 05 | Arkansas | 33 | New Hampshire |
| 06 | California | 34 | New Jersey |
| 08 | Colorado | 35 | New Mexico |
| 09 | Connecticut | 36 | New York |
| 10 | Delaware | 37 | North Carolina |
| 11 | District of Columbia | 38 | North Dakota |
| 12 | Florida | 39 | Ohio |
| 13 | Georgia | 40 | Oklahoma |
| 15 | Hawaii | 41 | Oregon |
| 16 | Idaho | 42 | Pennsylvania |
| 17 | Illinois | 44 | Rhode Island |
| 18 | Indiana | 45 | South Carolina |
| 19 | Iowa | 46 | South Dakota |
| 20 | Kansas | 47 | Tennessee |
| 21 | Kentucky | 48 | Texas |
| 22 | Louisiana | 49 | Utah |
| 23 | Maine | 50 | Vermont |
| 24 | Maryland | 51 | Virginia |
| 25 | Massachusetts | 53 | Washington |
| 26 | Michigan | 54 | West Virginia |
| 27 | Minnesota | 55 | Wisconsin |
| 28 | Mississippi | 56 | Wyoming |
| 29 | Missouri | 72 | Puerto Rico |

Use `python query_census.py --states` to display this reference.

## Finding Variables

Each dataset has different variables. To find them:

### Method 1: Browse Online
Visit: `https://api.census.gov/data/<year>/<dataset>/variables.html`

Examples:
- https://api.census.gov/data/2021/acs/acs5/variables.html
- https://api.census.gov/data/2020/dec/pl/variables.html

### Method 2: Use Census Data Explorer
https://data.census.gov/

### Method 3: API Variables Endpoint
```bash
curl "https://api.census.gov/data/2021/acs/acs5/variables.json"
```

## Custom Query Example

Run interactive custom query:

```bash
python query_census.py --custom
```

Then enter your parameters in JSON format:

```json
{
    "dataset": "2021/acs/acs5",
    "get": "NAME,B25001_001E",
    "for": "state:*"
}
```

This queries for total housing units by state from ACS 5-year data.

## Common Query Patterns

### All States - Single Variable
```python
{
    "dataset": "2021/acs/acs5",
    "get": "NAME,B01001_001E",  # Total population
    "for": "state:*"
}
```

### Specific State - Multiple Variables
```python
{
    "dataset": "2021/acs/acs5",
    "get": "NAME,B01001_001E,B19013_001E",  # Population and income
    "for": "state:06"  # California
}
```

### All Counties in a State
```python
{
    "dataset": "2021/acs/acs5",
    "get": "NAME,B01001_001E",
    "for": "county:*",
    "in": "state:06"  # California
}
```

### All Counties in All States
```python
{
    "dataset": "2020/dec/pl",
    "get": "NAME,P1_001N",
    "for": "county:*",
    "in": "state:*"
}
```

### Time Series Data
```python
{
    "dataset": "timeseries/poverty/saipe",
    "get": "NAME,SAEPOVRTALL_PT,YEAR",
    "for": "state:*",
    "time": "from 2015 to 2021"  # Multiple years
}
```

## Troubleshooting

### "Connector not found"
```bash
# Add the connector
python add_connectors.py census_api
```

### "Connector is inactive"
```bash
# Activate via API
curl -X PUT http://localhost:5000/api/v1/sources/census_api \
  -H "Content-Type: application/json" \
  -d '{"active": true}'
```

### "Invalid dataset or variable"
- Verify dataset exists: https://api.census.gov/data.html
- Check variable names: https://api.census.gov/data/<year>/<dataset>/variables.html
- Ensure variables exist in that specific dataset

### "Geography not available"
- Some geographies not available for all datasets
- County requires `in` parameter with state
- Use `state:*` for all states

### Rate Limiting
- Add API key for higher limits
- Space out queries if hitting limits
- Use caching (enabled by default)

## Tips

1. **Start Broad** - Query all states first, then drill down
2. **Use NAME Variable** - Always include "NAME" in get parameter for readability
3. **Check Variable Availability** - Not all variables in all datasets
4. **Reference Documentation** - Use Census API documentation for variable definitions
5. **Enable Caching** - Default caching speeds up repeat queries

## Resources

- **Census API Homepage**: https://www.census.gov/data/developers.html
- **Browse Datasets**: https://api.census.gov/data.html
- **API User Guide**: https://www.census.gov/data/developers/guidance/api-user-guide.html
- **Get API Key**: https://api.census.gov/data/key_signup.html
- **Data Explorer**: https://data.census.gov/
- **Variable Search**: https://api.census.gov/data/<dataset>/variables.html

## Integration

### Use in Scripts

```python
from query_census import execute_query

result = execute_query({
    "dataset": "2021/acs/acs5",
    "get": "NAME,B01001_001E",
    "for": "state:*"
}, use_cache=True, show_details=False)

if result["success"]:
    data = result["data"]["data"]
    for record in data:
        print(f"{record['NAME']}: {record['B01001_001E']}")
```

### Batch Processing

```bash
#!/bin/bash
# Query multiple datasets

for dataset in "2019/acs/acs5" "2020/acs/acs5" "2021/acs/acs5"; do
    python query_census.py --custom << EOF
{
    "dataset": "$dataset",
    "get": "NAME,B01001_001E",
    "for": "state:06"
}
EOF
done
```

## Summary

The `query_census.py` script provides:
- Quick access to Census data
- Pre-built example queries
- Custom query capability
- Clean, formatted output
- Built-in caching
- Error handling
- State FIPS reference

Perfect for:
- Exploring Census data
- Testing queries
- Learning the Census API
- Quick data retrieval
- Prototyping data pipelines
- Demographic research
