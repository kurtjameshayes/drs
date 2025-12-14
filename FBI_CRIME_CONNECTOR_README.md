# FBI Crime Data Connector - Query Execution Guide

This guide explains how to execute queries against the FBI Crime Data Explorer API using the `fbi_crime` connector.

## Overview

The FBI Crime Data connector supports **dynamic query parameters** that can be specified at runtime. This allows you to define stored queries with placeholders that are substituted when the query is executed.

## Quick Start

### Basic Query Execution

```python
from core.query_engine import QueryEngine

# Initialize the query engine
query_engine = QueryEngine()

# Execute a simple query with hard-coded parameters
result = query_engine.execute_query(
    "fbi_crime",
    {
        "endpoint": "arrest/national/all",
        "type": "counts",
        "from": "01-2023",
        "to": "12-2023"
    }
)

if result.get("success"):
    print("Query succeeded!")
    print(f"Records returned: {len(result['data']['data']['data'])}")
else:
    print(f"Query failed: {result.get('error')}")
```

### Direct Connector Usage with Dynamic Parameters

For more control, you can use the connector directly with dynamic parameters:

```python
from connectors.fbi_crime.connector import FBICrimeConnector

# Initialize the connector
connector = FBICrimeConnector({
    "url": "https://api.usa.gov/crime/fbi/cde",
    "api_key": "YOUR_API_KEY"
})

# Connect to the API
connector.connect()

# Define a query with dynamic placeholders
# Placeholders use the format: {parameter_name optional_description}
query_parameters = {
    "endpoint": "arrest/national/all",
    "type": "counts",
    "from": "{from mm-yyyy}",  # Dynamic - format hint in placeholder
    "to": "{to mm-yyyy}"       # Dynamic - format hint in placeholder
}

# Provide values for dynamic parameters at runtime
dynamic_values = {
    "from": "01-2023",
    "to": "12-2023"
}

# Execute the query
result = connector.query(query_parameters, dynamic_params=dynamic_values)

# Clean up
connector.disconnect()
```

## Dynamic Parameter Syntax

### Placeholder Format

Dynamic parameters use curly braces with the following format:

```
{parameter_name}
{parameter_name format_hint}
{parameter_name option1|option2|option3}
```

### Examples

| Placeholder | Description |
|-------------|-------------|
| `{from}` | Simple placeholder for 'from' parameter |
| `{from mm-yyyy}` | Placeholder with format hint (e.g., "01-2023") |
| `{to mm-yyyy}` | Placeholder with format hint |
| `{type counts\|arrests}` | Placeholder with allowed values hint |
| `{state_code}` | Placeholder for state code parameter |

### How Dynamic Parameters Are Resolved

When a query is executed, the connector:

1. **Identifies placeholders**: Any value matching `{...}` is treated as dynamic
2. **Extracts parameter name**: The first word inside braces becomes the parameter name
3. **Looks up values**: The parameter name is matched against the `dynamic_params` dictionary
4. **Substitutes values**: The placeholder is replaced with the actual value

## URL Building Process

When you execute a query, the connector builds the URL in this order:

1. **Base URL**: From the connector configuration (e.g., `https://api.usa.gov/crime/fbi/cde`)
2. **Endpoint**: Appended to the base URL (e.g., `arrest/national/all`)
3. **Hard-coded parameters**: Added as query string parameters
4. **Dynamic parameters**: Placeholders substituted with provided values
5. **API Key**: Added automatically from connector configuration

### Example URL Building

Given:
- Base URL: `https://api.usa.gov/crime/fbi/cde`
- Query parameters:
  ```json
  {
    "endpoint": "arrest/national/all",
    "type": "counts",
    "from": "{from mm-yyyy}",
    "to": "{to mm-yyyy}"
  }
  ```
- Dynamic values: `{"from": "01-2023", "to": "12-2023"}`
- API Key: `iiHnOKfno2Mgkt5AynpvPpUQTEyxE77jo1RU8PIv`

Result URL:
```
https://api.usa.gov/crime/fbi/cde/arrest/national/all?type=counts&from=01-2023&to=12-2023&API_KEY=iiHnOKfno2Mgkt5AynpvPpUQTEyxE77jo1RU8PIv
```

## Using Stored Queries

### Creating a Stored Query with Dynamic Parameters

```python
from models.stored_query import StoredQuery

stored_query = StoredQuery()

# Create a stored query with dynamic parameters
stored_query.create({
    "query_id": "fbi_national_arrests",
    "query_name": "National Arrests by Date Range",
    "description": "Get national arrest data for a specified date range",
    "connector_id": "fbi_crime",
    "parameters": {
        "endpoint": "arrest/national/all",
        "type": "counts",
        "from": "{from mm-yyyy}",
        "to": "{to mm-yyyy}"
    },
    "tags": ["fbi", "arrests", "national"]
})
```

### Executing a Stored Query with Runtime Values

```python
from core.query_engine import QueryEngine

query_engine = QueryEngine()

# Execute the stored query with parameter overrides
# The parameter_overrides will substitute dynamic placeholders
result = query_engine.execute_stored_query(
    "fbi_national_arrests",
    parameter_overrides={
        "from": "01-2023",
        "to": "12-2023"
    }
)
```

## Debug Output

The connector includes print statements showing the URL building process:

```
[FBI Query Builder] Step 1 - Base URL: https://api.usa.gov/crime/fbi/cde
[FBI Query Builder] Step 2 - Endpoint from query: arrest/national/all
[FBI Query Builder] Step 3 - URL without parameters: https://api.usa.gov/crime/fbi/cde/arrest/national/all
[FBI Query Builder] Found hard-coded parameter: type=counts
[FBI Query Builder] Found dynamic placeholder: from={from mm-yyyy} (param name: from)
[FBI Query Builder] Found dynamic placeholder: to={to mm-yyyy} (param name: to)
[FBI Query Builder] Step 4a - Adding hard-coded parameters: {'type': 'counts'}
[FBI Query Builder] Step 4b - Processing dynamic placeholders with values: {'from': '01-2023', 'to': '12-2023'}
[FBI Query Builder] Substituted from: {from mm-yyyy} -> 01-2023
[FBI Query Builder] Substituted to: {to mm-yyyy} -> 12-2023
[FBI Query Builder] Step 5 - Added API key parameter: API_KEY=***
[FBI Query Builder] Final URL (for logging): https://api.usa.gov/crime/fbi/cde/arrest/national/all?type=counts&from=01-2023&to=12-2023&API_KEY=***
[FBI Query Builder] Query parameters: {'type': 'counts', 'from': '01-2023', 'to': '12-2023', 'API_KEY': '***'}
```

## Common Endpoints

### CDE (Crime Data Explorer) Endpoints

| Endpoint | Description |
|----------|-------------|
| `arrest/national/all` | National arrest data |
| `arrest/national/{offense}` | National arrest data by offense |
| `arrest/states/{state_abbr}/all` | State arrest data |
| `summarized/state/{state_abbr}/all` | State summary data |

### SAPI (Statistics API) Endpoints

| Endpoint | Description |
|----------|-------------|
| `estimates/national` | National crime estimates |
| `estimates/states/{state_abbr}` | State crime estimates |
| `agencies` | Agency information |

## API Configuration

### CDE Base URL
```
https://api.usa.gov/crime/fbi/cde
```
- Uses `API_KEY` (uppercase) for authentication
- All parameters passed as query string

### SAPI Base URL
```
https://api.usa.gov/crime/fbi/sapi
```
- Uses `api_key` (lowercase) for authentication
- Includes `/api/` namespace in paths

## Complete Example

```python
#!/usr/bin/env python3
"""
Example: Query FBI Crime Data with Dynamic Parameters
"""

from core.query_engine import QueryEngine
from connectors.fbi_crime.connector import FBICrimeConnector

def example_direct_connector():
    """Example using the connector directly."""
    print("=" * 60)
    print("Direct Connector Usage")
    print("=" * 60)
    
    connector = FBICrimeConnector({
        "url": "https://api.usa.gov/crime/fbi/cde",
        "api_key": "YOUR_API_KEY"
    })
    
    connector.connect()
    
    # Query with dynamic parameters
    result = connector.query(
        parameters={
            "endpoint": "arrest/national/all",
            "type": "counts",
            "from": "{from mm-yyyy}",
            "to": "{to mm-yyyy}"
        },
        dynamic_params={
            "from": "01-2023",
            "to": "12-2023"
        }
    )
    
    if result.get("success"):
        print(f"\nSuccess! Retrieved {len(result['data']['data'])} records")
        print(f"Final URL: {result['metadata']['final_url']}")
    else:
        print(f"Error: {result.get('error')}")
    
    connector.disconnect()

def example_query_engine():
    """Example using the query engine."""
    print("\n" + "=" * 60)
    print("Query Engine Usage")
    print("=" * 60)
    
    engine = QueryEngine()
    
    # Simple query (no dynamic params needed for hard-coded values)
    result = engine.execute_query(
        "fbi_crime",
        {
            "endpoint": "arrest/national/all",
            "type": "counts",
            "from": "01-2023",
            "to": "12-2023"
        }
    )
    
    if result.get("success"):
        data = result.get("data", {})
        records = data.get("data", {}).get("data", [])
        print(f"\nSuccess! Retrieved {len(records)} records")
    else:
        print(f"Error: {result.get('error')}")

if __name__ == "__main__":
    example_direct_connector()
    example_query_engine()
```

## Error Handling

The connector will print a warning if a dynamic parameter doesn't have a value:

```
[FBI Query Builder] WARNING: No value provided for dynamic parameter 'from' (placeholder: {from mm-yyyy})
```

In this case, the parameter will not be included in the query string.

## API Key

Get your FBI Crime Data API key at: https://api.data.gov/signup

Configure it in the connector:

```python
# Via connector configuration
connector = FBICrimeConnector({
    "url": "https://api.usa.gov/crime/fbi/cde",
    "api_key": "YOUR_API_KEY"
})

# Or via MongoDB connector_config collection
{
    "source_id": "fbi_crime",
    "connector_type": "fbi_crime",
    "url": "https://api.usa.gov/crime/fbi/cde",
    "api_key": "YOUR_API_KEY",
    "active": true
}
```
