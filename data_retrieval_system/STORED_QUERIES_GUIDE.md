# Stored Query System - Complete Guide

## Overview

The Stored Query System allows you to save, manage, and execute queries in MongoDB. This enables you to:
- Reuse complex queries without rewriting parameters
- Execute queries programmatically via API or command line
- Organize queries with tags and descriptions
- Track query metadata and execution history
- Reference stored queries in cached results

## Architecture

### Components

1. **StoredQuery Model** (`models/stored_query.py`)
   - Manages query storage in MongoDB
   - Provides CRUD operations
   - Supports tagging and searching

2. **QueryResult Model** (`models/query_result.py`) - Updated
   - Now includes optional `query_id` reference in cached results
   - Links cached results to stored queries

3. **QueryEngine** (`core/query_engine.py`) - Enhanced
   - `execute_stored_query()` - Execute queries by ID
   - `get_stored_query()` - Retrieve query details
   - `list_stored_queries()` - List all queries
   - Original `execute_query()` now supports `query_id` parameter

4. **API Routes** (`api/routes.py`) - 7 New Endpoints
   - POST `/api/v1/queries` - Create stored query
   - GET `/api/v1/queries` - List stored queries
   - GET `/api/v1/queries/{id}` - Get specific query
   - PUT `/api/v1/queries/{id}` - Update query
   - DELETE `/api/v1/queries/{id}` - Delete query
   - POST `/api/v1/queries/{id}/execute` - Execute query
   - GET `/api/v1/queries/search` - Search queries

5. **Management Script** (`manage_queries.py`)
   - Command-line utility for query management
   - Interactive query creation
   - Query execution and testing

## Query Schema

```json
{
    "query_id": "unique_identifier",
    "query_name": "Human-readable name",
    "description": "Optional description",
    "connector_id": "Reference to connector (source_id)",
    "parameters": {
        "endpoint": "api/endpoint",
        "param1": "value1",
        "param2": "value2"
    },
    "tags": ["tag1", "tag2"],
    "active": true,
    "created_at": "2024-11-29T...",
    "updated_at": "2024-11-29T...",
    "created_by": "optional_user_id"
}
```

### Required Fields
- `query_id` - Unique identifier
- `query_name` - Display name
- `connector_id` - Must match existing connector source_id
- `parameters` - Query parameters as dictionary

### Optional Fields
- `description` - Query description
- `tags` - Array of tags for categorization
- `active` - Boolean (default: true)
- `created_by` - User identifier

## Usage

### 1. Command Line (manage_queries.py)

#### List All Queries
```bash
python manage_queries.py --list
```

#### List Queries for Specific Connector
```bash
python manage_queries.py --list --connector fbi_crime
```

#### List Only Active Queries
```bash
python manage_queries.py --list --active
```

#### Create Query from JSON File
```bash
# Create query.json
cat > query.json << EOF
{
    "query_id": "national_crime_2020",
    "query_name": "National Crime Estimates 2020",
    "connector_id": "fbi_crime",
    "description": "National crime statistics for 2020",
    "parameters": {
        "endpoint": "estimates/national",
        "from": "2020",
        "to": "2020"
    },
    "tags": ["crime", "national", "2020"]
}
EOF

# Create the query
python manage_queries.py --create query.json
```

#### Create Query Interactively
```bash
python manage_queries.py --create-interactive
```

#### Get Query Details
```bash
python manage_queries.py --get national_crime_2020
```

#### Execute Stored Query
```bash
python manage_queries.py --execute national_crime_2020
```

#### Delete Query
```bash
python manage_queries.py --delete national_crime_2020
```

#### Search Queries
```bash
python manage_queries.py --search crime
```

### 2. REST API

#### Create Stored Query
```bash
curl -X POST http://localhost:5000/api/v1/queries \
  -H "Content-Type: application/json" \
  -d '{
    "query_id": "ca_crime_trend",
    "query_name": "California Crime Trend",
    "connector_id": "fbi_crime",
    "description": "5-year crime trend for California",
    "parameters": {
        "endpoint": "estimates/states/CA",
        "from": "2017",
        "to": "2021"
    },
    "tags": ["crime", "california", "trend"]
  }'
```

#### List Stored Queries
```bash
# All queries
curl http://localhost:5000/api/v1/queries

# Filter by connector
curl "http://localhost:5000/api/v1/queries?connector_id=fbi_crime"

# Only active queries
curl "http://localhost:5000/api/v1/queries?active_only=true"

# Filter by tags
curl "http://localhost:5000/api/v1/queries?tags=crime&tags=national"
```

#### Get Specific Query
```bash
curl http://localhost:5000/api/v1/queries/ca_crime_trend
```

#### Update Query
```bash
curl -X PUT http://localhost:5000/api/v1/queries/ca_crime_trend \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Updated description",
    "active": false
  }'
```

#### Delete Query
```bash
curl -X DELETE http://localhost:5000/api/v1/queries/ca_crime_trend
```

#### Execute Stored Query
```bash
# Basic execution
curl -X POST http://localhost:5000/api/v1/queries/ca_crime_trend/execute

# With cache disabled
curl -X POST http://localhost:5000/api/v1/queries/ca_crime_trend/execute \
  -H "Content-Type: application/json" \
  -d '{"use_cache": false}'

# With parameter overrides
curl -X POST http://localhost:5000/api/v1/queries/ca_crime_trend/execute \
  -H "Content-Type: application/json" \
  -d '{
    "parameter_overrides": {
        "to": "2022"
    }
  }'
```

#### Search Queries
```bash
curl "http://localhost:5000/api/v1/queries/search?q=california"
```

### 3. Python Code

#### Using QueryEngine
```python
from core.query_engine import QueryEngine

query_engine = QueryEngine()

# Execute stored query
result = query_engine.execute_stored_query("ca_crime_trend")

# With parameter overrides
result = query_engine.execute_stored_query(
    "ca_crime_trend",
    parameter_overrides={"to": "2022"}
)

# Get stored query details
query = query_engine.get_stored_query("ca_crime_trend")

# List stored queries
queries = query_engine.list_stored_queries(
    connector_id="fbi_crime",
    active_only=True
)
```

#### Using StoredQuery Model
```python
from models.stored_query import StoredQuery

stored_query = StoredQuery()

# Create query
stored_query.create({
    "query_id": "my_query",
    "query_name": "My Query",
    "connector_id": "fbi_crime",
    "parameters": {"endpoint": "estimates/national"}
})

# Get query
query = stored_query.get_by_id("my_query")

# Update query
stored_query.update("my_query", {"active": False})

# Delete query
stored_query.delete("my_query")

# Search queries
results = stored_query.search("crime")

# Add/remove tags
stored_query.add_tag("my_query", "important")
stored_query.remove_tag("my_query", "important")
```

## Query Result Format

When executing a stored query, the result includes additional metadata:

```json
{
    "success": true,
    "source": "connector",
    "query_id": "ca_crime_trend",
    "query_name": "California Crime Trend",
    "query_description": "5-year crime trend for California",
    "data": {
        "data": [...],
        "metadata": {
            "source": "FBI Crime Data Explorer",
            "record_count": 5,
            "timestamp": "2024-11-29T..."
        }
    }
}
```

### Fields Added to Results
- `query_id` - Reference to stored query
- `query_name` - Name of the query (if executed from stored query)
- `query_description` - Description (if provided)

## Examples

### Example 1: FBI Crime Queries

```bash
# Create queries for different states
cat > fbi_queries.json << EOF
[
    {
        "query_id": "ca_crime_2020",
        "query_name": "California Crime 2020",
        "connector_id": "fbi_crime",
        "parameters": {
            "endpoint": "estimates/states/CA",
            "from": "2020",
            "to": "2020"
        },
        "tags": ["crime", "california", "2020"]
    },
    {
        "query_id": "tx_crime_2020",
        "query_name": "Texas Crime 2020",
        "connector_id": "fbi_crime",
        "parameters": {
            "endpoint": "estimates/states/TX",
            "from": "2020",
            "to": "2020"
        },
        "tags": ["crime", "texas", "2020"]
    }
]
EOF

# Import queries (in Python)
import json
from models.stored_query import StoredQuery

with open('fbi_queries.json') as f:
    queries = json.load(f)

sq = StoredQuery()
for query in queries:
    sq.create(query)
```

### Example 2: Census Queries

```json
{
    "query_id": "state_population_2020",
    "query_name": "State Population 2020",
    "connector_id": "census_api",
    "description": "Population for all states from 2020 Census",
    "parameters": {
        "dataset": "2020/dec/pl",
        "get": "NAME,P1_001N",
        "for": "state:*"
    },
    "tags": ["census", "population", "2020"]
}
```

### Example 3: NASS Queries

```json
{
    "query_id": "corn_production_iowa",
    "query_name": "Iowa Corn Production",
    "connector_id": "usda_quickstats",
    "description": "Annual corn production in Iowa",
    "parameters": {
        "commodity_desc": "CORN",
        "state_alpha": "IA",
        "statisticcat_desc": "PRODUCTION",
        "year": "2022"
    },
    "tags": ["agriculture", "corn", "iowa"]
}
```

## Parameter Overrides

Execute a stored query with different parameters:

```python
# Original query uses 2020-2021
result = query_engine.execute_stored_query(
    "ca_crime_trend",
    parameter_overrides={
        "from": "2019",
        "to": "2022"
    }
)
```

API version:
```bash
curl -X POST http://localhost:5000/api/v1/queries/ca_crime_trend/execute \
  -H "Content-Type: application/json" \
  -d '{
    "parameter_overrides": {
        "from": "2019",
        "to": "2022"
    }
  }'
```

## Query Organization

### Using Tags

```python
# Create queries with tags
stored_query.create({
    "query_id": "q1",
    "tags": ["production", "important", "daily"]
})

# List queries by tag
queries = stored_query.get_all(tags=["production"])

# Add tag to existing query
stored_query.add_tag("q1", "weekly")

# Remove tag
stored_query.remove_tag("q1", "daily")
```

### Using Active Status

```python
# Deactivate query
stored_query.update("q1", {"active": False})

# List only active queries
active_queries = stored_query.get_all(active_only=True)
```

## Integration with Cache

When a stored query is executed, the `query_id` is stored in the cache:

```python
# Execute stored query
result = query_engine.execute_stored_query("my_query")

# Cache entry includes query_id
{
    "query_hash": "abc123...",
    "source_id": "fbi_crime",
    "parameters": {...},
    "result": {...},
    "query_id": "my_query",  # Reference to stored query
    "created_at": "...",
    "expires_at": "..."
}
```

This allows you to:
- Track which queries generated cached results
- Invalidate cache for specific stored queries
- Analyze query usage patterns

## Best Practices

### 1. Naming Conventions
```
<connector>_<what>_<filter>

Examples:
- fbi_crime_national_2020
- census_population_ca_2020
- nass_corn_production_iowa
```

### 2. Use Descriptions
```json
{
    "query_id": "fbi_violent_crime_trend",
    "description": "5-year violent crime trend analysis for policy research"
}
```

### 3. Tag Effectively
```json
{
    "tags": [
        "crime",           # Domain
        "violent",         # Category
        "trend",           # Type
        "policy-research"  # Use case
    ]
}
```

### 4. Keep Queries Specific
Don't create overly generic queries - create specific queries for each use case

### 5. Document Parameter Meanings
```json
{
    "description": "National crime estimates. Parameters: from/to = year range (YYYY)"
}
```

## Troubleshooting

### Query Not Found
```
Error: Query not found: my_query

Solution: Check query_id spelling
python manage_queries.py --list
```

### Connector Not Found
```
Error: Source not found: invalid_connector

Solution: Ensure connector_id matches existing connector
python validate_connectors.py
```

### Invalid Parameters
```
Error: Query execution failed: bad request

Solution: Test parameters first with execute_query()
then save as stored query
```

### Permission Issues
Ensure MongoDB has proper permissions for stored_queries collection

## MongoDB Collections

### stored_queries
```
{
    _id: ObjectId,
    query_id: String (indexed, unique),
    query_name: String,
    connector_id: String (indexed),
    parameters: Object,
    tags: Array (indexed),
    active: Boolean (indexed),
    created_at: Date,
    updated_at: Date
}
```

### query_results (updated)
```
{
    _id: ObjectId,
    query_hash: String (indexed, unique),
    source_id: String (indexed),
    parameters: Object,
    result: Object,
    query_id: String (optional),  # NEW
    created_at: Date,
    expires_at: Date (TTL index)
}
```

## API Endpoints Summary

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/queries` | Create stored query |
| GET | `/api/v1/queries` | List stored queries |
| GET | `/api/v1/queries/{id}` | Get specific query |
| PUT | `/api/v1/queries/{id}` | Update query |
| DELETE | `/api/v1/queries/{id}` | Delete query |
| POST | `/api/v1/queries/{id}/execute` | Execute stored query |
| GET | `/api/v1/queries/search?q=term` | Search queries |

## Complete Example Workflow

```bash
# 1. Create stored query
python manage_queries.py --create-interactive

# 2. List queries
python manage_queries.py --list

# 3. Execute query
python manage_queries.py --execute my_query_id

# 4. Via API
curl -X POST http://localhost:5000/api/v1/queries/my_query_id/execute

# 5. In Python
from core.query_engine import QueryEngine
qe = QueryEngine()
result = qe.execute_stored_query("my_query_id")
```

## Summary

The Stored Query System provides:
- ✅ Query reusability
- ✅ Centralized query management
- ✅ API and CLI access
- ✅ Query organization with tags
- ✅ Parameter overrides
- ✅ Integration with caching
- ✅ Query execution tracking

Perfect for:
- Recurring reports
- Scheduled data pulls
- API integrations
- Dashboard queries
- Batch processing
- Query sharing across teams
