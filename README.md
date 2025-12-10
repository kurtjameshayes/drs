# Data Retrieval System

A flexible, extensible data retrieval framework built with Python and MongoDB.

## Features

- Connector-based architecture for multiple data sources
- MongoDB storage for configurations and caching
- RESTful API with Flask
- Support for USDA NASS QuickStats, Census.gov, and local files
- Query result caching with TTL
- JSON-formatted API queries
- Automatic retry with exponential backoff

## Quick Start

### Prerequisites

- Python 3.8+
- MongoDB 4.0+

### Installation

```bash
cd data_retrieval_system
pip install -r requirements.txt
cp .env.example .env
```

### Initialize Database

```bash
python init_db.py
```

### Add Connectors

```bash
# Add all connectors
python add_connectors.py

# Add specific connector
python add_connectors.py usda_quickstats

# List available connectors
python add_connectors.py --list
```

### Validate Connectors

```bash
# Validate all connectors
python validate_connectors.py

# Validate specific connector
python validate_connectors.py usda_quickstats
```

### Start API Server

```bash
python main.py
```

Server runs at `http://localhost:5000`

## API Endpoints

### Sources

- `GET /api/v1/sources` - List all sources
- `GET /api/v1/sources/{id}` - Get source info
- `POST /api/v1/sources` - Create source
- `PUT /api/v1/sources/{id}` - Update source
- `DELETE /api/v1/sources/{id}` - Delete source

### Queries

- `POST /api/v1/query` - Execute query
- `POST /api/v1/query/multi` - Multi-source query
- `POST /api/v1/query/validate` - Validate query

### Cache

- `GET /api/v1/cache/stats` - Cache statistics
- `DELETE /api/v1/cache/{id}` - Invalidate cache

## Example Query

```bash
curl -X POST http://localhost:5000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "source": "sample_csv",
    "filters": {
      "price": {"$gt": 50}
    },
    "limit": 10
  }'
```

## Advanced Query Joins & Analysis

Leverage the `QueryEngine` to execute multiple queries, join the results into a
pandas DataFrame, and feed the combined data into a rich analysis toolkit.

Refer to `ANALYSIS_GUIDE.md` for a step-by-step walkthrough and
`analysis_example.py` for a runnable script that uses in-memory data.

```python
from core.query_engine import QueryEngine

query_engine = QueryEngine()

joined_df = query_engine.execute_queries_to_dataframe(
    queries=[
        {
            "source_id": "census_api",
            "parameters": {"dataset": "2020/acs/acs5", "get": "NAME,B01003_001E", "for": "state:*"},
            "alias": "population"
        },
        {
            "source_id": "usda_quickstats",
            "parameters": {"commodity_desc": "CORN", "year": "2020", "format": "JSON"},
            "alias": "agriculture"
        }
    ],
    join_on=["state"],
    aggregation={
        "group_by": ["state"],
        "metrics": [{"column": "value", "agg": "sum", "alias": "total_value"}]
    }
)

analysis_plan = {
    "basic_statistics": True,
    "exploratory": True,
    "inferential_tests": [{"x": "total_value", "y": "B01003_001E", "test": "pearson"}],
    "linear_regression": {"features": ["total_value"], "target": "B01003_001E"},
    "predictive": {"features": ["total_value"], "target": "B01003_001E", "model_type": "forest"}
}

analysis = query_engine.analyze_queries(
    queries=[
        {"source_id": "census_api", "parameters": {"dataset": "2020/acs/acs5", "get": "NAME,B01003_001E", "for": "state:*"}},
        {"source_id": "usda_quickstats", "parameters": {"commodity_desc": "CORN", "year": "2020", "format": "JSON"}}
    ],
    join_on=["state"],
    analysis_plan=analysis_plan
)

dataframe = analysis["dataframe"]      # pandas.DataFrame
insights = analysis["analysis"]        # dict of statistical outputs
```

The `DataAnalysisEngine` unlocks:

- Descriptive statistics & exploratory summaries
- Inferential tests (Pearson, Spearman, t-tests)
- Time-series smoothing and volatility checks
- Linear and non-linear (random forest) regressions
- Multivariate PCA projections
- Predictive analysis with shared configuration

## Connectors

### USDA NASS QuickStats

```json
{
  "source_id": "usda_quickstats",
  "connector_type": "usda_nass",
  "url": "https://quickstats.nass.usda.gov/api",
  "api_key": "YOUR_API_KEY",
  "format": "JSON"
}
```

### Census.gov

```json
{
  "source_id": "census_acs5",
  "connector_type": "census",
  "url": "https://api.census.gov/data",
  "api_key": "YOUR_API_KEY"
}
```

### Local Files

```json
{
  "source_id": "local_data",
  "connector_type": "local_file",
  "file_path": "/path/to/data.csv",
  "file_type": "csv"
}
```

## Project Structure

```
data_retrieval_system/
├── config.py              # Configuration
├── main.py               # Application entry point
├── examples.py           # Usage examples
├── init_db.py            # Database initialization
├── requirements.txt      # Dependencies
├── core/                 # Core system
│   ├── base_connector.py
│   ├── connector_manager.py
│   ├── query_engine.py
│   └── cache_manager.py
├── connectors/           # Data source connectors
│   ├── usda_nass/
│   ├── census/
│   └── local_file/
├── models/               # MongoDB models
│   ├── connector_config.py
│   └── query_result.py
└── api/                  # REST API
    └── routes.py
```

## License

MIT
