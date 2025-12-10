# DataFrame Join & Analysis Guide

This guide walks through joining results from multiple connectors, loading them
into a pandas DataFrame, and running the built-in analytical suite.

## Prerequisites

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Configure and validate the connectors you intend to use (`add_connectors.py`,
   `validate_connectors.py`).
3. Ensure MongoDB is running so connector metadata and cache lookups succeed.

## Step 1: Join Multiple Queries into a DataFrame

```python
from core.query_engine import QueryEngine

query_engine = QueryEngine()

dataframe = query_engine.execute_queries_to_dataframe(
    queries=[
        {
            "source_id": "census_api",
            "parameters": {
                "dataset": "2020/acs/acs5",
                "get": "NAME,B01003_001E",
                "for": "state:*"
            },
            "alias": "population",
            "rename_columns": {"B01003_001E": "population"}
        },
        {
            "source_id": "usda_quickstats",
            "parameters": {
                "commodity_desc": "CORN",
                "year": "2020",
                "format": "JSON"
            },
            "alias": "agriculture",
            "rename_columns": {"value": "corn_value"}
        }
    ],
    join_on=["state"],
    how="inner",
    aggregation={
        "group_by": ["state", "NAME"],
        "metrics": [
            {"column": "corn_value", "agg": "sum", "alias": "total_corn_value"}
        ]
    }
)
```

- `queries` defines the connector source IDs and their parameters.
- `join_on` accepts a column or list of columns shared across datasets.
- Optional `aggregation` applies group-by metrics after the join.

## Step 2: Describe the Analysis Plan

The `DataAnalysisEngine` supports modular tasks. Provide a plan dict with any of
the following sections:

| Key | Description |
| --- | --- |
| `basic_statistics` | Include descriptive statistics and correlation matrix |
| `exploratory` | Capture sample records, distributions, and missing data |
| `inferential_tests` | List of `{x, y, test}` entries (pearson, spearman, ttest) |
| `time_series` | Dict with `time_column`, `target_column`, optional `freq` |
| `linear_regression` | Dict with `features`, `target`, optional test split |
| `random_forest` | Dict with `features`, `target`, optional tree params |
| `multivariate` | Dict with `features`, `n_components` for PCA projections |
| `predictive` | Dict describing a predictive run; `model_type` = `linear` \| `forest` |

Example plan:

```python
analysis_plan = {
    "basic_statistics": True,
    "exploratory": True,
    "inferential_tests": [
        {"x": "total_corn_value", "y": "population", "test": "pearson"}
    ],
    "time_series": {
        "time_column": "year",
        "target_column": "total_corn_value",
        "freq": "A"
    },
    "linear_regression": {
        "features": ["total_corn_value"],
        "target": "population"
    },
    "random_forest": {
        "features": ["total_corn_value"],
        "target": "population",
        "n_estimators": 300
    },
    "multivariate": {
        "features": ["total_corn_value", "population"],
        "n_components": 2
    },
    "predictive": {
        "features": ["total_corn_value"],
        "target": "population",
        "model_type": "forest"
    }
}
```

## Step 3: Run the Analysis Pipeline

```python
result = query_engine.analyze_queries(
    queries=[...],           # same structure as the DataFrame step
    join_on=["state"],
    analysis_plan=analysis_plan,
    how="inner"
)

dataframe = result["dataframe"]    # pandas.DataFrame
analysis = result["analysis"]      # dict of analytic outputs

print(dataframe.head())
print(analysis["linear_regression"])
```

`analysis` mirrors the requested sections, making it easy to serialize through
the API or feed downstream dashboards.

## Full Example

See `analysis_example.py` for a runnable sample that uses in-memory data,
showcases the join helpers, and prints summaries from the analysis suite.
