from flask import Flask, request, jsonify, render_template_string
from core.connector_manager import ConnectorManager
from core.query_engine import QueryEngine
from core.cache_manager import CacheManager
from models.connector_config import ConnectorConfig
from api.openapi_spec import OPENAPI_SPEC
import logging
import pandas as pd
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

config_model = ConnectorConfig()
connector_manager = ConnectorManager(config_model)
cache_manager = CacheManager()
query_engine = QueryEngine(connector_manager, cache_manager)

connector_manager.load_connectors()

# Helper function to convert DataFrame to dict
def dataframe_to_dict(df):
    """Convert pandas DataFrame to JSON-serializable dict."""
    return df.to_dict(orient='records')

# ============================================================================
# OpenAPI Documentation Routes
# ============================================================================

@app.route('/docs', methods=['GET'])
def api_docs():
    """Serve Swagger UI for API documentation."""
    swagger_ui_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Data Retrieval System API Documentation</title>
        <link rel="stylesheet" type="text/css" href="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.10.5/swagger-ui.css">
        <style>
            body { margin: 0; padding: 0; }
        </style>
    </head>
    <body>
        <div id="swagger-ui"></div>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.10.5/swagger-ui-bundle.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.10.5/swagger-ui-standalone-preset.js"></script>
        <script>
            window.onload = function() {
                const ui = SwaggerUIBundle({
                    url: "/openapi.json",
                    dom_id: '#swagger-ui',
                    deepLinking: true,
                    presets: [
                        SwaggerUIBundle.presets.apis,
                        SwaggerUIStandalonePreset
                    ],
                    plugins: [
                        SwaggerUIBundle.plugins.DownloadUrl
                    ],
                    layout: "StandaloneLayout"
                });
                window.ui = ui;
            };
        </script>
    </body>
    </html>
    """
    return render_template_string(swagger_ui_html)

@app.route('/openapi.json', methods=['GET'])
def openapi_spec():
    """Return OpenAPI 3.0 specification in JSON format."""
    return jsonify(OPENAPI_SPEC)

# ============================================================================
# System Routes
# ============================================================================

@app.route('/api/v1/health', methods=['GET'])
def health_check():
    """
    Health check endpoint
    ---
    tags:
      - System
    responses:
      200:
        description: System is healthy
        schema:
          type: object
          properties:
            status:
              type: string
              example: healthy
            stats:
              type: object
      500:
        description: System is unhealthy
        schema:
          type: object
          properties:
            status:
              type: string
              example: unhealthy
            error:
              type: string
    """
    try:
        stats = query_engine.get_query_stats()
        return jsonify({"status": "healthy", "stats": stats}), 200
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500

# ============================================================================
# Source Management Routes
# ============================================================================

@app.route('/api/v1/sources', methods=['GET'])
def list_sources():
    """
    List all available data sources
    ---
    tags:
      - Sources
    responses:
      200:
        description: Successfully retrieved sources
        schema:
          type: object
          properties:
            success:
              type: boolean
            sources:
              type: array
              items:
                type: object
                properties:
                  source_id:
                    type: string
                  capabilities:
                    type: object
                  connected:
                    type: boolean
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        sources = connector_manager.list_sources()
        return jsonify({"success": True, "sources": sources}), 200
    except Exception as e:
        logger.error(f"Error listing sources: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/sources/<source_id>', methods=['GET'])
def get_source_info(source_id):
    """
    Get information about a specific source
    ---
    tags:
      - Sources
    parameters:
      - name: source_id
        in: path
        type: string
        required: true
        description: Source identifier
    responses:
      200:
        description: Successfully retrieved source info
        schema:
          type: object
          properties:
            success:
              type: boolean
            source_id:
              type: string
            capabilities:
              type: object
      404:
        description: Source not found
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        connector = connector_manager.get_connector(source_id)
        if not connector:
            return jsonify({"success": False, "error": f"Source not found: {source_id}"}), 404
        return jsonify({"success": True, "source_id": source_id, "capabilities": connector.get_capabilities()}), 200
    except Exception as e:
        logger.error(f"Error getting source info: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/sources', methods=['POST'])
def create_source():
    """
    Create a new data source
    ---
    tags:
      - Sources
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - source_id
            - source_name
            - connector_type
          properties:
            source_id:
              type: string
              example: my_census_api
            source_name:
              type: string
              example: Census Data API
            connector_type:
              type: string
              example: census
            url:
              type: string
              example: https://api.census.gov/data
            api_key:
              type: string
              example: YOUR_API_KEY
    responses:
      201:
        description: Source created successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            config_id:
              type: string
            source_id:
              type: string
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        config_data = request.get_json()
        if not config_data:
            return jsonify({"success": False, "error": "Request body is required"}), 400
        required_fields = ["source_id", "source_name", "connector_type"]
        for field in required_fields:
            if field not in config_data:
                return jsonify({"success": False, "error": f"Missing required field: {field}"}), 400
        config_id = config_model.create(config_data)
        connector_manager.load_connectors()
        return jsonify({"success": True, "config_id": config_id, "source_id": config_data["source_id"]}), 201
    except Exception as e:
        logger.error(f"Error creating source: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/sources/<source_id>', methods=['PUT'])
def update_source(source_id):
    """
    Update an existing data source
    ---
    tags:
      - Sources
    parameters:
      - name: source_id
        in: path
        type: string
        required: true
        description: Source identifier
      - name: body
        in: body
        required: true
        schema:
          type: object
          properties:
            source_name:
              type: string
            url:
              type: string
            api_key:
              type: string
            active:
              type: boolean
    responses:
      200:
        description: Source updated successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            source_id:
              type: string
      404:
        description: Source not found
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        update_data = request.get_json()
        if not update_data:
            return jsonify({"success": False, "error": "Request body is required"}), 400
        updated = config_model.update(source_id, update_data)
        if updated:
            connector_manager.load_connectors()
            return jsonify({"success": True, "source_id": source_id}), 200
        else:
            return jsonify({"success": False, "error": f"Source not found: {source_id}"}), 404
    except Exception as e:
        logger.error(f"Error updating source: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/sources/<source_id>', methods=['DELETE'])
def delete_source(source_id):
    """
    Delete a data source
    ---
    tags:
      - Sources
    parameters:
      - name: source_id
        in: path
        type: string
        required: true
        description: Source identifier
    responses:
      200:
        description: Source deleted successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            source_id:
              type: string
      404:
        description: Source not found
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        deleted = config_model.delete(source_id)
        if deleted:
            if source_id in connector_manager.connectors:
                connector_manager.connectors[source_id].disconnect()
                del connector_manager.connectors[source_id]
            return jsonify({"success": True, "source_id": source_id}), 200
        else:
            return jsonify({"success": False, "error": f"Source not found: {source_id}"}), 404
    except Exception as e:
        logger.error(f"Error deleting source: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/sources/<source_id>/validate', methods=['POST'])
def validate_source(source_id):
    """
    Validate a data source connection
    ---
    tags:
      - Sources
    parameters:
      - name: source_id
        in: path
        type: string
        required: true
        description: Source identifier
    responses:
      200:
        description: Validation result
        schema:
          type: object
          properties:
            success:
              type: boolean
            valid:
              type: boolean
            message:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        valid = connector_manager.validate_connector(source_id)
        return jsonify({
            "success": True,
            "valid": valid,
            "message": "Connector is valid" if valid else "Connector validation failed"
        }), 200
    except Exception as e:
        logger.error(f"Error validating source: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================================================
# Query Execution Routes
# ============================================================================

@app.route('/api/v1/query', methods=['POST'])
def execute_query():
    """
    Execute a query against a data source
    ---
    tags:
      - Queries
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - source
          properties:
            source:
              type: string
              example: census_api
              description: Data source identifier
            filters:
              type: object
              example: {"state": "06"}
              description: Query filters
            fields:
              type: array
              items:
                type: string
              example: ["NAME", "B01003_001E"]
              description: Fields to retrieve
            limit:
              type: integer
              example: 10
              description: Maximum number of records
            offset:
              type: integer
              example: 0
              description: Record offset for pagination
            use_cache:
              type: boolean
              example: true
              description: Whether to use cached results
    responses:
      200:
        description: Query executed successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            source:
              type: string
            data:
              type: object
      400:
        description: Invalid request or query failed
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        query_data = request.get_json()
        if not query_data:
            return jsonify({"success": False, "error": "Request body is required"}), 400
        source_id = query_data.get("source")
        parameters = query_data.get("filters", {})
        use_cache = query_data.get("use_cache", True)
        if not source_id:
            return jsonify({"success": False, "error": "source parameter is required"}), 400
        if "fields" in query_data:
            parameters["columns"] = query_data["fields"]
        if "limit" in query_data:
            parameters["limit"] = query_data["limit"]
        if "offset" in query_data:
            parameters["offset"] = query_data["offset"]
        result = query_engine.execute_query(source_id, parameters, use_cache)
        status_code = 200 if result.get("success") else 400
        return jsonify(result), status_code
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/query/multi', methods=['POST'])
def execute_multi_query():
    """
    Execute queries across multiple data sources
    ---
    tags:
      - Queries
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - queries
          properties:
            queries:
              type: array
              items:
                type: object
                properties:
                  source_id:
                    type: string
                  parameters:
                    type: object
              example:
                - source_id: census_api
                  parameters: {"state": "06"}
                - source_id: usda_quickstats
                  parameters: {"commodity_desc": "CORN"}
            use_cache:
              type: boolean
              example: true
    responses:
      200:
        description: Queries executed successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            results:
              type: array
              items:
                type: object
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        query_data = request.get_json()
        if not query_data:
            return jsonify({"success": False, "error": "Request body is required"}), 400
        queries = query_data.get("queries", [])
        use_cache = query_data.get("use_cache", True)
        if not queries:
            return jsonify({"success": False, "error": "queries array is required"}), 400
        results = query_engine.execute_multi_source_query(queries, use_cache)
        return jsonify({"success": True, "results": results}), 200
    except Exception as e:
        logger.error(f"Error executing multi-query: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/query/validate', methods=['POST'])
def validate_query():
    """
    Validate a query without executing it
    ---
    tags:
      - Queries
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - source
          properties:
            source:
              type: string
              example: census_api
            filters:
              type: object
              example: {"state": "06"}
    responses:
      200:
        description: Validation result
        schema:
          type: object
          properties:
            valid:
              type: boolean
            source_id:
              type: string
            connector_type:
              type: string
            error:
              type: string
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        query_data = request.get_json()
        if not query_data:
            return jsonify({"success": False, "error": "Request body is required"}), 400
        source_id = query_data.get("source")
        parameters = query_data.get("filters", {})
        if not source_id:
            return jsonify({"success": False, "error": "source parameter is required"}), 400
        validation = query_engine.validate_query(source_id, parameters)
        return jsonify(validation), 200
    except Exception as e:
        logger.error(f"Error validating query: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/query/join', methods=['POST'])
def execute_query_join():
    """
    Execute multiple queries and join results into a DataFrame
    ---
    tags:
      - Queries
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - queries
            - join_on
          properties:
            queries:
              type: array
              items:
                type: object
                properties:
                  source_id:
                    type: string
                  parameters:
                    type: object
                  alias:
                    type: string
                  rename_columns:
                    type: object
              example:
                - source_id: census_api
                  parameters: {"dataset": "2020/acs/acs5"}
                  alias: population
                  rename_columns: {"state": "state_code"}
            join_on:
              type: string
              example: state
              description: Column name(s) to join on
            how:
              type: string
              example: inner
              description: Join type (inner, left, right, outer)
            aggregation:
              type: object
              properties:
                group_by:
                  type: array
                  items:
                    type: string
                metrics:
                  type: array
                  items:
                    type: object
            use_cache:
              type: boolean
              example: true
    responses:
      200:
        description: Queries joined successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            data:
              type: array
              items:
                type: object
            row_count:
              type: integer
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "Request body is required"}), 400

        queries = data.get("queries", [])
        join_on = data.get("join_on")
        how = data.get("how", "inner")
        aggregation = data.get("aggregation")
        use_cache = data.get("use_cache", True)

        if not queries or len(queries) < 2:
            return jsonify({"success": False, "error": "At least two queries are required"}), 400
        if not join_on:
            return jsonify({"success": False, "error": "join_on parameter is required"}), 400

        df = query_engine.execute_queries_to_dataframe(
            queries=queries,
            join_on=join_on,
            how=how,
            aggregation=aggregation,
            use_cache=use_cache
        )

        result_data = dataframe_to_dict(df)

        return jsonify({
            "success": True,
            "data": result_data,
            "row_count": len(result_data)
        }), 200

    except Exception as e:
        logger.error(f"Error executing query join: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/query/analyze', methods=['POST'])
def analyze_queries():
    """
    Execute multiple queries, join them, and run analysis
    ---
    tags:
      - Analysis
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - queries
            - join_on
            - analysis_plan
          properties:
            queries:
              type: array
              items:
                type: object
              example:
                - source_id: census_api
                  parameters: {"dataset": "2020/acs/acs5"}
                  alias: population
            join_on:
              type: string
              example: state
            analysis_plan:
              type: object
              example:
                basic_statistics: true
                exploratory: true
                linear_regression:
                  features: ["x1"]
                  target: "y"
            how:
              type: string
              example: inner
            aggregation:
              type: object
            use_cache:
              type: boolean
              example: true
    responses:
      200:
        description: Analysis completed successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            data:
              type: array
            analysis:
              type: object
            row_count:
              type: integer
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "Request body is required"}), 400

        queries = data.get("queries", [])
        join_on = data.get("join_on")
        analysis_plan = data.get("analysis_plan", {})
        how = data.get("how", "inner")
        aggregation = data.get("aggregation")
        use_cache = data.get("use_cache", True)

        if not queries or len(queries) < 2:
            return jsonify({"success": False, "error": "At least two queries are required"}), 400
        if not join_on:
            return jsonify({"success": False, "error": "join_on parameter is required"}), 400
        if not analysis_plan:
            return jsonify({"success": False, "error": "analysis_plan is required"}), 400

        result = query_engine.analyze_queries(
            queries=queries,
            join_on=join_on,
            analysis_plan=analysis_plan,
            how=how,
            aggregation=aggregation,
            use_cache=use_cache
        )

        result_data = dataframe_to_dict(result["dataframe"])

        return jsonify({
            "success": True,
            "data": result_data,
            "analysis": result["analysis"],
            "row_count": len(result_data)
        }), 200

    except Exception as e:
        logger.error(f"Error analyzing queries: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================================================
# Data Analysis Routes
# ============================================================================

@app.route('/api/v1/analysis/statistics', methods=['POST'])
def basic_statistics():
    """
    Calculate basic statistics for provided data
    ---
    tags:
      - Analysis
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - data
          properties:
            data:
              type: array
              items:
                type: object
              example:
                - x: 1
                  y: 2
                - x: 2
                  y: 4
    responses:
      200:
        description: Statistics calculated successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            statistics:
              type: object
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        data = request.get_json()
        if not data or "data" not in data:
            return jsonify({"success": False, "error": "data array is required"}), 400

        df = pd.DataFrame(data["data"])
        stats = query_engine.analysis_engine.basic_statistics(df)

        return jsonify({"success": True, "statistics": stats}), 200

    except Exception as e:
        logger.error(f"Error calculating statistics: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/analysis/regression', methods=['POST'])
def regression_analysis():
    """
    Perform regression analysis
    ---
    tags:
      - Analysis
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - data
            - features
            - target
          properties:
            data:
              type: array
              items:
                type: object
            features:
              type: array
              items:
                type: string
              example: ["x1", "x2"]
            target:
              type: string
              example: "y"
            model_type:
              type: string
              example: linear
              description: Model type (linear, forest, xgboost)
            test_size:
              type: number
              example: 0.2
            random_state:
              type: integer
              example: 42
    responses:
      200:
        description: Regression completed successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            result:
              type: object
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "Request body is required"}), 400

        required_fields = ["data", "features", "target"]
        for field in required_fields:
            if field not in data:
                return jsonify({"success": False, "error": f"Missing required field: {field}"}), 400

        df = pd.DataFrame(data["data"])
        features = data["features"]
        target = data["target"]
        model_type = data.get("model_type", "linear")
        test_size = data.get("test_size", 0.2)
        random_state = data.get("random_state", 42)

        result = query_engine.analysis_engine.predictive_analysis(
            df=df,
            features=features,
            target=target,
            model_type=model_type,
            test_size=test_size,
            random_state=random_state
        )

        return jsonify({"success": True, "result": result}), 200

    except Exception as e:
        logger.error(f"Error performing regression: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/analysis/classification', methods=['POST'])
def classification_analysis():
    """
    Perform XGBoost classification analysis
    ---
    tags:
      - Analysis
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - data
            - features
            - target
          properties:
            data:
              type: array
              items:
                type: object
            features:
              type: array
              items:
                type: string
              example: ["x1", "x2"]
            target:
              type: string
              example: "category"
            n_estimators:
              type: integer
              example: 100
            max_depth:
              type: integer
              example: 6
            learning_rate:
              type: number
              example: 0.1
            test_size:
              type: number
              example: 0.2
            random_state:
              type: integer
              example: 42
    responses:
      200:
        description: Classification completed successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            result:
              type: object
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "Request body is required"}), 400

        required_fields = ["data", "features", "target"]
        for field in required_fields:
            if field not in data:
                return jsonify({"success": False, "error": f"Missing required field: {field}"}), 400

        df = pd.DataFrame(data["data"])

        result = query_engine.analysis_engine.xgboost_classification(
            df=df,
            features=data["features"],
            target=data["target"],
            n_estimators=data.get("n_estimators", 100),
            max_depth=data.get("max_depth", 6),
            learning_rate=data.get("learning_rate", 0.1),
            test_size=data.get("test_size", 0.2),
            random_state=data.get("random_state", 42)
        )

        return jsonify({"success": True, "result": result}), 200

    except Exception as e:
        logger.error(f"Error performing classification: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/analysis/exploratory', methods=['POST'])
def exploratory_analysis():
    """
    Perform exploratory data analysis
    ---
    tags:
      - Analysis
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - data
          properties:
            data:
              type: array
              items:
                type: object
              example:
                - x: 1
                  y: 2
                  category: A
                - x: 2
                  y: 4
                  category: B
    responses:
      200:
        description: Exploratory analysis completed successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            result:
              type: object
              properties:
                data_types:
                  type: object
                sample_records:
                  type: array
                distribution:
                  type: object
                missing_percentage:
                  type: object
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        data = request.get_json()
        if not data or "data" not in data:
            return jsonify({"success": False, "error": "data array is required"}), 400

        df = pd.DataFrame(data["data"])
        result = query_engine.analysis_engine.exploratory_analysis(df)

        return jsonify({"success": True, "result": result}), 200

    except Exception as e:
        logger.error(f"Error performing exploratory analysis: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/analysis/multivariate', methods=['POST'])
def multivariate_analysis():
    """
    Perform multivariate analysis (PCA)
    ---
    tags:
      - Analysis
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - data
            - features
          properties:
            data:
              type: array
              items:
                type: object
              example:
                - x1: 1
                  x2: 2
                  x3: 3
                - x1: 4
                  x2: 5
                  x3: 6
            features:
              type: array
              items:
                type: string
              example: ["x1", "x2", "x3"]
            n_components:
              type: integer
              example: 2
              description: Number of principal components (default 2)
    responses:
      200:
        description: Multivariate analysis completed successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            result:
              type: object
              properties:
                explained_variance_ratio:
                  type: array
                  items:
                    type: number
                components:
                  type: array
                projected_samples:
                  type: array
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "Request body is required"}), 400

        required_fields = ["data", "features"]
        for field in required_fields:
            if field not in data:
                return jsonify({"success": False, "error": f"Missing required field: {field}"}), 400

        df = pd.DataFrame(data["data"])
        features = data["features"]
        n_components = data.get("n_components", 2)

        result = query_engine.analysis_engine.multivariate_analysis(
            df=df,
            features=features,
            n_components=n_components
        )

        return jsonify({"success": True, "result": result}), 200

    except Exception as e:
        logger.error(f"Error performing multivariate analysis: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/analysis/timeseries', methods=['POST'])
def timeseries_analysis():
    """
    Perform time series analysis
    ---
    tags:
      - Analysis
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - data
            - time_column
            - target_column
          properties:
            data:
              type: array
              items:
                type: object
              example:
                - date: "2024-01-01"
                  value: 100
                - date: "2024-01-02"
                  value: 105
            time_column:
              type: string
              example: "date"
            target_column:
              type: string
              example: "value"
            freq:
              type: string
              example: "D"
              description: Resampling frequency (D=daily, W=weekly, M=monthly, etc.)
            rolling_window:
              type: integer
              example: 7
              description: Window size for rolling mean calculation (default 7)
    responses:
      200:
        description: Time series analysis completed successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            result:
              type: object
              properties:
                recent_values:
                  type: object
                rolling_mean:
                  type: object
                volatility:
                  type: number
                trend_slope:
                  type: number
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "Request body is required"}), 400

        required_fields = ["data", "time_column", "target_column"]
        for field in required_fields:
            if field not in data:
                return jsonify({"success": False, "error": f"Missing required field: {field}"}), 400

        df = pd.DataFrame(data["data"])
        time_column = data["time_column"]
        target_column = data["target_column"]
        freq = data.get("freq")
        rolling_window = data.get("rolling_window", 7)

        result = query_engine.analysis_engine.time_series_analysis(
            df=df,
            time_column=time_column,
            target_column=target_column,
            freq=freq,
            rolling_window=rolling_window
        )

        return jsonify({"success": True, "result": result}), 200

    except Exception as e:
        logger.error(f"Error performing time series analysis: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/analysis/inferential', methods=['POST'])
def inferential_analysis():
    """
    Perform inferential statistical analysis
    ---
    tags:
      - Analysis
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - data
            - comparisons
          properties:
            data:
              type: array
              items:
                type: object
              example:
                - x: 1
                  y: 2
                - x: 2
                  y: 4
            comparisons:
              type: array
              items:
                type: object
                properties:
                  x:
                    type: string
                  y:
                    type: string
                  test:
                    type: string
                    enum: [pearson, spearman, ttest]
              example:
                - x: "x"
                  y: "y"
                  test: "pearson"
            alpha:
              type: number
              example: 0.05
              description: Significance level (default 0.05)
    responses:
      200:
        description: Inferential analysis completed successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            result:
              type: array
              items:
                type: object
                properties:
                  x:
                    type: string
                  y:
                    type: string
                  test:
                    type: string
                  statistic:
                    type: number
                  p_value:
                    type: number
                  significant:
                    type: boolean
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "Request body is required"}), 400

        required_fields = ["data", "comparisons"]
        for field in required_fields:
            if field not in data:
                return jsonify({"success": False, "error": f"Missing required field: {field}"}), 400

        df = pd.DataFrame(data["data"])
        comparisons = data["comparisons"]
        alpha = data.get("alpha", 0.05)

        result = query_engine.analysis_engine.inferential_analysis(
            df=df,
            comparisons=comparisons,
            alpha=alpha
        )

        return jsonify({"success": True, "result": result}), 200

    except Exception as e:
        logger.error(f"Error performing inferential analysis: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================================================
# Cache Management Routes
# ============================================================================

@app.route('/api/v1/cache/stats', methods=['GET'])
def get_cache_stats():
    """
    Get cache statistics
    ---
    tags:
      - Cache
    responses:
      200:
        description: Successfully retrieved cache stats
        schema:
          type: object
          properties:
            success:
              type: boolean
            stats:
              type: object
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        stats = cache_manager.get_stats()
        return jsonify({"success": True, "stats": stats}), 200
    except Exception as e:
        logger.error(f"Error getting cache stats: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/cache/<source_id>', methods=['DELETE'])
def invalidate_cache(source_id):
    """
    Invalidate cache for a specific source
    ---
    tags:
      - Cache
    parameters:
      - name: source_id
        in: path
        type: string
        required: true
        description: Source identifier
    responses:
      200:
        description: Cache invalidated successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            invalidated_count:
              type: integer
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        count = cache_manager.invalidate(source_id)
        return jsonify({"success": True, "invalidated_count": count}), 200
    except Exception as e:
        logger.error(f"Error invalidating cache: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================================================
# Stored Query Routes
# ============================================================================

@app.route('/api/v1/queries', methods=['POST'])
def create_stored_query():
    """
    Create a new stored query
    ---
    tags:
      - Stored Queries
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - query_id
            - query_name
            - connector_id
            - parameters
          properties:
            query_id:
              type: string
              example: my_census_query
            query_name:
              type: string
              example: Census Population Query
            connector_id:
              type: string
              example: census_api
            parameters:
              type: object
              example:
                dataset: 2020/acs/acs5
                get: NAME,B01003_001E
            description:
              type: string
            tags:
              type: array
              items:
                type: string
            active:
              type: boolean
    responses:
      201:
        description: Stored query created successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            message:
              type: string
            query_id:
              type: string
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        data = request.get_json()

        # Validate required fields
        required_fields = ['query_id', 'query_name', 'connector_id', 'parameters']
        for field in required_fields:
            if field not in data:
                return jsonify({"success": False, "error": f"Missing required field: {field}"}), 400

        # Create stored query
        query_engine.stored_query.create(data)

        return jsonify({
            "success": True,
            "message": "Stored query created successfully",
            "query_id": data['query_id']
        }), 201

    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.error(f"Error creating stored query: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/queries', methods=['GET'])
def list_stored_queries():
    """
    List stored queries with optional filtering
    ---
    tags:
      - Stored Queries
    parameters:
      - name: connector_id
        in: query
        type: string
        required: false
        description: Filter by connector ID
      - name: active_only
        in: query
        type: boolean
        required: false
        description: Only return active queries
      - name: tags
        in: query
        type: array
        items:
          type: string
        required: false
        description: Filter by tags
    responses:
      200:
        description: Successfully retrieved queries
        schema:
          type: object
          properties:
            success:
              type: boolean
            queries:
              type: array
              items:
                type: object
            count:
              type: integer
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        connector_id = request.args.get('connector_id')
        active_only = request.args.get('active_only', 'false').lower() == 'true'
        tags = request.args.getlist('tags')

        if tags:
            queries = query_engine.stored_query.get_all(
                connector_id=connector_id,
                active_only=active_only,
                tags=tags
            )
        else:
            queries = query_engine.stored_query.get_all(
                connector_id=connector_id,
                active_only=active_only
            )

        return jsonify({
            "success": True,
            "queries": queries,
            "count": len(queries)
        }), 200

    except Exception as e:
        logger.error(f"Error listing stored queries: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/queries/<query_id>', methods=['GET'])
def get_stored_query(query_id):
    """
    Get a specific stored query
    ---
    tags:
      - Stored Queries
    parameters:
      - name: query_id
        in: path
        type: string
        required: true
        description: Query identifier
    responses:
      200:
        description: Successfully retrieved query
        schema:
          type: object
          properties:
            success:
              type: boolean
            query:
              type: object
      404:
        description: Query not found
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        query = query_engine.get_stored_query(query_id)

        if not query:
            return jsonify({"success": False, "error": f"Query not found: {query_id}"}), 404

        return jsonify({"success": True, "query": query}), 200

    except Exception as e:
        logger.error(f"Error getting stored query: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/queries/<query_id>', methods=['PUT'])
def update_stored_query(query_id):
    """
    Update a stored query
    ---
    tags:
      - Stored Queries
    parameters:
      - name: query_id
        in: path
        type: string
        required: true
        description: Query identifier
      - name: body
        in: body
        required: true
        schema:
          type: object
          properties:
            query_name:
              type: string
            parameters:
              type: object
            description:
              type: string
            tags:
              type: array
              items:
                type: string
            active:
              type: boolean
    responses:
      200:
        description: Query updated successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            message:
              type: string
            query_id:
              type: string
      404:
        description: Query not found
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        data = request.get_json()

        success = query_engine.stored_query.update(query_id, data)

        if success:
            return jsonify({
                "success": True,
                "message": "Stored query updated successfully",
                "query_id": query_id
            }), 200
        else:
            return jsonify({"success": False, "error": f"Query not found: {query_id}"}), 404

    except Exception as e:
        logger.error(f"Error updating stored query: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/queries/<query_id>', methods=['DELETE'])
def delete_stored_query(query_id):
    """
    Delete a stored query
    ---
    tags:
      - Stored Queries
    parameters:
      - name: query_id
        in: path
        type: string
        required: true
        description: Query identifier
    responses:
      200:
        description: Query deleted successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            message:
              type: string
            query_id:
              type: string
      404:
        description: Query not found
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        success = query_engine.stored_query.delete(query_id)

        if success:
            return jsonify({
                "success": True,
                "message": "Stored query deleted successfully",
                "query_id": query_id
            }), 200
        else:
            return jsonify({"success": False, "error": f"Query not found: {query_id}"}), 404

    except Exception as e:
        logger.error(f"Error deleting stored query: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/queries/<query_id>/execute', methods=['POST'])
def execute_stored_query(query_id):
    """
    Execute a stored query
    ---
    tags:
      - Stored Queries
    parameters:
      - name: query_id
        in: path
        type: string
        required: true
        description: Query identifier
      - name: body
        in: body
        required: false
        schema:
          type: object
          properties:
            use_cache:
              type: boolean
              example: true
            parameter_overrides:
              type: object
              example:
                state: "06"
    responses:
      200:
        description: Query executed successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            data:
              type: object
            query_name:
              type: string
            query_description:
              type: string
      400:
        description: Query execution failed
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        data = request.get_json() or {}

        # Get optional parameters
        use_cache = data.get('use_cache', True)
        parameter_overrides = data.get('parameter_overrides')

        # Execute stored query
        result = query_engine.execute_stored_query(
            query_id,
            use_cache=use_cache,
            parameter_overrides=parameter_overrides
        )

        if result.get("success"):
            return jsonify(result), 200
        else:
            return jsonify(result), 400

    except Exception as e:
        logger.error(f"Error executing stored query: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/queries/search', methods=['GET'])
def search_stored_queries():
    """
    Search stored queries
    ---
    tags:
      - Stored Queries
    parameters:
      - name: q
        in: query
        type: string
        required: true
        description: Search term
    responses:
      200:
        description: Search completed successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            queries:
              type: array
              items:
                type: object
            count:
              type: integer
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Error occurred
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        search_term = request.args.get('q', '')

        if not search_term:
            return jsonify({"success": False, "error": "Search term required (q parameter)"}), 400

        queries = query_engine.stored_query.search(search_term)

        return jsonify({
            "success": True,
            "queries": queries,
            "count": len(queries)
        }), 200

    except Exception as e:
        logger.error(f"Error searching stored queries: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================================================
# Data Source Discovery Routes
# ============================================================================

@app.route('/api/v1/discovery', methods=['POST'])
def discover_data_source():
    """
    Discover and configure a new data source using AI-powered search
    ---
    tags:
      - Discovery
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - description
          properties:
            description:
              type: string
              example: US agricultural commodity prices and production statistics
              description: Natural language description of the desired data source
            require_selection_confirmation:
              type: boolean
              default: true
              description: If true, pause for user to confirm/modify source selection when multiple options are found
    responses:
      200:
        description: Discovery completed successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            source_id:
              type: string
              description: ID of the newly configured source
            config_id:
              type: string
              description: MongoDB ID of the configuration
            state:
              type: object
              description: Full workflow state including discovered sources and documentation
      400:
        description: Invalid request or discovery failed
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: object
              properties:
                agent_name:
                  type: string
                step:
                  type: string
                issue:
                  type: string
                details:
                  type: string
      500:
        description: Server error
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        from core.discovery.workflow import DataSourceDiscoveryWorkflow

        data = request.get_json()
        if not data or "description" not in data:
            return jsonify({
                "success": False,
                "error": "description field is required"
            }), 400

        description = data["description"]
        if not description or len(description.strip()) < 10:
            return jsonify({
                "success": False,
                "error": "description must be at least 10 characters"
            }), 400

        # Get optional configuration
        require_selection_confirmation = data.get("require_selection_confirmation", True)

        logger.info(f"Starting data source discovery for: {description[:100]}... (selection_confirmation={require_selection_confirmation})")

        workflow = DataSourceDiscoveryWorkflow(require_selection_confirmation=require_selection_confirmation)
        result = workflow.run(description)

        if result["success"]:
            # Reload connectors to pick up the new source
            connector_manager.load_connectors()

            return jsonify({
                "success": True,
                "workflow_id": result.get("workflow_id"),
                "source_id": result["source_id"],
                "config_id": result["config_id"],
                "state": result["state"]
            }), 200
        elif result.get("paused"):
            # Workflow is paused waiting for human input
            return jsonify({
                "success": False,
                "paused": True,
                "workflow_id": result.get("workflow_id"),
                "current_step": result.get("current_step"),
                "human_input_request": result.get("human_input_request"),
                "error": result.get("error"),
                "message": "Workflow paused - human input required. Use POST /api/v1/discovery/{workflow_id}/resume to provide input.",
                "state": result.get("state")
            }), 200
        else:
            return jsonify({
                "success": False,
                "workflow_id": result.get("workflow_id"),
                "error": result.get("error", {"issue": "Unknown error"}),
                "state": result.get("state")
            }), 400

    except ImportError as e:
        logger.error(f"Discovery module not available: {str(e)}")
        return jsonify({
            "success": False,
            "error": "Discovery module not available. Ensure ANTHROPIC_API_KEY and TAVILY_API_KEY are set."
        }), 500
    except Exception as e:
        logger.error(f"Error in data source discovery: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/v1/discovery/status', methods=['GET'])
def get_discovery_status():
    """
    Get the status and configuration of the discovery module
    ---
    tags:
      - Discovery
    responses:
      200:
        description: Discovery module status
        schema:
          type: object
          properties:
            success:
              type: boolean
            available:
              type: boolean
              description: Whether discovery module is available
            config:
              type: object
              properties:
                llm_model:
                  type: string
                max_search_results:
                  type: integer
                test_retries:
                  type: integer
                request_timeout:
                  type: integer
            missing_keys:
              type: array
              items:
                type: string
              description: List of missing API keys
      500:
        description: Server error
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        from config import Config

        missing_keys = []
        if not getattr(Config, 'ANTHROPIC_API_KEY', None):
            missing_keys.append("ANTHROPIC_API_KEY")
        if not getattr(Config, 'TAVILY_API_KEY', None):
            missing_keys.append("TAVILY_API_KEY")

        available = len(missing_keys) == 0

        config_info = {
            "llm_model": getattr(Config, 'DISCOVERY_LLM_MODEL', 'claude-sonnet-4-20250514'),
            "max_search_results": getattr(Config, 'DISCOVERY_MAX_SEARCH_RESULTS', 10),
            "test_retries": getattr(Config, 'DISCOVERY_TEST_RETRIES', 3),
            "request_timeout": getattr(Config, 'DISCOVERY_REQUEST_TIMEOUT', 30),
        }

        return jsonify({
            "success": True,
            "available": available,
            "config": config_info,
            "missing_keys": missing_keys
        }), 200

    except Exception as e:
        logger.error(f"Error getting discovery status: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/v1/discovery/validate', methods=['POST'])
def validate_discovery_description():
    """
    Validate a data source description before running discovery
    ---
    tags:
      - Discovery
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - description
          properties:
            description:
              type: string
              example: Weather data for US cities
              description: Natural language description to validate
    responses:
      200:
        description: Validation result
        schema:
          type: object
          properties:
            success:
              type: boolean
            valid:
              type: boolean
            description:
              type: string
            length:
              type: integer
            suggestions:
              type: array
              items:
                type: string
      400:
        description: Invalid request
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
      500:
        description: Server error
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: string
    """
    try:
        data = request.get_json()
        if not data or "description" not in data:
            return jsonify({
                "success": False,
                "error": "description field is required"
            }), 400

        description = data["description"].strip()
        suggestions = []
        valid = True

        # Check length
        if len(description) < 10:
            valid = False
            suggestions.append("Description should be at least 10 characters long")

        if len(description) < 20:
            suggestions.append("Consider adding more detail about the type of data you need")

        # Check for specificity
        generic_terms = ["data", "information", "stuff", "things"]
        words = description.lower().split()
        if all(word in generic_terms for word in words if len(word) > 3):
            suggestions.append("Be more specific about what kind of data you're looking for")

        # Suggest including domain
        domain_keywords = ["api", "database", "statistics", "records", "census", "weather", "financial", "government"]
        if not any(kw in description.lower() for kw in domain_keywords):
            suggestions.append("Consider specifying the domain or type of source (e.g., 'government API', 'statistics database')")

        return jsonify({
            "success": True,
            "valid": valid,
            "description": description,
            "length": len(description),
            "suggestions": suggestions
        }), 200

    except Exception as e:
        logger.error(f"Error validating discovery description: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================================================
# Workflow Management Routes
# ============================================================================

@app.route('/api/v1/discovery/workflows', methods=['GET'])
def list_workflows():
    """
    List recent discovery workflows
    ---
    tags:
      - Discovery
    parameters:
      - name: status
        in: query
        type: string
        required: false
        description: Filter by status (pending, running, paused, completed, failed, cancelled)
      - name: limit
        in: query
        type: integer
        required: false
        default: 20
        description: Maximum number of workflows to return
    responses:
      200:
        description: List of workflows
        schema:
          type: object
          properties:
            success:
              type: boolean
            workflows:
              type: array
              items:
                type: object
            count:
              type: integer
      500:
        description: Server error
    """
    try:
        from core.discovery.workflow import DataSourceDiscoveryWorkflow
        from models.workflow_state import WorkflowStatus

        status_filter = request.args.get('status')
        limit = int(request.args.get('limit', 20))

        workflow = DataSourceDiscoveryWorkflow()

        if status_filter:
            try:
                status = WorkflowStatus(status_filter)
                workflows = workflow.workflow_state_model.get_recent_workflows(limit=limit, status=status)
            except ValueError:
                return jsonify({
                    "success": False,
                    "error": f"Invalid status: {status_filter}. Valid values: pending, running, paused, completed, failed, cancelled"
                }), 400
        else:
            workflows = workflow.workflow_state_model.get_recent_workflows(limit=limit)

        return jsonify({
            "success": True,
            "workflows": workflows,
            "count": len(workflows)
        }), 200

    except Exception as e:
        logger.error(f"Error listing workflows: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/v1/discovery/workflows/paused', methods=['GET'])
def list_paused_workflows():
    """
    List all workflows that are paused waiting for human input
    ---
    tags:
      - Discovery
    responses:
      200:
        description: List of paused workflows
        schema:
          type: object
          properties:
            success:
              type: boolean
            workflows:
              type: array
              items:
                type: object
                properties:
                  workflow_id:
                    type: string
                  user_description:
                    type: string
                  pause_reason:
                    type: string
                  pause_details:
                    type: string
                  human_input_required:
                    type: object
                  paused_at:
                    type: string
            count:
              type: integer
      500:
        description: Server error
    """
    try:
        from core.discovery.workflow import DataSourceDiscoveryWorkflow

        workflow = DataSourceDiscoveryWorkflow()
        paused = workflow.get_paused_workflows()

        return jsonify({
            "success": True,
            "workflows": paused,
            "count": len(paused)
        }), 200

    except Exception as e:
        logger.error(f"Error listing paused workflows: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/v1/discovery/<workflow_id>', methods=['GET'])
def get_workflow_status(workflow_id):
    """
    Get the status of a specific discovery workflow
    ---
    tags:
      - Discovery
    parameters:
      - name: workflow_id
        in: path
        type: string
        required: true
        description: Workflow identifier
    responses:
      200:
        description: Workflow status
        schema:
          type: object
          properties:
            success:
              type: boolean
            workflow:
              type: object
              properties:
                workflow_id:
                  type: string
                status:
                  type: string
                current_step:
                  type: string
                steps_completed:
                  type: array
                  items:
                    type: string
                pause_reason:
                  type: string
                human_input_required:
                  type: object
                error:
                  type: object
                source_id:
                  type: string
                config_id:
                  type: string
      404:
        description: Workflow not found
      500:
        description: Server error
    """
    try:
        from core.discovery.workflow import DataSourceDiscoveryWorkflow

        workflow = DataSourceDiscoveryWorkflow()
        status = workflow.get_workflow_status(workflow_id)

        if not status:
            return jsonify({
                "success": False,
                "error": f"Workflow not found: {workflow_id}"
            }), 404

        return jsonify({
            "success": True,
            "workflow": status
        }), 200

    except Exception as e:
        logger.error(f"Error getting workflow status: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/v1/discovery/<workflow_id>/resume', methods=['POST'])
def resume_workflow(workflow_id):
    """
    Resume a paused workflow with human input
    ---
    tags:
      - Discovery
    parameters:
      - name: workflow_id
        in: path
        type: string
        required: true
        description: Workflow identifier
      - name: body
        in: body
        required: true
        schema:
          type: object
          properties:
            api_key:
              type: string
              description: API key for the data source (if paused for API key)
            oauth_token:
              type: string
              description: OAuth token (if paused for OAuth)
            username:
              type: string
              description: Username (if paused for username/password auth)
            password:
              type: string
              description: Password (if paused for username/password auth)
            confirmed:
              type: boolean
              description: Confirmation response (if paused for selection confirmation)
            selected_index:
              type: integer
              description: Index of selected option (if choosing different from recommended)
            action:
              type: string
              enum: [retry, skip, cancel]
              description: Action to take (if paused for error guidance)
          example:
            confirmed: true
    responses:
      200:
        description: Workflow resumed and completed/paused again
        schema:
          type: object
          properties:
            success:
              type: boolean
            workflow_id:
              type: string
            source_id:
              type: string
            config_id:
              type: string
            paused:
              type: boolean
              description: True if workflow is paused again waiting for more input
            current_step:
              type: string
              description: Current step in the workflow
            human_input_request:
              type: object
              description: Details about required input if paused again
            cancelled:
              type: boolean
              description: True if workflow was cancelled by user
      400:
        description: Invalid request or workflow not paused
        schema:
          type: object
          properties:
            success:
              type: boolean
            error:
              type: object
      404:
        description: Workflow not found
      500:
        description: Server error
    """
    try:
        from core.discovery.workflow import DataSourceDiscoveryWorkflow

        data = request.get_json()
        if not data:
            return jsonify({
                "success": False,
                "error": "Request body is required with human input"
            }), 400

        workflow = DataSourceDiscoveryWorkflow()
        result = workflow.resume(workflow_id, data)

        if (result.get("error") or {}).get("issue") == "Workflow not found":
            return jsonify(result), 404

        if (result.get("error") or {}).get("issue") == "Workflow not paused":
            return jsonify(result), 400

        if result.get("cancelled"):
            return jsonify({
                "success": False,
                "workflow_id": result["workflow_id"],
                "cancelled": True,
                "message": "Workflow cancelled by user"
            }), 200

        if result["success"]:
            # Reload connectors to pick up the new source
            connector_manager.load_connectors()

            return jsonify({
                "success": True,
                "workflow_id": result["workflow_id"],
                "source_id": result["source_id"],
                "config_id": result["config_id"],
                "paused": False
            }), 200
        elif result.get("paused"):
            return jsonify({
                "success": False,
                "workflow_id": result["workflow_id"],
                "paused": True,
                "current_step": result.get("current_step"),
                "human_input_request": result.get("human_input_request"),
                "error": result.get("error"),
                "message": "Workflow still requires additional input"
            }), 200
        else:
            return jsonify({
                "success": False,
                "workflow_id": result["workflow_id"],
                "error": result.get("error"),
                "paused": False
            }), 400

    except Exception as e:
        logger.error(f"Error resuming workflow: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/v1/discovery/<workflow_id>/retry', methods=['POST'])
def retry_workflow(workflow_id):
    """
    Retry a failed workflow from a specific step or from where it failed
    ---
    tags:
      - Discovery
    parameters:
      - name: workflow_id
        in: path
        type: string
        required: true
        description: Workflow identifier
      - name: body
        in: body
        required: false
        schema:
          type: object
          properties:
            from_step:
              type: string
              enum: [search, selection, examine, documentation, api_key, testing, config, report]
              description: Optional step to retry from. If not provided, retries from the failed step.
          example:
            from_step: "testing"
    responses:
      200:
        description: Workflow retried successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            workflow_id:
              type: string
            source_id:
              type: string
            config_id:
              type: string
            paused:
              type: boolean
            current_step:
              type: string
            human_input_request:
              type: object
            error:
              type: object
      400:
        description: Workflow not in failed status
      404:
        description: Workflow not found
      500:
        description: Server error
    """
    try:
        from core.discovery.workflow import DataSourceDiscoveryWorkflow

        # Get optional from_step parameter
        data = request.get_json() or {}
        from_step = data.get("from_step")

        workflow = DataSourceDiscoveryWorkflow()
        result = workflow.retry_failed_workflow(workflow_id, from_step=from_step)

        # Handle not found
        if (result.get("error") or {}).get("issue") == "Workflow not found":
            return jsonify(result), 404

        # Handle not failed status
        if (result.get("error") or {}).get("issue") == "Workflow not in failed status":
            return jsonify(result), 400

        # Handle successful completion
        if result["success"]:
            # Reload connectors to pick up the new source
            connector_manager.load_connectors()

            return jsonify({
                "success": True,
                "workflow_id": result["workflow_id"],
                "source_id": result.get("source_id"),
                "config_id": result.get("config_id"),
                "paused": False,
                "message": "Workflow retried and completed successfully"
            }), 200

        # Handle paused (needs human input again)
        elif result.get("paused"):
            return jsonify({
                "success": False,
                "workflow_id": result["workflow_id"],
                "paused": True,
                "current_step": result.get("current_step"),
                "human_input_request": result.get("human_input_request"),
                "error": result.get("error"),
                "message": "Workflow retried but requires human input"
            }), 200

        # Handle failed again
        else:
            return jsonify({
                "success": False,
                "workflow_id": result["workflow_id"],
                "error": result.get("error"),
                "paused": False,
                "message": "Workflow retry failed"
            }), 400

    except Exception as e:
        logger.error(f"Error retrying workflow: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/v1/discovery/<workflow_id>/cancel', methods=['POST'])
def cancel_workflow(workflow_id):
    """
    Cancel a running or paused workflow
    ---
    tags:
      - Discovery
    parameters:
      - name: workflow_id
        in: path
        type: string
        required: true
        description: Workflow identifier
    responses:
      200:
        description: Workflow cancelled successfully
        schema:
          type: object
          properties:
            success:
              type: boolean
            workflow_id:
              type: string
            message:
              type: string
      404:
        description: Workflow not found
      500:
        description: Server error
    """
    try:
        from core.discovery.workflow import DataSourceDiscoveryWorkflow

        workflow = DataSourceDiscoveryWorkflow()

        # Check if workflow exists
        status = workflow.get_workflow_status(workflow_id)
        if not status:
            return jsonify({
                "success": False,
                "error": f"Workflow not found: {workflow_id}"
            }), 404

        success = workflow.cancel_workflow(workflow_id)

        if success:
            return jsonify({
                "success": True,
                "workflow_id": workflow_id,
                "message": "Workflow cancelled successfully"
            }), 200
        else:
            return jsonify({
                "success": False,
                "error": "Failed to cancel workflow"
            }), 400

    except Exception as e:
        logger.error(f"Error cancelling workflow: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500


# ============================================================================
# Error Handlers
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({"success": False, "error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"success": False, "error": "Internal server error"}), 500

if __name__ == '__main__':
    from config import Config
    app.run(host=Config.API_HOST, port=Config.API_PORT, debug=True)
