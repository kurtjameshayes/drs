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
