from flask import Flask, request, jsonify
from core.connector_manager import ConnectorManager
from core.query_engine import QueryEngine
from core.cache_manager import CacheManager
from models.connector_config import ConnectorConfig
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

config_model = ConnectorConfig()
connector_manager = ConnectorManager(config_model)
cache_manager = CacheManager()
query_engine = QueryEngine(connector_manager, cache_manager)

connector_manager.load_connectors()

@app.route('/api/v1/health', methods=['GET'])
def health_check():
    try:
        stats = query_engine.get_query_stats()
        return jsonify({"status": "healthy", "stats": stats}), 200
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500

@app.route('/api/v1/sources', methods=['GET'])
def list_sources():
    try:
        sources = connector_manager.list_sources()
        return jsonify({"success": True, "sources": sources}), 200
    except Exception as e:
        logger.error(f"Error listing sources: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/sources/<source_id>', methods=['GET'])
def get_source_info(source_id):
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

@app.route('/api/v1/query', methods=['POST'])
def execute_query():
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

@app.route('/api/v1/cache/stats', methods=['GET'])
def get_cache_stats():
    try:
        stats = cache_manager.get_stats()
        return jsonify({"success": True, "stats": stats}), 200
    except Exception as e:
        logger.error(f"Error getting cache stats: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/v1/cache/<source_id>', methods=['DELETE'])
def invalidate_cache(source_id):
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
    """Create a new stored query."""
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
    """List stored queries with optional filtering."""
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
    """Get a specific stored query."""
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
    """Update a stored query."""
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
    """Delete a stored query."""
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
    """Execute a stored query."""
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
    """Search stored queries."""
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

@app.errorhandler(404)
def not_found(error):
    return jsonify({"success": False, "error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"success": False, "error": "Internal server error"}), 500

if __name__ == '__main__':
    from config import Config
    app.run(host=Config.API_HOST, port=Config.API_PORT, debug=True)
