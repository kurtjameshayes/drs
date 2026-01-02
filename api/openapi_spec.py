"""
OpenAPI 3.0 Specification for Data Retrieval System API
"""

OPENAPI_SPEC = {
    "openapi": "3.0.0",
    "info": {
        "title": "Data Retrieval System API",
        "description": "A flexible, extensible data retrieval framework with analysis capabilities",
        "version": "1.0.0",
        "contact": {
            "name": "API Support"
        }
    },
    "servers": [
        {
            "url": "/",
            "description": "Current server"
        }
    ],
    "tags": [
        {"name": "System", "description": "System health and status"},
        {"name": "Sources", "description": "Data source management"},
        {"name": "Queries", "description": "Query execution"},
        {"name": "Analysis", "description": "Data analysis operations"},
        {"name": "Cache", "description": "Cache management"},
        {"name": "Stored Queries", "description": "Stored query management"},
        {"name": "Discovery", "description": "AI-powered data source discovery"}
    ],
    "paths": {
        "/api/v1/health": {
            "get": {
                "tags": ["System"],
                "summary": "Health check endpoint",
                "responses": {
                    "200": {
                        "description": "System is healthy",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "status": {"type": "string", "example": "healthy"},
                                        "stats": {"type": "object"}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        "/api/v1/sources": {
            "get": {
                "tags": ["Sources"],
                "summary": "List all available data sources",
                "responses": {
                    "200": {
                        "description": "Successfully retrieved sources",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "success": {"type": "boolean"},
                                        "sources": {
                                            "type": "array",
                                            "items": {"type": "object"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "post": {
                "tags": ["Sources"],
                "summary": "Create a new data source",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["source_id", "source_name", "connector_type"],
                                "properties": {
                                    "source_id": {"type": "string", "example": "my_census_api"},
                                    "source_name": {"type": "string", "example": "Census Data API"},
                                    "connector_type": {"type": "string", "example": "census"},
                                    "url": {"type": "string"},
                                    "api_key": {"type": "string"}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "201": {
                        "description": "Source created successfully"
                    }
                }
            }
        },
        "/api/v1/sources/{source_id}": {
            "get": {
                "tags": ["Sources"],
                "summary": "Get information about a specific source",
                "parameters": [
                    {
                        "name": "source_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"}
                    }
                ],
                "responses": {
                    "200": {"description": "Successfully retrieved source info"}
                }
            },
            "put": {
                "tags": ["Sources"],
                "summary": "Update an existing data source",
                "parameters": [
                    {
                        "name": "source_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"}
                    }
                ],
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "source_name": {"type": "string"},
                                    "url": {"type": "string"},
                                    "api_key": {"type": "string"},
                                    "active": {"type": "boolean"}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Source updated successfully"}
                }
            },
            "delete": {
                "tags": ["Sources"],
                "summary": "Delete a data source",
                "parameters": [
                    {
                        "name": "source_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"}
                    }
                ],
                "responses": {
                    "200": {"description": "Source deleted successfully"}
                }
            }
        },
        "/api/v1/sources/{source_id}/validate": {
            "post": {
                "tags": ["Sources"],
                "summary": "Validate a data source connection",
                "parameters": [
                    {
                        "name": "source_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"}
                    }
                ],
                "responses": {
                    "200": {"description": "Validation result"}
                }
            }
        },
        "/api/v1/query": {
            "post": {
                "tags": ["Queries"],
                "summary": "Execute a query against a data source",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["source"],
                                "properties": {
                                    "source": {"type": "string", "example": "census_api"},
                                    "filters": {"type": "object"},
                                    "fields": {"type": "array", "items": {"type": "string"}},
                                    "limit": {"type": "integer"},
                                    "offset": {"type": "integer"},
                                    "use_cache": {"type": "boolean"}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Query executed successfully"}
                }
            }
        },
        "/api/v1/query/multi": {
            "post": {
                "tags": ["Queries"],
                "summary": "Execute queries across multiple data sources",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["queries"],
                                "properties": {
                                    "queries": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "source_id": {"type": "string"},
                                                "parameters": {"type": "object"}
                                            }
                                        }
                                    },
                                    "use_cache": {"type": "boolean"}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Queries executed successfully"}
                }
            }
        },
        "/api/v1/query/validate": {
            "post": {
                "tags": ["Queries"],
                "summary": "Validate a query without executing it",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["source"],
                                "properties": {
                                    "source": {"type": "string"},
                                    "filters": {"type": "object"}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Validation result"}
                }
            }
        },
        "/api/v1/query/join": {
            "post": {
                "tags": ["Queries"],
                "summary": "Execute multiple queries and join results into a DataFrame",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["queries", "join_on"],
                                "properties": {
                                    "queries": {"type": "array"},
                                    "join_on": {"type": "string"},
                                    "how": {"type": "string"},
                                    "aggregation": {"type": "object"},
                                    "use_cache": {"type": "boolean"}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Queries joined successfully"}
                }
            }
        },
        "/api/v1/query/analyze": {
            "post": {
                "tags": ["Analysis"],
                "summary": "Execute multiple queries, join them, and run analysis",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["queries", "join_on", "analysis_plan"],
                                "properties": {
                                    "queries": {"type": "array"},
                                    "join_on": {"type": "string"},
                                    "analysis_plan": {"type": "object"},
                                    "how": {"type": "string"},
                                    "aggregation": {"type": "object"},
                                    "use_cache": {"type": "boolean"}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Analysis completed successfully"}
                }
            }
        },
        "/api/v1/analysis/statistics": {
            "post": {
                "tags": ["Analysis"],
                "summary": "Calculate basic statistics for provided data",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["data"],
                                "properties": {
                                    "data": {
                                        "type": "array",
                                        "items": {"type": "object"}
                                    }
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Statistics calculated successfully"}
                }
            }
        },
        "/api/v1/analysis/regression": {
            "post": {
                "tags": ["Analysis"],
                "summary": "Perform regression analysis",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["data", "features", "target"],
                                "properties": {
                                    "data": {"type": "array"},
                                    "features": {"type": "array", "items": {"type": "string"}},
                                    "target": {"type": "string"},
                                    "model_type": {"type": "string"},
                                    "test_size": {"type": "number"},
                                    "random_state": {"type": "integer"}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Regression completed successfully"}
                }
            }
        },
        "/api/v1/analysis/classification": {
            "post": {
                "tags": ["Analysis"],
                "summary": "Perform XGBoost classification analysis",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["data", "features", "target"],
                                "properties": {
                                    "data": {"type": "array"},
                                    "features": {"type": "array", "items": {"type": "string"}},
                                    "target": {"type": "string"},
                                    "n_estimators": {"type": "integer"},
                                    "max_depth": {"type": "integer"},
                                    "learning_rate": {"type": "number"}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Classification completed successfully"}
                }
            }
        },
        "/api/v1/analysis/exploratory": {
            "post": {
                "tags": ["Analysis"],
                "summary": "Perform exploratory data analysis",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["data"],
                                "properties": {
                                    "data": {
                                        "type": "array",
                                        "items": {"type": "object"}
                                    }
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Exploratory analysis completed successfully"}
                }
            }
        },
        "/api/v1/analysis/multivariate": {
            "post": {
                "tags": ["Analysis"],
                "summary": "Perform multivariate analysis (PCA)",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["data", "features"],
                                "properties": {
                                    "data": {"type": "array"},
                                    "features": {"type": "array", "items": {"type": "string"}},
                                    "n_components": {"type": "integer", "default": 2}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Multivariate analysis completed successfully"}
                }
            }
        },
        "/api/v1/analysis/timeseries": {
            "post": {
                "tags": ["Analysis"],
                "summary": "Perform time series analysis",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["data", "time_column", "target_column"],
                                "properties": {
                                    "data": {"type": "array"},
                                    "time_column": {"type": "string"},
                                    "target_column": {"type": "string"},
                                    "freq": {"type": "string"},
                                    "rolling_window": {"type": "integer", "default": 7}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Time series analysis completed successfully"}
                }
            }
        },
        "/api/v1/analysis/inferential": {
            "post": {
                "tags": ["Analysis"],
                "summary": "Perform inferential statistical analysis",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["data", "comparisons"],
                                "properties": {
                                    "data": {"type": "array"},
                                    "comparisons": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "x": {"type": "string"},
                                                "y": {"type": "string"},
                                                "test": {"type": "string", "enum": ["pearson", "spearman", "ttest"]}
                                            }
                                        }
                                    },
                                    "alpha": {"type": "number", "default": 0.05}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Inferential analysis completed successfully"}
                }
            }
        },
        "/api/v1/cache/stats": {
            "get": {
                "tags": ["Cache"],
                "summary": "Get cache statistics",
                "responses": {
                    "200": {"description": "Successfully retrieved cache stats"}
                }
            }
        },
        "/api/v1/cache/{source_id}": {
            "delete": {
                "tags": ["Cache"],
                "summary": "Invalidate cache for a specific source",
                "parameters": [
                    {
                        "name": "source_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"}
                    }
                ],
                "responses": {
                    "200": {"description": "Cache invalidated successfully"}
                }
            }
        },
        "/api/v1/queries": {
            "get": {
                "tags": ["Stored Queries"],
                "summary": "List stored queries with optional filtering",
                "parameters": [
                    {
                        "name": "connector_id",
                        "in": "query",
                        "schema": {"type": "string"}
                    },
                    {
                        "name": "active_only",
                        "in": "query",
                        "schema": {"type": "boolean"}
                    }
                ],
                "responses": {
                    "200": {"description": "Successfully retrieved queries"}
                }
            },
            "post": {
                "tags": ["Stored Queries"],
                "summary": "Create a new stored query",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["query_id", "query_name", "connector_id", "parameters"],
                                "properties": {
                                    "query_id": {"type": "string"},
                                    "query_name": {"type": "string"},
                                    "connector_id": {"type": "string"},
                                    "parameters": {"type": "object"}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "201": {"description": "Stored query created successfully"}
                }
            }
        },
        "/api/v1/queries/{query_id}": {
            "get": {
                "tags": ["Stored Queries"],
                "summary": "Get a specific stored query",
                "parameters": [
                    {
                        "name": "query_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"}
                    }
                ],
                "responses": {
                    "200": {"description": "Successfully retrieved query"}
                }
            },
            "put": {
                "tags": ["Stored Queries"],
                "summary": "Update a stored query",
                "parameters": [
                    {
                        "name": "query_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"}
                    }
                ],
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "query_name": {"type": "string"},
                                    "parameters": {"type": "object"},
                                    "active": {"type": "boolean"}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Query updated successfully"}
                }
            },
            "delete": {
                "tags": ["Stored Queries"],
                "summary": "Delete a stored query",
                "parameters": [
                    {
                        "name": "query_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"}
                    }
                ],
                "responses": {
                    "200": {"description": "Query deleted successfully"}
                }
            }
        },
        "/api/v1/queries/{query_id}/execute": {
            "post": {
                "tags": ["Stored Queries"],
                "summary": "Execute a stored query",
                "parameters": [
                    {
                        "name": "query_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"}
                    }
                ],
                "requestBody": {
                    "required": False,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "use_cache": {"type": "boolean"},
                                    "parameter_overrides": {"type": "object"}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {"description": "Query executed successfully"}
                }
            }
        },
        "/api/v1/queries/search": {
            "get": {
                "tags": ["Stored Queries"],
                "summary": "Search stored queries",
                "parameters": [
                    {
                        "name": "q",
                        "in": "query",
                        "required": True,
                        "schema": {"type": "string"}
                    }
                ],
                "responses": {
                    "200": {"description": "Search completed successfully"}
                }
            }
        },
        "/api/v1/discovery": {
            "post": {
                "tags": ["Discovery"],
                "summary": "Discover and configure a new data source using AI-powered search",
                "description": "Uses a LangGraph workflow with 6 agents to search for, evaluate, and configure data sources based on natural language descriptions. Requires ANTHROPIC_API_KEY and TAVILY_API_KEY.",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["description"],
                                "properties": {
                                    "description": {
                                        "type": "string",
                                        "description": "Natural language description of the desired data source",
                                        "example": "US agricultural commodity prices and production statistics"
                                    }
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "Discovery completed successfully",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "success": {"type": "boolean"},
                                        "source_id": {"type": "string", "description": "ID of the newly configured source"},
                                        "config_id": {"type": "string", "description": "MongoDB ID of the configuration"},
                                        "state": {"type": "object", "description": "Full workflow state"}
                                    }
                                }
                            }
                        }
                    },
                    "400": {
                        "description": "Invalid request or discovery failed",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "success": {"type": "boolean"},
                                        "error": {
                                            "type": "object",
                                            "properties": {
                                                "agent_name": {"type": "string"},
                                                "step": {"type": "string"},
                                                "issue": {"type": "string"},
                                                "details": {"type": "string"}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "500": {"description": "Server error"}
                }
            }
        },
        "/api/v1/discovery/status": {
            "get": {
                "tags": ["Discovery"],
                "summary": "Get the status and configuration of the discovery module",
                "description": "Check if the discovery module is available and view its configuration settings.",
                "responses": {
                    "200": {
                        "description": "Discovery module status",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "success": {"type": "boolean"},
                                        "available": {"type": "boolean", "description": "Whether discovery module is available"},
                                        "config": {
                                            "type": "object",
                                            "properties": {
                                                "llm_model": {"type": "string"},
                                                "max_search_results": {"type": "integer"},
                                                "test_retries": {"type": "integer"},
                                                "request_timeout": {"type": "integer"}
                                            }
                                        },
                                        "missing_keys": {
                                            "type": "array",
                                            "items": {"type": "string"},
                                            "description": "List of missing API keys"
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "500": {"description": "Server error"}
                }
            }
        },
        "/api/v1/discovery/validate": {
            "post": {
                "tags": ["Discovery"],
                "summary": "Validate a data source description before running discovery",
                "description": "Check if a description is suitable for discovery and get suggestions for improvement.",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["description"],
                                "properties": {
                                    "description": {
                                        "type": "string",
                                        "description": "Natural language description to validate",
                                        "example": "Weather data for US cities"
                                    }
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "Validation result",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "success": {"type": "boolean"},
                                        "valid": {"type": "boolean"},
                                        "description": {"type": "string"},
                                        "length": {"type": "integer"},
                                        "suggestions": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "400": {"description": "Invalid request"},
                    "500": {"description": "Server error"}
                }
            }
        }
    }
}
