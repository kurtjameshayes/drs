# CLAUDE.MD - Data Retrieval System (DRS)

## Project Overview

The Data Retrieval System (DRS) is a flexible, extensible data retrieval framework built with Python, Flask, and MongoDB. It provides a unified interface for querying multiple data sources (USDA NASS, Census.gov, FBI Crime Data, local files) with caching, analysis capabilities, and AI-powered data source discovery.

## Core Architecture

### Components

1. **Core System** (`core/`)
   - `base_connector.py` - Abstract base class for all connectors
   - `connector_manager.py` - Manages connector lifecycle and routing
   - `query_engine.py` - Query execution, joining, and analysis orchestration
   - `cache_manager.py` - MongoDB-based caching with TTL
   - `data_analysis.py` - Statistical analysis engine (DataAnalysisEngine)
   - `analysis_plan_manager.py` - Manages saved analysis plans

2. **Connectors** (`connectors/`)
   - `usda_nass/` - USDA NASS QuickStats API connector
   - `census/` - Census.gov API connector
   - `fbi_crime/` - FBI Crime Data API connector
   - `local_file/` - Local CSV/JSON/Excel file connector
   - Each connector extends `BaseConnector` and implements `execute_query()`

3. **AI Discovery System** (`core/discovery/`)
   - **Agents** (`agents/`) - LangGraph-based workflow with specialized agents:
     - `search_agent.py` - Finds data sources via Tavily and registries
     - `examination_agent.py` - Evaluates access methods
     - `selection_agent.py` - Selects best source
     - `documentation_agent.py` - Extracts API documentation
     - `api_key_agent.py` - Automated API key acquisition via browser automation
     - `testing_agent.py` - Tests data source connectivity
     - `configuration_agent.py` - Stores configuration in MongoDB
     - `workflow_decision_agent.py` - Orchestrates agent workflow
   - **Services** (`services/`)
     - `api_key_store.py` - Encrypted API key storage using Fernet
     - `browser_automation_service.py` - Selenium-based form automation
     - `arcade_email_service.py` - Email verification via Arcade
   - `workflow.py` - LangGraph workflow definition
   - `state.py` - Workflow state management
   - `tools.py` - LLM tools for agents
   - `prompts.py` - Agent prompts

4. **Models** (`models/`)
   - `connector_config.py` - MongoDB schema for connector configurations
   - `query_result.py` - Cached query results
   - `analysis_query_result.py` - Analysis results with metadata
   - `stored_query.py` - Saved query templates
   - `workflow_state.py` - Discovery workflow state persistence

5. **API** (`api/`)
   - `routes.py` - Flask REST API endpoints
   - `openapi_spec.py` - OpenAPI/Swagger specification

### Data Flow

```
User Request → API Routes → Query Engine → Connector Manager → Specific Connector → External API/File
                                ↓
                          Cache Manager (MongoDB)
                                ↓
                    Data Analysis Engine (optional)
                                ↓
                            Response
```

## Key Design Patterns

### 1. Connector Pattern
All data sources implement `BaseConnector`:
```python
class CustomConnector(BaseConnector):
    def execute_query(self, parameters):
        # Implement data fetching logic
        return data
```

### 2. Caching Strategy
- Query results cached in MongoDB with TTL
- Cache key: hash of (source_id, parameters)
- Automatic invalidation via TTL or manual via API

### 3. Analysis Pipeline
```python
QueryEngine.analyze_queries([queries], join_on, analysis_plan)
→ Execute queries
→ Join into pandas DataFrame
→ Run statistical analysis
→ Return combined results
```

### 4. AI Discovery Workflow
- StateGraph with conditional edges
- Each agent returns state updates
- Workflow decision agent determines next step
- Error handling with retry logic

## Critical Files

### Entry Points
- `main.py` - CLI entry point for server
- `flask_app.py` - Flask application factory
- `init_db.py` - Database initialization

### Configuration
- `config.py` - Central configuration (loads from .env)
- `.env` - Environment variables (NEVER commit)
- `.env.example` - Template for required env vars

### Utilities
- `add_connectors.py` - Add/update connector configurations
- `validate_connectors.py` - Validate connector setup
- `execute_query.py` - CLI query execution
- `manage_queries.py` - Manage stored queries

## Development Workflow

### Setup
1. Install dependencies: `pip install -r requirements.txt`
2. Generate encryption key: `python scripts/generate_encryption_key.py`
3. Copy `.env.example` to `.env` and configure
4. Initialize DB: `python init_db.py`
5. Add connectors: `python add_connectors.py`

### Testing
- Tests in `tests/` directory
- Run: `pytest tests/`
- Uses mongomock for MongoDB testing
- Key test files:
  - `test_query_engine_analysis.py` - Query and analysis tests
  - `test_data_analysis_engine.py` - Statistical analysis tests
  - `test_connector_manager.py` - Connector routing tests
  - `test_*_connector.py` - Individual connector tests

### Adding New Connectors
1. Create directory in `connectors/`
2. Implement `connector.py` extending `BaseConnector`
3. Add `__init__.py` with connector export
4. Register in `connectors/__init__.py`
5. Add configuration via `add_connectors.py`
6. Write tests in `tests/test_*_connector.py`

## Important Context

### API Key Management
- **CRITICAL**: `API_KEY_ENCRYPTION_KEY` must be set in `.env`
- Generated via `scripts/generate_encryption_key.py`
- Without it, API keys are lost on server restart
- Keys encrypted with Fernet (symmetric encryption)
- Stored in MongoDB `connector_configs` collection

### MongoDB Collections
- `connector_configs` - Data source configurations
- `query_cache` - Cached query results with TTL
- `stored_queries` - Saved query templates
- `analysis_query_results` - Analysis results with metadata
- `workflow_states` - Discovery workflow states

### Environment Variables
Required:
- `MONGO_URI` - MongoDB connection string
- `API_KEY_ENCRYPTION_KEY` - For secure API key storage

Optional (for AI discovery):
- `ANTHROPIC_API_KEY` - Claude API access
- `TAVILY_API_KEY` - Web search for discovery
- `DISCOVERY_LLM_MODEL` - Default: `claude-sonnet-4-20250514`

### Common Tasks

#### Run the API Server
```bash
python main.py
# Server runs at http://localhost:5000
```

#### Execute a Query
```bash
python execute_query.py <source_id> --param key=value
```

#### Add a Connector
```bash
python add_connectors.py <connector_type>
# Or add all: python add_connectors.py
```

#### Validate Connectors
```bash
python validate_connectors.py
# Or specific: python validate_connectors.py <source_id>
```

#### Discover New Data Source
```bash
curl -X POST http://localhost:5000/api/v1/discovery \
  -H "Content-Type: application/json" \
  -d '{"description": "your data source description"}'
```

## Code Conventions

### Error Handling
- Return structured error responses: `{"success": False, "error": {...}}`
- Log errors with context
- Use try-except with specific exception types
- Retry logic for network operations (exponential backoff)

### Query Parameters
- Connectors receive `parameters` dict
- Parameters validated against connector schema
- Support for JSONPath extraction via `output_path`

### Response Format
```python
{
    "success": True,
    "data": [...],
    "metadata": {
        "source_id": "...",
        "cached": False,
        "timestamp": "..."
    }
}
```

### Testing Patterns
- Use mongomock for MongoDB tests
- Mock external API calls
- Test both success and error cases
- Include integration tests for query engine

## Documentation Files

Reference these guides for specific topics:
- `README.md` - General project overview
- `ANALYSIS_GUIDE.md` - Query analysis and joins
- `DISCOVERY_AGENTS_GUIDE.md` - AI discovery system details
- `ADD_CONNECTORS_GUIDE.md` - Adding new connectors
- `VALIDATION_GUIDE.md` - Connector validation
- `STORED_QUERIES_GUIDE.md` - Managing saved queries
- `CENSUS_QUERY_GUIDE.md` - Census.gov connector usage
- `FBI_QUERY_GUIDE.md` - FBI Crime Data connector usage
- `NASS_ADVANCED_EXAMPLES.md` - USDA NASS examples

## Git Workflow

### Branches
- `main` - Production branch
- `claude/*` - Feature branches (created by Claude Code)

### Commit Messages
- Follow conventional commits format
- Examples:
  - `feat: Add new FBI Crime Data connector`
  - `fix: Handle missing API keys in discovery workflow`
  - `docs: Update CLAUDE.MD with testing section`
  - `refactor: Simplify query engine join logic`

### Before Pushing
1. Run tests: `pytest tests/`
2. Validate connectors: `python validate_connectors.py`
3. Check for sensitive data (API keys, .env files)
4. Ensure .gitignore covers sensitive files

## Security Considerations

### Secrets Management
- **NEVER** commit `.env` files
- **NEVER** commit API keys in code
- Use `API_KEY_ENCRYPTION_KEY` for secure storage
- Rotate keys periodically
- Use environment variables for sensitive config

### Input Validation
- Validate all query parameters
- Sanitize file paths for local_file connector
- Validate URLs for API connectors
- Check user input for injection attacks

### API Security
- Rate limiting (if enabled)
- Request size limits
- Timeout enforcement
- HTTPS for production deployments

## Performance Tips

### Caching
- Set appropriate TTL for query cache
- Use cache for expensive queries
- Clear cache when data sources update

### Query Optimization
- Use filters at source level (push down)
- Limit result sets early
- Join on indexed fields when possible
- Use stored queries for repeated patterns

### Analysis Performance
- Large datasets: sample before analysis
- Use pandas efficiently (vectorization)
- Limit PCA components for large feature sets
- Consider async execution for multiple queries

## Troubleshooting

### MongoDB Connection Issues
- Check `MONGO_URI` in `.env`
- Ensure MongoDB is running
- Verify network connectivity

### API Key Issues
- Regenerate `API_KEY_ENCRYPTION_KEY`
- Re-add connector configurations
- Check API key validity with source

### Discovery Failures
- Check `ANTHROPIC_API_KEY` and `TAVILY_API_KEY`
- Review workflow state in MongoDB
- Check LLM logs for agent errors
- Verify browser automation dependencies (Selenium, webdriver)

### Query Errors
- Validate parameters against connector schema
- Check connector logs
- Test connector directly: `validate_connectors.py`
- Verify external API is accessible

## Future Enhancements

Areas for potential improvement:
- GraphQL API support
- Real-time streaming queries
- Advanced query scheduling/cron
- Multi-tenant support
- Enhanced authentication/authorization
- Connector marketplace
- Data quality validation
- Query result versioning
- Distributed caching (Redis)
- Webhook support for data updates

## Support

For issues or questions:
- Check documentation in `.md` files
- Review test files for usage examples
- Inspect error logs for debugging
- Validate environment configuration

---

**Last Updated**: 2026-01-20
**Maintained By**: kurtjameshayes
**License**: MIT
