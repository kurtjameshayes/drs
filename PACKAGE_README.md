# Data Retrieval System - Complete Package

## 📦 Package Version 2.0

**Release Date:** November 30, 2024  
**Complete System with Stored Query Management**

## 🎯 What's Included

This package contains a complete, production-ready data retrieval system with:

- ✅ **4 Data Connectors** (USDA NASS, US Census, FBI Crime Data, Local Files)
- ✅ **Stored Query System** (Save, manage, and execute queries)
- ✅ **REST API** (18 endpoints for complete control)
- ✅ **Caching Layer** (MongoDB-based with TTL)
- ✅ **Management Tools** (CLI utilities for queries and connectors)
- ✅ **Complete Documentation** (8+ guides and examples)
- ✅ **Example Scripts** (24 pre-built example queries)

## 📁 Package Contents

### Core System (30 Python files)
```
data_retrieval_system/
├── models/                    # Data models (3 files)
│   ├── connector_config.py   # Connector configuration
│   ├── query_result.py       # Result caching
│   └── stored_query.py       # Query storage
│
├── core/                      # Core components (4 files)
│   ├── base_connector.py     # Base connector interface
│   ├── connector_manager.py  # Connector lifecycle management
│   ├── query_engine.py       # Query orchestration
│   └── cache_manager.py      # Cache management
│
├── api/                       # REST API (1 file)
│   └── routes.py             # 18 API endpoints
│
├── connectors/                # Data source connectors (4 connectors)
│   ├── usda_nass/            # USDA QuickStats
│   ├── census/               # US Census Bureau
│   ├── fbi_crime/            # FBI Crime Data Explorer
│   └── local_file/           # Local file connector
│
├── config.py                  # System configuration
├── main.py                    # API server
├── init_db.py                 # Database initialization
├── add_connectors.py          # Connector management (with update)
├── validate_connectors.py     # Connector validation
├── manage_queries.py          # Query management CLI
├── query_nass.py              # NASS query examples
├── query_census.py            # Census query examples
├── query_fbi.py               # FBI query examples
└── .env                       # Environment configuration
```

### Documentation (8 guides)
- **README.md** - System overview
- **STORED_QUERIES_GUIDE.md** - Query management guide
- **FBI_QUERY_GUIDE.md** - FBI connector usage
- **CENSUS_QUERY_GUIDE.md** - Census connector usage
- **NASS_ADVANCED_EXAMPLES.md** - NASS advanced examples
- **NASS_TROUBLESHOOTING.md** - NASS troubleshooting
- **VALIDATION_GUIDE.md** - Connector validation
- **ADD_CONNECTORS_GUIDE.md** - Adding connectors

### Configuration Files
- **.env** - Environment variables (MongoDB, API keys)
- **requirements.txt** - Python dependencies

## 🚀 Quick Start

### 1. Prerequisites

- Python 3.8+
- MongoDB (local or Atlas)
- API keys (see below)

### 2. Installation

```bash
# Extract the package
tar -xzf data_retrieval_system.tar.gz
# or
unzip data_retrieval_system.zip

# Navigate to directory
cd data_retrieval_system

# Install dependencies
pip install -r requirements.txt
```

### 3. Configuration

**Edit .env file:**
```bash
# MongoDB connection
MONGO_URI=mongodb+srv://your-connection-string

# API keys (get from respective services)
USDA_NASS_API_KEY=your-key-here
CENSUS_API_KEY=your-key-here
FBI_CRIME_API_KEY=your-key-here
```

**Get API Keys:**
- USDA NASS: https://quickstats.nass.usda.gov/api
- US Census: https://api.census.gov/data/key_signup.html
- FBI Crime: https://api.data.gov/signup/

### 4. Initialize System

```bash
# Initialize database
python init_db.py

# Add connectors with API keys
python add_connectors.py

# Validate connectors
python validate_connectors.py
```

### 5. Run Example Queries

```bash
# USDA NASS examples
python query_nass.py --example 1

# US Census examples
python query_census.py --example 1

# FBI Crime examples
python query_fbi.py --example 1
```

### 6. Start API Server

```bash
# Start server (default: http://localhost:5000)
python main.py
```

## 📊 System Features

### 1. Data Connectors

#### USDA NASS QuickStats
- Agricultural statistics
- Crop production, prices, yields
- State and county level data
- 8 pre-built example queries

#### US Census Bureau
- Demographic data
- American Community Survey (ACS)
- Decennial Census
- Geographic queries
- 8 pre-built example queries

#### FBI Crime Data Explorer
- National crime statistics
- State-level data
- Violent and property crime
- Trend analysis
- 8 pre-built example queries

#### Local File Connector
- CSV, JSON, Excel files
- SQLite databases
- Configurable parsing

### 2. Stored Query System ⭐ NEW

Save and reuse queries:

```bash
# Create a stored query
python manage_queries.py --create-interactive

# List queries
python manage_queries.py --list

# Execute stored query
python manage_queries.py --execute query_id

# Search queries
python manage_queries.py --search crime
```

Python API:
```python
from core.query_engine import QueryEngine

qe = QueryEngine()
result = qe.execute_stored_query("my_query_id")
```

### 3. REST API (18 Endpoints)

**Connector Management:**
- GET `/api/v1/sources` - List connectors
- POST `/api/v1/sources` - Create connector
- PUT `/api/v1/sources/{id}` - Update connector
- DELETE `/api/v1/sources/{id}` - Delete connector

**Query Execution:**
- POST `/api/v1/query` - Execute query
- POST `/api/v1/query/multi` - Multi-source query

**Stored Queries:** ⭐ NEW
- POST `/api/v1/queries` - Create stored query
- GET `/api/v1/queries` - List queries
- GET `/api/v1/queries/{id}` - Get query
- PUT `/api/v1/queries/{id}` - Update query
- DELETE `/api/v1/queries/{id}` - Delete query
- POST `/api/v1/queries/{id}/execute` - Execute
- GET `/api/v1/queries/search` - Search queries

**Cache & Health:**
- GET `/api/v1/cache/stats` - Cache statistics
- DELETE `/api/v1/cache/{id}` - Clear cache
- GET `/api/v1/health` - System health

### 4. Caching System

- MongoDB-based result caching
- Configurable TTL (Time To Live)
- Query hash-based indexing
- Cache hit/miss tracking
- Query reference tracking ⭐ NEW

### 5. Management Tools

**add_connectors.py** - Connector management
- Add/update connectors
- Configure API keys
- Set active status

**validate_connectors.py** - Test connectors
- Validate connections
- Test API keys
- Check data retrieval

**manage_queries.py** ⭐ NEW - Query management
- Create, list, update, delete queries
- Execute stored queries
- Search and filter
- Interactive creation

## 📖 Usage Examples

### Execute Direct Query
```python
from core.query_engine import QueryEngine

qe = QueryEngine()
result = qe.execute_query(
    "fbi_crime",
    {
        "endpoint": "estimates/national",
        "from": "2020",
        "to": "2021"
    }
)
```

### Create and Execute Stored Query
```python
from models.stored_query import StoredQuery
from core.query_engine import QueryEngine

# Create query
sq = StoredQuery()
sq.create({
    "query_id": "national_crime",
    "query_name": "National Crime Trend",
    "connector_id": "fbi_crime",
    "parameters": {
        "endpoint": "estimates/national",
        "from": "2015",
        "to": "2021"
    },
    "tags": ["crime", "national"]
})

# Execute query
qe = QueryEngine()
result = qe.execute_stored_query("national_crime")
```

### API Usage
```bash
# Execute stored query
curl -X POST http://localhost:5000/api/v1/queries/national_crime/execute

# List all queries
curl http://localhost:5000/api/v1/queries

# Search queries
curl "http://localhost:5000/api/v1/queries/search?q=crime"
```

## 🔧 Configuration

### Environment Variables (.env)

```bash
# MongoDB
MONGO_URI=mongodb://localhost:27017/
DATABASE_NAME=data_retrieval_system

# API Configuration
API_HOST=0.0.0.0
API_PORT=5000

# Cache Settings
CACHE_TTL=3600

# API Keys
USDA_NASS_API_KEY=your-key
CENSUS_API_KEY=your-key
FBI_CRIME_API_KEY=your-key
FBI_CRIME_API_URL=https://api.usa.gov/crime/fbi/sapi
```

### MongoDB Collections

**connector_configs** - Connector metadata
```json
{
    "source_id": "string",
    "source_name": "string",
    "connector_type": "string",
    "url": "string",
    "api_key": "string",
    "active": boolean
}
```

**stored_queries** - Saved queries ⭐ NEW
```json
{
    "query_id": "string",
    "query_name": "string",
    "connector_id": "string",
    "parameters": {},
    "tags": [],
    "active": boolean
}
```

**query_results** - Cached results
```json
{
    "query_hash": "string",
    "source_id": "string",
    "parameters": {},
    "result": {},
    "query_id": "string",
    "expires_at": "datetime"
}
```

## 🧪 Testing

### Run Example Queries
```bash
# Test all NASS examples
python query_nass.py

# Test specific example
python query_nass.py --example 1

# List available examples
python query_nass.py --list
```

### Validate Connectors
```bash
# Validate all connectors
python validate_connectors.py

# Validate specific connector
python validate_connectors.py fbi_crime
```

### Test Stored Queries
```bash
# Create test query
python manage_queries.py --create-interactive

# List queries
python manage_queries.py --list

# Execute query
python manage_queries.py --execute test_query
```

## 📈 System Statistics

- **Total Python Files:** 30
- **Lines of Code:** ~4,300
- **API Endpoints:** 18
- **Data Connectors:** 4
- **Example Queries:** 24 (8 per connector)
- **Documentation Files:** 8
- **Management Scripts:** 4

## 🔒 Security Notes

1. **API Keys:** Store in .env file, never commit to version control
2. **MongoDB:** Use authentication in production
3. **API Server:** Consider adding authentication/authorization
4. **Network:** Use firewall rules to restrict access
5. **HTTPS:** Use reverse proxy (nginx) with SSL in production

## 🤝 API Pre-configured

All API keys are already configured in this package:

- ✅ USDA NASS QuickStats API key
- ✅ US Census Bureau API key
- ✅ FBI Crime Data Explorer API key
- ✅ MongoDB connection string (Atlas)

**Just run and go!** No additional configuration needed for testing.

## 📝 What's New in Version 2.0

### Stored Query System ⭐
- Save queries in MongoDB
- Execute by ID via API or CLI
- Search and filter queries
- Tag-based organization
- Parameter overrides
- Query metadata tracking

### Enhanced Caching
- Query references in cache
- Track which queries generated results
- Better cache analytics

### Updated Tools
- `add_connectors.py` - Now updates existing connectors
- `manage_queries.py` - New CLI for query management
- Enhanced API with 7 new endpoints

### Documentation
- Complete stored query guide
- Updated examples
- Enhanced troubleshooting

## 🐛 Troubleshooting

### MongoDB Connection Issues
```bash
# Check MongoDB is running
mongosh

# Verify connection string in .env
# For Atlas: mongodb+srv://...
# For local: mongodb://localhost:27017/
```

### API Key Issues
```bash
# Validate API keys
python validate_connectors.py

# Update API keys
python add_connectors.py
```

### Import Errors
```bash
# Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

### Cache Issues
```bash
# Clear all cache
curl -X DELETE http://localhost:5000/api/v1/cache/fbi_crime

# View cache stats
curl http://localhost:5000/api/v1/cache/stats
```

## 📚 Additional Resources

- **USDA NASS API Docs:** https://quickstats.nass.usda.gov/api
- **Census API Docs:** https://www.census.gov/data/developers.html
- **FBI Crime Data:** https://crime-data-explorer.fr.cloud.gov/pages/docApi
- **MongoDB Docs:** https://docs.mongodb.com/

## 🎓 Learning Path

1. **Start Simple:** Run example queries with `query_nass.py`, `query_census.py`, `query_fbi.py`
2. **Try API:** Start server with `python main.py` and test endpoints
3. **Explore Queries:** Use `manage_queries.py --create-interactive` to save queries
4. **Build Dashboard:** Use stored queries in your applications
5. **Extend System:** Add custom connectors or modify existing ones

## 💡 Use Cases

- **Data Analysis:** Pull agricultural, demographic, or crime data
- **Dashboards:** Build real-time data dashboards
- **Research:** Academic research with reproducible queries
- **Reports:** Automated report generation
- **APIs:** Backend for data-driven applications
- **Integration:** Connect to BI tools (Tableau, Power BI)

## 🔄 Update History

**Version 2.0** (November 30, 2024)
- Added stored query system
- 7 new API endpoints
- Query management CLI tool
- Enhanced caching with query references
- Updated documentation

**Version 1.0** (November 28, 2024)
- Initial release
- 4 data connectors
- Basic API (11 endpoints)
- Caching system
- Example queries

## 📞 Support

For issues or questions:
1. Check documentation in the package
2. Review troubleshooting guides
3. Validate connectors with `validate_connectors.py`
4. Check API health: `curl http://localhost:5000/api/v1/health`

## 📄 License

This package is provided as-is for use in data retrieval applications.

## 🎉 Quick Commands Reference

```bash
# Setup
pip install -r requirements.txt
python init_db.py
python add_connectors.py

# Query Examples
python query_nass.py --example 1
python query_census.py --example 1
python query_fbi.py --example 1

# Stored Queries
python manage_queries.py --create-interactive
python manage_queries.py --list
python manage_queries.py --execute query_id

# Validation
python validate_connectors.py
python validate_connectors.py fbi_crime

# API Server
python main.py

# Management
python add_connectors.py          # Add/update connectors
python manage_queries.py --help   # Query management help
```

---

**Ready to use!** Start with: `python query_fbi.py --example 1` to test the system.

For complete documentation, see STORED_QUERIES_GUIDE.md and other guides in the package.
