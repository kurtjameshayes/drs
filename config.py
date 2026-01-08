import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://kurtjhayes_db_user:Rvw6cndMQjWOilXj@cluster0.ngyd1r7.mongodb.net/?appName=Cluster0")
    DATABASE_NAME = os.getenv("DATABASE_NAME", "data_retrieval_system")
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    CACHE_TTL = int(os.getenv("CACHE_TTL", 3600))
    CACHE_ENABLED = os.getenv("CACHE_ENABLED", "false").lower() == "true"
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", 5000))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
    RETRY_BACKOFF_FACTOR = float(os.getenv("RETRY_BACKOFF_FACTOR", 2.0))

    # LangGraph Discovery Workflow Configuration
    ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
    TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")
    DISCOVERY_LLM_MODEL = os.getenv("DISCOVERY_LLM_MODEL", "claude-sonnet-4-20250514")
    DISCOVERY_MAX_SEARCH_RESULTS = int(os.getenv("DISCOVERY_MAX_SEARCH_RESULTS", 10))
    DISCOVERY_TEST_RETRIES = int(os.getenv("DISCOVERY_TEST_RETRIES", 3))
    DISCOVERY_TEST_BACKOFF = float(os.getenv("DISCOVERY_TEST_BACKOFF", 2.0))
    DISCOVERY_REQUEST_TIMEOUT = int(os.getenv("DISCOVERY_REQUEST_TIMEOUT", 30))

    # API Key Gathering Configuration
    ARCADE_API_KEY = os.getenv("ARCADE_API_KEY", "")
    DISCOVERY_EMAIL = os.getenv("DISCOVERY_EMAIL", "")
    ARCADE_USER_ID = os.getenv("ARCADE_USER_ID", "")  # Optional, defaults to DISCOVERY_EMAIL
    API_KEY_CHECK_INTERVAL = int(os.getenv("API_KEY_CHECK_INTERVAL", 30))  # Seconds between email checks
    API_KEY_CHECK_MAX_ATTEMPTS = int(os.getenv("API_KEY_CHECK_MAX_ATTEMPTS", 20))  # Max email check attempts
    API_KEY_ENCRYPTION_KEY = os.getenv("API_KEY_ENCRYPTION_KEY", "")  # Fernet key for API key encryption

    # Workflow Orchestration Configuration
    ENABLE_INTELLIGENT_WORKFLOW_ROUTING = os.getenv("ENABLE_INTELLIGENT_WORKFLOW_ROUTING", "true").lower() == "true"
