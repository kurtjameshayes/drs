import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Database Configuration
    MONGO_URI = os.getenv("MONGO_URI")
    DATABASE_NAME = os.getenv("DATABASE_NAME", "data_retrieval_system")

    # Redis Configuration
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

    # Cache Configuration
    CACHE_TTL = int(os.getenv("CACHE_TTL", 3600))
    CACHE_ENABLED = os.getenv("CACHE_ENABLED", "false").lower() == "true"

    # API Server Configuration
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", 5000))

    # Retry Configuration
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
    RETRY_BACKOFF_FACTOR = float(os.getenv("RETRY_BACKOFF_FACTOR", 2.0))

    # API Keys for Connectors
    USDA_NASS_API_KEY = os.getenv("USDA_NASS_API_KEY")
    CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")
    FBI_CRIME_API_KEY = os.getenv("FBI_CRIME_API_KEY")
