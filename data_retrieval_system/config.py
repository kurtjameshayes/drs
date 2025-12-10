import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://kurtjhayes_db_user:Rvw6cndMQjWOilXj@cluster0.ngyd1r7.mongodb.net/?appName=Cluster0")
    DATABASE_NAME = os.getenv("DATABASE_NAME", "data_retrieval_system")
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    CACHE_TTL = int(os.getenv("CACHE_TTL", 3600))
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", 5000))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
    RETRY_BACKOFF_FACTOR = float(os.getenv("RETRY_BACKOFF_FACTOR", 2.0))
