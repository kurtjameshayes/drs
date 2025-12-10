from pymongo import MongoClient
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from config import Config
import hashlib
import json

class QueryResult:
    """
    Model for storing and retrieving query results from MongoDB for caching.
    """
    
    def __init__(self, db_client: MongoClient = None):
        if db_client is None:
            db_client = MongoClient(Config.MONGO_URI)
        self.db = db_client[Config.DATABASE_NAME]
        self.collection = self.db.query_results
        self._create_indexes()
    
    def _create_indexes(self):
        """Create indexes for efficient querying and TTL."""
        self.collection.create_index("query_hash", unique=True)
        self.collection.create_index("source_id")
        self.collection.create_index(
            "expires_at",
            expireAfterSeconds=0
        )
    
    def _generate_hash(self, source_id: str, parameters: Dict[str, Any]) -> str:
        """
        Generate a hash for the query to use as cache key.
        
        Args:
            source_id: Data source identifier
            parameters: Query parameters
            
        Returns:
            str: Hash of the query
        """
        query_string = json.dumps({
            "source_id": source_id,
            "parameters": parameters
        }, sort_keys=True)
        return hashlib.sha256(query_string.encode()).hexdigest()
    
    def save(self, source_id: str, parameters: Dict[str, Any], 
             result: Dict[str, Any], ttl: int = None, query_id: str = None) -> str:
        """
        Save query result to cache.
        
        Args:
            source_id: Data source identifier
            parameters: Query parameters
            result: Query result data
            ttl: Time to live in seconds (defaults to Config.CACHE_TTL)
            query_id: Optional reference to stored query
            
        Returns:
            str: Query hash
        """
        if ttl is None:
            ttl = Config.CACHE_TTL
        
        query_hash = self._generate_hash(source_id, parameters)
        expires_at = datetime.utcnow() + timedelta(seconds=ttl)
        
        cache_entry = {
            "query_hash": query_hash,
            "source_id": source_id,
            "parameters": parameters,
            "result": result,
            "created_at": datetime.utcnow(),
            "expires_at": expires_at,
            "hit_count": 0
        }
        
        # Add query_id if provided
        if query_id:
            cache_entry["query_id"] = query_id
        
        self.collection.update_one(
            {"query_hash": query_hash},
            {"$set": cache_entry},
            upsert=True
        )
        
        return query_hash
    
    def get(self, source_id: str, parameters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached query result.
        
        Args:
            source_id: Data source identifier
            parameters: Query parameters
            
        Returns:
            Dict containing cached result or None if not found/expired
        """
        query_hash = self._generate_hash(source_id, parameters)
        
        cache_entry = self.collection.find_one({
            "query_hash": query_hash,
            "expires_at": {"$gt": datetime.utcnow()}
        })
        
        if cache_entry:
            self.collection.update_one(
                {"query_hash": query_hash},
                {"$inc": {"hit_count": 1}}
            )
            cache_entry["_id"] = str(cache_entry["_id"])
            return cache_entry["result"]
        
        return None
    
    def invalidate(self, source_id: str, parameters: Dict[str, Any] = None) -> int:
        """
        Invalidate cached results.
        
        Args:
            source_id: Data source identifier
            parameters: Optional specific query parameters to invalidate
            
        Returns:
            int: Number of invalidated entries
        """
        if parameters:
            query_hash = self._generate_hash(source_id, parameters)
            result = self.collection.delete_one({"query_hash": query_hash})
            return result.deleted_count
        else:
            result = self.collection.delete_many({"source_id": source_id})
            return result.deleted_count
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dict containing cache statistics
        """
        total_entries = self.collection.count_documents({})
        active_entries = self.collection.count_documents({
            "expires_at": {"$gt": datetime.utcnow()}
        })
        
        pipeline = [
            {"$group": {
                "_id": None,
                "total_hits": {"$sum": "$hit_count"}
            }}
        ]
        hit_stats = list(self.collection.aggregate(pipeline))
        total_hits = hit_stats[0]["total_hits"] if hit_stats else 0
        
        return {
            "total_entries": total_entries,
            "active_entries": active_entries,
            "expired_entries": total_entries - active_entries,
            "total_hits": total_hits
        }
