from typing import Dict, Any, Optional
from models.query_result import QueryResult
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CacheManager:
    """
    Manages caching of query results using MongoDB.
    """
    
    def __init__(self, query_result_model: QueryResult = None):
        """
        Initialize cache manager.
        
        Args:
            query_result_model: QueryResult model instance
        """
        self.query_result_model = query_result_model or QueryResult()
    
    def get(self, source_id: str, parameters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached query result.
        
        Args:
            source_id: Data source identifier
            parameters: Query parameters
            
        Returns:
            Cached result or None if not found
        """
        try:
            result = self.query_result_model.get(source_id, parameters)
            if result:
                logger.info(f"Cache hit for {source_id}")
                return result
            else:
                logger.info(f"Cache miss for {source_id}")
                return None
        except Exception as e:
            logger.error(f"Cache retrieval error: {str(e)}")
            return None
    
    def set(self, source_id: str, parameters: Dict[str, Any], 
            result: Dict[str, Any], ttl: int = None, query_id: str = None) -> bool:
        """
        Store query result in cache.
        
        Args:
            source_id: Data source identifier
            parameters: Query parameters
            result: Query result to cache
            ttl: Time to live in seconds
            query_id: Optional reference to stored query
            
        Returns:
            bool: True if successful
        """
        try:
            self.query_result_model.save(source_id, parameters, result, ttl, query_id)
            logger.info(f"Cached result for {source_id}")
            return True
        except Exception as e:
            logger.error(f"Cache storage error: {str(e)}")
            return False
    
    def invalidate(self, source_id: str, parameters: Dict[str, Any] = None) -> int:
        """
        Invalidate cached results.
        
        Args:
            source_id: Data source identifier
            parameters: Optional specific query to invalidate
            
        Returns:
            Number of invalidated entries
        """
        try:
            count = self.query_result_model.invalidate(source_id, parameters)
            logger.info(f"Invalidated {count} cache entries for {source_id}")
            return count
        except Exception as e:
            logger.error(f"Cache invalidation error: {str(e)}")
            return 0
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dict containing cache statistics
        """
        try:
            return self.query_result_model.get_stats()
        except Exception as e:
            logger.error(f"Failed to get cache stats: {str(e)}")
            return {
                "error": str(e)
            }
