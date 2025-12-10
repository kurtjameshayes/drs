"""
Stored Query Model

Manages stored queries in MongoDB with references to connector configurations.
"""

from pymongo import MongoClient, ASCENDING, DESCENDING
from config import Config
from datetime import datetime
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class StoredQuery:
    """
    Model for managing stored queries in MongoDB.
    
    Schema:
        query_id: Unique identifier for the query
        query_name: Human-readable name
        description: Optional description
        connector_id: Reference to connector_config source_id
        parameters: Query parameters as dictionary
        created_at: Creation timestamp
        updated_at: Last update timestamp
        tags: Optional list of tags for categorization
        active: Whether the query is active
        created_by: Optional user identifier
    """
    
    def __init__(self):
        """Initialize StoredQuery model."""
        self.client = MongoClient(Config.MONGO_URI)
        self.db = self.client[Config.DATABASE_NAME]
        self.collection = self.db['stored_queries']
        self._ensure_indexes()
    
    def _ensure_indexes(self):
        """Create indexes for the stored_queries collection."""
        try:
            # Unique index on query_id
            self.collection.create_index([("query_id", ASCENDING)], unique=True)
            
            # Index on connector_id for filtering
            self.collection.create_index([("connector_id", ASCENDING)])
            
            # Index on tags for categorization
            self.collection.create_index([("tags", ASCENDING)])
            
            # Index on active status
            self.collection.create_index([("active", ASCENDING)])
            
            # Compound index for common queries
            self.collection.create_index([
                ("connector_id", ASCENDING),
                ("active", ASCENDING)
            ])
            
            logger.info("StoredQuery indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating StoredQuery indexes: {str(e)}")
    
    def create(self, query_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new stored query.
        
        Args:
            query_data: Dictionary containing query information
                Required fields: query_id, query_name, connector_id, parameters
                Optional fields: description, tags, created_by
        
        Returns:
            dict: Created query document
        """
        # Validate required fields
        required_fields = ['query_id', 'query_name', 'connector_id', 'parameters']
        for field in required_fields:
            if field not in query_data:
                raise ValueError(f"Missing required field: {field}")
        
        # Add timestamps
        now = datetime.utcnow()
        query_data['created_at'] = now
        query_data['updated_at'] = now
        
        # Set defaults
        if 'active' not in query_data:
            query_data['active'] = True
        
        if 'tags' not in query_data:
            query_data['tags'] = []
        
        try:
            result = self.collection.insert_one(query_data)
            logger.info(f"Created stored query: {query_data['query_id']}")
            return query_data
        except Exception as e:
            logger.error(f"Error creating stored query: {str(e)}")
            raise
    
    def get_by_id(self, query_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a stored query by ID.
        
        Args:
            query_id: Query identifier
            
        Returns:
            dict: Query document or None if not found
        """
        try:
            query = self.collection.find_one({"query_id": query_id})
            if query:
                query.pop('_id', None)  # Remove MongoDB internal ID
            return query
        except Exception as e:
            logger.error(f"Error getting query {query_id}: {str(e)}")
            return None
    
    def get_all(self, 
                connector_id: Optional[str] = None,
                active_only: bool = False,
                tags: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get all stored queries with optional filtering.
        
        Args:
            connector_id: Filter by connector ID
            active_only: Only return active queries
            tags: Filter by tags (queries with any of these tags)
            
        Returns:
            list: List of query documents
        """
        try:
            # Build filter
            filter_dict = {}
            
            if connector_id:
                filter_dict['connector_id'] = connector_id
            
            if active_only:
                filter_dict['active'] = True
            
            if tags:
                filter_dict['tags'] = {'$in': tags}
            
            # Get queries
            queries = list(self.collection.find(filter_dict).sort("query_name", ASCENDING))
            
            # Remove MongoDB internal IDs
            for query in queries:
                query.pop('_id', None)
            
            return queries
        except Exception as e:
            logger.error(f"Error getting queries: {str(e)}")
            return []
    
    def update(self, query_id: str, update_data: Dict[str, Any]) -> bool:
        """
        Update a stored query.
        
        Args:
            query_id: Query identifier
            update_data: Dictionary of fields to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Add update timestamp
            update_data['updated_at'] = datetime.utcnow()
            
            # Don't allow updating query_id
            update_data.pop('query_id', None)
            
            result = self.collection.update_one(
                {"query_id": query_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                logger.info(f"Updated stored query: {query_id}")
                return True
            else:
                logger.warning(f"No query found to update: {query_id}")
                return False
        except Exception as e:
            logger.error(f"Error updating query {query_id}: {str(e)}")
            return False
    
    def delete(self, query_id: str) -> bool:
        """
        Delete a stored query.
        
        Args:
            query_id: Query identifier
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.delete_one({"query_id": query_id})
            
            if result.deleted_count > 0:
                logger.info(f"Deleted stored query: {query_id}")
                return True
            else:
                logger.warning(f"No query found to delete: {query_id}")
                return False
        except Exception as e:
            logger.error(f"Error deleting query {query_id}: {str(e)}")
            return False
    
    def search(self, search_term: str) -> List[Dict[str, Any]]:
        """
        Search queries by name or description.
        
        Args:
            search_term: Term to search for
            
        Returns:
            list: List of matching query documents
        """
        try:
            # Case-insensitive regex search
            regex = {"$regex": search_term, "$options": "i"}
            
            queries = list(self.collection.find({
                "$or": [
                    {"query_name": regex},
                    {"description": regex}
                ]
            }).sort("query_name", ASCENDING))
            
            # Remove MongoDB internal IDs
            for query in queries:
                query.pop('_id', None)
            
            return queries
        except Exception as e:
            logger.error(f"Error searching queries: {str(e)}")
            return []
    
    def get_by_connector(self, connector_id: str, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get all queries for a specific connector.
        
        Args:
            connector_id: Connector identifier
            active_only: Only return active queries
            
        Returns:
            list: List of query documents
        """
        return self.get_all(connector_id=connector_id, active_only=active_only)
    
    def add_tag(self, query_id: str, tag: str) -> bool:
        """
        Add a tag to a query.
        
        Args:
            query_id: Query identifier
            tag: Tag to add
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.update_one(
                {"query_id": query_id},
                {
                    "$addToSet": {"tags": tag},
                    "$set": {"updated_at": datetime.utcnow()}
                }
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error adding tag to query {query_id}: {str(e)}")
            return False
    
    def remove_tag(self, query_id: str, tag: str) -> bool:
        """
        Remove a tag from a query.
        
        Args:
            query_id: Query identifier
            tag: Tag to remove
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.update_one(
                {"query_id": query_id},
                {
                    "$pull": {"tags": tag},
                    "$set": {"updated_at": datetime.utcnow()}
                }
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error removing tag from query {query_id}: {str(e)}")
            return False
    
    def count(self, connector_id: Optional[str] = None, active_only: bool = False) -> int:
        """
        Count stored queries.
        
        Args:
            connector_id: Filter by connector ID
            active_only: Only count active queries
            
        Returns:
            int: Number of queries
        """
        try:
            filter_dict = {}
            
            if connector_id:
                filter_dict['connector_id'] = connector_id
            
            if active_only:
                filter_dict['active'] = True
            
            return self.collection.count_documents(filter_dict)
        except Exception as e:
            logger.error(f"Error counting queries: {str(e)}")
            return 0
