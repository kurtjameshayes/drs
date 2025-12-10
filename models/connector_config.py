from pymongo import MongoClient
from typing import Dict, Any, Optional, List
from datetime import datetime
from config import Config

class ConnectorConfig:
    """
    Model for storing and retrieving connector configurations from MongoDB.
    """
    
    def __init__(self, db_client: MongoClient = None):
        if db_client is None:
            db_client = MongoClient(Config.MONGO_URI)
        self.db = db_client[Config.DATABASE_NAME]
        self.collection = self.db.connector_configs
        self._create_indexes()
    
    def _create_indexes(self):
        """Create indexes for efficient querying."""
        self.collection.create_index("source_id", unique=True)
        self.collection.create_index("connector_type")
        self.collection.create_index("active")
    
    def create(self, config_data: Dict[str, Any]) -> str:
        """
        Create a new connector configuration.
        
        Args:
            config_data: Configuration data for the connector
            
        Returns:
            str: ID of the created configuration
        """
        config_data["created_at"] = datetime.utcnow()
        config_data["updated_at"] = datetime.utcnow()
        config_data["active"] = config_data.get("active", True)
        
        result = self.collection.insert_one(config_data)
        return str(result.inserted_id)
    
    def get_by_source_id(self, source_id: str) -> Optional[Dict[str, Any]]:
        """
        Get connector configuration by source ID.
        
        Args:
            source_id: Unique identifier for the data source
            
        Returns:
            Dict containing configuration or None if not found
        """
        config = self.collection.find_one({"source_id": source_id})
        if config:
            config["_id"] = str(config["_id"])
        return config
    
    def get_all(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get all connector configurations.
        
        Args:
            active_only: If True, return only active connectors
            
        Returns:
            List of configuration dictionaries
        """
        query = {"active": True} if active_only else {}
        configs = list(self.collection.find(query))
        for config in configs:
            config["_id"] = str(config["_id"])
        return configs
    
    def update(self, source_id: str, update_data: Dict[str, Any]) -> bool:
        """
        Update connector configuration.
        
        Args:
            source_id: Unique identifier for the data source
            update_data: Data to update
            
        Returns:
            bool: True if update successful, False otherwise
        """
        update_data["updated_at"] = datetime.utcnow()
        result = self.collection.update_one(
            {"source_id": source_id},
            {"$set": update_data}
        )
        return result.modified_count > 0
    
    def delete(self, source_id: str) -> bool:
        """
        Delete connector configuration.
        
        Args:
            source_id: Unique identifier for the data source
            
        Returns:
            bool: True if deletion successful, False otherwise
        """
        result = self.collection.delete_one({"source_id": source_id})
        return result.deleted_count > 0
    
    def get_by_type(self, connector_type: str) -> List[Dict[str, Any]]:
        """
        Get all connectors of a specific type.
        
        Args:
            connector_type: Type of connector (e.g., 'usda_nass', 'census', 'local_file')
            
        Returns:
            List of configuration dictionaries
        """
        configs = list(self.collection.find({
            "connector_type": connector_type,
            "active": True
        }))
        for config in configs:
            config["_id"] = str(config["_id"])
        return configs
