from typing import Dict, Any, Optional, List
from core.base_connector import BaseConnector
from models.connector_config import ConnectorConfig
import importlib
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConnectorManager:
    """
    Manages connector lifecycle, registration, and request routing.
    """
    
    def __init__(self, config_model: ConnectorConfig = None):
        """
        Initialize the connector manager.
        
        Args:
            config_model: ConnectorConfig instance for loading configurations
        """
        self.config_model = config_model or ConnectorConfig()
        self.connectors: Dict[str, BaseConnector] = {}
        self.connector_classes: Dict[str, type] = {}
        self._register_builtin_connectors()
    
    def _register_builtin_connectors(self):
        """Register built-in connector types."""
        self.connector_classes = {
            "usda_nass": "connectors.usda_nass.connector.USDANASSConnector",
            "census": "connectors.census.connector.CensusConnector",
            "local_file": "connectors.local_file.connector.LocalFileConnector",
            "fbi_crime": "connectors.fbi_crime.connector.FBICrimeConnector"
        }
    
    def _load_connector_class(self, connector_type: str) -> Optional[type]:
        """
        Dynamically load connector class.
        
        Args:
            connector_type: Type of connector to load
            
        Returns:
            Connector class or None if not found
        """
        if connector_type not in self.connector_classes:
            logger.error(f"Unknown connector type: {connector_type}")
            return None
        
        try:
            module_path, class_name = self.connector_classes[connector_type].rsplit(".", 1)
            module = importlib.import_module(module_path)
            connector_class = getattr(module, class_name)
            return connector_class
        except Exception as e:
            logger.error(f"Failed to load connector {connector_type}: {str(e)}")
            return None
    
    def load_connectors(self):
        """Load all active connectors from configuration."""
        configs = self.config_model.get_all(active_only=True)
        
        for config in configs:
            source_id = config["source_id"]
            connector_type = config["connector_type"]
            
            try:
                connector_class = self._load_connector_class(connector_type)
                if connector_class:
                    connector = connector_class(config)
                    if connector.connect():
                        self.connectors[source_id] = connector
                        logger.info(f"Loaded connector: {source_id}")
                    else:
                        logger.warning(f"Failed to connect: {source_id}")
            except Exception as e:
                logger.error(f"Error loading connector {source_id}: {str(e)}")
    
    def get_connector(self, source_id: str) -> Optional[BaseConnector]:
        """
        Get a connector by source ID.
        
        Args:
            source_id: Unique identifier for the data source
            
        Returns:
            BaseConnector instance or None if not found
        """
        if source_id not in self.connectors:
            config = self.config_model.get_by_source_id(source_id)
            if config and config.get("active"):
                connector_type = config["connector_type"]
                connector_class = self._load_connector_class(connector_type)
                if connector_class:
                    connector = connector_class(config)
                    if connector.connect():
                        self.connectors[source_id] = connector
                    else:
                        return None
        
        return self.connectors.get(source_id)
    
    def query(self, source_id: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute query through appropriate connector.
        
        Args:
            source_id: Data source identifier
            parameters: Query parameters
            
        Returns:
            Dict containing query results
        """
        connector = self.get_connector(source_id)
        if not connector:
            raise ValueError(f"Connector not found or unavailable: {source_id}")
        
        try:
            result = connector.query(parameters)
            return {
                "success": True,
                "data": result,
                "source_id": source_id
            }
        except Exception as e:
            logger.error(f"Query failed for {source_id}: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "source_id": source_id
            }
    
    def list_sources(self) -> List[Dict[str, Any]]:
        """
        List all available data sources.
        
        Returns:
            List of source information
        """
        sources = []
        for source_id, connector in self.connectors.items():
            sources.append({
                "source_id": source_id,
                "capabilities": connector.get_capabilities(),
                "connected": connector.connected
            })
        return sources
    
    def validate_connector(self, source_id: str) -> bool:
        """
        Validate a connector's connection and credentials.
        
        Args:
            source_id: Data source identifier
            
        Returns:
            bool: True if validation successful
        """
        connector = self.get_connector(source_id)
        if not connector:
            return False
        
        try:
            return connector.validate()
        except Exception as e:
            logger.error(f"Validation failed for {source_id}: {str(e)}")
            return False
    
    def disconnect_all(self):
        """Disconnect all connectors and cleanup resources."""
        for source_id, connector in self.connectors.items():
            try:
                connector.disconnect()
                logger.info(f"Disconnected: {source_id}")
            except Exception as e:
                logger.error(f"Error disconnecting {source_id}: {str(e)}")
        
        self.connectors.clear()
    
    def register_connector_type(self, connector_type: str, class_path: str):
        """
        Register a custom connector type.
        
        Args:
            connector_type: Unique type identifier
            class_path: Full path to connector class (module.path.ClassName)
        """
        self.connector_classes[connector_type] = class_path
        logger.info(f"Registered connector type: {connector_type}")
