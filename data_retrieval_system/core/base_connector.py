from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime

class BaseConnector(ABC):
    """
    Abstract base class for all data source connectors.
    Each connector must implement the standard interface methods.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the connector with configuration.
        
        Args:
            config: Dictionary containing connector-specific configuration
        """
        self.config = config
        self.source_id = config.get("source_id")
        self.source_name = config.get("source_name")
        self.connected = False
        
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the data source.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """
        Close connection and cleanup resources.
        
        Returns:
            bool: True if disconnection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def query(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute query with specified parameters.
        
        Args:
            parameters: Query parameters specific to the data source
            
        Returns:
            Dict containing query results and metadata
        """
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """
        Validate connection and credentials.
        
        Returns:
            bool: True if validation successful, False otherwise
        """
        pass
    
    @abstractmethod
    def transform(self, data: Any) -> Dict[str, Any]:
        """
        Transform source data to standardized format.
        
        Args:
            data: Raw data from the source
            
        Returns:
            Dict containing standardized data format with metadata
        """
        pass
    
    def get_capabilities(self) -> Dict[str, Any]:
        """
        Get connector capabilities and supported features.
        
        Returns:
            Dict describing connector capabilities
        """
        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "connected": self.connected,
            "supports_pagination": False,
            "supports_filtering": False,
            "supports_sorting": False
        }
    
    def _create_metadata(self, data_count: int, query_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create standardized metadata for the response.
        
        Args:
            data_count: Number of records returned
            query_params: Original query parameters
            
        Returns:
            Dict containing metadata
        """
        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "timestamp": datetime.utcnow().isoformat(),
            "record_count": data_count,
            "query_parameters": query_params,
            "version": "1.0"
        }
