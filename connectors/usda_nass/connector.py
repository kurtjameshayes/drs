import requests
from typing import Dict, Any, List
from core.base_connector import BaseConnector
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class USDANASSConnector(BaseConnector):
    """
    Connector for USDA NASS QuickStats API.
    
    API Documentation: https://quickstats.nass.usda.gov/api
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get("url", "https://quickstats.nass.usda.gov/api")
        self.api_key = config.get("api_key")
        self.format = config.get("format", "JSON")
        self.max_retries = config.get("max_retries", 3)
        self.retry_delay = config.get("retry_delay", 1)
        
        if not self.api_key:
            raise ValueError("API key is required for USDA NASS connector")
    
    def connect(self) -> bool:
        """Establish connection by validating API key."""
        try:
            self.connected = self.validate()
            return self.connected
        except Exception as e:
            logger.error(f"Connection failed: {str(e)}")
            return False
    
    def disconnect(self) -> bool:
        """Close connection (no persistent connection for REST API)."""
        self.connected = False
        return True
    
    def validate(self) -> bool:
        """Validate API key by making a test request."""
        try:
            test_params = {
                "key": self.api_key,
                "format": self.format,
                "commodity_desc": "CORN",
                "year": "2020",
                "state_alpha": "IA",
                "statisticcat_desc": "PRODUCTION"
            }
            
            response = requests.get(
                f"{self.base_url}/api_GET",
                params=test_params,
                timeout=10
            )
            
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            return False
    
    def query(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute query against USDA NASS QuickStats API.
        
        Args:
            parameters: Query parameters. Common parameters include:
                - commodity_desc: Commodity name
                - year: Year or year range
                - state_alpha: State abbreviation
                - county_name: County name
                - statisticcat_desc: Statistic category
                - short_desc: Short description filter
                
        Returns:
            Dict containing query results and metadata
        """
        if not self.connected:
            self.connect()
        
        # Add API key and format
        query_params = {
            "key": self.api_key,
            "format": self.format,
            **parameters
        }
        
        # Execute query with retry logic
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    f"{self.base_url}/api_GET",
                    params=query_params,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json() if self.format == "JSON" else response.text
                    return self.transform(data)
                elif response.status_code == 429:  # Rate limit
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Rate limited. Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"API error: {response.status_code} - {response.text}")
            
            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Timeout on attempt {attempt + 1}. Retrying...")
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Error on attempt {attempt + 1}: {str(e)}. Retrying...")
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise
        
        raise Exception("Max retries exceeded")
    
    def transform(self, data: Any) -> Dict[str, Any]:
        """
        Transform USDA NASS data to standardized format.
        
        Args:
            data: Raw API response data
            
        Returns:
            Dict containing standardized data with metadata
        """
        if isinstance(data, dict) and "data" in data:
            records = data["data"]
        elif isinstance(data, list):
            records = data
        else:
            records = []
        
        # Create standardized response
        standardized = {
            "metadata": self._create_metadata(len(records), {}),
            "data": records,
            "schema": {
                "fields": self._infer_schema(records) if records else []
            }
        }
        
        return standardized
    
    def _infer_schema(self, records: List[Dict]) -> List[Dict[str, str]]:
        """
        Infer schema from data records.
        
        Args:
            records: List of data records
            
        Returns:
            List of field definitions
        """
        if not records:
            return []
        
        sample = records[0]
        fields = []
        
        for key, value in sample.items():
            field_type = "string"
            if isinstance(value, (int, float)):
                field_type = "number"
            elif isinstance(value, bool):
                field_type = "boolean"
            
            fields.append({
                "name": key,
                "type": field_type
            })
        
        return fields
    
    def get_capabilities(self) -> Dict[str, Any]:
        """Get connector capabilities."""
        capabilities = super().get_capabilities()
        capabilities.update({
            "supports_pagination": True,
            "supports_filtering": True,
            "supports_sorting": False,
            "data_formats": ["JSON", "CSV", "XML"],
            "api_documentation": "https://quickstats.nass.usda.gov/api"
        })
        return capabilities
