import requests
from typing import Dict, Any, List, Optional
from core.base_connector import BaseConnector
import logging
import time
from pymongo import MongoClient
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CensusConnector(BaseConnector):
    """
    Connector for Census.gov API.
    
    API Documentation: https://www.census.gov/data/developers/guidance/api-user-guide.html
    """
    
    def __init__(self, config: Dict[str, Any], attr_lookup: Optional["AttrNameLookup"] = None):
        super().__init__(config)
        self.base_url = config.get("url", "https://api.census.gov/data")
        self.api_key = config.get("api_key")  # Optional but recommended
        self.max_retries = config.get("max_retries", 3)
        self.retry_delay = config.get("retry_delay", 1)
        self._attr_lookup: Optional["AttrNameLookup"] = attr_lookup
        self._attr_lookup_unavailable = False
    
    def connect(self) -> bool:
        """Establish connection by validating API access."""
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
        """Validate API access by making a test request."""
        try:
            # Test with a simple request to the 2020 ACS 5-year data
            test_url = f"{self.base_url}/2020/acs/acs5"
            params = {
                "get": "NAME",
                "for": "state:01"
            }
            
            if self.api_key:
                params["key"] = self.api_key
            
            response = requests.get(test_url, params=params, timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            return False
    
    def query(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute query against Census.gov API.
        
        Args:
            parameters: Query parameters including:
                - dataset: Dataset identifier (e.g., "2020/acs/acs5")
                - get: Variables to retrieve (comma-separated)
                - for: Geography selection
                - in: Optional parent geography
                - additional filters
                
        Returns:
            Dict containing query results and metadata
        """
        if not self.connected:
            self.connect()
        
        # Extract dataset from parameters
        dataset = parameters.get("dataset")
        if not dataset:
            raise ValueError("Dataset parameter is required")
        
        # Build query URL
        query_url = f"{self.base_url}/{dataset}"
        
        # Build query parameters
        query_params = {k: v for k, v in parameters.items() if k != "dataset"}
        
        if self.api_key:
            query_params["key"] = self.api_key
        
        # Execute query with retry logic
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    query_url,
                    params=query_params,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
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
        Transform Census data to standardized format.
        
        Census API returns data as array of arrays with first row as headers.
        
        Args:
            data: Raw API response data
            
        Returns:
            Dict containing standardized data with metadata
        """
        if not data or len(data) < 2:
            return {
                "metadata": self._create_metadata(0, {}),
                "data": [],
                "schema": {"fields": []}
            }
        
        # First row contains headers
        headers = data[0]
        
        # Convert remaining rows to dictionaries
        records = []
        for row in data[1:]:
            record = {}
            for i, header in enumerate(headers):
                record[header] = row[i] if i < len(row) else None
            records.append(record)
        
        # Create standardized response
        standardized = {
            "metadata": self._create_metadata(len(records), {}),
            "data": records,
            "schema": {
                "fields": self._create_schema_from_headers(headers)
            }
        }
        
        return standardized
    
    def _create_schema_from_headers(self, headers: List[str]) -> List[Dict[str, str]]:
        """
        Create schema definition from headers.
        
        Args:
            headers: List of column headers
            
        Returns:
            List of field definitions
        """
        fields = []
        for header in headers:
            fields.append({
                "name": header,
                "type": "string"  # Census API returns all as strings
            })
        return fields
    
    def process_result(self, result: Dict[str, Any], parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Replace column headers with descriptions from the attr_name collection.
        """
        attr_lookup = self._get_attr_lookup()
        if not attr_lookup:
            return result
        
        records = result.get("data")
        if not isinstance(records, list) or not records:
            return result
        
        schema = result.get("schema", {})
        fields = schema.get("fields", []) if isinstance(schema, dict) else []
        
        headers = [
            field.get("name") for field in fields
            if isinstance(field, dict) and field.get("name")
        ]
        if not headers:
            # Fallback to keys from the first record if schema is missing.
            headers = list(records[0].keys())
        
        header_map = attr_lookup.get_descriptions(headers)
        if not header_map:
            return result
        
        renamed_records = []
        for record in records:
            renamed_record = {}
            for key, value in record.items():
                new_key = header_map.get(key, key)
                renamed_record[new_key] = value
            renamed_records.append(renamed_record)
        result["data"] = renamed_records
        
        if isinstance(fields, list):
            for field in fields:
                name = field.get("name")
                if name in header_map:
                    field["name"] = header_map[name]
        
        return result
    
    def get_capabilities(self) -> Dict[str, Any]:
        """Get connector capabilities."""
        capabilities = super().get_capabilities()
        capabilities.update({
            "supports_pagination": False,
            "supports_filtering": True,
            "supports_sorting": False,
            "supports_geography": True,
            "data_formats": ["JSON"],
            "api_documentation": "https://www.census.gov/data/developers/guidance.html"
        })
        return capabilities
    
    def get_available_datasets(self) -> List[Dict[str, Any]]:
        """
        Get list of available Census datasets.
        
        Returns:
            List of dataset information
        """
        try:
            response = requests.get(f"{self.base_url}.json", timeout=10)
            if response.status_code == 200:
                return response.json().get("dataset", [])
        except Exception as e:
            logger.error(f"Failed to retrieve datasets: {str(e)}")
        
        return []
    
    def get_dataset_variables(self, dataset: str) -> Dict[str, Any]:
        """
        Get available variables for a dataset.
        
        Args:
            dataset: Dataset identifier
            
        Returns:
            Dict of variable definitions
        """
        try:
            variables_url = f"{self.base_url}/{dataset}/variables.json"
            response = requests.get(variables_url, timeout=10)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Failed to retrieve variables: {str(e)}")
        
        return {}
    
    def _get_attr_lookup(self) -> Optional["AttrNameLookup"]:
        """
        Lazily initialize the attribute name lookup helper.
        """
        if self._attr_lookup_unavailable:
            return None
        
        if self._attr_lookup is None:
            try:
                self._attr_lookup = AttrNameLookup()
            except Exception as exc:
                logger.warning(
                    "Attr name lookup unavailable for CensusConnector: %s",
                    exc
                )
                self._attr_lookup_unavailable = True
                return None
        return self._attr_lookup


class AttrNameLookup:
    """
    Helper class to resolve Census variable codes to descriptions via MongoDB.
    """

    def __init__(
        self,
        mongo_client: Optional[MongoClient] = None,
        collection=None,
    ):
        if collection is not None:
            self._collection = collection
        else:
            client = mongo_client or MongoClient(Config.MONGO_URI)
            self._collection = client[Config.DATABASE_NAME].attr_name

    def get_descriptions(self, variable_codes: List[str]) -> Dict[str, str]:
        if not variable_codes:
            return {}

        cursor = self._collection.find(
            {"variable_code": {"$in": variable_codes}},
            {"variable_code": 1, "description": 1},
        )

        descriptions: Dict[str, str] = {}
        for doc in cursor:
            variable_code = doc.get("variable_code")
            if not variable_code or variable_code in descriptions:
                continue
            description = doc.get("description")
            if description:
                descriptions[variable_code] = description

        return descriptions
