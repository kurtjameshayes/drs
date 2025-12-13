from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from urllib.parse import urlencode
from datetime import datetime
import logging

try:
    from jsonpath_ng import parse as jsonpath_parse
    JSONPATH_AVAILABLE = True
except ImportError:
    JSONPATH_AVAILABLE = False

logger = logging.getLogger(__name__)

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
    def query(self, parameters: Dict[str, Any],
              dynamic_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute query with specified parameters.
        
        Args:
            parameters: Query parameters specific to the data source
            dynamic_params: Optional dynamic parameter values for placeholder substitution
            
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

    def _compose_request_url(self, url: str, params: Optional[Dict[str, Any]] = None) -> str:
        """
        Build a full query URL with encoded parameters for logging/debugging.
        """
        if not params:
            return url

        try:
            filtered_params = {k: v for k, v in params.items() if v is not None}
            query_string = urlencode(filtered_params, doseq=True)
        except Exception:
            query_string = ""

        if query_string:
            separator = "&" if "?" in url else "?"
            return f"{url}{separator}{query_string}"

        return f"{url} params={params}"

    def process_result(self, result: Dict[str, Any], parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Post-process a connector result before returning it to callers.

        Connectors can override this hook to add connector-specific formatting
        or enrichment. By default no changes are applied.

        Args:
            result: The result payload returned from `query`.
            parameters: The parameters used to generate the query.

        Returns:
            Dict containing the (optionally) transformed result.
        """
        return result

    def extract_with_jsonpath(self, data: Any, data_path: str) -> Any:
        """
        Extract data from API response using JSONPath expression.
        
        This method can be used by connectors to extract specific parts of
        an API response before transformation.
        
        Args:
            data: Raw API response data
            data_path: JSONPath expression (e.g., '$.data', '$.results[*]')
            
        Returns:
            Extracted data based on JSONPath, or original data if extraction fails
        """
        if not JSONPATH_AVAILABLE:
            logger.warning("jsonpath-ng not available, using fallback extraction")
            return self._extract_with_fallback(data, data_path)
        
        try:
            # Parse the JSONPath expression
            jsonpath_expr = jsonpath_parse(data_path)
            
            # Find all matches
            matches = jsonpath_expr.find(data)
            
            if not matches:
                logger.warning(f"JSONPath '{data_path}' found no matches, returning original data")
                return data
            
            # If single match, return its value directly
            if len(matches) == 1:
                extracted = matches[0].value
                logger.info(f"JSONPath '{data_path}' extracted single value (type: {type(extracted).__name__})")
                return extracted
            
            # If multiple matches, return list of values
            extracted = [match.value for match in matches]
            logger.info(f"JSONPath '{data_path}' extracted {len(extracted)} values")
            return extracted
            
        except Exception as e:
            logger.error(f"JSONPath extraction failed for '{data_path}': {str(e)}")
            logger.warning("Falling back to simple path extraction")
            return self._extract_with_fallback(data, data_path)

    def _extract_with_fallback(self, data: Any, data_path: str) -> Any:
        """
        Fallback extraction using simple dot notation when jsonpath-ng is not available.
        
        Supports simple paths like:
        - '$.data' -> data['data']
        - '$.results' -> data['results']
        - 'data' -> data['data']
        - 'data.items' -> data['data']['items']
        
        Args:
            data: Raw API response data
            data_path: Simple path expression
            
        Returns:
            Extracted data or original data if extraction fails
        """
        if not isinstance(data, dict):
            return data
        
        # Clean up the path - remove leading '$.' if present
        path = data_path.strip()
        if path.startswith('$.'):
            path = path[2:]
        elif path.startswith('$'):
            path = path[1:]
        
        # Split by dots and navigate
        parts = [p for p in path.split('.') if p]
        
        current = data
        for part in parts:
            # Handle array notation like 'items[0]' or 'items[*]'
            if '[' in part:
                key = part[:part.index('[')]
                if key and isinstance(current, dict) and key in current:
                    current = current[key]
                # For [*] or [index], just return the array/item
                if isinstance(current, list):
                    # If [*], return the whole list; if [n], return nth element
                    bracket_content = part[part.index('[')+1:part.index(']')]
                    if bracket_content != '*':
                        try:
                            idx = int(bracket_content)
                            current = current[idx]
                        except (ValueError, IndexError):
                            pass
            elif isinstance(current, dict) and part in current:
                current = current[part]
            else:
                logger.warning(f"Fallback extraction: key '{part}' not found, returning original data")
                return data
        
        logger.info(f"Fallback extraction using path '{data_path}' succeeded")
        return current
