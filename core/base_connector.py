from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from urllib.parse import urlencode
from datetime import datetime
import logging

from jsonpath_ng import parse as jsonpath_parse
from jsonpath_ng.exceptions import JsonPathParserError

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

    def _extract_data_by_path(self, raw_data: Any) -> Any:
        """
        Extract data from the raw response using the configured JSONPath data_path.

        If no data_path is configured, returns the raw_data unchanged.
        The data_path should be a valid JSONPath expression (e.g., '$.data', '$.results').

        Args:
            raw_data: The raw JSON response from the API

        Returns:
            The extracted data if data_path is configured and matches,
            otherwise the original raw_data
        """
        data_path = self.config.get("data_path")
        if not data_path:
            return raw_data

        try:
            jsonpath_expr = jsonpath_parse(data_path)
            matches = jsonpath_expr.find(raw_data)

            if not matches:
                logger.warning(
                    "JSONPath '%s' did not match any data in response for source_id=%s. "
                    "Returning original data.",
                    data_path,
                    self.source_id,
                )
                return raw_data

            # If there's a single match, return its value directly
            # If multiple matches, return a list of values
            if len(matches) == 1:
                return matches[0].value
            else:
                return [match.value for match in matches]

        except JsonPathParserError as e:
            logger.error(
                "Invalid JSONPath expression '%s' for source_id=%s: %s. "
                "Returning original data.",
                data_path,
                self.source_id,
                str(e),
            )
            return raw_data
        except Exception as e:
            logger.error(
                "Error extracting data with JSONPath '%s' for source_id=%s: %s. "
                "Returning original data.",
                data_path,
                self.source_id,
                str(e),
            )
            return raw_data
    
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
