"""
FBI Crime Data Explorer API Connector

This connector provides access to the FBI's Crime Data Explorer API,
which includes national and state-level crime statistics.

API Documentation: https://crime-data-explorer.fr.cloud.gov/pages/docApi
"""

import requests
import time
from typing import Dict, List, Any, Optional
from core.base_connector import BaseConnector
import logging

logger = logging.getLogger(__name__)


class FBICrimeConnector(BaseConnector):
    """
    Connector for FBI Crime Data Explorer API.
    
    Provides access to:
    - National crime estimates
    - State crime estimates
    - Agency data
    - Offense data
    - Arrest data
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize FBI Crime Data connector.
        
        Args:
            config: Configuration dictionary containing:
                - url: Base API URL
                - api_key: FBI Crime Data API key
                - format: Response format (default: JSON)
        """
        super().__init__(config)
        self.base_url = config.get('url', 'https://api.usa.gov/crime/fbi/sapi')
        self.api_key = config.get('api_key')
        self.format = config.get('format', 'JSON').upper()
        self.session = None
        self.max_retries = config.get('max_retries', 3)
        self.retry_delay = config.get('retry_delay', 1)
        
    def connect(self) -> bool:
        """
        Establish connection to FBI Crime Data API.
        
        Returns:
            bool: True if connection successful
        """
        try:
            self.session = requests.Session()
            self.session.headers.update({
                'Accept': 'application/json'
            })
            
            # Test connection with a simple request
            test_url = f"{self.base_url}/api/estimates/national/2020/2020"
            params = {'api_key': self.api_key}
            
            response = self.session.get(test_url, params=params, timeout=10)
            response.raise_for_status()
            
            self.connected = True
            logger.info("Successfully connected to FBI Crime Data API")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to connect to FBI Crime Data API: {str(e)}")
            self.connected = False
            return False
    
    def disconnect(self) -> bool:
        """
        Close connection to FBI Crime Data API.
        
        Returns:
            bool: True if disconnection successful
        """
        try:
            if self.session:
                self.session.close()
                self.session = None
            
            self.connected = False
            logger.info("Disconnected from FBI Crime Data API")
            return True
            
        except Exception as e:
            logger.error(f"Error disconnecting from FBI Crime Data API: {str(e)}")
            return False
    
    def query(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a query against FBI Crime Data API.
        
        Args:
            parameters: Query parameters including:
                - endpoint: API endpoint (e.g., 'estimates/national', 'estimates/states/CA')
                - from: Start year (optional)
                - to: End year (optional)
                - variables: Specific variables to retrieve (optional)
                - Additional endpoint-specific parameters
        
        Returns:
            dict: Query results with metadata
        """
        if not self.connected:
            if not self.connect():
                raise ConnectionError("Failed to connect to FBI Crime Data API")
        
        try:
            endpoint = parameters.get('endpoint', 'estimates/national')
            from_year = parameters.get('from', '2020')
            to_year = parameters.get('to', '2020')
            
            # Build URL - FBI Crime Data API structure: /api/{endpoint}/{from_year}/{to_year}
            if endpoint.startswith('api/'):
                url = f"{self.base_url}/{endpoint}/{from_year}/{to_year}"
            else:
                url = f"{self.base_url}/api/{endpoint}/{from_year}/{to_year}"
            
            # Build query parameters
            params = {'api_key': self.api_key}
            
            # Add other parameters (excluding endpoint, from, to which are in URL)
            for key, value in parameters.items():
                if key not in ['endpoint', 'from', 'to', 'api_key']:
                    params[key] = value
            
            # Execute request with retry logic
            response = self._execute_with_retry(url, params)
            
            # Parse response
            data = response.json()
            
            # Transform to standard format
            transformed_data = self.transform(data)
            
            return {
                'success': True,
                'data': transformed_data,
                'metadata': {
                    'endpoint': endpoint,
                    'parameters': parameters,
                    'status_code': response.status_code
                }
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"FBI Crime Data API request failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error querying FBI Crime Data API: {str(e)}")
            raise
    
    def _execute_with_retry(self, url: str, params: Dict[str, Any]) -> requests.Response:
        """
        Execute request with exponential backoff retry logic.
        
        Args:
            url: Request URL
            params: Query parameters
            
        Returns:
            requests.Response: HTTP response
        """
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                # Check for rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', self.retry_delay))
                    logger.warning(f"Rate limited. Retrying after {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}). "
                                 f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Request failed after {self.max_retries} attempts")
        
        raise last_exception
    
    def validate(self) -> bool:
        """
        Validate FBI Crime Data API connection and credentials.
        
        Returns:
            bool: True if validation successful
        """
        try:
            if not self.api_key:
                logger.error("API key is required for FBI Crime Data API")
                return False
            
            # Test connection
            if not self.connect():
                return False
            
            # Test a simple query
            test_params = {
                'endpoint': 'estimates/national',
                'from': '2020',
                'to': '2020'
            }
            
            result = self.query(test_params)
            
            if result.get('success'):
                logger.info("FBI Crime Data API validation successful")
                return True
            else:
                logger.error("FBI Crime Data API validation failed")
                return False
                
        except Exception as e:
            logger.error(f"FBI Crime Data API validation error: {str(e)}")
            return False
        finally:
            self.disconnect()
    
    def transform(self, data: Any) -> Dict[str, Any]:
        """
        Transform FBI Crime Data API response to standard format.
        
        Args:
            data: Raw API response data
            
        Returns:
            dict: Transformed data in standard format
        """
        try:
            # FBI Crime Data API returns data in various formats
            # Most endpoints return a 'results' array
            
            if isinstance(data, dict):
                # Check for common response structures
                if 'results' in data:
                    records = data['results']
                elif 'data' in data:
                    records = data['data']
                else:
                    # Treat the whole response as a single record
                    records = [data]
            elif isinstance(data, list):
                records = data
            else:
                records = [{'value': data}]
            
            # Ensure records is a list
            if not isinstance(records, list):
                records = [records]
            
            return {
                'data': records,
                'metadata': {
                    'source': 'FBI Crime Data Explorer',
                    'record_count': len(records),
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S')
                }
            }
            
        except Exception as e:
            logger.error(f"Error transforming FBI Crime Data: {str(e)}")
            raise
    
    def get_capabilities(self) -> Dict[str, Any]:
        """
        Get connector capabilities.
        
        Returns:
            dict: Connector capabilities
        """
        return {
            'name': 'FBI Crime Data Explorer',
            'version': '1.0',
            'supported_endpoints': [
                'estimates/national',
                'estimates/states/{state_abbr}',
                'agencies',
                'agencies/count',
                'offenses',
                'arrests/national'
            ],
            'features': {
                'pagination': True,
                'filtering': True,
                'time_series': True,
                'geographic_queries': True
            },
            'data_types': [
                'crime_estimates',
                'agency_data',
                'offense_data',
                'arrest_data'
            ],
            'geographic_levels': [
                'national',
                'state',
                'agency'
            ]
        }
    
    def get_available_endpoints(self) -> List[str]:
        """
        Get list of available API endpoints.
        
        Returns:
            list: Available endpoints
        """
        return [
            'estimates/national',
            'estimates/states/{state_abbr}',
            'estimates/states/{state_abbr}/{offense}',
            'agencies',
            'agencies/{ori}',
            'agencies/count',
            'offenses',
            'arrests/national',
            'arrests/states/{state_abbr}'
        ]
    
    def get_state_abbreviations(self) -> List[str]:
        """
        Get list of valid state abbreviations.
        
        Returns:
            list: State abbreviations
        """
        return [
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL',
            'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME',
            'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH',
            'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI',
            'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
        ]
