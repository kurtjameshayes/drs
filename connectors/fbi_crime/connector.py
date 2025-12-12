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
        default_base_url = 'https://api.usa.gov/crime/fbi/sapi'
        self.base_url = config.get('url', default_base_url).rstrip('/')
        self.api_key = config.get('api_key')
        self.format = config.get('format', 'JSON').upper()
        self.session = None
        self.max_retries = config.get('max_retries', 3)
        self.retry_delay = config.get('retry_delay', 1)

        base_url_lower = self.base_url.lower()
        self._api_key_param_explicit = 'api_key_param' in config
        self.api_key_param = config.get('api_key_param')
        if not self.api_key_param:
            self.api_key_param = 'API_KEY' if 'cde' in base_url_lower else 'api_key'

        api_namespace = config.get('api_namespace')
        if api_namespace is None:
            self.api_namespace = '' if 'cde' in base_url_lower else 'api'
        else:
            self.api_namespace = api_namespace.strip('/')

        self._year_mode_explicit = 'year_mode' in config
        year_mode = config.get('year_mode')
        if year_mode not in {'path', 'query'}:
            self.year_mode = 'query' if 'cde' in base_url_lower else 'path'
        else:
            self.year_mode = year_mode
        
    def connect(self) -> bool:
        """
        Establish connection resources without performing validation requests.
        
        Returns:
            bool: True if session initialization succeeded
        """
        try:
            if not self.session:
                self.session = requests.Session()
                self.session.headers.update({'Accept': 'application/json'})
            self.connected = True
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to initialize FBI Crime Data session: {str(e)}")
            self.connected = False
            return False
        except Exception as e:
            logger.error(f"Unexpected error initializing FBI Crime Data session: {str(e)}")
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
            request_year_mode = self._resolve_year_mode(endpoint)
            api_key_param = self._resolve_api_key_param(endpoint)
            from_year = parameters.get('from')
            to_year = parameters.get('to')

            if request_year_mode == 'path':
                from_year = from_year or '2020'
                to_year = to_year or '2020'

            params = self._prepare_query_params(parameters, api_key_param=api_key_param)
            url = self._build_request_url(
                endpoint,
                from_year,
                to_year,
                params,
                year_mode=request_year_mode,
            )

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
                full_url = self._compose_request_url(url, params)
                logger.info(
                    "Executing FBI Crime API query attempt=%s url=%s",
                    attempt + 1,
                    full_url,
                )

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

    def _prepare_query_params(
        self,
        parameters: Dict[str, Any],
        api_key_param: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Build the request parameters, excluding keys that are embedded in the path.
        """
        params = self._build_auth_params(api_key_param=api_key_param)
        excluded_keys = {'endpoint', 'from', 'to', 'api_key', 'API_KEY', self.api_key_param}
        for key, value in parameters.items():
            if key not in excluded_keys and value is not None:
                params[key] = value
        return params

    def _build_auth_params(self, api_key_param: Optional[str] = None) -> Dict[str, Any]:
        """Return the authentication/query params required for API access."""
        params: Dict[str, Any] = {}
        key_name = api_key_param or self.api_key_param
        if self.api_key and key_name:
            params[key_name] = self.api_key
        return params

    def _build_request_url(
        self,
        endpoint: str,
        from_value: Optional[str],
        to_value: Optional[str],
        params: Dict[str, Any],
        year_mode: Optional[str] = None,
    ) -> str:
        """
        Construct the request URL, adapting to the legacy SAPI (`/api/.../year/year`)
        and the newer CDE endpoints that expect query parameters.
        """
        endpoint = (endpoint or '').strip()

        if endpoint.startswith('http://') or endpoint.startswith('https://'):
            url = endpoint
        else:
            clean_endpoint = endpoint.lstrip('/')
            namespace = self.api_namespace.strip('/') if self.api_namespace else ''

            if namespace:
                if clean_endpoint.startswith(f"{namespace}/"):
                    route = clean_endpoint
                elif clean_endpoint == namespace:
                    route = namespace
                else:
                    route = "/".join(part for part in [namespace, clean_endpoint] if part)
            else:
                route = clean_endpoint

            parts: List[str] = [self.base_url]
            if route:
                parts.append(route)

            active_year_mode = year_mode or self.year_mode
            if active_year_mode == 'path':
                if from_value:
                    parts.append(str(from_value))
                if to_value:
                    parts.append(str(to_value))

            normalized_parts: List[str] = []
            for idx, part in enumerate(parts):
                if not part:
                    continue
                normalized_parts.append(part.rstrip('/') if idx == 0 else part.strip('/'))

            url = "/".join(normalized_parts)

        active_year_mode = year_mode or self.year_mode
        if active_year_mode == 'query':
            if from_value and 'from' not in params:
                params['from'] = from_value
            if to_value and 'to' not in params:
                params['to'] = to_value

        return url

    def _resolve_api_key_param(self, endpoint: Optional[str]) -> str:
        """
        Determine which API key parameter name to use for a request.
        """
        if self._api_key_param_explicit:
            return self.api_key_param

        if self._is_cde_endpoint(endpoint):
            return 'API_KEY'

        return self.api_key_param

    def _resolve_year_mode(self, endpoint: Optional[str]) -> str:
        """
        Determine whether to encode years in the path or as query parameters.
        """
        if self._year_mode_explicit:
            return self.year_mode

        if self._is_cde_endpoint(endpoint):
            return 'query'

        return self.year_mode

    @staticmethod
    def _is_cde_endpoint(endpoint: Optional[str]) -> bool:
        if not endpoint:
            return False
        endpoint_lower = endpoint.strip().lower()
        if endpoint_lower.startswith('http://') or endpoint_lower.startswith('https://'):
            return '/crime/fbi/cde/' in endpoint_lower
        return False
    
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
