from typing import Dict, Any, List, Optional, Union
import logging

import pandas as pd

from core.cache_manager import CacheManager
from core.connector_manager import ConnectorManager
from core.data_analysis import DataAnalysisEngine
from models.stored_query import StoredQuery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QueryEngine:
    """
    Orchestrates data retrieval requests with caching and optimization.
    """
    
    def __init__(self, connector_manager: ConnectorManager = None,
                 cache_manager: CacheManager = None,
                 analysis_engine: DataAnalysisEngine = None,
                 stored_query: StoredQuery = None):
        """
        Initialize query engine.
        
        Args:
            connector_manager: ConnectorManager instance
            cache_manager: CacheManager instance
            analysis_engine: Optional DataAnalysisEngine instance
            stored_query: Optional StoredQuery instance
        """
        self.connector_manager = connector_manager or ConnectorManager()
        self.cache_manager = cache_manager or CacheManager()
        self.analysis_engine = analysis_engine or DataAnalysisEngine()
        self.stored_query = stored_query or StoredQuery()
        self.use_cache = True
    
    def execute_query(self, source_id: str, parameters: Dict[str, Any], 
                     use_cache: bool = None, query_id: str = None) -> Dict[str, Any]:
        """
        Execute a query against a data source.
        
        Args:
            source_id: Data source identifier
            parameters: Query parameters
            use_cache: Override default cache behavior
            query_id: Optional reference to stored query
            
        Returns:
            Dict containing query results
        """
        # Determine if cache should be used
        should_use_cache = use_cache if use_cache is not None else self.use_cache
        
        # Try to get from cache first
        if should_use_cache:
            cached_result = self.cache_manager.get(source_id, parameters)
            if cached_result:
                result = {
                    "success": True,
                    "source": "cache",
                    "data": cached_result
                }
                # Add query_id if provided
                if query_id:
                    result["query_id"] = query_id
                return result
        
        # Execute query through connector
        try:
            result = self.connector_manager.query(source_id, parameters)
            
            if result.get("success"):
                # Cache the result with query_id if provided
                if should_use_cache:
                    self.cache_manager.set(source_id, parameters, result["data"], query_id=query_id)
                
                result["source"] = "connector"
                
                # Add query_id if provided
                if query_id:
                    result["query_id"] = query_id
                
                return result
            else:
                if query_id:
                    result["query_id"] = query_id
                return result
                
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            error_result = {
                "success": False,
                "error": str(e),
                "source_id": source_id
            }
            if query_id:
                error_result["query_id"] = query_id
            return error_result
    
    def execute_stored_query(self, query_id: str, use_cache: bool = None, 
                            parameter_overrides: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Execute a stored query by ID.
        
        Args:
            query_id: Stored query identifier
            use_cache: Override default cache behavior
            parameter_overrides: Optional parameters to override stored query parameters
            
        Returns:
            Dict containing query results
        """
        try:
            # Get stored query
            stored_query = self.stored_query.get_by_id(query_id)
            
            if not stored_query:
                return {
                    "success": False,
                    "error": f"Stored query not found: {query_id}",
                    "query_id": query_id
                }
            
            # Check if query is active
            if not stored_query.get("active", True):
                return {
                    "success": False,
                    "error": f"Stored query is inactive: {query_id}",
                    "query_id": query_id
                }
            
            # Get connector_id and parameters
            connector_id = stored_query["connector_id"]
            parameters = stored_query["parameters"].copy()
            
            # Apply parameter overrides if provided
            if parameter_overrides:
                parameters.update(parameter_overrides)
            
            # Execute the query with query_id reference
            result = self.execute_query(connector_id, parameters, use_cache, query_id=query_id)
            
            # Add stored query metadata to result
            if result.get("success"):
                result["query_name"] = stored_query.get("query_name")
                result["query_description"] = stored_query.get("description")
            
            return result
            
        except Exception as e:
            logger.error(f"Stored query execution failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "query_id": query_id
            }
    
    def get_stored_query(self, query_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a stored query by ID.
        
        Args:
            query_id: Stored query identifier
            
        Returns:
            Dict containing stored query or None if not found
        """
        return self.stored_query.get_by_id(query_id)
    
    def list_stored_queries(self, connector_id: str = None, 
                           active_only: bool = False) -> List[Dict[str, Any]]:
        """
        List stored queries.
        
        Args:
            connector_id: Filter by connector ID
            active_only: Only return active queries
            
        Returns:
            List of stored queries
        """
        return self.stored_query.get_all(connector_id=connector_id, active_only=active_only)
    
    def execute_multi_source_query(self, queries: List[Dict[str, Any]], 
                                   use_cache: bool = None) -> List[Dict[str, Any]]:
        """
        Execute queries across multiple data sources.
        
        Args:
            queries: List of query specifications, each containing:
                - source_id: Data source identifier
                - parameters: Query parameters
            use_cache: Override default cache behavior
            
        Returns:
            List of query results
        """
        results = []
        
        for query_spec in queries:
            source_id = query_spec.get("source_id")
            parameters = query_spec.get("parameters", {})
            
            if not source_id:
                results.append({
                    "success": False,
                    "error": "source_id is required"
                })
                continue
            
            result = self.execute_query(source_id, parameters, use_cache)
            results.append(result)
        
        return results
    
    def aggregate_results(self, results: List[Dict[str, Any]], 
                         aggregation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Aggregate results from multiple queries.
        
        Args:
            results: List of query results
            aggregation: Aggregation specification
            
        Returns:
            Dict containing aggregated results
        """
        # Extract successful results
        successful_results = [
            r["data"]["data"] for r in results 
            if r.get("success") and "data" in r.get("data", {})
        ]
        
        if not successful_results:
            return {
                "success": False,
                "error": "No successful results to aggregate"
            }
        
        # Simple aggregation strategies
        agg_type = aggregation.get("type", "merge")
        
        if agg_type == "merge":
            # Merge all records
            merged_data = []
            for result_set in successful_results:
                merged_data.extend(result_set)
            
            return {
                "success": True,
                "data": merged_data,
                "record_count": len(merged_data)
            }
        
        elif agg_type == "union":
            # Union with deduplication (requires unique key)
            unique_key = aggregation.get("unique_key")
            if not unique_key:
                return {
                    "success": False,
                    "error": "unique_key required for union aggregation"
                }
            
            seen = set()
            union_data = []
            
            for result_set in successful_results:
                for record in result_set:
                    key_value = record.get(unique_key)
                    if key_value not in seen:
                        seen.add(key_value)
                        union_data.append(record)
            
            return {
                "success": True,
                "data": union_data,
                "record_count": len(union_data)
            }
        
        else:
            return {
                "success": False,
                "error": f"Unknown aggregation type: {agg_type}"
            }

    def execute_queries_to_dataframe(
        self,
        queries: List[Dict[str, Any]],
        join_on: Union[List[str], str],
        how: str = "inner",
        aggregation: Optional[Dict[str, Any]] = None,
        use_cache: bool = None,
    ) -> pd.DataFrame:
        """
        Execute two or more queries, join their results, and return a DataFrame.

        Args:
            queries: List of query specifications with keys:
                - source_id (str): required
                - parameters (dict): optional
                - alias (str): optional label used in suffixes
                - rename_columns (dict): optional column rename map applied before joins
            join_on: Column name or list of column names to join on
            how: pandas merge strategy (inner, left, right, outer)
            aggregation: Optional aggregation definition:
                {
                    "group_by": ["state"],
                    "metrics": [
                        {"column": "value", "agg": "sum", "alias": "total_value"}
                    ]
                }
            use_cache: Override cache usage for underlying queries

        Returns:
            pd.DataFrame containing the joined (and optionally aggregated) data.
        """
        if not queries or len(queries) < 2:
            raise ValueError("At least two queries are required to build a DataFrame")

        join_keys = [join_on] if isinstance(join_on, str) else join_on
        if not join_keys:
            raise ValueError("join_on parameter is required")

        dataframes = []
        for spec in queries:
            source_id = spec.get("source_id")
            if not source_id:
                raise ValueError("Each query spec must include a source_id")

            parameters = spec.get("parameters", {})
            alias = spec.get("alias", source_id)
            rename_map = spec.get("rename_columns")

            result = self.execute_query(source_id, parameters, use_cache)
            if not result.get("success"):
                raise ValueError(f"Query failed for {source_id}: {result.get('error')}")

            records = self._extract_records(result)
            df = pd.DataFrame(records)

            if rename_map:
                df = df.rename(columns=rename_map)

            missing_keys = [key for key in join_keys if key not in df.columns]
            if missing_keys:
                raise ValueError(
                    f"Join keys {missing_keys} not present in query result for {source_id}"
                )

            dataframes.append({"alias": alias, "df": df})

        joined_df = dataframes[0]["df"]
        for entry in dataframes[1:]:
            joined_df = pd.merge(
                joined_df,
                entry["df"],
                on=join_keys,
                how=how,
                suffixes=("", f"_{entry['alias']}"),
            )

        if aggregation:
            joined_df = self._apply_aggregation(joined_df, aggregation)

        return joined_df

    def analyze_queries(
        self,
        queries: List[Dict[str, Any]],
        join_on: Union[List[str], str],
        analysis_plan: Dict[str, Any],
        how: str = "inner",
        aggregation: Optional[Dict[str, Any]] = None,
        use_cache: bool = None,
    ) -> Dict[str, Any]:
        """
        Build a DataFrame from multiple queries and run an analysis plan.
        Returns both the DataFrame and the computed analytical summaries.
        """
        dataframe = self.execute_queries_to_dataframe(
            queries=queries,
            join_on=join_on,
            how=how,
            aggregation=aggregation,
            use_cache=use_cache,
        )

        analysis_results = self.analysis_engine.run_suite(dataframe, analysis_plan)
        return {
            "dataframe": dataframe,
            "analysis": analysis_results,
        }

    @staticmethod
    def _extract_records(result: Dict[str, Any]) -> List[Dict[str, Any]]:
        payload = result.get("data", {})
        if isinstance(payload, dict):
            data = payload.get("data", [])
        else:
            data = payload
        return data or []

    @staticmethod
    def _apply_aggregation(df: pd.DataFrame, aggregation: Dict[str, Any]) -> pd.DataFrame:
        group_by = aggregation.get("group_by")
        metrics = aggregation.get("metrics", [])

        if not group_by or not metrics:
            raise ValueError("Aggregation requires group_by and metrics definitions")

        agg_ops = {metric["column"]: metric.get("agg", "sum") for metric in metrics}
        alias_map = {
            metric["column"]: metric.get("alias", f"{metric['column']}_{metric.get('agg', 'sum')}")
            for metric in metrics if metric.get("alias")
        }

        aggregated = df.groupby(group_by, dropna=False).agg(agg_ops).reset_index()
        if alias_map:
            aggregated = aggregated.rename(columns=alias_map)
        return aggregated
    
    def validate_query(self, source_id: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a query without executing it.
        
        Args:
            source_id: Data source identifier
            parameters: Query parameters
            
        Returns:
            Dict containing validation results
        """
        try:
            connector = self.connector_manager.get_connector(source_id)
            if not connector:
                return {
                    "valid": False,
                    "error": f"Connector not found: {source_id}"
                }
            
            # Check if connector is connected
            if not connector.connected:
                return {
                    "valid": False,
                    "error": f"Connector not connected: {source_id}"
                }
            
            # Basic parameter validation
            if not isinstance(parameters, dict):
                return {
                    "valid": False,
                    "error": "Parameters must be a dictionary"
                }
            
            return {
                "valid": True,
                "source_id": source_id,
                "connector_type": connector.config.get("connector_type")
            }
            
        except Exception as e:
            return {
                "valid": False,
                "error": str(e)
            }
    
    def get_query_stats(self) -> Dict[str, Any]:
        """
        Get query execution statistics.
        
        Returns:
            Dict containing statistics
        """
        return {
            "cache_stats": self.cache_manager.get_stats(),
            "available_sources": len(self.connector_manager.list_sources())
        }
