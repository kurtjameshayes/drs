"""
Analysis Plan Manager - handles all functionality related to executing and managing analysis plans.

This module provides a centralized manager for:
- CRUD operations on analysis plans stored in MongoDB
- Executing analysis plans against stored queries
- Validating analysis plan configurations
- Storing and retrieving execution results
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Union

import pandas as pd
from pymongo import ASCENDING, MongoClient
from pymongo.errors import PyMongoError

from config import Config
from core.query_engine import QueryEngine
from models.analysis_query_result import AnalysisQueryResultStore


logger = logging.getLogger(__name__)


class AnalysisPlanManager:
    """
    Manages analysis plans including CRUD operations, execution, and result storage.
    
    Analysis plans define how to join multiple stored queries and run analytics
    on the resulting combined dataset.
    """
    
    DEFAULT_COLLECTION_NAME = "analysis_plans"
    
    def __init__(
        self,
        db_client: Optional[MongoClient] = None,
        collection_name: str = DEFAULT_COLLECTION_NAME,
        query_engine: Optional[QueryEngine] = None,
        results_store: Optional[AnalysisQueryResultStore] = None,
    ) -> None:
        """
        Initialize the analysis plan manager.
        
        Args:
            db_client: Optional MongoDB client (uses Config.MONGO_URI by default)
            collection_name: MongoDB collection for storing analysis plans
            query_engine: Optional QueryEngine instance
            results_store: Optional AnalysisQueryResultStore instance
        """
        self._client = db_client or MongoClient(Config.MONGO_URI)
        self._db = self._client[Config.DATABASE_NAME]
        self.collection = self._db[collection_name]
        self._collection_name = collection_name
        
        self._query_engine = query_engine
        self._results_store = results_store
        
        self._ensure_indexes()
    
    def _ensure_indexes(self) -> None:
        """Create indexes for efficient querying."""
        self.collection.create_index([("plan_id", ASCENDING)], unique=True)
        self.collection.create_index("name")
        self.collection.create_index("created_at")
        self.collection.create_index("updated_at")
        self.collection.create_index("active")
    
    @property
    def query_engine(self) -> QueryEngine:
        """Lazy-load QueryEngine on first access."""
        if self._query_engine is None:
            self._query_engine = QueryEngine()
        return self._query_engine
    
    @property
    def results_store(self) -> AnalysisQueryResultStore:
        """Lazy-load AnalysisQueryResultStore on first access."""
        if self._results_store is None:
            self._results_store = AnalysisQueryResultStore()
        return self._results_store
    
    # =========================================================================
    # CRUD Operations
    # =========================================================================
    
    def create_plan(
        self,
        plan_id: str,
        name: str,
        queries: List[Dict[str, Any]],
        join_on: Union[str, List[str]],
        analysis_plan: Dict[str, Any],
        *,
        description: Optional[str] = None,
        join_type: str = "inner",
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        active: bool = True,
    ) -> Dict[str, Any]:
        """
        Create a new analysis plan.
        
        Args:
            plan_id: Unique identifier for the plan
            name: Human-readable name for the plan
            queries: List of query specifications with query_id and optional overrides
            join_on: Column(s) to join on
            analysis_plan: Configuration for analytics to run (passed to DataAnalysisEngine)
            description: Optional description of the plan
            join_type: Join strategy (inner, left, right, outer)
            tags: Optional list of tags for categorization
            metadata: Optional additional metadata
            active: Whether the plan is active
            
        Returns:
            The created plan document
            
        Raises:
            ValueError: If plan_id already exists or validation fails
        """
        # Validate inputs
        self._validate_plan_id(plan_id)
        self._validate_queries(queries)
        self._validate_join_on(join_on)
        self._validate_join_type(join_type)
        
        # Check for existing plan
        existing = self.get_plan(plan_id)
        if existing:
            raise ValueError(f"Analysis plan '{plan_id}' already exists. Use update_plan() instead.")
        
        now = datetime.now(timezone.utc)
        join_columns = [join_on] if isinstance(join_on, str) else list(join_on)
        
        document = {
            "plan_id": plan_id,
            "name": name,
            "description": description,
            "queries": self._normalize_queries(queries),
            "join_on": join_columns,
            "join_type": join_type,
            "plan": analysis_plan,
            "tags": tags or [],
            "metadata": metadata or {},
            "active": active,
            "created_at": now,
            "updated_at": now,
        }
        
        self.collection.insert_one(document)
        logger.info("Created analysis plan: %s", plan_id)
        
        # Remove MongoDB _id for return value
        document.pop("_id", None)
        return document
    
    def get_plan(self, plan_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an analysis plan by ID.
        
        Args:
            plan_id: The plan identifier
            
        Returns:
            The plan document or None if not found
        """
        plan_doc = (
            self.collection.find_one({"plan_id": plan_id})
            or self.collection.find_one({"_id": plan_id})
            or self.collection.find_one({"name": plan_id})
        )
        
        if plan_doc and "_id" in plan_doc:
            plan_doc["_id"] = str(plan_doc["_id"])
        
        return plan_doc
    
    def update_plan(
        self,
        plan_id: str,
        *,
        name: Optional[str] = None,
        queries: Optional[List[Dict[str, Any]]] = None,
        join_on: Optional[Union[str, List[str]]] = None,
        analysis_plan: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None,
        join_type: Optional[str] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        active: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Update an existing analysis plan.
        
        Args:
            plan_id: The plan identifier
            **kwargs: Fields to update (only non-None values are applied)
            
        Returns:
            The updated plan document
            
        Raises:
            ValueError: If plan not found or validation fails
        """
        existing = self.get_plan(plan_id)
        if not existing:
            raise ValueError(f"Analysis plan '{plan_id}' not found.")
        
        updates: Dict[str, Any] = {"updated_at": datetime.now(timezone.utc)}
        
        if name is not None:
            updates["name"] = name
        
        if queries is not None:
            self._validate_queries(queries)
            updates["queries"] = self._normalize_queries(queries)
        
        if join_on is not None:
            self._validate_join_on(join_on)
            updates["join_on"] = [join_on] if isinstance(join_on, str) else list(join_on)
        
        if analysis_plan is not None:
            updates["plan"] = analysis_plan
        
        if description is not None:
            updates["description"] = description
        
        if join_type is not None:
            self._validate_join_type(join_type)
            updates["join_type"] = join_type
        
        if tags is not None:
            updates["tags"] = tags
        
        if metadata is not None:
            updates["metadata"] = metadata
        
        if active is not None:
            updates["active"] = active
        
        self.collection.update_one({"plan_id": plan_id}, {"$set": updates})
        logger.info("Updated analysis plan: %s", plan_id)
        
        return self.get_plan(plan_id)
    
    def delete_plan(self, plan_id: str) -> bool:
        """
        Delete an analysis plan.
        
        Args:
            plan_id: The plan identifier
            
        Returns:
            True if deleted, False if not found
        """
        result = self.collection.delete_one({"plan_id": plan_id})
        if result.deleted_count > 0:
            logger.info("Deleted analysis plan: %s", plan_id)
            return True
        return False
    
    def list_plans(
        self,
        *,
        active_only: bool = False,
        tags: Optional[List[str]] = None,
        limit: int = 100,
        skip: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        List analysis plans with optional filtering.
        
        Args:
            active_only: Only return active plans
            tags: Filter by tags (plans must have all specified tags)
            limit: Maximum number of results
            skip: Number of results to skip (for pagination)
            
        Returns:
            List of plan documents
        """
        query: Dict[str, Any] = {}
        
        if active_only:
            query["active"] = True
        
        if tags:
            query["tags"] = {"$all": tags}
        
        cursor = (
            self.collection.find(query)
            .sort("updated_at", -1)
            .skip(skip)
            .limit(limit)
        )
        
        plans = []
        for doc in cursor:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
            plans.append(doc)
        
        return plans
    
    # =========================================================================
    # Execution
    # =========================================================================
    
    def execute_plan(
        self,
        plan_id: str,
        *,
        store_results: bool = True,
        use_cache: bool = None,
    ) -> Dict[str, Any]:
        """
        Execute an analysis plan.
        
        Args:
            plan_id: The plan identifier
            store_results: Whether to store results in AnalysisQueryResultStore
            use_cache: Override cache behavior for query execution
            
        Returns:
            Dict containing:
                - dataframe: The joined DataFrame
                - analysis: Analysis results from DataAnalysisEngine
                - plan_document: The executed plan document
                - stored_document: The storage document (if store_results=True)
                
        Raises:
            ValueError: If plan not found or execution fails
        """
        plan_doc = self.get_plan(plan_id)
        if not plan_doc:
            raise ValueError(f"Analysis plan '{plan_id}' not found.")
        
        if not plan_doc.get("active", True):
            raise ValueError(f"Analysis plan '{plan_id}' is inactive.")
        
        logger.info("Executing analysis plan: %s", plan_id)
        
        # Extract plan components
        plan_queries = self.extract_plan_queries(plan_doc)
        join_on = self.extract_join_columns(plan_doc, plan_queries)
        join_strategy = self.extract_join_strategy(plan_doc)
        analysis_plan = self.extract_analysis_plan_definition(plan_doc)
        
        # Build query specs from stored queries
        query_specs = self.build_query_specs_from_stored_queries(plan_queries)
        
        # Execute queries and build DataFrame
        dataframe = self.query_engine.execute_queries_to_dataframe(
            queries=query_specs,
            join_on=join_on,
            how=join_strategy,
            use_cache=use_cache,
        )
        
        # Run analysis
        analysis_result = self.query_engine.analyze_queries(
            queries=query_specs,
            join_on=join_on,
            analysis_plan=analysis_plan,
            how=join_strategy,
            use_cache=use_cache,
        )
        
        result = {
            "dataframe": dataframe,
            "analysis": analysis_result["analysis"],
            "plan_document": plan_doc,
            "query_specs": query_specs,
            "join_columns": join_on,
            "join_strategy": join_strategy,
        }
        
        # Store results if requested
        if store_results:
            plan_label = self._get_plan_label(plan_doc, plan_id)
            stored_doc = self.results_store.save_joined_results(
                plan_id=plan_id,
                plan_name=plan_label,
                join_columns=join_on,
                join_strategy=join_strategy,
                query_specs=query_specs,
                dataframe=dataframe,
                analysis_summary=analysis_result["analysis"],
                metadata={
                    "analysis_plan_collection": self._collection_name,
                    "plan_document_id": plan_doc.get("_id"),
                    "description": plan_doc.get("description"),
                    "tags": plan_doc.get("tags"),
                },
            )
            result["stored_document"] = stored_doc
            logger.info(
                "Stored %d rows for plan '%s'",
                stored_doc.get("record_count", 0),
                plan_id,
            )
        
        logger.info("Completed execution of analysis plan: %s", plan_id)
        return result
    
    def execute_plan_dataframe_only(
        self,
        plan_id: str,
        *,
        use_cache: bool = None,
    ) -> pd.DataFrame:
        """
        Execute an analysis plan and return only the joined DataFrame.
        
        This is a lightweight version that skips analysis and result storage.
        
        Args:
            plan_id: The plan identifier
            use_cache: Override cache behavior
            
        Returns:
            The joined DataFrame
        """
        plan_doc = self.get_plan(plan_id)
        if not plan_doc:
            raise ValueError(f"Analysis plan '{plan_id}' not found.")
        
        plan_queries = self.extract_plan_queries(plan_doc)
        join_on = self.extract_join_columns(plan_doc, plan_queries)
        join_strategy = self.extract_join_strategy(plan_doc)
        query_specs = self.build_query_specs_from_stored_queries(plan_queries)
        
        return self.query_engine.execute_queries_to_dataframe(
            queries=query_specs,
            join_on=join_on,
            how=join_strategy,
            use_cache=use_cache,
        )
    
    # =========================================================================
    # Validation
    # =========================================================================
    
    def validate_plan(self, plan_id: str) -> Dict[str, Any]:
        """
        Validate an analysis plan without executing it.
        
        Checks:
        - Plan exists and is active
        - All referenced stored queries exist
        - Join columns are valid
        - Analysis plan configuration is valid
        
        Args:
            plan_id: The plan identifier
            
        Returns:
            Validation result with 'valid' boolean and any errors/warnings
        """
        errors: List[str] = []
        warnings: List[str] = []
        
        plan_doc = self.get_plan(plan_id)
        if not plan_doc:
            return {
                "valid": False,
                "errors": [f"Analysis plan '{plan_id}' not found."],
                "warnings": [],
            }
        
        if not plan_doc.get("active", True):
            warnings.append("Analysis plan is marked as inactive.")
        
        # Validate queries
        try:
            plan_queries = self.extract_plan_queries(plan_doc)
        except ValueError as e:
            errors.append(str(e))
            plan_queries = []
        
        # Check stored queries exist
        for query in plan_queries:
            query_id = query.get("query_id")
            if query_id:
                stored_query = self.query_engine.get_stored_query(query_id)
                if not stored_query:
                    errors.append(f"Stored query '{query_id}' not found.")
                elif not stored_query.get("active", True):
                    warnings.append(f"Stored query '{query_id}' is inactive.")
        
        # Validate join columns
        try:
            join_on = self.extract_join_columns(plan_doc, plan_queries)
            if not join_on:
                errors.append("No join columns specified.")
        except ValueError as e:
            errors.append(str(e))
        
        # Validate join strategy
        try:
            self.extract_join_strategy(plan_doc)
        except ValueError as e:
            errors.append(str(e))
        
        # Validate analysis plan
        try:
            analysis_plan = self.extract_analysis_plan_definition(plan_doc)
            if not analysis_plan:
                warnings.append("Analysis plan definition is empty.")
        except ValueError as e:
            errors.append(str(e))
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "plan_id": plan_id,
            "query_count": len(plan_queries),
        }
    
    # =========================================================================
    # Helper Methods for Plan Extraction
    # =========================================================================
    
    def build_query_specs_from_stored_queries(
        self,
        plan_queries: Sequence[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Convert stored query definitions plus plan overrides into query specs.
        
        Args:
            plan_queries: List of query definitions from the analysis plan
            
        Returns:
            List of query specifications ready for QueryEngine
        """
        specs: List[Dict[str, Any]] = []
        
        for plan_query in plan_queries:
            query_id = plan_query.get("query_id") or plan_query.get("id")
            if not query_id:
                raise ValueError("Each plan query must include a 'query_id' field.")
            
            stored_query = self.query_engine.get_stored_query(query_id)
            if not stored_query:
                raise ValueError(
                    f"Stored query '{query_id}' was not found. "
                    "Use manage_queries.py or the API to create it first."
                )
            
            spec: Dict[str, Any] = {
                "source_id": stored_query["connector_id"],
                "parameters": dict(stored_query.get("parameters", {})),
                "alias": (
                    plan_query.get("alias")
                    or stored_query.get("alias")
                    or stored_query.get("query_name")
                    or query_id
                ),
            }
            
            # Merge plan-level parameter overrides
            if isinstance(plan_query.get("parameters"), dict):
                spec["parameters"].update(plan_query["parameters"])
            
            # Merge rename columns
            rename_columns: Dict[str, Any] = {}
            if isinstance(stored_query.get("rename_columns"), dict):
                rename_columns.update(stored_query["rename_columns"])
            if isinstance(plan_query.get("rename_columns"), dict):
                rename_columns.update(plan_query["rename_columns"])
            if rename_columns:
                spec["rename_columns"] = rename_columns
            
            # Copy other plan-specific keys
            plan_specific_keys = {
                "query_id",
                "id",
                "join_column",
                "join_columns",
                "join_on",
            }
            for key, value in plan_query.items():
                if key in plan_specific_keys or key in {"alias", "parameters", "rename_columns"}:
                    continue
                spec[key] = value
            
            specs.append(spec)
        
        return specs
    
    def extract_plan_queries(self, plan_doc: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract and normalize queries from a plan document.
        
        Args:
            plan_doc: The plan document
            
        Returns:
            List of normalized query specifications
        """
        queries = plan_doc.get("queries")
        if not queries and plan_doc.get("query_ids"):
            queries = [{"query_id": query_id} for query_id in plan_doc["query_ids"]]
        
        if not queries:
            raise ValueError(
                f"Analysis plan '{self._get_plan_identifier(plan_doc)}' must include a 'queries' list."
            )
        
        normalized: List[Dict[str, Any]] = []
        for entry in queries:
            if isinstance(entry, dict):
                query_id = entry.get("query_id") or entry.get("id")
                if not query_id:
                    raise ValueError(
                        "Each query entry within an analysis plan must provide 'query_id'."
                    )
                normalized_entry = dict(entry)
                normalized_entry["query_id"] = query_id
                normalized.append(normalized_entry)
            elif isinstance(entry, str):
                normalized.append({"query_id": entry})
            else:
                raise ValueError(
                    "Analysis plan queries must be dictionaries or query_id strings."
                )
        
        return normalized
    
    def extract_join_columns(
        self,
        plan_doc: Dict[str, Any],
        plan_queries: Sequence[Dict[str, Any]],
    ) -> List[str]:
        """
        Extract join columns from plan document or per-query specifications.
        
        Args:
            plan_doc: The plan document
            plan_queries: List of query specifications
            
        Returns:
            List of join column names
        """
        plan_level_join = self._as_string_list(
            plan_doc.get("join_on")
            or plan_doc.get("join_columns")
            or plan_doc.get("join_column")
            or plan_doc.get("join_keys")
        )
        if plan_level_join:
            return plan_level_join
        
        per_query_joins: List[List[str]] = []
        for query in plan_queries:
            query_join = self._as_string_list(
                query.get("join_on") or query.get("join_columns") or query.get("join_column")
            )
            if query_join:
                per_query_joins.append(query_join)
        
        if not per_query_joins:
            raise ValueError(
                "Analysis plan must specify join columns via 'join_on' or per-query join fields."
            )
        
        canonical = per_query_joins[0]
        for candidate in per_query_joins[1:]:
            if candidate != canonical:
                raise ValueError(
                    "Conflicting join columns detected across plan queries. "
                    "Add a plan-level 'join_on' list to disambiguate."
                )
        
        return canonical
    
    def extract_join_strategy(self, plan_doc: Dict[str, Any]) -> str:
        """
        Extract join strategy from plan document.
        
        Args:
            plan_doc: The plan document
            
        Returns:
            Join strategy (inner, left, right, outer)
        """
        join_candidates = (
            plan_doc.get("join_type"),
            plan_doc.get("join_how"),
            plan_doc.get("join_strategy"),
            plan_doc.get("how"),
        )
        for candidate in join_candidates:
            if not candidate:
                continue
            join_value = str(candidate).lower()
            if join_value in {"inner", "left", "right", "outer"}:
                return join_value
            raise ValueError(
                f"Join strategy '{candidate}' is invalid. Choose inner, left, right, or outer."
            )
        
        return "inner"
    
    def extract_analysis_plan_definition(self, plan_doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract the nested analysis plan payload from a document.
        
        Args:
            plan_doc: The plan document
            
        Returns:
            The analysis plan definition for DataAnalysisEngine
        """
        plan_payload: Optional[Dict[str, Any]] = None
        for key in ("plan", "analysis_plan", "definition"):
            candidate = plan_doc.get(key)
            if isinstance(candidate, dict):
                plan_payload = candidate
                break
        
        if plan_payload is None:
            # Fall back to extracting non-metadata fields
            metadata_keys = {
                "_id",
                "plan_id",
                "plan_name",
                "name",
                "description",
                "created_at",
                "updated_at",
                "tags",
                "collection",
                "queries",
                "query_ids",
                "join_on",
                "join_columns",
                "join_column",
                "join_how",
                "join_type",
                "join_strategy",
                "how",
                "active",
                "metadata",
            }
            plan_payload = {
                key: value for key, value in plan_doc.items() if key not in metadata_keys
            }
        
        if not plan_payload:
            raise ValueError(
                f"Analysis plan document '{self._get_plan_identifier(plan_doc)}' "
                "does not contain a valid plan definition."
            )
        
        return plan_payload
    
    # =========================================================================
    # Private Helpers
    # =========================================================================
    
    def _validate_plan_id(self, plan_id: str) -> None:
        """Validate plan_id format."""
        if not plan_id or not isinstance(plan_id, str):
            raise ValueError("plan_id must be a non-empty string.")
        if len(plan_id) > 255:
            raise ValueError("plan_id must be 255 characters or less.")
    
    def _validate_queries(self, queries: List[Dict[str, Any]]) -> None:
        """Validate queries list."""
        if not queries or len(queries) < 2:
            raise ValueError("At least two queries are required for an analysis plan.")
        
        for i, query in enumerate(queries):
            if not isinstance(query, (dict, str)):
                raise ValueError(f"Query at index {i} must be a dict or string.")
            if isinstance(query, dict):
                query_id = query.get("query_id") or query.get("id")
                if not query_id:
                    raise ValueError(f"Query at index {i} must have a 'query_id' field.")
    
    def _validate_join_on(self, join_on: Union[str, List[str]]) -> None:
        """Validate join_on specification."""
        if not join_on:
            raise ValueError("join_on must be specified.")
        if isinstance(join_on, str) and not join_on.strip():
            raise ValueError("join_on must be a non-empty string.")
        if isinstance(join_on, list) and not all(isinstance(c, str) and c.strip() for c in join_on):
            raise ValueError("join_on list must contain non-empty strings.")
    
    def _validate_join_type(self, join_type: str) -> None:
        """Validate join type."""
        valid_types = {"inner", "left", "right", "outer"}
        if join_type.lower() not in valid_types:
            raise ValueError(f"join_type must be one of: {', '.join(valid_types)}")
    
    def _normalize_queries(self, queries: List[Union[Dict[str, Any], str]]) -> List[Dict[str, Any]]:
        """Normalize queries to consistent dict format."""
        normalized = []
        for query in queries:
            if isinstance(query, str):
                normalized.append({"query_id": query})
            else:
                normalized.append(dict(query))
        return normalized
    
    def _get_plan_identifier(self, plan_doc: Dict[str, Any], fallback: Optional[str] = None) -> str:
        """Get a human-readable identifier for a plan document."""
        return str(
            plan_doc.get("plan_id")
            or plan_doc.get("name")
            or plan_doc.get("_id")
            or fallback
            or "unknown"
        )
    
    def _get_plan_label(self, plan_doc: Dict[str, Any], fallback: Optional[str] = None) -> str:
        """Get a display label for a plan document."""
        return str(
            plan_doc.get("plan_name")
            or plan_doc.get("name")
            or plan_doc.get("plan_id")
            or fallback
            or "unknown"
        )
    
    @staticmethod
    def _as_string_list(value: Any) -> Optional[List[str]]:
        """Convert a value to a list of strings."""
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            return [stripped] if stripped else None
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            items = []
            for entry in value:
                if isinstance(entry, str):
                    stripped = entry.strip()
                    if stripped:
                        items.append(stripped)
            return items or None
        return None
