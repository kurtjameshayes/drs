"""Tests for the AnalysisPlanManager class."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import mongomock
import pandas as pd
import pytest

from core.analysis_plan_manager import AnalysisPlanManager


@pytest.fixture
def mock_mongo_client():
    """Create a mock MongoDB client using mongomock."""
    return mongomock.MongoClient()


@pytest.fixture
def mock_query_engine():
    """Create a mock QueryEngine."""
    engine = MagicMock()
    engine.get_stored_query.return_value = {
        "query_id": "test-query-1",
        "connector_id": "census_api",
        "query_name": "Test Query",
        "parameters": {"year": 2020},
        "active": True,
    }
    return engine


@pytest.fixture
def mock_results_store():
    """Create a mock AnalysisQueryResultStore."""
    store = MagicMock()
    store.save_joined_results.return_value = {
        "plan_id": "test-plan",
        "record_count": 10,
    }
    return store


@pytest.fixture
def manager(mock_mongo_client, mock_query_engine, mock_results_store):
    """Create an AnalysisPlanManager with mock dependencies."""
    mgr = AnalysisPlanManager(
        db_client=mock_mongo_client,
        collection_name="test_analysis_plans",
        query_engine=mock_query_engine,
        results_store=mock_results_store,
    )
    return mgr


@pytest.fixture
def sample_analysis_plan():
    """Sample analysis plan configuration."""
    return {
        "basic_statistics": True,
        "exploratory": True,
        "linear_regression": {
            "features": ["population"],
            "target": "value",
        },
    }


@pytest.fixture
def sample_queries():
    """Sample queries list for an analysis plan."""
    return [
        {"query_id": "census-population"},
        {"query_id": "usda-crops", "alias": "crops_data"},
    ]


class TestAnalysisPlanManagerCRUD:
    """Test CRUD operations for analysis plans."""

    def test_create_plan_success(self, manager, sample_analysis_plan, sample_queries):
        """Test successful plan creation."""
        plan = manager.create_plan(
            plan_id="test-plan-001",
            name="Test Analysis Plan",
            queries=sample_queries,
            join_on="state",
            analysis_plan=sample_analysis_plan,
            description="A test analysis plan",
            join_type="inner",
            tags=["test", "demo"],
        )

        assert plan["plan_id"] == "test-plan-001"
        assert plan["name"] == "Test Analysis Plan"
        assert plan["description"] == "A test analysis plan"
        assert plan["join_on"] == ["state"]
        assert plan["join_type"] == "inner"
        assert plan["tags"] == ["test", "demo"]
        assert plan["active"] is True
        assert len(plan["queries"]) == 2
        assert "created_at" in plan
        assert "updated_at" in plan

    def test_create_plan_with_string_query_ids(self, manager, sample_analysis_plan):
        """Test creating plan with string query IDs."""
        queries = ["query-1", "query-2"]
        plan = manager.create_plan(
            plan_id="test-plan-strings",
            name="String Queries Plan",
            queries=queries,
            join_on=["state", "year"],
            analysis_plan=sample_analysis_plan,
        )

        assert len(plan["queries"]) == 2
        assert plan["queries"][0] == {"query_id": "query-1"}
        assert plan["queries"][1] == {"query_id": "query-2"}

    def test_create_plan_duplicate_raises_error(
        self, manager, sample_analysis_plan, sample_queries
    ):
        """Test that creating a duplicate plan raises an error."""
        manager.create_plan(
            plan_id="duplicate-plan",
            name="First Plan",
            queries=sample_queries,
            join_on="state",
            analysis_plan=sample_analysis_plan,
        )

        with pytest.raises(ValueError, match="already exists"):
            manager.create_plan(
                plan_id="duplicate-plan",
                name="Second Plan",
                queries=sample_queries,
                join_on="state",
                analysis_plan=sample_analysis_plan,
            )

    def test_create_plan_validation_errors(self, manager, sample_analysis_plan):
        """Test validation errors during plan creation."""
        # Empty plan_id
        with pytest.raises(ValueError, match="plan_id must be"):
            manager.create_plan(
                plan_id="",
                name="Test",
                queries=[{"query_id": "q1"}, {"query_id": "q2"}],
                join_on="state",
                analysis_plan=sample_analysis_plan,
            )

        # Less than 2 queries
        with pytest.raises(ValueError, match="At least two queries"):
            manager.create_plan(
                plan_id="single-query",
                name="Test",
                queries=[{"query_id": "q1"}],
                join_on="state",
                analysis_plan=sample_analysis_plan,
            )

        # Invalid join type
        with pytest.raises(ValueError, match="join_type must be one of"):
            manager.create_plan(
                plan_id="invalid-join",
                name="Test",
                queries=[{"query_id": "q1"}, {"query_id": "q2"}],
                join_on="state",
                join_type="invalid",
                analysis_plan=sample_analysis_plan,
            )

    def test_get_plan_by_id(self, manager, sample_analysis_plan, sample_queries):
        """Test retrieving a plan by ID."""
        manager.create_plan(
            plan_id="get-test",
            name="Get Test Plan",
            queries=sample_queries,
            join_on="state",
            analysis_plan=sample_analysis_plan,
        )

        retrieved = manager.get_plan("get-test")
        assert retrieved is not None
        assert retrieved["plan_id"] == "get-test"
        assert retrieved["name"] == "Get Test Plan"

    def test_get_plan_not_found(self, manager):
        """Test retrieving a non-existent plan."""
        retrieved = manager.get_plan("non-existent")
        assert retrieved is None

    def test_update_plan_success(self, manager, sample_analysis_plan, sample_queries):
        """Test successful plan update."""
        manager.create_plan(
            plan_id="update-test",
            name="Original Name",
            queries=sample_queries,
            join_on="state",
            analysis_plan=sample_analysis_plan,
            description="Original description",
        )

        updated = manager.update_plan(
            "update-test",
            name="Updated Name",
            description="Updated description",
            tags=["updated"],
            active=False,
        )

        assert updated["name"] == "Updated Name"
        assert updated["description"] == "Updated description"
        assert updated["tags"] == ["updated"]
        assert updated["active"] is False
        # Original fields should remain
        assert updated["plan_id"] == "update-test"
        assert len(updated["queries"]) == 2

    def test_update_plan_not_found(self, manager):
        """Test updating a non-existent plan."""
        with pytest.raises(ValueError, match="not found"):
            manager.update_plan("non-existent", name="New Name")

    def test_delete_plan_success(self, manager, sample_analysis_plan, sample_queries):
        """Test successful plan deletion."""
        manager.create_plan(
            plan_id="delete-test",
            name="Delete Test Plan",
            queries=sample_queries,
            join_on="state",
            analysis_plan=sample_analysis_plan,
        )

        result = manager.delete_plan("delete-test")
        assert result is True

        # Verify deletion
        retrieved = manager.get_plan("delete-test")
        assert retrieved is None

    def test_delete_plan_not_found(self, manager):
        """Test deleting a non-existent plan."""
        result = manager.delete_plan("non-existent")
        assert result is False

    def test_list_plans(self, manager, sample_analysis_plan, sample_queries):
        """Test listing plans."""
        for i in range(3):
            manager.create_plan(
                plan_id=f"list-test-{i}",
                name=f"List Test Plan {i}",
                queries=sample_queries,
                join_on="state",
                analysis_plan=sample_analysis_plan,
                tags=["common"] + ([f"tag-{i}"] if i > 0 else []),
                active=i != 1,  # Make second plan inactive
            )

        # List all plans
        all_plans = manager.list_plans()
        assert len(all_plans) == 3

        # List active only
        active_plans = manager.list_plans(active_only=True)
        assert len(active_plans) == 2

        # List by tags
        tagged_plans = manager.list_plans(tags=["tag-2"])
        assert len(tagged_plans) == 1
        assert tagged_plans[0]["plan_id"] == "list-test-2"

    def test_list_plans_pagination(self, manager, sample_analysis_plan, sample_queries):
        """Test plan listing with pagination."""
        for i in range(5):
            manager.create_plan(
                plan_id=f"page-test-{i}",
                name=f"Page Test Plan {i}",
                queries=sample_queries,
                join_on="state",
                analysis_plan=sample_analysis_plan,
            )

        # First page
        page1 = manager.list_plans(limit=2, skip=0)
        assert len(page1) == 2

        # Second page
        page2 = manager.list_plans(limit=2, skip=2)
        assert len(page2) == 2

        # Last page
        page3 = manager.list_plans(limit=2, skip=4)
        assert len(page3) == 1


class TestAnalysisPlanManagerExtraction:
    """Test extraction methods for plan components."""

    def test_extract_plan_queries_from_list(self, manager):
        """Test extracting queries from a list."""
        plan_doc = {
            "plan_id": "test",
            "queries": [
                {"query_id": "q1", "alias": "query_one"},
                {"id": "q2"},  # Using 'id' instead of 'query_id'
            ],
        }

        queries = manager.extract_plan_queries(plan_doc)
        assert len(queries) == 2
        assert queries[0]["query_id"] == "q1"
        assert queries[0]["alias"] == "query_one"
        assert queries[1]["query_id"] == "q2"

    def test_extract_plan_queries_from_query_ids(self, manager):
        """Test extracting queries from query_ids list."""
        plan_doc = {
            "plan_id": "test",
            "query_ids": ["q1", "q2", "q3"],
        }

        queries = manager.extract_plan_queries(plan_doc)
        assert len(queries) == 3
        assert all(q["query_id"] == f"q{i+1}" for i, q in enumerate(queries))

    def test_extract_plan_queries_from_strings(self, manager):
        """Test extracting queries from string entries."""
        plan_doc = {
            "plan_id": "test",
            "queries": ["q1", "q2"],
        }

        queries = manager.extract_plan_queries(plan_doc)
        assert len(queries) == 2
        assert queries[0] == {"query_id": "q1"}

    def test_extract_plan_queries_missing_raises_error(self, manager):
        """Test that missing queries raises an error."""
        plan_doc = {"plan_id": "test"}

        with pytest.raises(ValueError, match="must include a 'queries' list"):
            manager.extract_plan_queries(plan_doc)

    def test_extract_join_columns_plan_level(self, manager):
        """Test extracting join columns from plan level."""
        plan_doc = {"join_on": ["state", "year"]}
        plan_queries = [{"query_id": "q1"}, {"query_id": "q2"}]

        join_cols = manager.extract_join_columns(plan_doc, plan_queries)
        assert join_cols == ["state", "year"]

    def test_extract_join_columns_from_string(self, manager):
        """Test extracting join columns from a string value."""
        plan_doc = {"join_column": "state"}
        plan_queries = [{"query_id": "q1"}, {"query_id": "q2"}]

        join_cols = manager.extract_join_columns(plan_doc, plan_queries)
        assert join_cols == ["state"]

    def test_extract_join_columns_from_queries(self, manager):
        """Test extracting join columns from per-query specifications."""
        plan_doc = {}
        plan_queries = [
            {"query_id": "q1", "join_on": ["state"]},
            {"query_id": "q2", "join_columns": ["state"]},
        ]

        join_cols = manager.extract_join_columns(plan_doc, plan_queries)
        assert join_cols == ["state"]

    def test_extract_join_columns_conflict_raises_error(self, manager):
        """Test that conflicting join columns raise an error."""
        plan_doc = {}
        plan_queries = [
            {"query_id": "q1", "join_on": ["state"]},
            {"query_id": "q2", "join_on": ["county"]},
        ]

        with pytest.raises(ValueError, match="Conflicting join columns"):
            manager.extract_join_columns(plan_doc, plan_queries)

    def test_extract_join_strategy_defaults_to_inner(self, manager):
        """Test that join strategy defaults to 'inner'."""
        assert manager.extract_join_strategy({}) == "inner"

    def test_extract_join_strategy_various_keys(self, manager):
        """Test extracting join strategy from various keys."""
        assert manager.extract_join_strategy({"join_type": "left"}) == "left"
        assert manager.extract_join_strategy({"join_how": "right"}) == "right"
        assert manager.extract_join_strategy({"how": "outer"}) == "outer"

    def test_extract_join_strategy_invalid_raises_error(self, manager):
        """Test that invalid join strategy raises an error."""
        with pytest.raises(ValueError, match="is invalid"):
            manager.extract_join_strategy({"join_type": "invalid"})

    def test_extract_analysis_plan_definition(self, manager):
        """Test extracting analysis plan definition."""
        plan_doc = {
            "plan_id": "test",
            "plan": {
                "basic_statistics": True,
                "linear_regression": {"features": ["x"], "target": "y"},
            },
        }

        analysis_plan = manager.extract_analysis_plan_definition(plan_doc)
        assert analysis_plan["basic_statistics"] is True
        assert "linear_regression" in analysis_plan


class TestAnalysisPlanManagerValidation:
    """Test plan validation functionality."""

    def test_validate_plan_success(
        self, manager, mock_query_engine, sample_analysis_plan, sample_queries
    ):
        """Test successful plan validation."""
        manager.create_plan(
            plan_id="valid-plan",
            name="Valid Plan",
            queries=sample_queries,
            join_on="state",
            analysis_plan=sample_analysis_plan,
        )

        result = manager.validate_plan("valid-plan")
        assert result["valid"] is True
        assert len(result["errors"]) == 0
        assert result["query_count"] == 2

    def test_validate_plan_not_found(self, manager):
        """Test validating a non-existent plan."""
        result = manager.validate_plan("non-existent")
        assert result["valid"] is False
        assert "not found" in result["errors"][0]

    def test_validate_plan_inactive_warning(
        self, manager, sample_analysis_plan, sample_queries
    ):
        """Test that inactive plan produces a warning."""
        manager.create_plan(
            plan_id="inactive-plan",
            name="Inactive Plan",
            queries=sample_queries,
            join_on="state",
            analysis_plan=sample_analysis_plan,
            active=False,
        )

        result = manager.validate_plan("inactive-plan")
        assert any("inactive" in w.lower() for w in result["warnings"])

    def test_validate_plan_missing_stored_query(
        self, manager, mock_query_engine, sample_analysis_plan, sample_queries
    ):
        """Test validation when stored query is missing."""
        manager.create_plan(
            plan_id="missing-query-plan",
            name="Missing Query Plan",
            queries=sample_queries,
            join_on="state",
            analysis_plan=sample_analysis_plan,
        )

        # Make query engine return None for stored queries
        mock_query_engine.get_stored_query.return_value = None

        result = manager.validate_plan("missing-query-plan")
        assert result["valid"] is False
        assert any("not found" in e for e in result["errors"])


class TestAnalysisPlanManagerQuerySpecs:
    """Test query spec building functionality."""

    def test_build_query_specs_from_stored_queries(self, manager, mock_query_engine):
        """Test building query specs from stored queries."""
        mock_query_engine.get_stored_query.return_value = {
            "query_id": "test-query",
            "connector_id": "census_api",
            "query_name": "Test Query",
            "parameters": {"year": 2020, "state": "AL"},
            "rename_columns": {"POP": "population"},
            "active": True,
        }

        plan_queries = [
            {
                "query_id": "test-query",
                "alias": "census_data",
                "parameters": {"state": "TX"},  # Override state
                "rename_columns": {"VALUE": "total_value"},  # Additional rename
            }
        ]

        specs = manager.build_query_specs_from_stored_queries(plan_queries)

        assert len(specs) == 1
        spec = specs[0]
        assert spec["source_id"] == "census_api"
        assert spec["alias"] == "census_data"
        assert spec["parameters"]["year"] == 2020
        assert spec["parameters"]["state"] == "TX"  # Overridden
        assert spec["rename_columns"]["POP"] == "population"  # From stored
        assert spec["rename_columns"]["VALUE"] == "total_value"  # From plan

    def test_build_query_specs_missing_stored_query(self, manager, mock_query_engine):
        """Test error when stored query is not found."""
        mock_query_engine.get_stored_query.return_value = None

        plan_queries = [{"query_id": "missing-query"}]

        with pytest.raises(ValueError, match="was not found"):
            manager.build_query_specs_from_stored_queries(plan_queries)

    def test_build_query_specs_missing_query_id(self, manager):
        """Test error when query_id is missing."""
        plan_queries = [{"alias": "no_id"}]

        with pytest.raises(ValueError, match="must include a 'query_id' field"):
            manager.build_query_specs_from_stored_queries(plan_queries)


class TestAnalysisPlanManagerExecution:
    """Test plan execution functionality."""

    def test_execute_plan_success(
        self, manager, mock_query_engine, mock_results_store, sample_analysis_plan, sample_queries
    ):
        """Test successful plan execution."""
        manager.create_plan(
            plan_id="exec-test",
            name="Execution Test Plan",
            queries=sample_queries,
            join_on="state",
            analysis_plan=sample_analysis_plan,
        )

        # Set up mock return values
        sample_df = pd.DataFrame({"state": ["AL", "TX"], "value": [100, 200]})
        mock_query_engine.execute_queries_to_dataframe.return_value = sample_df
        mock_query_engine.analyze_queries.return_value = {
            "dataframe": sample_df,
            "analysis": {"basic_statistics": {"row_count": 2}},
        }

        result = manager.execute_plan("exec-test")

        assert "dataframe" in result
        assert "analysis" in result
        assert "plan_document" in result
        assert "stored_document" in result
        mock_results_store.save_joined_results.assert_called_once()

    def test_execute_plan_not_found(self, manager):
        """Test execution of non-existent plan."""
        with pytest.raises(ValueError, match="not found"):
            manager.execute_plan("non-existent")

    def test_execute_plan_inactive(self, manager, sample_analysis_plan, sample_queries):
        """Test execution of inactive plan."""
        manager.create_plan(
            plan_id="inactive-exec-test",
            name="Inactive Execution Test",
            queries=sample_queries,
            join_on="state",
            analysis_plan=sample_analysis_plan,
            active=False,
        )

        with pytest.raises(ValueError, match="inactive"):
            manager.execute_plan("inactive-exec-test")

    def test_execute_plan_without_storing(
        self, manager, mock_query_engine, mock_results_store, sample_analysis_plan, sample_queries
    ):
        """Test execution without storing results."""
        manager.create_plan(
            plan_id="no-store-test",
            name="No Store Test Plan",
            queries=sample_queries,
            join_on="state",
            analysis_plan=sample_analysis_plan,
        )

        sample_df = pd.DataFrame({"state": ["AL"], "value": [100]})
        mock_query_engine.execute_queries_to_dataframe.return_value = sample_df
        mock_query_engine.analyze_queries.return_value = {
            "dataframe": sample_df,
            "analysis": {},
        }

        result = manager.execute_plan("no-store-test", store_results=False)

        assert "dataframe" in result
        assert "stored_document" not in result
        mock_results_store.save_joined_results.assert_not_called()

    def test_execute_plan_dataframe_only(
        self, manager, mock_query_engine, sample_analysis_plan, sample_queries
    ):
        """Test lightweight execution returning only DataFrame."""
        manager.create_plan(
            plan_id="df-only-test",
            name="DataFrame Only Test",
            queries=sample_queries,
            join_on="state",
            analysis_plan=sample_analysis_plan,
        )

        expected_df = pd.DataFrame({"state": ["AL", "TX"], "value": [100, 200]})
        mock_query_engine.execute_queries_to_dataframe.return_value = expected_df

        result = manager.execute_plan_dataframe_only("df-only-test")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        # analyze_queries should not be called
        mock_query_engine.analyze_queries.assert_not_called()


class TestAnalysisPlanManagerHelpers:
    """Test helper methods."""

    def test_as_string_list_with_string(self, manager):
        """Test _as_string_list with string input."""
        assert manager._as_string_list("state") == ["state"]
        assert manager._as_string_list("  state  ") == ["state"]
        assert manager._as_string_list("") is None
        assert manager._as_string_list("   ") is None

    def test_as_string_list_with_list(self, manager):
        """Test _as_string_list with list input."""
        assert manager._as_string_list(["state", "year"]) == ["state", "year"]
        assert manager._as_string_list(["state", "", "year"]) == ["state", "year"]
        assert manager._as_string_list([]) is None
        assert manager._as_string_list(["", "  "]) is None

    def test_as_string_list_with_none(self, manager):
        """Test _as_string_list with None input."""
        assert manager._as_string_list(None) is None

    def test_get_plan_identifier(self, manager):
        """Test _get_plan_identifier method."""
        assert manager._get_plan_identifier({"plan_id": "p1"}) == "p1"
        assert manager._get_plan_identifier({"name": "Test Plan"}) == "Test Plan"
        assert manager._get_plan_identifier({}, fallback="default") == "default"
        assert manager._get_plan_identifier({}) == "unknown"

    def test_get_plan_label(self, manager):
        """Test _get_plan_label method."""
        assert manager._get_plan_label({"plan_name": "My Plan"}) == "My Plan"
        assert manager._get_plan_label({"name": "Test"}) == "Test"
        assert manager._get_plan_label({"plan_id": "p1"}) == "p1"
        assert manager._get_plan_label({}, fallback="fallback") == "fallback"
