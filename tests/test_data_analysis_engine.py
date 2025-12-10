import pandas as pd
import pytest

from core.data_analysis import DataAnalysisEngine


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "state": ["AL", "AK", "AZ", "AR", "CA", "CO"],
            "category": ["A", "B", "A", "B", "A", "B"],
            "value": [10, 20, 30, 40, 50, 60],
            "harvest": [1, 2, 3, 4, 5, 6],
            "population": [100, 200, 300, 400, 500, 600],
            "date": pd.date_range("2023-01-01", periods=6, freq="M"),
        }
    )


def test_basic_and_exploratory_statistics(sample_df):
    engine = DataAnalysisEngine()
    stats = engine.basic_statistics(sample_df)
    exploratory = engine.exploratory_analysis(sample_df)

    assert stats["row_count"] == 6
    assert "value" in stats["numeric_summary"]
    assert "data_types" in exploratory
    assert "state" in exploratory["distribution"]


def test_inferential_and_time_series(sample_df):
    engine = DataAnalysisEngine()

    inferential = engine.inferential_analysis(
        sample_df,
        comparisons=[{"x": "value", "y": "population", "test": "pearson"}],
    )
    assert len(inferential) == 1
    assert "p_value" in inferential[0]

    ts = engine.time_series_analysis(
        sample_df,
        time_column="date",
        target_column="value",
        freq="M",
    )
    assert "recent_values" in ts
    assert "trend_slope" in ts


def test_regression_methods(sample_df):
    engine = DataAnalysisEngine()

    linear = engine.linear_regression(
        sample_df,
        features=["harvest"],
        target="value",
    )
    assert "coefficients" in linear

    forest = engine.random_forest_regression(
        sample_df,
        features=["harvest"],
        target="value",
        n_estimators=50,
    )
    assert "feature_importance" in forest


def test_multivariate_and_predictive(sample_df):
    engine = DataAnalysisEngine()

    multivariate = engine.multivariate_analysis(
        sample_df,
        features=["value", "population", "harvest"],
        n_components=2,
    )
    assert len(multivariate["components"]) == 2

    predictive = engine.predictive_analysis(
        sample_df,
        features=["harvest"],
        target="value",
        model_type="forest",
        n_estimators=25,
    )
    assert predictive["model_type"] == "forest"


def test_run_suite(sample_df):
    engine = DataAnalysisEngine()
    plan = {
        "basic_statistics": True,
        "exploratory": True,
        "inferential_tests": [{"x": "value", "y": "population"}],
        "time_series": {"time_column": "date", "target_column": "value", "freq": "M"},
        "linear_regression": {"features": ["harvest"], "target": "value"},
        "multivariate": {"features": ["harvest", "value"], "n_components": 2},
    }
    results = engine.run_suite(sample_df, plan)

    assert "basic_statistics" in results
    assert "linear_regression" in results
