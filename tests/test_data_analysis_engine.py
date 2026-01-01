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


def test_linear_regression_graphing_data(sample_df):
    """Test that linear regression returns sufficient data for graphing predictions vs actuals."""
    engine = DataAnalysisEngine()

    result = engine.linear_regression(
        sample_df,
        features=["harvest"],
        target="value",
    )

    # Verify graphing_data is present
    assert "graphing_data" in result
    graphing_data = result["graphing_data"]

    # Verify all required fields for graphing
    assert "predictions" in graphing_data
    assert "actuals" in graphing_data
    assert "features" in graphing_data
    assert "feature_names" in graphing_data
    assert "n_samples" in graphing_data

    # Verify data lengths match for pairing predictions with actuals
    assert len(graphing_data["predictions"]) == len(graphing_data["actuals"])
    assert len(graphing_data["features"]) == len(graphing_data["predictions"])
    assert graphing_data["n_samples"] == len(graphing_data["predictions"])

    # Verify feature names are correct
    assert graphing_data["feature_names"] == ["harvest"]


def test_random_forest_regression_graphing_data(sample_df):
    """Test that random forest regression returns sufficient data for graphing predictions vs actuals."""
    engine = DataAnalysisEngine()

    result = engine.random_forest_regression(
        sample_df,
        features=["harvest"],
        target="value",
        n_estimators=50,
    )

    # Verify graphing_data is present
    assert "graphing_data" in result
    graphing_data = result["graphing_data"]

    # Verify all required fields for graphing
    assert "predictions" in graphing_data
    assert "actuals" in graphing_data
    assert "features" in graphing_data
    assert "feature_names" in graphing_data
    assert "n_samples" in graphing_data

    # Verify data lengths match for pairing predictions with actuals
    assert len(graphing_data["predictions"]) == len(graphing_data["actuals"])
    assert len(graphing_data["features"]) == len(graphing_data["predictions"])
    assert graphing_data["n_samples"] == len(graphing_data["predictions"])

    # Verify feature names are correct
    assert graphing_data["feature_names"] == ["harvest"]


def test_predictive_analysis_graphing_data(sample_df):
    """Test that predictive_analysis wrapper also includes graphing data."""
    engine = DataAnalysisEngine()

    # Test with forest model
    result = engine.predictive_analysis(
        sample_df,
        features=["harvest"],
        target="value",
        model_type="forest",
        n_estimators=25,
    )

    assert "graphing_data" in result
    graphing_data = result["graphing_data"]
    assert len(graphing_data["predictions"]) == len(graphing_data["actuals"])


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


def test_xgboost_regression(sample_df):
    engine = DataAnalysisEngine()

    xgb_result = engine.xgboost_regression(
        sample_df,
        features=["harvest"],
        target="value",
        n_estimators=50,
        max_depth=3,
        learning_rate=0.1,
    )
    assert "feature_importance" in xgb_result
    assert "r2_score" in xgb_result
    assert "rmse" in xgb_result
    assert "model_params" in xgb_result
    assert xgb_result["model_params"]["n_estimators"] == 50


def test_xgboost_regression_graphing_data(sample_df):
    """Test that XGBoost regression returns sufficient data for graphing predictions vs actuals."""
    engine = DataAnalysisEngine()

    result = engine.xgboost_regression(
        sample_df,
        features=["harvest"],
        target="value",
        n_estimators=50,
    )

    # Verify graphing_data is present
    assert "graphing_data" in result
    graphing_data = result["graphing_data"]

    # Verify all required fields for graphing
    assert "predictions" in graphing_data
    assert "actuals" in graphing_data
    assert "features" in graphing_data
    assert "feature_names" in graphing_data
    assert "n_samples" in graphing_data

    # Verify data lengths match for pairing predictions with actuals
    assert len(graphing_data["predictions"]) == len(graphing_data["actuals"])
    assert len(graphing_data["features"]) == len(graphing_data["predictions"])
    assert graphing_data["n_samples"] == len(graphing_data["predictions"])

    # Verify feature names are correct
    assert graphing_data["feature_names"] == ["harvest"]


def test_xgboost_classification(sample_df):
    engine = DataAnalysisEngine()

    xgbc_result = engine.xgboost_classification(
        sample_df,
        features=["value", "population"],
        target="category",
        n_estimators=50,
        max_depth=3,
    )
    assert "accuracy" in xgbc_result
    assert "f1_score" in xgbc_result
    assert "feature_importance" in xgbc_result
    assert "class_labels" in xgbc_result
    assert "num_classes" in xgbc_result
    assert xgbc_result["num_classes"] == 2  # "A" and "B"


def test_xgboost_classification_graphing_data(sample_df):
    """Test that XGBoost classification returns sufficient data for graphing predictions vs actuals."""
    engine = DataAnalysisEngine()

    result = engine.xgboost_classification(
        sample_df,
        features=["value", "population"],
        target="category",
        n_estimators=50,
    )

    # Verify graphing_data is present
    assert "graphing_data" in result
    graphing_data = result["graphing_data"]

    # Verify all required fields for graphing
    assert "predictions" in graphing_data
    assert "actuals" in graphing_data
    assert "features" in graphing_data
    assert "feature_names" in graphing_data
    assert "n_samples" in graphing_data

    # Verify data lengths match for pairing predictions with actuals
    assert len(graphing_data["predictions"]) == len(graphing_data["actuals"])
    assert len(graphing_data["features"]) == len(graphing_data["predictions"])
    assert graphing_data["n_samples"] == len(graphing_data["predictions"])

    # Verify feature names are correct
    assert graphing_data["feature_names"] == ["value", "population"]


def test_predictive_analysis_xgboost(sample_df):
    engine = DataAnalysisEngine()

    predictive = engine.predictive_analysis(
        sample_df,
        features=["harvest"],
        target="value",
        model_type="xgboost",
        n_estimators=25,
    )
    assert predictive["model_type"] == "xgboost"
    assert "feature_importance" in predictive


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


def test_run_suite_with_xgboost(sample_df):
    engine = DataAnalysisEngine()
    plan = {
        "xgboost": {
            "features": ["harvest"],
            "target": "value",
            "n_estimators": 25,
            "learning_rate": 0.1,
        },
        "xgboost_classification": {
            "features": ["value", "population"],
            "target": "category",
            "n_estimators": 25,
        },
        "predictive": {
            "features": ["harvest"],
            "target": "value",
            "model_type": "xgboost",
            "n_estimators": 25,
        },
    }
    results = engine.run_suite(sample_df, plan)

    assert "xgboost_regression" in results
    assert "xgboost_classification" in results
    assert "predictive_analysis" in results
    assert results["predictive_analysis"]["model_type"] == "xgboost"
