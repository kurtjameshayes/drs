from typing import Dict, Any, List, Optional

import pandas as pd
import numpy as np
from scipy import stats
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split


class DataAnalysisEngine:
    """
    Provides analytical tooling on top of query result DataFrames.
    Supports descriptive statistics, exploratory/inferential analysis,
    regressions, time-series utilities, multivariate techniques, and
    predictive modeling helpers.
    """

    def basic_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        numeric_df = df.select_dtypes(include="number")
        categorical_df = df.select_dtypes(exclude="number")

        describe_all = df.describe(include="all").fillna(value=np.nan)
        stats_payload = {
            "row_count": int(len(df)),
            "column_count": int(df.shape[1]),
            "numeric_summary": describe_all.select_dtypes(include="number").to_dict(),
            "categorical_summary": {
                col: categorical_df[col].value_counts().head(5).to_dict()
                for col in categorical_df.columns
            },
            "missing_values": df.isna().sum().to_dict(),
            "unique_values": df.nunique(dropna=True).to_dict(),
            "correlations": numeric_df.corr().to_dict() if not numeric_df.empty else {},
        }
        return stats_payload

    def exploratory_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        sample_records = df.head(5).to_dict(orient="records")
        distribution = {
            col: df[col].value_counts().head(5).to_dict()
            for col in df.columns if df[col].dtype == "object"
        }
        return {
            "data_types": df.dtypes.astype(str).to_dict(),
            "sample_records": sample_records,
            "distribution": distribution,
            "missing_percentage": (df.isna().mean() * 100).round(2).to_dict(),
        }

    def inferential_analysis(
        self,
        df: pd.DataFrame,
        comparisons: List[Dict[str, Any]],
        alpha: float = 0.05,
    ) -> List[Dict[str, Any]]:
        results = []
        for item in comparisons:
            col_x = item.get("x")
            col_y = item.get("y")
            test = item.get("test", "pearson").lower()

            if not col_x or not col_y:
                raise ValueError("comparisons entries require 'x' and 'y' keys")

            clean_df = df[[col_x, col_y]].dropna()
            if clean_df.empty:
                results.append({
                    "x": col_x,
                    "y": col_y,
                    "test": test,
                    "error": "Insufficient overlapping data",
                })
                continue

            if test == "pearson":
                stat, p_value = stats.pearsonr(clean_df[col_x], clean_df[col_y])
            elif test == "spearman":
                stat, p_value = stats.spearmanr(clean_df[col_x], clean_df[col_y])
            elif test == "ttest":
                stat, p_value = stats.ttest_ind(
                    clean_df[col_x], clean_df[col_y], equal_var=False
                )
            else:
                raise ValueError(f"Unsupported inferential test: {test}")

            results.append({
                "x": col_x,
                "y": col_y,
                "test": test,
                "statistic": float(stat),
                "p_value": float(p_value),
                "significant": bool(p_value < alpha),
            })
        return results

    def time_series_analysis(
        self,
        df: pd.DataFrame,
        time_column: str,
        target_column: str,
        freq: Optional[str] = None,
        rolling_window: int = 7,
    ) -> Dict[str, Any]:
        ts_df = df[[time_column, target_column]].dropna()
        if ts_df.empty:
            raise ValueError("No data available for time series analysis")

        ts_df[time_column] = pd.to_datetime(ts_df[time_column])
        ts_df = ts_df.sort_values(time_column).set_index(time_column)
        series = ts_df[target_column].astype(float)

        if freq:
            series = series.resample(freq).mean().interpolate()

        rolling = series.rolling(window=rolling_window, min_periods=1).mean()
        pct_change = series.pct_change()
        slope = float(np.polyfit(range(len(series)), series.values, 1)[0]) if len(series) > 1 else 0.0

        return {
            "recent_values": series.tail(20).to_dict(),
            "rolling_mean": rolling.tail(20).to_dict(),
            "volatility": float(pct_change.std()) if not pct_change.empty else None,
            "trend_slope": slope,
        }

    def linear_regression(
        self,
        df: pd.DataFrame,
        features: List[str],
        target: str,
        test_size: float = 0.2,
        random_state: int = 42,
    ) -> Dict[str, Any]:
        dataset = df[features + [target]].dropna()
        if len(dataset) < 2:
            raise ValueError("Not enough rows for regression analysis")

        X = dataset[features].values
        y = dataset[target].values

        if len(dataset) > 4:
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=test_size, random_state=random_state
            )
        else:
            X_train, X_test, y_train, y_test = X, X, y, y

        model = LinearRegression()
        model.fit(X_train, y_train)
        predictions = model.predict(X_test)

        mse = mean_squared_error(y_test, predictions)
        return {
            "coefficients": dict(zip(features, model.coef_.tolist())),
            "intercept": float(model.intercept_),
            "r2_score": float(r2_score(y_test, predictions)),
            "rmse": float(np.sqrt(mse)),
            "predictions_sample": predictions[:5].tolist(),
        }

    def random_forest_regression(
        self,
        df: pd.DataFrame,
        features: List[str],
        target: str,
        n_estimators: int = 200,
        max_depth: Optional[int] = None,
        test_size: float = 0.2,
        random_state: int = 42,
    ) -> Dict[str, Any]:
        dataset = df[features + [target]].dropna()
        if len(dataset) < 5:
            raise ValueError("Not enough rows for random forest regression")

        X = dataset[features].values
        y = dataset[target].values

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )

        model = RandomForestRegressor(
            n_estimators=n_estimators,
            max_depth=max_depth,
            random_state=random_state,
        )
        model.fit(X_train, y_train)
        predictions = model.predict(X_test)

        mse = mean_squared_error(y_test, predictions)
        return {
            "feature_importance": dict(zip(features, model.feature_importances_.tolist())),
            "r2_score": float(r2_score(y_test, predictions)),
            "rmse": float(np.sqrt(mse)),
            "predictions_sample": predictions[:5].tolist(),
        }

    def multivariate_analysis(
        self,
        df: pd.DataFrame,
        features: List[str],
        n_components: int = 2,
    ) -> Dict[str, Any]:
        dataset = df[features].dropna()
        if dataset.empty:
            raise ValueError("No data available for multivariate analysis")

        components = min(n_components, len(features), len(dataset))
        pca = PCA(n_components=components)
        transformed = pca.fit_transform(dataset.values)

        return {
            "explained_variance_ratio": pca.explained_variance_ratio_.tolist(),
            "components": pca.components_.tolist(),
            "projected_samples": transformed[:10].tolist(),
        }

    def predictive_analysis(
        self,
        df: pd.DataFrame,
        features: List[str],
        target: str,
        model_type: str = "linear",
        **kwargs,
    ) -> Dict[str, Any]:
        model_type = model_type.lower()
        if model_type == "linear":
            result = self.linear_regression(df, features, target, **kwargs)
        elif model_type in {"forest", "random_forest"}:
            result = self.random_forest_regression(df, features, target, **kwargs)
        else:
            raise ValueError(f"Unsupported predictive model: {model_type}")

        result["model_type"] = model_type
        return result

    def run_suite(self, df: pd.DataFrame, plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a configurable analysis plan against a DataFrame.
        Example plan:
        {
            "basic_statistics": True,
            "exploratory": True,
            "linear_regression": {"features": ["x1", "x2"], "target": "y"},
            "random_forest": {"features": ["x1"], "target": "y"},
            "time_series": {"time_column": "date", "target_column": "value", "freq": "M"},
            "inferential_tests": [{"x": "x1", "y": "y", "test": "pearson"}],
            "multivariate": {"features": ["x1", "x2", "x3"], "n_components": 2},
            "predictive": {"features": ["x1", "x2"], "target": "y", "model_type": "forest"}
        }
        """
        results: Dict[str, Any] = {}

        if plan.get("basic_statistics"):
            results["basic_statistics"] = self.basic_statistics(df)

        if plan.get("exploratory"):
            results["exploratory_analysis"] = self.exploratory_analysis(df)

        if plan.get("inferential_tests"):
            results["inferential_analysis"] = self.inferential_analysis(
                df, plan["inferential_tests"], plan.get("alpha", 0.05)
            )

        if plan.get("time_series"):
            ts_cfg = plan["time_series"]
            results["time_series_analysis"] = self.time_series_analysis(
                df,
                time_column=ts_cfg["time_column"],
                target_column=ts_cfg["target_column"],
                freq=ts_cfg.get("freq"),
                rolling_window=ts_cfg.get("rolling_window", 7),
            )

        if plan.get("linear_regression"):
            lr_cfg = plan["linear_regression"]
            results["linear_regression"] = self.linear_regression(
                df,
                features=lr_cfg["features"],
                target=lr_cfg["target"],
                test_size=lr_cfg.get("test_size", 0.2),
                random_state=lr_cfg.get("random_state", 42),
            )

        if plan.get("random_forest"):
            rf_cfg = plan["random_forest"]
            results["random_forest_regression"] = self.random_forest_regression(
                df,
                features=rf_cfg["features"],
                target=rf_cfg["target"],
                n_estimators=rf_cfg.get("n_estimators", 200),
                max_depth=rf_cfg.get("max_depth"),
                test_size=rf_cfg.get("test_size", 0.2),
                random_state=rf_cfg.get("random_state", 42),
            )

        if plan.get("multivariate"):
            mv_cfg = plan["multivariate"]
            results["multivariate_analysis"] = self.multivariate_analysis(
                df,
                features=mv_cfg["features"],
                n_components=mv_cfg.get("n_components", 2),
            )

        if plan.get("predictive"):
            pred_cfg = plan["predictive"]
            results["predictive_analysis"] = self.predictive_analysis(
                df,
                features=pred_cfg["features"],
                target=pred_cfg["target"],
                model_type=pred_cfg.get("model_type", "linear"),
                **{k: v for k, v in pred_cfg.items() if k not in {"features", "target", "model_type"}}
            )

        return results
