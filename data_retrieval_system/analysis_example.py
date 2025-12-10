#!/usr/bin/env python3
"""Example: run analytics on real saved queries backed by live connectors."""
from __future__ import annotations

import argparse
from pprint import pprint
from typing import List, Sequence

from core.query_engine import QueryEngine


def build_query_specs_from_saved_queries(
    engine: QueryEngine, query_ids: Sequence[str]
) -> List[dict]:
    """Convert stored query definitions into QueryEngine-friendly specs."""

    specs: List[dict] = []
    for query_id in query_ids:
        stored_query = engine.get_stored_query(query_id)
        if not stored_query:
            raise ValueError(
                f"Stored query '{query_id}' was not found. "
                "Use manage_queries.py or the API to create it first."
            )

        spec = {
            "source_id": stored_query["connector_id"],
            "parameters": stored_query.get("parameters", {}),
            "alias": stored_query.get("alias")
            or stored_query.get("query_name")
            or query_id,
        }

        rename_columns = stored_query.get("rename_columns")
        if rename_columns:
            spec["rename_columns"] = rename_columns

        specs.append(spec)

    return specs


def build_analysis(
    query_ids: Sequence[str],
    join_on: Sequence[str],
    how: str,
    target_column: str,
    feature_columns: Sequence[str],
):
    if len(query_ids) < 2:
        raise ValueError("Provide at least two stored query IDs to build a join.")

    engine = QueryEngine()
    query_specs = build_query_specs_from_saved_queries(engine, query_ids)

    dataframe = engine.execute_queries_to_dataframe(
        queries=query_specs,
        join_on=list(join_on),
        how=how,
    )

    analysis_plan = {
        "basic_statistics": True,
        "linear_regression": {
            "features": list(feature_columns),
            "target": target_column,
        },
        "predictive": {
            "features": list(feature_columns),
            "target": target_column,
            "model_type": "forest",
            "n_estimators": 50,
        },
    }

    analysis_result = engine.analyze_queries(
        queries=query_specs,
        join_on=list(join_on),
        analysis_plan=analysis_plan,
        how=how,
    )

    return engine, query_specs, dataframe, analysis_result


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Join multiple stored queries and run the analysis engine using live APIs."
        )
    )
    parser.add_argument(
        "--query-ids",
        nargs="+",
        required=True,
        help=(
            "Stored query IDs to join. These must already exist in MongoDB. "
            "Use manage_queries.py or add_census_queries.py to create them."
        ),
    )
    parser.add_argument(
        "--join-on",
        nargs="+",
        default=["state", "year"],
        help="Columns shared across the saved queries used for the join",
    )
    parser.add_argument(
        "--how",
        default="inner",
        choices=["inner", "left", "right", "outer"],
        help="Join strategy passed to pandas.merge",
    )
    parser.add_argument(
        "--target-column",
        default="population",
        help="Column to predict in regression/predictive analyses",
    )
    parser.add_argument(
        "--feature-columns",
        nargs="+",
        default=["corn_value"],
        help="Feature columns used for modeling",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    (
        engine,
        query_specs,
        dataframe,
        analysis,
    ) = build_analysis(
        query_ids=args.query_ids,
        join_on=args.join_on,
        how=args.how,
        target_column=args.target_column,
        feature_columns=args.feature_columns,
    )

    print("Saved queries executed:")
    for idx, query_id in enumerate(args.query_ids):
        stored_query = engine.get_stored_query(query_id)
        alias = query_specs[idx]["alias"]
        connector_id = stored_query["connector_id"] if stored_query else "?"
        print(f"- {alias} ({query_id}) -> {connector_id}")

    print("\nJoined DataFrame sample:\n")
    print(dataframe.head())

    print("\nLinear Regression Summary:\n")
    pprint(analysis["analysis"].get("linear_regression"))

    print("\nPredictive Analysis Summary:\n")
    pprint(analysis["analysis"].get("predictive_analysis"))


if __name__ == "__main__":
    main()
