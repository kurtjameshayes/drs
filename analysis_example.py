#!/usr/bin/env python3
"""Example: run analytics on real saved queries backed by live connectors."""
from __future__ import annotations

import argparse
from pprint import pprint
from typing import Any, Dict, List, Sequence

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from config import Config
from core.query_engine import QueryEngine


def build_query_specs_from_saved_queries(
    engine: QueryEngine, plan_queries: Sequence[Dict[str, Any]]
) -> List[dict]:
    """Convert stored query definitions plus plan overrides into specs."""

    specs: List[dict] = []
    for plan_query in plan_queries:
        query_id = plan_query.get("query_id") or plan_query.get("id")
        if not query_id:
            raise ValueError("Each plan query must include a 'query_id' field.")

        stored_query = engine.get_stored_query(query_id)
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

        if isinstance(plan_query.get("parameters"), dict):
            spec["parameters"].update(plan_query["parameters"])

        rename_columns: Dict[str, Any] = {}
        if isinstance(stored_query.get("rename_columns"), dict):
            rename_columns.update(stored_query["rename_columns"])
        if isinstance(plan_query.get("rename_columns"), dict):
            rename_columns.update(plan_query["rename_columns"])
        if rename_columns:
            spec["rename_columns"] = rename_columns

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


def load_analysis_plan_document(
    plan_id: str, collection_name: str = "analysis_plans"
) -> Dict[str, Any]:
    """Fetch an analysis plan document from MongoDB."""

    try:
        with MongoClient(Config.MONGO_URI) as client:
            collection = client[Config.DATABASE_NAME][collection_name]
            plan_doc = (
                collection.find_one({"plan_id": plan_id})
                or collection.find_one({"_id": plan_id})
                or collection.find_one({"name": plan_id})
            )
    except PyMongoError as exc:
        raise RuntimeError(
            f"Failed to load analysis plan '{plan_id}' from MongoDB: {exc}"
        ) from exc

    if not plan_doc:
        raise ValueError(
            f"Analysis plan '{plan_id}' not found in collection '{collection_name}'."
        )

    if "_id" in plan_doc:
        plan_doc["_id"] = str(plan_doc["_id"])

    return plan_doc


def _plan_identifier(plan_doc: Dict[str, Any], fallback: str | None = None) -> str:
    return str(
        plan_doc.get("plan_id")
        or plan_doc.get("name")
        or plan_doc.get("_id")
        or fallback
        or "unknown"
    )


def extract_analysis_plan_definition(plan_doc: Dict[str, Any]) -> Dict[str, Any]:
    """Extract the nested analysis plan payload from a document."""

    plan_payload: Dict[str, Any] | None = None
    for key in ("plan", "analysis_plan", "definition"):
        candidate = plan_doc.get(key)
        if isinstance(candidate, dict):
            plan_payload = candidate
            break

    if plan_payload is None:
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
            "how",
        }
        plan_payload = {
            key: value for key, value in plan_doc.items() if key not in metadata_keys
        }

    if not plan_payload:
        raise ValueError(
            f"Analysis plan document '{_plan_identifier(plan_doc)}' "
            "does not contain a valid plan definition."
        )

    return plan_payload


def _as_string_list(value: Any) -> List[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else None
    if isinstance(value, Sequence):
        items = []
        for entry in value:
            if isinstance(entry, str):
                stripped = entry.strip()
                if stripped:
                    items.append(stripped)
        return items or None
    return None


def extract_plan_queries(plan_doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    queries = plan_doc.get("queries")
    if not queries and plan_doc.get("query_ids"):
        queries = [{"query_id": query_id} for query_id in plan_doc["query_ids"]]

    if not queries:
        raise ValueError(
            f"Analysis plan '{_plan_identifier(plan_doc)}' must include a 'queries' list."
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
    plan_doc: Dict[str, Any], plan_queries: Sequence[Dict[str, Any]]
) -> List[str]:
    plan_level_join = _as_string_list(
        plan_doc.get("join_on")
        or plan_doc.get("join_columns")
        or plan_doc.get("join_column")
        or plan_doc.get("join_keys")
    )
    if plan_level_join:
        return plan_level_join

    per_query_joins: List[List[str]] = []
    for query in plan_queries:
        query_join = _as_string_list(
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


def extract_join_strategy(plan_doc: Dict[str, Any]) -> str:
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


def build_analysis(
    plan_queries: Sequence[Dict[str, Any]],
    join_on: Sequence[str],
    how: str,
    analysis_plan: Dict[str, Any],
):
    if len(plan_queries) < 2:
        raise ValueError("Provide at least two stored query IDs to build a join.")

    engine = QueryEngine()
    query_specs = build_query_specs_from_saved_queries(engine, plan_queries)

    dataframe = engine.execute_queries_to_dataframe(
        queries=query_specs,
        join_on=list(join_on),
        how=how,
    )

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
            "Join stored queries defined by an analysis plan and run analytics."
        )
    )
    parser.add_argument(
        "--analysis-plan-id",
        required=True,
        help="Identifier of the analysis plan stored in MongoDB",
    )
    parser.add_argument(
        "--analysis-plan-collection",
        default="analysis_plans",
        help="Optional override for the MongoDB collection storing analysis plans",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        plan_document = load_analysis_plan_document(
            plan_id=args.analysis_plan_id,
            collection_name=args.analysis_plan_collection,
        )
        plan_queries = extract_plan_queries(plan_document)
        join_on = extract_join_columns(plan_document, plan_queries)
        join_strategy = extract_join_strategy(plan_document)
        analysis_plan = extract_analysis_plan_definition(plan_document)
    except (ValueError, RuntimeError) as exc:
        raise SystemExit(str(exc))

    plan_label = (
        plan_document.get("plan_name")
        or plan_document.get("name")
        or args.analysis_plan_id
    )

    (
        engine,
        query_specs,
        dataframe,
        analysis,
    ) = build_analysis(
        plan_queries=plan_queries,
        join_on=join_on,
        how=join_strategy,
        analysis_plan=analysis_plan,
    )

    print(
        f"Analysis plan '{plan_label}' ({args.analysis_plan_id})\n"
        f"- Join columns: {', '.join(join_on)}\n"
        f"- Join strategy: {join_strategy}"
    )

    print("\nSaved queries executed:")
    for idx, plan_query in enumerate(plan_queries):
        query_id = plan_query["query_id"]
        alias = query_specs[idx]["alias"]
        connector_id = query_specs[idx]["source_id"]
        query_join = _as_string_list(
            plan_query.get("join_on")
            or plan_query.get("join_columns")
            or plan_query.get("join_column")
        )
        join_suffix = f" (join on {', '.join(query_join)})" if query_join else ""
        print(f"- {alias} ({query_id}) -> {connector_id}{join_suffix}")

    print("\nJoined DataFrame sample:\n")
    print(dataframe.head())

    print("\nLinear Regression Summary:\n")
    pprint(analysis["analysis"].get("linear_regression"))

    print("\nPredictive Analysis Summary:\n")
    pprint(analysis["analysis"].get("predictive_analysis"))


if __name__ == "__main__":
    main()
