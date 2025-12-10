import numpy as np
import pandas as pd
import mongomock

from models.analysis_query_result import AnalysisQueryResultStore


def _sample_dataframe(values):
    return pd.DataFrame(
        [
            {"state": state, "year": 2020, "value": amount}
            for state, amount in values
        ]
    )


def test_save_joined_results_creates_document():
    client = mongomock.MongoClient()
    store = AnalysisQueryResultStore(db_client=client)
    dataframe = _sample_dataframe([("AL", 10), ("AK", 20)])

    analysis_summary = {
        "basic_statistics": {"row_count": np.int64(2), "column_count": np.int64(3)}
    }

    stored_doc = store.save_joined_results(
        plan_id="plan-alpha",
        plan_name="Alpha Plan",
        join_columns=["state", "year"],
        join_strategy="inner",
        query_specs=[{"source_id": "census_api", "parameters": {}}],
        dataframe=dataframe,
        analysis_summary=analysis_summary,
        metadata={"description": "Test plan"},
    )

    persisted = store.collection.find_one({"plan_id": "plan-alpha"})
    assert persisted is not None
    assert persisted["record_count"] == len(dataframe)
    assert persisted["results"][0]["state"] == "AL"
    assert persisted["analysis_summary"]["basic_statistics"]["row_count"] == 2
    assert stored_doc["metadata"]["description"] == "Test plan"


def test_save_joined_results_overwrites_existing_document():
    client = mongomock.MongoClient()
    store = AnalysisQueryResultStore(db_client=client)

    first_df = _sample_dataframe([("AL", 10)])
    second_df = _sample_dataframe([("TX", 99), ("WA", 42)])

    store.save_joined_results(
        plan_id="plan-repeat",
        plan_name="Repeat Plan",
        join_columns=["state"],
        join_strategy="inner",
        query_specs=[{"source_id": "census_api", "parameters": {}}],
        dataframe=first_df,
        analysis_summary={"basic_statistics": {"row_count": 1}},
    )

    store.save_joined_results(
        plan_id="plan-repeat",
        plan_name="Repeat Plan",
        join_columns=["state"],
        join_strategy="inner",
        query_specs=[{"source_id": "usda_quickstats", "parameters": {}}],
        dataframe=second_df,
        analysis_summary=None,
        metadata={"version": 2},
    )

    persisted = store.collection.find_one({"plan_id": "plan-repeat"})
    assert store.collection.count_documents({"plan_id": "plan-repeat"}) == 1
    assert persisted["record_count"] == len(second_df)
    assert persisted["results"][0]["state"] == "TX"
    assert persisted["metadata"]["version"] == 2


def test_save_joined_results_spills_large_payload_into_chunks():
    client = mongomock.MongoClient()
    store = AnalysisQueryResultStore(
        db_client=client,
        max_document_bytes=2_000,
        chunk_target_bytes=1_000,
    )

    long_text = "x" * 400
    dataframe = pd.DataFrame(
        [{"state": f"ST{i}", "year": 2020, "value": f"{long_text}{i}"} for i in range(10)]
    )

    stored_doc = store.save_joined_results(
        plan_id="plan-large",
        plan_name="Large Plan",
        join_columns=["state"],
        join_strategy="inner",
        query_specs=[{"source_id": "census_api", "parameters": {}}],
        dataframe=dataframe,
        analysis_summary=None,
        metadata=None,
    )

    persisted = store.collection.find_one({"plan_id": "plan-large"})
    assert persisted is not None
    assert persisted["results"] is None
    assert persisted["results_stored_externally"] is True

    external_meta = persisted["results_external"]
    assert external_meta["chunk_count"] >= 2
    assert external_meta["total_records"] == len(dataframe)

    chunks = list(
        store.chunk_collection.find({"plan_id": "plan-large"}).sort("chunk_index", 1)
    )
    assert len(chunks) == external_meta["chunk_count"]
    assert sum(chunk["record_count"] for chunk in chunks) == len(dataframe)
    assert stored_doc["results"] is None
