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
