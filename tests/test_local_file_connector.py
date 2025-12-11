from connectors.local_file.connector import LocalFileConnector


def test_local_file_connector_query_filters_and_limit(tmp_path):
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("state,value\nCA,10\nNY,5\nCA,7\n", encoding="utf-8")

    connector = LocalFileConnector({"file_path": str(csv_path)})
    assert connector.connect() is True

    result = connector.query({
        "filters": {
            "state": "CA",
            "value": {"$gt": 8},
        },
        "limit": 1,
    })

    assert result["metadata"]["record_count"] == 1
    assert result["data"][0]["state"] == "CA"
    assert result["schema"]["fields"][1]["type"] == "number"
