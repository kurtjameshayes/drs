import query_census


def test_check_connector_status_ready_with_api_key(monkeypatch):
    class FakeConnectorConfig:
        def get_by_source_id(self, source_id):
            assert source_id == "census_api"
            return {"active": True, "api_key": "abc"}

    monkeypatch.setattr(query_census, "ConnectorConfig", lambda: FakeConnectorConfig())

    status, message = query_census.check_connector_status()

    assert status is True
    assert "with API key" in message


def test_check_connector_status_missing_config(monkeypatch):
    class FakeConnectorConfig:
        def get_by_source_id(self, source_id):
            return None

    monkeypatch.setattr(query_census, "ConnectorConfig", lambda: FakeConnectorConfig())

    status, message = query_census.check_connector_status()

    assert status is False
    assert "not found" in message.lower()


def test_execute_query_passes_parameters_to_engine(monkeypatch):
    class FakeEngine:
        def __init__(self):
            self.calls = []

        def execute_query(self, source_id, parameters, use_cache):
            self.calls.append((source_id, parameters, use_cache))
            return {"success": True, "data": {"metadata": {}, "data": []}}

    fake_engine = FakeEngine()
    monkeypatch.setattr(query_census, "QueryEngine", lambda: fake_engine)

    params = {"dataset": "2020/dec/pl", "get": "NAME", "for": "state:*"}

    result = query_census.execute_query(params, use_cache=False, show_details=False)

    assert fake_engine.calls == [("census_api", params, False)]
    assert result["success"] is True
