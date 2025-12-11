import query_nass


def test_check_connector_status_requires_api_key(monkeypatch):
    class FakeConnectorConfig:
        def get_by_source_id(self, source_id):
            return {"active": True, "api_key": ""}

    monkeypatch.setattr(query_nass, "ConnectorConfig", lambda: FakeConnectorConfig())

    status, message = query_nass.check_connector_status()

    assert status is False
    assert "no api key" in message.lower()


def test_check_connector_status_ready(monkeypatch):
    class FakeConnectorConfig:
        def get_by_source_id(self, source_id):
            return {"active": True, "api_key": "token"}

    monkeypatch.setattr(query_nass, "ConnectorConfig", lambda: FakeConnectorConfig())

    status, message = query_nass.check_connector_status()

    assert status is True
    assert "is ready" in message.lower()


def test_execute_query_invokes_query_engine(monkeypatch):
    class FakeEngine:
        def __init__(self):
            self.calls = []

        def execute_query(self, source_id, parameters, use_cache):
            self.calls.append((source_id, parameters, use_cache))
            return {"success": True, "data": {"metadata": {}, "data": []}}

    fake_engine = FakeEngine()
    monkeypatch.setattr(query_nass, "QueryEngine", lambda: fake_engine)

    params = {"commodity_desc": "CORN", "year": "2022"}

    result = query_nass.execute_query(params, use_cache=False, show_details=False)

    assert fake_engine.calls == [("usda_quickstats", params, False)]
    assert result["success"] is True
