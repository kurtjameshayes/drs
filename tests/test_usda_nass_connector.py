import pytest

from connectors.usda_nass import connector as usda_module
from connectors.usda_nass.connector import USDANASSConnector


class DummyResponse:
    def __init__(self, json_data, status_code=200):
        self._json_data = json_data
        self.status_code = status_code

    def json(self):
        return self._json_data


def test_usda_nass_connector_requires_api_key():
    with pytest.raises(ValueError):
        USDANASSConnector({})


def test_usda_nass_connector_query_includes_credentials(monkeypatch):
    connector = USDANASSConnector({"api_key": "secret", "url": "https://example.com"})
    connector.connected = True

    captured = {}

    def fake_get(url, params, timeout):
        captured["url"] = url
        captured["params"] = params
        return DummyResponse({"data": [{"Value": "123", "state_alpha": "IA"}]})

    monkeypatch.setattr(usda_module.requests, "get", fake_get)

    result = connector.query({"state_alpha": "IA"})

    assert captured["url"] == "https://example.com/api_GET"
    assert captured["params"]["key"] == "secret"
    assert captured["params"]["format"] == connector.format
    assert result["metadata"]["record_count"] == 1
    assert result["data"][0]["Value"] == "123"
