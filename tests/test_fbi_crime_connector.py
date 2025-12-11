from connectors.fbi_crime import connector as fbi_module
from connectors.fbi_crime.connector import FBICrimeConnector


class DummyResponse:
    def __init__(self, status_code=200, json_data=None, headers=None):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.headers = headers or {}

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if 400 <= self.status_code < 600:
            raise RuntimeError(f"HTTP {self.status_code}")


class DummySession:
    def __init__(self, responses):
        self._responses = responses
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        response = self._responses[self.calls]
        self.calls += 1
        return response


def test_fbi_crime_connector_query_builds_url_and_params(monkeypatch):
    connector = FBICrimeConnector({"api_key": "token"})
    connector.connected = True

    captured = {}

    def fake_execute(url, params):
        captured["url"] = url
        captured["params"] = params
        return DummyResponse(200, json_data={"results": [{"year": "2020"}]})

    monkeypatch.setattr(connector, "_execute_with_retry", fake_execute)

    result = connector.query({
        "endpoint": "estimates/states/CA",
        "from": "2019",
        "to": "2020",
        "per_page": 5,
    })

    assert captured["url"] == "https://api.usa.gov/crime/fbi/sapi/api/estimates/states/CA/2019/2020"
    assert captured["params"]["per_page"] == 5
    assert captured["params"]["api_key"] == "token"
    assert result["data"]["data"][0]["year"] == "2020"


def test_fbi_crime_connector_query_handles_cde_base_url(monkeypatch):
    connector = FBICrimeConnector({
        "api_key": "token",
        "url": "https://api.usa.gov/crime/fbi/cde",
    })
    connector.connected = True

    captured = {}

    def fake_execute(url, params):
        captured["url"] = url
        captured["params"] = params
        return DummyResponse(200, json_data={"results": []})

    monkeypatch.setattr(connector, "_execute_with_retry", fake_execute)

    connector.query({
        "endpoint": "arrest/national/all",
        "from": "01-2023",
        "to": "12-2023",
        "type": "counts",
    })

    assert captured["url"] == "https://api.usa.gov/crime/fbi/cde/arrest/national/all"
    assert captured["params"]["from"] == "01-2023"
    assert captured["params"]["to"] == "12-2023"
    assert captured["params"]["type"] == "counts"
    assert captured["params"]["API_KEY"] == "token"
    assert "api_key" not in captured["params"]


def test_fbi_crime_connector_execute_with_retry_handles_rate_limit(monkeypatch):
    connector = FBICrimeConnector({"api_key": "token", "retry_delay": 0})
    connector.session = DummySession([
        DummyResponse(429, headers={"Retry-After": "0"}),
        DummyResponse(200, json_data={"results": []}),
    ])

    monkeypatch.setattr(fbi_module.time, "sleep", lambda *_, **__: None)

    response = connector._execute_with_retry("https://example", {"api_key": "token"})

    assert connector.session.calls == 2
    assert response.status_code == 200
