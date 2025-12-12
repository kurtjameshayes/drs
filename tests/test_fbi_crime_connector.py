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
    """Test that the connector builds URLs correctly for SAPI endpoints."""
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

    # With new dynamic param handling, from/to are added as query params
    assert captured["url"] == "https://api.usa.gov/crime/fbi/sapi/api/estimates/states/CA"
    assert captured["params"]["per_page"] == 5
    assert captured["params"]["from"] == "2019"
    assert captured["params"]["to"] == "2020"
    assert captured["params"]["api_key"] == "token"
    assert result["data"]["data"][0]["year"] == "2020"


def test_fbi_crime_connector_query_handles_cde_base_url(monkeypatch):
    """Test that the connector handles CDE-style URLs correctly."""
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


def test_fbi_crime_connector_query_handles_full_cde_endpoint(monkeypatch):
    """Test that the connector handles full CDE URLs as endpoint."""
    connector = FBICrimeConnector({"api_key": "token"})
    connector.connected = True

    captured = {}

    def fake_execute(url, params):
        captured["url"] = url
        captured["params"] = params
        return DummyResponse(200, json_data={"results": []})

    monkeypatch.setattr(connector, "_execute_with_retry", fake_execute)

    connector.query({
        "endpoint": "https://api.usa.gov/crime/fbi/cde/arrest/national/all",
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


def test_fbi_crime_connector_dynamic_params(monkeypatch):
    """Test that the connector substitutes dynamic placeholders correctly."""
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

    # Query with dynamic placeholders
    connector.query(
        parameters={
            "endpoint": "arrest/national/all",
            "type": "counts",
            "from": "{from mm-yyyy}",
            "to": "{to mm-yyyy}",
        },
        dynamic_params={
            "from": "01-2023",
            "to": "12-2023",
        }
    )

    assert captured["url"] == "https://api.usa.gov/crime/fbi/cde/arrest/national/all"
    assert captured["params"]["from"] == "01-2023"
    assert captured["params"]["to"] == "12-2023"
    assert captured["params"]["type"] == "counts"
    assert captured["params"]["API_KEY"] == "token"


def test_fbi_crime_connector_dynamic_params_with_different_placeholder_names(monkeypatch):
    """Test that dynamic placeholders with descriptive names work correctly."""
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

    # Query with dynamic placeholders - using descriptive format hints
    connector.query(
        parameters={
            "endpoint": "arrest/national/all",
            "type": "{type arrests|offenses}",
            "from": "{from}",
            "to": "{to}",
        },
        dynamic_params={
            "from": "01-2020",
            "to": "12-2020",
            "type": "arrests",
        }
    )

    assert captured["params"]["from"] == "01-2020"
    assert captured["params"]["to"] == "12-2020"
    assert captured["params"]["type"] == "arrests"


def test_fbi_crime_connector_mixed_params(monkeypatch):
    """Test that the connector handles a mix of hard-coded and dynamic parameters."""
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

    # Mix of hard-coded and dynamic parameters
    connector.query(
        parameters={
            "endpoint": "arrest/national/all",
            "type": "counts",  # hard-coded
            "variable": "Total",  # hard-coded
            "from": "{from mm-yyyy}",  # dynamic
            "to": "{to mm-yyyy}",  # dynamic
        },
        dynamic_params={
            "from": "06-2022",
            "to": "06-2023",
        }
    )

    assert captured["params"]["type"] == "counts"
    assert captured["params"]["variable"] == "Total"
    assert captured["params"]["from"] == "06-2022"
    assert captured["params"]["to"] == "06-2023"


def test_fbi_crime_connector_is_dynamic_placeholder():
    """Test the _is_dynamic_placeholder method."""
    connector = FBICrimeConnector({"api_key": "token"})
    
    # Valid placeholders
    assert connector._is_dynamic_placeholder("{from}") is True
    assert connector._is_dynamic_placeholder("{from mm-yyyy}") is True
    assert connector._is_dynamic_placeholder("{to}") is True
    assert connector._is_dynamic_placeholder("{state_code}") is True
    assert connector._is_dynamic_placeholder("  {from}  ") is True
    
    # Not placeholders
    assert connector._is_dynamic_placeholder("2023") is False
    assert connector._is_dynamic_placeholder("counts") is False
    assert connector._is_dynamic_placeholder("") is False
    assert connector._is_dynamic_placeholder(None) is False
    assert connector._is_dynamic_placeholder(123) is False
    assert connector._is_dynamic_placeholder("{partial") is False


def test_fbi_crime_connector_extract_param_name():
    """Test the _extract_param_name_from_placeholder method."""
    connector = FBICrimeConnector({"api_key": "token"})
    
    assert connector._extract_param_name_from_placeholder("{from}") == "from"
    assert connector._extract_param_name_from_placeholder("{from mm-yyyy}") == "from"
    assert connector._extract_param_name_from_placeholder("{to}") == "to"
    assert connector._extract_param_name_from_placeholder("{state_code}") == "state_code"
    assert connector._extract_param_name_from_placeholder("{type arrests|offenses}") == "type"


def test_fbi_crime_connector_execute_with_retry_handles_rate_limit(monkeypatch):
    """Test that retry logic handles rate limiting correctly."""
    connector = FBICrimeConnector({"api_key": "token", "retry_delay": 0})
    connector.session = DummySession([
        DummyResponse(429, headers={"Retry-After": "0"}),
        DummyResponse(200, json_data={"results": []}),
    ])

    monkeypatch.setattr(fbi_module.time, "sleep", lambda *_, **__: None)

    response = connector._execute_with_retry("https://example", {"api_key": "token"})

    assert connector.session.calls == 2
    assert response.status_code == 200
