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
    # New standardized output format: data is at result["data"] directly
    assert result["data"][0]["year"] == "2020"


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


def test_fbi_crime_connector_data_path_extracts_data(monkeypatch):
    """Test that data_path in connector config extracts data from API response using JSONPath."""
    # data_path is configured in the connector config (from MongoDB)
    connector = FBICrimeConnector({
        "api_key": "token",
        "url": "https://api.usa.gov/crime/fbi/cde",
        "data_path": "$.data"  # Configured in connector_config
    })
    connector.connected = True

    captured = {}

    def fake_execute(url, params):
        captured["url"] = url
        captured["params"] = params
        # API response with nested data structure
        return DummyResponse(200, json_data={
            "pagination": {"count": 100, "page": 1},
            "data": [
                {"year": 2020, "arrests": 1000},
                {"year": 2021, "arrests": 1100}
            ],
            "metadata": {"source": "FBI"}
        })

    monkeypatch.setattr(connector, "_execute_with_retry", fake_execute)

    # Query - data_path comes from connector config, not query params
    result = connector.query({
        "endpoint": "arrest/national/all",
        "type": "counts",
    })

    # New standardized output format: data is at result["data"] directly
    # ConnectorManager wraps with success, so connector doesn't return success key
    records = result["data"]
    assert len(records) == 2
    assert records[0]["year"] == 2020
    assert records[1]["year"] == 2021
    # Verify data_path is not sent as a query parameter
    assert "data_path" not in captured["params"]


def test_fbi_crime_connector_data_path_not_added_to_query_params(monkeypatch):
    """Test that data_path from config is not added as a URL query parameter."""
    connector = FBICrimeConnector({
        "api_key": "token",
        "url": "https://api.usa.gov/crime/fbi/cde",
        "data_path": "$.data"  # Configured in connector_config
    })
    connector.connected = True

    captured = {}

    def fake_execute(url, params):
        captured["url"] = url
        captured["params"] = params
        return DummyResponse(200, json_data={"data": []})

    monkeypatch.setattr(connector, "_execute_with_retry", fake_execute)

    connector.query({
        "endpoint": "arrest/national/all",
        "type": "counts",
        "from": "01-2023",
        "to": "12-2023",
    })

    # data_path should NOT be in query params
    assert "data_path" not in captured["params"]
    # Other params should be present
    assert captured["params"]["type"] == "counts"
    assert captured["params"]["from"] == "01-2023"
    assert captured["params"]["to"] == "12-2023"


def test_fbi_crime_connector_data_path_with_nested_path(monkeypatch):
    """Test that data_path works with nested paths like $.response.data.items."""
    connector = FBICrimeConnector({
        "api_key": "token",
        "url": "https://api.usa.gov/crime/fbi/cde",
        "data_path": "$.response.data.items"  # Configured in connector_config
    })
    connector.connected = True

    def fake_execute(url, params):
        return DummyResponse(200, json_data={
            "response": {
                "status": "ok",
                "data": {
                    "items": [
                        {"id": 1, "value": "A"},
                        {"id": 2, "value": "B"}
                    ]
                }
            }
        })

    monkeypatch.setattr(connector, "_execute_with_retry", fake_execute)

    result = connector.query({
        "endpoint": "test/endpoint",
    })

    # New standardized output format: data is at result["data"] directly
    records = result["data"]
    assert len(records) == 2
    assert records[0]["id"] == 1
    assert records[1]["value"] == "B"


def test_fbi_crime_connector_data_path_no_match_returns_original(monkeypatch):
    """Test that when data_path finds no matches, original data is returned."""
    connector = FBICrimeConnector({
        "api_key": "token",
        "url": "https://api.usa.gov/crime/fbi/cde",
        "data_path": "$.nonexistent.path"  # Configured in connector_config
    })
    connector.connected = True

    original_data = {
        "results": [{"year": 2020}],
        "count": 1
    }

    def fake_execute(url, params):
        return DummyResponse(200, json_data=original_data)

    monkeypatch.setattr(connector, "_execute_with_retry", fake_execute)

    result = connector.query({
        "endpoint": "test/endpoint",
    })

    # New standardized output format: data is at result["data"] directly
    # Since path didn't match, original data structure should be used
    # The transform method should extract 'results' key
    records = result["data"]
    assert len(records) == 1
    assert records[0]["year"] == 2020


def test_fbi_crime_connector_data_path_in_metadata(monkeypatch):
    """Test that data_path from config is included in the result metadata."""
    connector = FBICrimeConnector({
        "api_key": "token",
        "url": "https://api.usa.gov/crime/fbi/cde",
        "data_path": "$.data"  # Configured in connector_config
    })
    connector.connected = True

    def fake_execute(url, params):
        return DummyResponse(200, json_data={"data": []})

    monkeypatch.setattr(connector, "_execute_with_retry", fake_execute)

    result = connector.query({
        "endpoint": "test/endpoint",
    })

    assert result["metadata"]["data_path"] == "$.data"


def test_fbi_crime_connector_no_data_path(monkeypatch):
    """Test that queries without data_path in config work as before (backward compatibility)."""
    connector = FBICrimeConnector({
        "api_key": "token",
        "url": "https://api.usa.gov/crime/fbi/cde",
        # No data_path configured
    })
    connector.connected = True

    def fake_execute(url, params):
        return DummyResponse(200, json_data={
            "results": [{"year": 2020}, {"year": 2021}]
        })

    monkeypatch.setattr(connector, "_execute_with_retry", fake_execute)

    result = connector.query({
        "endpoint": "test/endpoint",
        "type": "counts"
    })

    # New standardized output format: data is at result["data"] directly
    # Without data_path, transform should use default 'results' extraction
    records = result["data"]
    assert len(records) == 2
    assert result["metadata"].get("data_path") is None


def test_fbi_crime_connector_tooltip_y_axis_header_reshapes_time_series(monkeypatch):
    """
    When FBI CDE responses include tooltip metadata (leftYAxisHeaders.yAxisHeaderActual),
    and data_path extracts the data element, the connector should reshape rows into:
      [{"date": <date>, "<y_axis_header_actual>": <value>}, ...]
    """
    connector = FBICrimeConnector(
        {
            "api_key": "token",
            "url": "https://api.usa.gov/crime/fbi/cde",
            "data_path": "$.data",
        }
    )
    connector.connected = True

    def fake_execute(url, params):
        return DummyResponse(
            200,
            json_data={
                "tooltip": {"leftYAxisHeaders": {"yAxisHeaderActual": "Violent Crime"}},
                "data": [
                    {"date": "2023-01", "value": 10},
                    {"date": "2023-02", "value": 12},
                ],
            },
        )

    monkeypatch.setattr(connector, "_execute_with_retry", fake_execute)

    result = connector.query({"endpoint": "some/timeseries/endpoint"})

    assert result["data"] == [
        {"date": "2023-01", "Violent Crime": 10},
        {"date": "2023-02", "Violent Crime": 12},
    ]


def test_fbi_crime_connector_reshape_parallel_arrays_with_nonstandard_key(monkeypatch):
    """
    When FBI CDE responses have parallel arrays (e.g., {"date": [...], "arrests": [...]}),
    the connector should reshape them even when the value key is not a standard name.
    """
    connector = FBICrimeConnector(
        {
            "api_key": "token",
            "url": "https://api.usa.gov/crime/fbi/cde",
            "data_path": "$.data",
        }
    )
    connector.connected = True

    def fake_execute(url, params):
        return DummyResponse(
            200,
            json_data={
                "tooltip": {"leftYAxisHeaders": {"yAxisHeaderActual": "Arrest Count"}},
                "data": {
                    "date": ["2023-01", "2023-02", "2023-03"],
                    "arrests": [100, 150, 120],
                },
            },
        )

    monkeypatch.setattr(connector, "_execute_with_retry", fake_execute)

    result = connector.query({"endpoint": "arrest/national/all"})

    assert result["data"] == [
        {"date": "2023-01", "Arrest Count": 100},
        {"date": "2023-02", "Arrest Count": 150},
        {"date": "2023-03", "Arrest Count": 120},
    ]
