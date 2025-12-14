import pytest
from core.base_connector import BaseConnector
from connectors.census.connector import CensusConnector


class FakeAttrLookup:
    def __init__(self, mapping):
        self.mapping = mapping

    def get_descriptions(self, variable_codes):
        return {code: self.mapping[code] for code in variable_codes if code in self.mapping}


def test_census_connector_process_result_replaces_headers():
    connector = CensusConnector(
        {"source_id": "census_api", "source_name": "Census API"},
        attr_lookup=FakeAttrLookup({
            "POP": "Population",
            "NAME": "Geographic Area",
        }),
    )

    result = {
        "data": [
            {"POP": "100", "NAME": "Alabama"},
            {"POP": "200", "NAME": "Alaska"},
        ],
        "schema": {
            "fields": [
                {"name": "POP"},
                {"name": "NAME"},
            ]
        },
        "metadata": {},
    }

    processed = connector.process_result(result, parameters={})

    assert processed["data"][0]["Population"] == "100"
    assert processed["data"][0]["Geographic Area"] == "Alabama"
    assert "POP" not in processed["data"][0]
    assert processed["schema"]["fields"][0]["name"] == "Population"
    assert processed["schema"]["fields"][1]["name"] == "Geographic Area"


def test_census_connector_process_result_replaces_b22010_code():
    description = (
        "RECEIPT OF FOOD STAMPS/SNAP IN THE PAST 12 MONTHS BY DISABILITY STATUS "
        "FOR HOUSEHOLDS: Estimate!!Total:"
    )
    connector = CensusConnector(
        {"source_id": "census_api", "source_name": "Census API"},
        attr_lookup=FakeAttrLookup({
            "B22010_001E": description,
            "NAME": "Name",
        }),
    )

    result = {
        "data": [
            {"B22010_001E": "123", "NAME": "Some County"},
        ],
        "schema": {
            "fields": [
                {"name": "B22010_001E"},
                {"name": "NAME"},
            ]
        },
        "metadata": {},
    }

    processed = connector.process_result(result, parameters={"dataset": "2020/acs/acs5"})
    record = processed["data"][0]

    assert description in record
    assert record[description] == "123"
    assert "B22010_001E" not in record
    assert processed["metadata"]["column_name_overrides"]["B22010_001E"] == description
    assert processed["metadata"]["attribute_descriptions"]["B22010_001E"] == description
    assert processed["metadata"]["dataset"] == "2020/acs/acs5"
    assert any("attr_name" in note for note in processed["metadata"].get("notes", []))


class TestCensusConnectorInheritance:
    """Tests to verify CensusConnector properly inherits from BaseConnector."""

    def test_census_connector_inherits_from_base_connector(self):
        """Verify CensusConnector is a subclass of BaseConnector."""
        assert issubclass(CensusConnector, BaseConnector)

    def test_census_connector_instance_is_base_connector_instance(self):
        """Verify CensusConnector instances are also BaseConnector instances."""
        connector = CensusConnector(
            {"source_id": "test_census", "source_name": "Test Census"},
        )
        assert isinstance(connector, BaseConnector)


class TestCensusConnectorTransform:
    """Tests for CensusConnector.transform method."""

    def test_transform_returns_empty_for_insufficient_data(self):
        """Verify transform handles empty or minimal data correctly."""
        connector = CensusConnector(
            {"source_id": "test_census", "source_name": "Test Census"},
        )

        # Empty data
        result = connector.transform([])
        assert result["data"] == []
        assert result["schema"]["fields"] == []

        # Only headers, no data rows
        result = connector.transform([["NAME", "POP"]])
        assert result["data"] == []
        assert result["schema"]["fields"] == []

    def test_transform_converts_census_array_format(self):
        """Verify transform converts Census API array format to records."""
        connector = CensusConnector(
            {"source_id": "test_census", "source_name": "Test Census"},
        )

        # Census API returns data as array of arrays, first row is headers
        raw_data = [
            ["NAME", "POP", "state"],
            ["Alabama", "5024279", "01"],
            ["Alaska", "733391", "02"],
        ]

        result = connector.transform(raw_data)

        assert len(result["data"]) == 2
        assert result["data"][0]["NAME"] == "Alabama"
        assert result["data"][0]["POP"] == "5024279"
        assert result["data"][0]["state"] == "01"
        assert result["data"][1]["NAME"] == "Alaska"
        assert result["schema"]["fields"][0]["name"] == "NAME"
        assert result["schema"]["fields"][0]["type"] == "string"

    def test_transform_handles_mismatched_row_lengths(self):
        """Verify transform handles rows with fewer columns than headers."""
        connector = CensusConnector(
            {"source_id": "test_census", "source_name": "Test Census"},
        )

        raw_data = [
            ["NAME", "POP", "DENSITY"],
            ["Wyoming", "576851"],  # Missing DENSITY
        ]

        result = connector.transform(raw_data)

        assert len(result["data"]) == 1
        assert result["data"][0]["NAME"] == "Wyoming"
        assert result["data"][0]["POP"] == "576851"
        assert result["data"][0]["DENSITY"] is None


class TestCensusConnectorBaseMethodAvailability:
    """Tests to ensure all expected BaseConnector methods are available."""

    def test_census_connector_has_create_metadata_method(self):
        """Verify CensusConnector has _create_metadata from BaseConnector."""
        connector = CensusConnector(
            {"source_id": "test_census", "source_name": "Test Census"},
        )
        assert hasattr(connector, "_create_metadata")
        assert callable(getattr(connector, "_create_metadata"))

    def test_census_connector_has_compose_request_url_method(self):
        """Verify CensusConnector has _compose_request_url from BaseConnector."""
        connector = CensusConnector(
            {"source_id": "test_census", "source_name": "Test Census"},
        )
        assert hasattr(connector, "_compose_request_url")
        assert callable(getattr(connector, "_compose_request_url"))

    def test_census_connector_has_process_result_method(self):
        """Verify CensusConnector has process_result method (overridden)."""
        connector = CensusConnector(
            {"source_id": "test_census", "source_name": "Test Census"},
        )
        assert hasattr(connector, "process_result")
        assert callable(getattr(connector, "process_result"))
