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
        # POP should be converted to integer
        assert result["data"][0]["POP"] == 5024279
        assert isinstance(result["data"][0]["POP"], int)
        # state codes like "01" should remain strings (leading zero)
        assert result["data"][0]["state"] == "01"
        assert result["data"][1]["NAME"] == "Alaska"
        assert result["schema"]["fields"][0]["name"] == "NAME"
        assert result["schema"]["fields"][0]["type"] == "string"
        # POP field should have integer type in schema
        assert result["schema"]["fields"][1]["type"] == "integer"

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
        assert result["data"][0]["POP"] == 576851  # converted to int
        assert result["data"][0]["DENSITY"] is None

    def test_transform_keeps_zip_code_tabulation_area_as_string(self):
        """Verify zip code tabulation area values remain as strings."""
        connector = CensusConnector(
            {"source_id": "test_census", "source_name": "Test Census"},
        )

        raw_data = [
            ["NAME", "B22010_001E", "zip code tabulation area"],
            ["ZCTA5 90210", "1234", "90210"],
            ["ZCTA5 02101", "5678", "02101"],  # Leading zero preserved
        ]

        result = connector.transform(raw_data)

        assert len(result["data"]) == 2
        # B22010_001E should be converted to integer
        assert result["data"][0]["B22010_001E"] == 1234
        assert isinstance(result["data"][0]["B22010_001E"], int)
        # zip code tabulation area should remain string
        assert result["data"][0]["zip code tabulation area"] == "90210"
        assert isinstance(result["data"][0]["zip code tabulation area"], str)
        # Leading zeros preserved for ZCTA
        assert result["data"][1]["zip code tabulation area"] == "02101"
        # Schema should reflect correct types
        zcta_field = next(
            f for f in result["schema"]["fields"]
            if f["name"] == "zip code tabulation area"
        )
        assert zcta_field["type"] == "string"

    def test_transform_converts_float_values(self):
        """Verify float values are properly converted."""
        connector = CensusConnector(
            {"source_id": "test_census", "source_name": "Test Census"},
        )

        raw_data = [
            ["NAME", "MEDIAN_INCOME", "PERCENTAGE"],
            ["Test County", "75432.50", "12.5"],
        ]

        result = connector.transform(raw_data)

        assert result["data"][0]["MEDIAN_INCOME"] == 75432.50
        assert isinstance(result["data"][0]["MEDIAN_INCOME"], float)
        assert result["data"][0]["PERCENTAGE"] == 12.5
        assert isinstance(result["data"][0]["PERCENTAGE"], float)
        # Schema should show float type
        income_field = next(
            f for f in result["schema"]["fields"] if f["name"] == "MEDIAN_INCOME"
        )
        assert income_field["type"] == "float"

    def test_transform_keeps_non_numeric_strings(self):
        """Verify non-numeric string values remain as strings."""
        connector = CensusConnector(
            {"source_id": "test_census", "source_name": "Test Census"},
        )

        raw_data = [
            ["NAME", "CODE", "DESCRIPTION"],
            ["Test", "ABC123", "Some text value"],
        ]

        result = connector.transform(raw_data)

        assert result["data"][0]["CODE"] == "ABC123"
        assert isinstance(result["data"][0]["CODE"], str)
        assert result["data"][0]["DESCRIPTION"] == "Some text value"
        assert isinstance(result["data"][0]["DESCRIPTION"], str)


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
