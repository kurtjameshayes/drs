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
