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
