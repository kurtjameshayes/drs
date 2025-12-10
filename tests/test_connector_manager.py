from typing import Dict, Any

from core.base_connector import BaseConnector
from core.connector_manager import ConnectorManager


class DummyConnector(BaseConnector):
    def __init__(self):
        super().__init__({
            "source_id": "dummy",
            "source_name": "Dummy Source",
        })
        self.connected = True

    def connect(self) -> bool:
        self.connected = True
        return True

    def disconnect(self) -> bool:
        self.connected = False
        return True

    def validate(self) -> bool:
        return True

    def query(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "data": [{"original": 1}],
            "metadata": {},
            "schema": {"fields": [{"name": "original"}]},
        }

    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return data

    def process_result(self, result: Dict[str, Any], parameters: Dict[str, Any]) -> Dict[str, Any]:
        result["processed"] = True
        return result


def test_connector_manager_applies_process_result():
    manager = ConnectorManager()
    manager.connectors["dummy"] = DummyConnector()

    response = manager.query("dummy", {})

    assert response["success"] is True
    assert response["data"]["processed"] is True
