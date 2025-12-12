"""
Tests for the JSONPath data_path extraction feature in connectors.
"""
import pytest
from typing import Dict, Any

from core.base_connector import BaseConnector


class ConcreteConnector(BaseConnector):
    """A concrete implementation of BaseConnector for testing."""
    
    def connect(self) -> bool:
        self.connected = True
        return True
    
    def disconnect(self) -> bool:
        self.connected = False
        return True
    
    def query(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        return {"data": []}
    
    def validate(self) -> bool:
        return True
    
    def transform(self, data: Any) -> Dict[str, Any]:
        # Use the data_path extraction
        extracted = self._extract_data_by_path(data)
        return {"data": extracted}


class TestDataPathExtraction:
    """Tests for the _extract_data_by_path method."""
    
    def test_extract_data_no_data_path_configured(self):
        """When no data_path is configured, return raw data unchanged."""
        config = {"source_id": "test", "source_name": "Test Source"}
        connector = ConcreteConnector(config)
        
        raw_data = {"results": [{"id": 1}, {"id": 2}]}
        result = connector._extract_data_by_path(raw_data)
        
        assert result is raw_data
    
    def test_extract_data_with_simple_jsonpath(self):
        """Extract data using a simple JSONPath like $.data"""
        config = {
            "source_id": "test",
            "source_name": "Test Source",
            "data_path": "$.data"
        }
        connector = ConcreteConnector(config)
        
        raw_data = {"data": [{"id": 1}, {"id": 2}], "metadata": {"count": 2}}
        result = connector._extract_data_by_path(raw_data)
        
        assert result == [{"id": 1}, {"id": 2}]
    
    def test_extract_data_with_results_path(self):
        """Extract data using $.results path."""
        config = {
            "source_id": "test",
            "source_name": "Test Source",
            "data_path": "$.results"
        }
        connector = ConcreteConnector(config)
        
        raw_data = {"results": [{"name": "a"}, {"name": "b"}], "status": "ok"}
        result = connector._extract_data_by_path(raw_data)
        
        assert result == [{"name": "a"}, {"name": "b"}]
    
    def test_extract_nested_data(self):
        """Extract data from nested structure like $.response.data"""
        config = {
            "source_id": "test",
            "source_name": "Test Source",
            "data_path": "$.response.data"
        }
        connector = ConcreteConnector(config)
        
        raw_data = {
            "response": {
                "data": [{"value": 100}, {"value": 200}],
                "page": 1
            },
            "status": "success"
        }
        result = connector._extract_data_by_path(raw_data)
        
        assert result == [{"value": 100}, {"value": 200}]
    
    def test_extract_data_path_no_match_returns_original(self):
        """When data_path doesn't match, return original data."""
        config = {
            "source_id": "test",
            "source_name": "Test Source",
            "data_path": "$.nonexistent"
        }
        connector = ConcreteConnector(config)
        
        raw_data = {"data": [{"id": 1}]}
        result = connector._extract_data_by_path(raw_data)
        
        # Should return original data when path doesn't match
        assert result is raw_data
    
    def test_extract_data_invalid_jsonpath_returns_original(self):
        """When data_path is invalid JSONPath, return original data."""
        config = {
            "source_id": "test",
            "source_name": "Test Source",
            "data_path": "$[[[invalid"
        }
        connector = ConcreteConnector(config)
        
        raw_data = {"data": [{"id": 1}]}
        result = connector._extract_data_by_path(raw_data)
        
        # Should return original data on parse error
        assert result is raw_data
    
    def test_extract_single_value(self):
        """Extract a single scalar value."""
        config = {
            "source_id": "test",
            "source_name": "Test Source",
            "data_path": "$.count"
        }
        connector = ConcreteConnector(config)
        
        raw_data = {"count": 42, "data": []}
        result = connector._extract_data_by_path(raw_data)
        
        assert result == 42
    
    def test_extract_array_elements_with_wildcard(self):
        """Extract array elements using wildcard notation."""
        config = {
            "source_id": "test",
            "source_name": "Test Source",
            "data_path": "$.items[*].value"
        }
        connector = ConcreteConnector(config)
        
        raw_data = {"items": [{"value": 1}, {"value": 2}, {"value": 3}]}
        result = connector._extract_data_by_path(raw_data)
        
        # Multiple matches should return a list
        assert result == [1, 2, 3]
    
    def test_extract_with_empty_data_path(self):
        """Empty string data_path should return original data."""
        config = {
            "source_id": "test",
            "source_name": "Test Source",
            "data_path": ""
        }
        connector = ConcreteConnector(config)
        
        raw_data = {"data": [{"id": 1}]}
        result = connector._extract_data_by_path(raw_data)
        
        assert result is raw_data
    
    def test_extract_root_path(self):
        """Using $ as data_path should return the entire structure."""
        config = {
            "source_id": "test",
            "source_name": "Test Source",
            "data_path": "$"
        }
        connector = ConcreteConnector(config)
        
        raw_data = {"data": [{"id": 1}]}
        result = connector._extract_data_by_path(raw_data)
        
        assert result == {"data": [{"id": 1}]}


class TestConnectorTransformWithDataPath:
    """Tests that connector transform methods use data_path correctly."""
    
    def test_transform_uses_data_path(self):
        """Verify that transform method uses data_path extraction."""
        config = {
            "source_id": "test",
            "source_name": "Test Source",
            "data_path": "$.results"
        }
        connector = ConcreteConnector(config)
        
        raw_data = {"results": [{"a": 1}], "meta": "info"}
        result = connector.transform(raw_data)
        
        assert result["data"] == [{"a": 1}]
