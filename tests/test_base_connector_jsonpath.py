"""Tests for the base connector JSONPath extraction functionality."""

import pytest
from core.base_connector import BaseConnector


class DummyConnector(BaseConnector):
    """Concrete implementation of BaseConnector for testing."""
    
    def connect(self) -> bool:
        self.connected = True
        return True
    
    def disconnect(self) -> bool:
        self.connected = False
        return True
    
    def query(self, parameters, dynamic_params=None):
        return {"success": True, "data": []}
    
    def validate(self) -> bool:
        return True
    
    def transform(self, data):
        return {"data": data, "metadata": {}}


class TestBaseConnectorJsonPath:
    """Test suite for JSONPath extraction in BaseConnector."""
    
    @pytest.fixture
    def connector(self):
        """Create a test connector instance."""
        return DummyConnector({"source_id": "test", "source_name": "Test"})
    
    def test_extract_simple_path(self, connector):
        """Test extraction with simple path like $.data."""
        data = {
            "data": [{"id": 1}, {"id": 2}],
            "metadata": {"count": 2}
        }
        
        result = connector.extract_with_jsonpath(data, "$.data")
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["id"] == 1
    
    def test_extract_nested_path(self, connector):
        """Test extraction with nested path."""
        data = {
            "response": {
                "body": {
                    "items": [{"name": "item1"}, {"name": "item2"}]
                }
            }
        }
        
        result = connector.extract_with_jsonpath(data, "$.response.body.items")
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["name"] == "item1"
    
    def test_extract_single_value(self, connector):
        """Test extraction that returns a single value."""
        data = {
            "status": "ok",
            "count": 42
        }
        
        result = connector.extract_with_jsonpath(data, "$.count")
        
        assert result == 42
    
    def test_extract_string_value(self, connector):
        """Test extraction of a string value."""
        data = {
            "message": "Hello, World!",
            "code": 200
        }
        
        result = connector.extract_with_jsonpath(data, "$.message")
        
        assert result == "Hello, World!"
    
    def test_extract_no_match_returns_original(self, connector):
        """Test that non-matching path returns original data."""
        data = {
            "results": [{"year": 2020}],
            "count": 1
        }
        
        result = connector.extract_with_jsonpath(data, "$.nonexistent")
        
        # Should return original data when no match
        assert result == data
    
    def test_extract_array_element(self, connector):
        """Test extraction of specific array element."""
        data = {
            "items": [
                {"id": 1, "name": "first"},
                {"id": 2, "name": "second"},
                {"id": 3, "name": "third"}
            ]
        }
        
        result = connector.extract_with_jsonpath(data, "$.items[0]")
        
        assert result["id"] == 1
        assert result["name"] == "first"
    
    def test_extract_all_array_elements(self, connector):
        """Test extraction of all array elements with [*]."""
        data = {
            "items": [
                {"id": 1},
                {"id": 2},
                {"id": 3}
            ]
        }
        
        result = connector.extract_with_jsonpath(data, "$.items[*]")
        
        # [*] should return all elements as a list
        assert isinstance(result, list)
        assert len(result) == 3
    
    def test_extract_deeply_nested(self, connector):
        """Test extraction from deeply nested structure."""
        data = {
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "value": "deep"
                        }
                    }
                }
            }
        }
        
        result = connector.extract_with_jsonpath(data, "$.level1.level2.level3.level4.value")
        
        assert result == "deep"
    
    def test_extract_preserves_complex_structure(self, connector):
        """Test that extraction preserves complex nested structures."""
        data = {
            "wrapper": {
                "payload": {
                    "users": [
                        {"id": 1, "profile": {"name": "Alice", "age": 30}},
                        {"id": 2, "profile": {"name": "Bob", "age": 25}}
                    ]
                }
            }
        }
        
        result = connector.extract_with_jsonpath(data, "$.wrapper.payload.users")
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["profile"]["name"] == "Alice"
        assert result[1]["profile"]["age"] == 25


class TestBaseConnectorFallbackExtraction:
    """Test suite for fallback extraction when jsonpath-ng is not available."""
    
    @pytest.fixture
    def connector(self):
        """Create a test connector instance."""
        return DummyConnector({"source_id": "test", "source_name": "Test"})
    
    def test_fallback_simple_path(self, connector):
        """Test fallback extraction with simple path."""
        data = {"data": [{"id": 1}]}
        
        result = connector._extract_with_fallback(data, "$.data")
        
        assert result == [{"id": 1}]
    
    def test_fallback_without_dollar_prefix(self, connector):
        """Test fallback extraction without $ prefix."""
        data = {"results": [{"year": 2020}]}
        
        result = connector._extract_with_fallback(data, "results")
        
        assert result == [{"year": 2020}]
    
    def test_fallback_nested_path(self, connector):
        """Test fallback extraction with nested path."""
        data = {
            "response": {
                "body": {
                    "items": ["a", "b", "c"]
                }
            }
        }
        
        result = connector._extract_with_fallback(data, "$.response.body.items")
        
        assert result == ["a", "b", "c"]
    
    def test_fallback_invalid_path_returns_original(self, connector):
        """Test that invalid path returns original data."""
        data = {"valid_key": "value"}
        
        result = connector._extract_with_fallback(data, "$.invalid_key")
        
        assert result == data
    
    def test_fallback_non_dict_returns_original(self, connector):
        """Test that non-dict data returns original."""
        data = [1, 2, 3]
        
        result = connector._extract_with_fallback(data, "$.something")
        
        assert result == [1, 2, 3]
    
    def test_fallback_array_index(self, connector):
        """Test fallback extraction with array index."""
        data = {
            "items": [
                {"id": 1},
                {"id": 2},
                {"id": 3}
            ]
        }
        
        result = connector._extract_with_fallback(data, "$.items[1]")
        
        assert result["id"] == 2
    
    def test_fallback_array_wildcard(self, connector):
        """Test fallback extraction with array wildcard [*]."""
        data = {
            "items": [
                {"id": 1},
                {"id": 2}
            ]
        }
        
        result = connector._extract_with_fallback(data, "$.items[*]")
        
        # [*] should return the entire list
        assert isinstance(result, list)
        assert len(result) == 2
