import pytest
from sources.marketing_source import MarketingSource
from unittest.mock import MagicMock

@pytest.fixture
def mock_db_connector():
    return MagicMock()

def test_transform_list(mock_db_connector):
    source = MarketingSource("dummy_path", mock_db_connector)
    input_data = [
        {"id": 1, "full_name": "John Doe", "Email": "JOHN@EXAMPLE.COM"},
        {"id": 2, "Name": "Jane", "email": "jane@example.com"}
    ]
    
    expected = [
        {"id": 1, "name": "John Doe", "email": "john@example.com"},
        {"id": 2, "name": "Jane", "email": "jane@example.com"}
    ]
    
    result = source.transform(input_data)
    assert result == expected

def test_transform_single_dict(mock_db_connector):
    source = MarketingSource("dummy_path", mock_db_connector)
    input_data = {"id": 1, "full_name": "John Doe", "Email": "JOHN@EXAMPLE.COM"}
    
    expected = [{"id": 1, "name": "John Doe", "email": "john@example.com"}]
    
    result = source.transform(input_data)
    assert result == expected

def test_transform_preserves_extra_fields(mock_db_connector):
    source = MarketingSource("dummy_path", mock_db_connector)
    input_data = {"id": 1, "full_name": "John", "campaign": "Summer"}
    
    result = source.transform(input_data)
    assert result[0]['campaign'] == "Summer"

def test_transform_invalid_input(mock_db_connector):
    source = MarketingSource("dummy_path", mock_db_connector)
    with pytest.raises(ValueError):
        source.transform("invalid_string")
