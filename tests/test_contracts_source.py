import pytest
from sources.contracts_source import Contracts2Source
from unittest.mock import MagicMock
import pandas as pd

@pytest.fixture
def mock_db_connector():
    return MagicMock()

def test_transform_contracts(mock_db_connector):
    source = Contracts2Source("dummy_path", mock_db_connector)
    input_data = [
        {
            "id": 1, 
            "publication_date": "2023-01-01", 
            "signing_date": None, 
            "amount": 100.0
        },
        {
            "id": 2, 
            "publication_date": None, 
            "signing_date": "2023-02-01", 
            "amount": float('nan')
        }
    ]
    
    # transform expects a list of dicts (or single dict) and returns a list of dicts
    result = source.transform(input_data)
    
    assert len(result) == 2
    
    # Check first item
    assert result[0]['publication_date'] == '2023-01-01'
    assert result[0]['signing_date'] is None
    assert result[0]['amount'] == 100.0
    
    # Check second item
    assert result[1]['publication_date'] is None
    assert result[1]['signing_date'] == '2023-02-01'
    assert result[1]['amount'] is None # NaN should be converted to None
