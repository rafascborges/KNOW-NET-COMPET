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

def test_transform_contract_type(mock_db_connector):
    source = Contracts2Source("dummy_path", mock_db_connector)
    input_data = [
        {
            "contract_type": "Aquisição de bens móveis<br/>Sociedade",
            "cpvs": "123|456"
        },
        {
            "contract_type": "Invalid Type<br/>Locação de bens móveis",
            "cpvs": "789"
        }
    ]
    
    result = source.transform(input_data)
    
    # Check first item
    # Both are allowed
    types_1 = set(result[0]['contract_type'])
    assert "Aquisição de bens móveis" in types_1
    assert "Sociedade" in types_1
    
    # Check second item
    # "Invalid Type" -> "Outros Tipos", "Locação de bens móveis" is allowed
    types_2 = set(result[1]['contract_type'])
    assert "Outros Tipos" in types_2
    assert "Locação de bens móveis" in types_2
    
    # Check CPVs
    # "123" and "456" are expected from "123|456"
    assert "123" in result[0]['cpvs']
    assert "456" in result[0]['cpvs']
    
    # "789" is expected from "789-ABC" (assuming input was changed or logic handles it)
    # Let's update the input in the test to verify the hyphen split
    
def test_transform_contract_type_with_hyphen(mock_db_connector):
    source = Contracts2Source("dummy_path", mock_db_connector)
    input_data = [
        {
            "contract_type": "Sociedade",
            "cpvs": "123-ABC|456-DEF"
        }
    ]
    
    result = source.transform(input_data)
    
    assert "123" in result[0]['cpvs']
    assert "456" in result[0]['cpvs']
    assert "ABC" not in result[0]['cpvs']
