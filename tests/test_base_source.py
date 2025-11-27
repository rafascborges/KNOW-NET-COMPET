import pytest
import json
import csv
from pathlib import Path
from unittest.mock import MagicMock, patch
from elt_core.base_source import BaseDataSource

class ConcreteSource(BaseDataSource):
    def transform(self, data):
        return data

@pytest.fixture
def mock_db_connector():
    return MagicMock()

def test_extract_json(tmp_path, mock_db_connector):
    file_path = tmp_path / "test.json"
    data = [{"key": "value"}]
    with open(file_path, "w") as f:
        json.dump(data, f)
        
    source = ConcreteSource(file_path, mock_db_connector)
    # extract() returns a generator yielding batches
    batches = list(source.extract())
    assert len(batches) == 1
    assert batches[0] == data

def test_extract_csv(tmp_path, mock_db_connector):
    file_path = tmp_path / "test.csv"
    with open(file_path, "w") as f:
        f.write("col1,col2\nval1,val2")
        
    source = ConcreteSource(file_path, mock_db_connector)
    batches = list(source.extract())
    assert len(batches) == 1
    assert batches[0] == [{"col1": "val1", "col2": "val2"}]

def test_extract_file_not_found(mock_db_connector):
    source = ConcreteSource("non_existent.json", mock_db_connector)
    with pytest.raises(FileNotFoundError):
        next(source.extract())

def test_load_bronze(mock_db_connector):
    source = ConcreteSource("dummy.json", mock_db_connector)
    data = [{"foo": "bar"}, {"baz": "qux"}]
    
    source.load_bronze(data)
    
    mock_db_connector.save_documents_bulk.assert_called_once()
    args = mock_db_connector.save_documents_bulk.call_args
    assert args[0][0] == 'concrete_bronze'
    assert len(args[0][1]) == 2
    # Should be flat structure now
    assert args[0][1][0] == {"foo": "bar"}
    assert 'ingested_at' not in args[0][1][0]

def test_load_silver(mock_db_connector):
    source = ConcreteSource("dummy.json", mock_db_connector)
    data = [{"foo": "bar"}, {"baz": "qux"}]
    
    source.load_silver(data)
    
    mock_db_connector.save_documents_bulk.assert_called_once()
    args = mock_db_connector.save_documents_bulk.call_args
    assert args[0][0] == 'concrete_silver'
    assert len(args[0][1]) == 2
    # Should be flat structure now
    assert args[0][1][0] == {"foo": "bar"}
    assert 'processed_at' not in args[0][1][0]

def test_run_staged_elt(tmp_path, mock_db_connector):
    # Setup source file
    file_path = tmp_path / "test.json"
    data = [{"key": "value"}]
    with open(file_path, "w") as f:
        json.dump(data, f)
        
    source = ConcreteSource(file_path, mock_db_connector)
    
    # Mock get_all_documents to return what we "saved" in phase 1
    # Now it returns flat docs
    mock_bronze_docs = [{"key": "value"}]
    mock_db_connector.get_all_documents.return_value = mock_bronze_docs
    
    source.run()
    
    # Verify Phase 1: Ingestion
    assert mock_db_connector.save_documents_bulk.call_count >= 2
    
    # Verify Phase 2: Transformation fetched data
    mock_db_connector.get_all_documents.assert_called_once_with("concrete_bronze")

def test_get_data(mock_db_connector):
    source = ConcreteSource("dummy.json", mock_db_connector)
    source.get_data("bronze")
    mock_db_connector.get_all_documents.assert_called_with("concrete_bronze")
    
    source.get_data("silver")
    mock_db_connector.get_all_documents.assert_called_with("concrete_silver")

def test_custom_id_column(mock_db_connector):
    source = ConcreteSource("dummy.json", mock_db_connector, id_column="my_id")
    data = [{"my_id": 123, "val": "a"}, {"my_id": 456, "val": "b"}]
    
    source.load_bronze(data)
    
    args = mock_db_connector.save_documents_bulk.call_args
    docs = args[0][1]
    assert docs[0]['_id'] == '123'
    assert docs[1]['_id'] == '456'
