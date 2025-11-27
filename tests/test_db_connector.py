import pytest
from unittest.mock import MagicMock, patch
from elt_core.db_connector import DBConnector

@patch('elt_core.db_connector.requests.Session')
def test_init_connection(mock_session_cls):
    DBConnector()
    mock_session_cls.assert_called_once()

@patch('elt_core.db_connector.requests.Session')
def test_get_or_create_db_existing(mock_session_cls):
    mock_session = mock_session_cls.return_value
    # Mock GET response for existing DB (200 OK)
    mock_session.get.return_value.status_code = 200
    
    connector = DBConnector("http://mock-url")
    db_url = connector.get_or_create_db("test_db")
    
    assert db_url == "http://mock-url/test_db"
    mock_session.get.assert_called_with("http://mock-url/test_db")
    mock_session.put.assert_not_called()

@patch('elt_core.db_connector.requests.Session')
def test_get_or_create_db_new(mock_session_cls):
    mock_session = mock_session_cls.return_value
    # Mock GET response for missing DB (404 Not Found)
    mock_session.get.return_value.status_code = 404
    # Mock PUT response for creating DB (201 Created)
    mock_session.put.return_value.status_code = 201
    
    connector = DBConnector("http://mock-url")
    db_url = connector.get_or_create_db("test_db")
    
    assert db_url == "http://mock-url/test_db"
    mock_session.get.assert_called_with("http://mock-url/test_db")
    mock_session.put.assert_called_with("http://mock-url/test_db")

@patch('elt_core.db_connector.requests.Session')
def test_save_document(mock_session_cls):
    mock_session = mock_session_cls.return_value
    # Mock get_or_create_db calls
    mock_session.get.return_value.status_code = 200
    # Mock POST response
    mock_session.post.return_value.status_code = 201
    mock_session.post.return_value.json.return_value = {"id": "1", "rev": "1-abc"}
    
    connector = DBConnector("http://mock-url")
    result = connector.save_document("test_db", {"doc": 1})
    
    assert result == {"id": "1", "rev": "1-abc"}
    # Expect data=stringified_json and headers
    mock_session.post.assert_called_with(
        "http://mock-url/test_db", 
        data='{"doc":1}', 
        headers={'Content-Type': 'application/json'}
    )
