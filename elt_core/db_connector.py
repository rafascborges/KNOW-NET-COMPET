import os
import requests
from requests.adapters import HTTPAdapter, Retry
import traceback
import numpy as np
import datetime
import ujson

class DBConnector:
    def __init__(self, url=None):
        # Default to localhost with admin:password. 
        # In a real scenario, use environment variables for credentials.
        self.url = url or os.getenv('COUCHDB_URL', 'http://admin:password@localhost:5984/')
        self.session = self._make_session()

    def _make_session(self, retries=5, backoff=0.5):
        """Create a configured requests session with retry/backoff."""
        session = requests.Session()
        retry = Retry(
            total=retries,
            backoff_factor=backoff,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _sanitize_for_json(self, obj):
        """
        Recursively convert numpy types to Python types for JSON serialization.
        """
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (np.integer, np.floating)):
            val = obj.item()
            if isinstance(val, float) and np.isnan(val):
                return None
            return val
        if isinstance(obj, float) and np.isnan(obj):
            return None
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        if isinstance(obj, dict):
            return {k: self._sanitize_for_json(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._sanitize_for_json(i) for i in obj]
        return obj

    def get_or_create_db(self, db_name):
        """
        Get a database if it exists, otherwise create it.
        """
        try:
            db_url = f"{self.url.rstrip('/')}/{db_name}"
            resp = self.session.get(db_url)
            
            if resp.status_code == 404:
                create_resp = self.session.put(db_url)
                create_resp.raise_for_status()
                return db_url
            
            resp.raise_for_status()
            return db_url
        except Exception as e:
            print(f"Error getting/creating database {db_name}: {e}")
            traceback.print_exc()
            raise

    def save_document(self, db_name, doc):
        """
        Save a document to the specified database.
        """
        try:
            self.get_or_create_db(db_name)
            db_url = f"{self.url.rstrip('/')}/{db_name}"
            
            clean_doc = self._sanitize_for_json(doc)
            # Use ujson for faster serialization
            headers = {'Content-Type': 'application/json'}
            resp = self.session.post(db_url, data=ujson.dumps(clean_doc), headers=headers)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Error saving document to {db_name}: {e}")
            traceback.print_exc()
            raise

    def save_documents_bulk(self, db_name, docs):
        """
        Save multiple documents using the _bulk_docs endpoint.
        """
        try:
            self.get_or_create_db(db_name)
            db_url = f"{self.url.rstrip('/')}/{db_name}/_bulk_docs"
            
            clean_docs = self._sanitize_for_json(docs)
            payload = {"docs": clean_docs}
            
            # Use ujson for faster serialization
            headers = {'Content-Type': 'application/json'}
            resp = self.session.post(db_url, data=ujson.dumps(payload), headers=headers)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Error saving bulk documents to {db_name}: {e}")
            traceback.print_exc()
            raise

    def get_all_documents(self, db_name):
        """
        Fetch all documents from the specified database.
        Returns a list of document dictionaries.
        """
        try:
            # _all_docs endpoint with include_docs=true
            db_url = f"{self.url.rstrip('/')}/{db_name}/_all_docs"
            params = {"include_docs": "true"}
            
            resp = self.session.get(db_url, params=params)
            resp.raise_for_status()
            
            # Use ujson for faster parsing
            data = ujson.loads(resp.content)
            # Extract the actual documents from the 'rows'
            # each row has {id, key, value, doc}
            docs = [row['doc'] for row in data.get('rows', []) if 'doc' in row]
            return docs
        except Exception as e:
            print(f"Error fetching all documents from {db_name}: {e}")
            traceback.print_exc()
            raise
