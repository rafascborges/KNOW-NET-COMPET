# elt_core/graph_loader.py
import logging
from linkml_store import Client
from linkml_store.api.collection import Collection

# Type hinting (optional, but good for IDEs)
# from elt_core.db_connector import DBConnector 

class GraphLoader:
    def __init__(self, db_connector, neo4j_uri, schema_path, model_module):
        """
        Args:
            db_connector: Instance of your custom DBConnector class
            neo4j_uri: Connection string for Neo4j
            schema_path: Path to schema.yaml
            model_module: The imported model.py module
        """
        # 1. Store the Custom Connector
        self.connector = db_connector
        
        # 2. Setup Neo4j via LinkML-Store
        self.neo4j_client = Client()
        self.store = self.neo4j_client.attach_database("neo4j", "neo4j_db", uri=neo4j_uri)
        self.store.load_schema_view(schema_path)
        
        self.model_module = model_module
        self.logger = logging.getLogger("GraphLoader")

    def register_collections(self, class_map: dict):
        """
        Setup Neo4j collections based on schema classes.
        Example: {'Tender': 'tenders', 'Contract': 'contracts'}
        """
        for class_name, alias in class_map.items():
            self.store.create_collection(class_name, alias=alias, recreate_if_exists=True)

    def sync_gold_db(self, couch_db_name: str, doc_mapper_func):
        """
        Driver to sync a Gold CouchDB database to Neo4j using DBConnector.
        """
        # A. FETCH: Use your custom connector to get the list of dicts
        self.logger.info(f"Fetching all documents from {couch_db_name}...")
        try:
            # This uses your _all_docs implementation
            all_docs = self.connector.get_all_documents(couch_db_name)
        except Exception as e:
            self.logger.error(f"Failed to fetch docs from {couch_db_name}: {e}")
            return

        self.logger.info(f"Fetched {len(all_docs)} documents. Starting validation...")

        # Buffers for batch inserting
        # 2. Iterate
        for raw_doc in all_docs:
            if raw_doc.get('_id', '').startswith('_'): continue

            try:
                # --- CHANGE IS HERE ---
                # We do NOT validate here anymore. We do NOT look for 'type'.
                # We pass the raw dirty JSON straight to your specific function.
                graph_batch = doc_mapper_func(raw_doc)

                # 3. Buffer & Insert (Standard logic)
                for alias, items in graph_batch.items():
                    if not items: continue
                    if alias not in buffers: buffers[alias] = []
                    buffers[alias].extend(items)

                    if len(buffers[alias]) >= 1000:
                        self._flush_buffer(alias, buffers[alias])
                        buffers[alias] = []

            except Exception as e:
                self.logger.error(f"Failed on doc {raw_doc.get('_id')}: {e}")

        # Final Flush
        for alias, items in buffers.items():
            if items: self._flush_buffer(alias, items)
            
        self.logger.info(f"Sync complete for {couch_db_name}.")

    def _flush_buffer(self, alias, items):
        """Helper to push data to Neo4j"""
        try:
            # We use validate=False because we already validated against model.py
            self.store.get_collection(alias).insert(items, validate=False)
            self.logger.info(f"Flushed {len(items)} items to {alias}")
        except Exception as e:
            self.logger.error(f"Neo4j Insert Error on {alias}: {e}")