# elt_core/graph_loader.py
import logging
from neo4j import GraphDatabase
from typing import Callable, Dict, List, Any
import traceback


class GraphLoader:
    def __init__(self, db_connector, neo4j_uri: str, neo4j_auth: tuple, model_module):
        """
        Initialize GraphLoader with Neo4j driver.
        
        Args:
            db_connector: Instance of your custom DBConnector class
            neo4j_uri: Neo4j connection URI (e.g., "bolt://localhost:7687")
            neo4j_auth: Tuple of (username, password)
            model_module: The imported model.py module for validation
        """
        # 1. Store the Custom Connector
        self.connector = db_connector
        
        # 2. Initialize Neo4j driver
        self.driver = GraphDatabase.driver(neo4j_uri, auth=neo4j_auth)
        
        self.model_module = model_module
        self.logger = logging.getLogger("GraphLoader")
        
        # Track validation errors for review
        self.validation_errors = []

    def close(self):
        """Close the Neo4j driver connection."""
        if self.driver:
            self.driver.close()
            self.logger.info("Neo4j driver connection closed")

    def sync_gold_db(self, couch_db_name: str, doc_mapper_func: Callable):
        """
        Sync a Gold CouchDB database to Neo4j with LinkML validation.
        
        Args:
            couch_db_name: Name of the CouchDB database to sync
            doc_mapper_func: Function that maps raw docs to graph entities
        """
        # A. FETCH: Use your custom connector to get the list of dicts
        self.logger.info(f"Fetching all documents from {couch_db_name}...")
        try:
            all_docs = self.connector.get_all_documents(couch_db_name)
        except Exception as e:
            self.logger.error(f"Failed to fetch docs from {couch_db_name}: {e}")
            return

        self.logger.info(f"Fetched {len(all_docs)} documents. Starting validation and sync...")

        # Buffers for batch inserting
        buffers = {}
        
        # Reset error tracking
        self.validation_errors = []

        # 2. Iterate through documents
        for idx, raw_doc in enumerate(all_docs):
            if raw_doc.get('_id', '').startswith('_'): 
                continue

            try:
                # --- Call the mapper function ---
                # The mapper validates and converts the raw document into a graph batch
                graph_batch = doc_mapper_func(raw_doc)

                # 3. Buffer & Insert
                for alias, items in graph_batch.items():
                    if not items: 
                        continue
                    if alias not in buffers: 
                        buffers[alias] = []
                    
                    # Ensure items is a list
                    if not isinstance(items, list):
                        items = [items]
                    
                    buffers[alias].extend(items)

                    # Flush when buffer reaches threshold
                    if len(buffers[alias]) >= 1000:
                        self._flush_buffer(alias, buffers[alias])
                        buffers[alias] = []

            except Exception as e:
                doc_id = raw_doc.get('_id', 'unknown')
                error_msg = f"Doc {doc_id}: {type(e).__name__}: {str(e)}"
                self.logger.error(f"Validation/mapping failed - {error_msg}")
                self.validation_errors.append({
                    'doc_id': doc_id,
                    'error': error_msg,
                    'traceback': traceback.format_exc()
                })

        # Final Flush
        for alias, items in buffers.items():
            if items: 
                self._flush_buffer(alias, items)
        
        # Summary
        self.logger.info(f"Sync complete for {couch_db_name}.")
        if self.validation_errors:
            self.logger.warning(f"Total validation errors: {len(self.validation_errors)}")
            self.logger.warning("Review errors with: loader.validation_errors")

    def _flush_buffer(self, alias: str, items: List[Dict]):
        """
        Execute batch insert using Cypher queries.
        
        Args:
            alias: Entity type alias (e.g., 'contracts', 'tenders')
            items: List of entity dictionaries to insert
        """
        from elt_core.neo4j_queries import generate_batch_merge_nodes_query
        
        # Deduplicate items by ID before inserting
        seen = {}
        unique_items = []
        for item in items:
            item_id = item.get('id')
            if item_id and item_id not in seen:
                seen[item_id] = True
                unique_items.append(item)
        
        duplicates_removed = len(items) - len(unique_items)
        
        if not unique_items:
            return
        
        # Map alias to Neo4j label
        label_map = {
            'tenders': 'Tender',
            'contracts': 'Contract',
            'locations': 'Location',
            'documents': 'Document',
            'cpvs': 'CPV'
        }
        
        label = label_map.get(alias, alias.capitalize())
        
        # Generate batch query
        try:
            query, params = generate_batch_merge_nodes_query(
                label=label,
                id_field='id',
                batch_items=unique_items
            )
            
            # Execute in Neo4j session
            with self.driver.session() as session:
                result = session.run(query, params)
                record = result.single()
                created_count = record['created_count'] if record else len(unique_items)
                
                if duplicates_removed > 0:
                    self.logger.info(
                        f"Flushed {created_count} {alias} items "
                        f"(removed {duplicates_removed} duplicates in buffer)"
                    )
                else:
                    self.logger.info(f"Flushed {created_count} {alias} items")
                    
        except Exception as e:
            self.logger.error(f"Failed to flush {alias} batch: {type(e).__name__}: {e}")
            self.logger.error(traceback.format_exc())

    def execute_cypher(self, query: str, parameters: Dict = None):
        """
        Execute a custom Cypher query.
        
        Args:
            query: Cypher query string
            parameters: Query parameters
            
        Returns:
            Query results
        """
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]