# elt_core/graph_loader.py
import logging
import os
import sys
import time
import traceback
from pathlib import Path

from neo4j import GraphDatabase
from typing import Callable, Dict, List, Any

from elt_core.neo4j_queries import generate_batch_merge_nodes_query
from elt_core.neo4j_queries import generate_batch_merge_relationships_query

# Configure logging for the pipeline
# Create logs directory
logs_dir = Path(__file__).parent.parent / 'logs'
logs_dir.mkdir(exist_ok=True)

# Generate log filename with timestamp

log_file = logs_dir / f'graph_loader.log'
#If previous log exists, delete it
if log_file.exists():
    log_file.unlink()

# Configure logging to write to both console and file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Console output
        logging.FileHandler(log_file)       # File output
    ],
    force=True  # Override any existing configuration
)

# Reduce noise from verbose libraries
logging.getLogger('neo4j').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)



class GraphLoader:
    def __init__(self, db_connector, neo4j_uri: str, neo4j_auth: tuple):
        """
        Initialize GraphLoader with Neo4j driver.
        
        Args:
            db_connector: Instance of your custom DBConnector class
            neo4j_uri: Neo4j connection URI (e.g., "bolt://localhost:7687")
            neo4j_auth: Tuple of (username, password)
        """
        self.connector = db_connector
        self.driver = GraphDatabase.driver(neo4j_uri, auth=neo4j_auth)
        self.logger = logging.getLogger("GraphLoader")
        
        # Track validation errors for review
        self.validation_errors = []

    def close(self):
        """Close the Neo4j driver connection."""
        if self.driver:
            self.driver.close()
            self.logger.info("Neo4j driver connection closed")
    
    def init_neo4j_schema(self, constraints_file: str = 'constraints.cypher'):
        """
        Initialize Neo4j schema by creating constraints and indexes.
        
        This dramatically improves relationship creation performance by enabling
        O(log n) index lookups instead of O(n) full node scans.
        
        Args:
            constraints_file: Path to Cypher file with constraint definitions
        """
        
        # Find constraints file
        if not os.path.isabs(constraints_file):
            # Look in the same directory as the module
            base_dir = Path(__file__).parent.parent
            constraints_path = base_dir / constraints_file
        else:
            constraints_path = Path(constraints_file)
        
        if not constraints_path.exists():
            self.logger.warning(f"Constraints file not found: {constraints_path}")
            self.logger.warning("Skipping schema initialization")
            return
        
        self.logger.info(f"Initializing Neo4j schema from {constraints_path}...")
        
        # Read and parse constraints file
        with open(constraints_path, 'r') as f:
            content = f.read()
        
        # Split by semicolons and filter out comments/empty lines
        statements = []
        for stmt in content.split(';'):
            # Remove comments and whitespace
            lines = [line.split('//')[0].strip() for line in stmt.split('\n')]
            clean_stmt = ' '.join(lines).strip()
            if clean_stmt and not clean_stmt.startswith('//'):
                statements.append(clean_stmt)
        
        # Execute each constraint
        constraints_created = 0
        
        with self.driver.session() as session:
            for statement in statements:
                try:
                    session.run(statement)
                    # Extract constraint name for logging
                    if 'CONSTRAINT' in statement:
                        parts = statement.split()
                        constraint_name = parts[2] if len(parts) > 2 else 'unnamed'
                        self.logger.info(f"  ✓ Created constraint: {constraint_name}")
                        constraints_created += 1
                except Exception as e:
                    error_msg = str(e)
                    if 'already exists' in error_msg.lower() or 'equivalent' in error_msg.lower():
                        
                        self.logger.debug(f"  → Constraint already exists (skipped)")
                    else:
                        self.logger.warning(f"  ✗ Failed to create constraint: {error_msg}")
        
        self.logger.info(f"Schema initialization complete: {constraints_created} created")

    def sync_gold_db(self, couch_db_name: str, doc_mapper_func: Callable, batch_size: int = 1000):
        """
        Sync a Gold CouchDB database to Neo4j with LinkML validation.
        
        Processes documents in batches for better performance and memory usage.
        
        Args:
            couch_db_name: Name of the CouchDB database to sync
            doc_mapper_func: Function that maps raw docs to graph entities
            batch_size: Number of documents to process in each batch (default: 1000)
        """
        
        # A. FETCH: Use your custom connector to get the list of dicts
        self.logger.info(f"Fetching all documents from {couch_db_name}...")
        fetch_start = time.time()
        try:
            all_docs = self.connector.get_all_documents(couch_db_name)
        except Exception as e:
            self.logger.error(f"Failed to fetch docs from {couch_db_name}: {e}")
            return
        fetch_time = time.time() - fetch_start

        self.logger.info(f"Fetched {len(all_docs)} documents in {fetch_time:.2f}s. Starting batch processing...")

        # Reset error tracking
        self.validation_errors = []
        
        # Cumulative statistics
        total_nodes_created = 0
        total_relationships_created = 0
        total_docs_processed = 0
        total_docs_failed = 0
        
        # Timing statistics
        total_validation_time = 0
        total_node_insert_time = 0
        total_rel_insert_time = 0
        
        # Process documents in batches
        total_batches = (len(all_docs) + batch_size - 1) // batch_size
        batch_start_time = time.time()
        
        for batch_idx in range(0, len(all_docs), batch_size):
            batch = all_docs[batch_idx:batch_idx + batch_size]
            current_batch_num = (batch_idx // batch_size) + 1
            
            # Accumulators for this batch
            batch_entities = {}
            batch_relationships = []
            batch_success = 0
            batch_failed = 0
            
            # Time validation/mapping
            validation_start = time.time()
            
            # Process each document in the batch
            for raw_doc in batch:
                if raw_doc.get('_id', '').startswith('_'):
                    continue
                
                try:
                    # Map the document to graph entities
                    graph_batch = doc_mapper_func(raw_doc)
                    
                    # Accumulate entities by type
                    for entity_type, items in graph_batch.items():
                        if entity_type == 'relationships':
                            # Collect relationships separately
                            batch_relationships.extend(items if isinstance(items, list) else [items])
                        elif items:
                            # Collect nodes
                            if entity_type not in batch_entities:
                                batch_entities[entity_type] = []
                            batch_entities[entity_type].extend(items if isinstance(items, list) else [items])
                    
                    batch_success += 1
                
                except Exception as e:
                    doc_id = raw_doc.get('_id', 'unknown')
                    error_msg = f"Doc {doc_id}: {type(e).__name__}: {str(e)}"
                    self.logger.error(f"Validation/mapping failed - {error_msg}")
                    self.validation_errors.append({
                        'doc_id': doc_id,
                        'error': error_msg,
                        'traceback': traceback.format_exc()
                    })
                    batch_failed += 1
            
            validation_time = time.time() - validation_start
            total_validation_time += validation_time
            
            # Insert all nodes for this batch
            node_insert_start = time.time()
            batch_nodes = self._insert_batch_nodes(batch_entities)
            node_insert_time = time.time() - node_insert_start
            total_node_insert_time += node_insert_time
            total_nodes_created += batch_nodes
            
            # Insert all relationships for this batch
            rel_insert_start = time.time()
            batch_rels = 0
            if batch_relationships:
                batch_rels = self._insert_batch_relationships(batch_relationships)
                total_relationships_created += batch_rels
            rel_insert_time = time.time() - rel_insert_start
            total_rel_insert_time += rel_insert_time
            
            # Update statistics
            total_docs_processed += batch_success
            total_docs_failed += batch_failed
            
            # Calculate rates
            elapsed = time.time() - batch_start_time
            docs_per_sec = total_docs_processed / elapsed if elapsed > 0 else 0
            eta_seconds = (len(all_docs) - total_docs_processed) / docs_per_sec if docs_per_sec > 0 else 0
            eta_mins = eta_seconds / 60
            
            # Progress log message with timing breakdown
            self.logger.info(
                f"Batch {current_batch_num}/{total_batches} | "
                f"Docs: {total_docs_processed}/{len(all_docs)} ({docs_per_sec:.1f}/s) | "
                f"Nodes: {total_nodes_created} | Rels: {total_relationships_created} | "
                f"Errors: {total_docs_failed} | "
                f"Time: V={validation_time:.1f}s N={node_insert_time:.1f}s R={rel_insert_time:.1f}s | "
                f"ETA: {eta_mins:.1f}m"
            )
        
        total_time = time.time() - batch_start_time
        
        # Summary with timing breakdown
        self.logger.info(f"Sync complete for {couch_db_name}.")
        self.logger.info(f"  Documents processed: {total_docs_processed}/{len(all_docs)}")
        self.logger.info(f"  Documents failed: {total_docs_failed}")
        self.logger.info(f"  Total nodes created: {total_nodes_created}")
        self.logger.info(f"  Total relationships created: {total_relationships_created}")
        self.logger.info(f"")
        self.logger.info(f"  Timing breakdown:")
        self.logger.info(f"    Fetch data: {fetch_time:.2f}s ({fetch_time/total_time*100:.1f}%)")
        self.logger.info(f"    Validation/mapping: {total_validation_time:.2f}s ({total_validation_time/total_time*100:.1f}%)")
        self.logger.info(f"    Node insertion: {total_node_insert_time:.2f}s ({total_node_insert_time/total_time*100:.1f}%)")
        self.logger.info(f"    Relationship insertion: {total_rel_insert_time:.2f}s ({total_rel_insert_time/total_time*100:.1f}%)")
        self.logger.info(f"    Total time: {total_time:.2f}s ({total_docs_processed/total_time:.1f} docs/s)")
        
        if self.validation_errors:
            self.logger.warning(f"Total validation errors: {len(self.validation_errors)}")
            self.logger.warning("Review errors with: loader.validation_errors")

    def _insert_batch_nodes(self, batch_entities: Dict[str, List[Dict]]) -> int:
        """
        Insert all nodes from a batch, deduplicated by ID.
        
        Args:
            batch_entities: Dict mapping entity type to list of entity dicts
            
        Returns:
            Total number of nodes created across all entity types
        """
        total_created = 0
        
        for entity_type, items in batch_entities.items():
            if not items:
                continue
            
            # Deduplicate items by ID
            seen = {}
            unique_items = []
            for item in items:
                item_id = item.get('id')
                if item_id and item_id not in seen:
                    seen[item_id] = True
                    unique_items.append(item)
            
            duplicates_removed = len(items) - len(unique_items)
            
            if not unique_items:
                continue
            
            # Get label
            # Check if first letter is already capitalized
            if entity_type[0].isupper():
                label = entity_type
            else:
                label = entity_type.capitalize()
            
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
                    total_created += created_count
                    
                    if duplicates_removed > 0:
                        self.logger.debug(
                            f"Inserted {created_count} {entity_type} "
                            f"(removed {duplicates_removed} duplicates)"
                        )
                    else:
                        self.logger.debug(f"Inserted {created_count} {entity_type}")
                        
            except Exception as e:
                self.logger.error(f"Failed to insert {entity_type} batch: {type(e).__name__}: {e}")
                self.logger.error(traceback.format_exc())
        
        return total_created
    
    def _insert_batch_relationships(self, relationships: List[Dict]) -> int:
        """
        Create relationships in Neo4j using batch queries.
        
        Args:
            relationships: List of relationship dicts with keys:
                - from_label: Source node label
                - from_id: Source node ID
                - to_label: Target node label
                - to_id: Target node ID
                - rel_type: Relationship type
                
        Returns:
            Total number of relationships created
        """
        
        if not relationships:
            return 0
        
        try:
            # Generate queries grouped by relationship type
            queries = generate_batch_merge_relationships_query(relationships)
            
            total_created = 0
            with self.driver.session() as session:
                for query, params in queries:
                    result = session.run(query, params)
                    record = result.single()
                    created_count = record['created_count'] if record else 0
                    total_created += created_count
            
            self.logger.debug(f"Created {total_created} relationships")
            return total_created
            
        except Exception as e:
            self.logger.error(f"Failed to create relationships: {type(e).__name__}: {e}")
            self.logger.error(traceback.format_exc())
            return 0

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