"""
Neo4j Cypher query generation utilities.

This module provides functions to generate parameterized Cypher queries
for creating/updating nodes and relationships in Neo4j.
"""

from typing import Dict, Any, Tuple, Optional

def generate_batch_merge_nodes_query(
    label: str,
    id_field: str,
    batch_items: list
) -> Tuple[str, Dict[str, Any]]:
    """
    Generate a batched MERGE query for multiple nodes of the same type.
    
    Uses UNWIND for efficient batch processing.
    
    Args:
        label: Node label
        id_field: Name of the unique identifier field
        batch_items: List of property dictionaries
    
    Returns:
        Tuple of (cypher_query, parameters_dict)
    """
    cypher = f"""
    UNWIND $batch AS item
    MERGE (n:{label} {{{id_field}: item.{id_field}}})
    SET n = item
    RETURN count(n) as created_count
    """
    
    params = {"batch": batch_items}
    
    return cypher.strip(), params


def generate_batch_merge_relationships_query(
    relationships: list
) -> list:
    """
    Generate batched MERGE queries for multiple relationships, grouped by type.
    
    Since Cypher doesn't support dynamic relationship types in a single query,
    we group relationships by type and return multiple queries.
    
    Args:
        relationships: List of relationship dicts with keys:
            - from_label: Source node label
            - from_id: Source node ID
            - to_label: Target node label
            - to_id: Target node ID
            - rel_type: Relationship type
            - properties: (optional) Dict of relationship properties
    
    Returns:
        List of (cypher_query, parameters_dict) tuples, one per relationship type
    
    Example:
        >>> rels = [
        ...     {
        ...         'from_label': 'Contract',
        ...         'from_id': '123',
        ...         'to_label': 'Location',
        ...         'to_id': 'loc:portugal',
        ...         'rel_type': 'EXECUTED_AT_LOCATION',
        ...         'properties': {'role': 'dm'}  # optional
        ...     },
        ...     ...
        ... ]
        >>> queries = generate_batch_merge_relationships_query(rels)
        >>> # Returns list of (query, params) tuples
    """
    # Group relationships by type
    grouped = {}
    for rel in relationships:
        rel_type = rel['rel_type']
        if rel_type not in grouped:
            grouped[rel_type] = []
        grouped[rel_type].append(rel)
    
    # Generate a query for each relationship type
    queries = []
    for rel_type, rels in grouped.items():
        # Check if any relationship in this group has properties
        has_properties = any('properties' in r and r['properties'] for r in rels)
        
        if has_properties:
            # Ensure all items have a 'properties' key to avoid Neo4j NO_VALUE error
            for r in rels:
                if 'properties' not in r:
                    r['properties'] = {}
            # Use SET to add properties to the relationship
            cypher = f"""
            UNWIND $batch AS item
            MATCH (from:{rels[0]['from_label']} {{id: item.from_id}})
            MATCH (to:{rels[0]['to_label']} {{id: item.to_id}})
            MERGE (from)-[r:{rel_type}]->(to)
            SET r += item.properties
            RETURN count(r) as created_count
            """
        else:
            cypher = f"""
            UNWIND $batch AS item
            MATCH (from:{rels[0]['from_label']} {{id: item.from_id}})
            MATCH (to:{rels[0]['to_label']} {{id: item.to_id}})
            MERGE (from)-[r:{rel_type}]->(to)
            RETURN count(r) as created_count
            """
        
        params = {"batch": rels}
        queries.append((cypher.strip(), params))
    
    return queries
