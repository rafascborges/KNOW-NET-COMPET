"""
Neo4j Cypher query generation utilities.

This module provides functions to generate parameterized Cypher queries
for creating/updating nodes and relationships in Neo4j.
"""

from typing import Dict, Any, Tuple, Optional


def generate_merge_node_query(
    label: str, 
    id_field: str, 
    properties: dict
) -> Tuple[str, Dict[str, Any]]:
    """
    Generate MERGE query for creating/updating a node.
    
    Uses MERGE to avoid duplicates and SET to update all properties.
    
    Args:
        label: Node label (e.g., "Contract", "Location")
        id_field: Name of the unique identifier field (e.g., "id")
        properties: Dictionary of all node properties including the ID
    
    Returns:
        Tuple of (cypher_query, parameters_dict)
    
    Example:
        >>> query, params = generate_merge_node_query(
        ...     "Location", 
        ...     "id", 
        ...     {"id": "loc:portugal", "country": "Portugal"}
        ... )
        >>> # Returns:
        >>> # ("MERGE (n:Location {id: $id}) SET n = $props RETURN n",
        >>> #  {"id": "loc:portugal", "props": {...}})
    """
    if not properties or id_field not in properties:
        raise ValueError(f"Properties must contain the id_field '{id_field}'")
    
    id_value = properties[id_field]
    
    # Build the MERGE clause with the ID
    cypher = f"MERGE (n:{label} {{{id_field}: $id}}) SET n = $props RETURN n"
    
    # Parameters: separate ID for matching, full props for SET
    params = {
        "id": id_value,
        "props": properties
    }
    
    return cypher, params


def generate_merge_relationship_query(
    from_label: str,
    from_id: str,
    to_label: str,
    to_id: str,
    rel_type: str,
    properties: Optional[Dict[str, Any]] = None
) -> Tuple[str, Dict[str, Any]]:
    """
    Generate MERGE query for creating a relationship between two nodes.
    
    Args:
        from_label: Label of the source node
        from_id: ID value of the source node
        to_label: Label of the target node
        to_id: ID value of the target node
        rel_type: Relationship type (e.g., "EXECUTED_AT_LOCATION")
        properties: Optional dictionary of relationship properties
    
    Returns:
        Tuple of (cypher_query, parameters_dict)
    
    Example:
        >>> query, params = generate_merge_relationship_query(
        ...     "Contract", "10000001",
        ...     "Location", "loc:portugal",
        ...     "EXECUTED_AT_LOCATION"
        ... )
    """
    # Build the Cypher query
    cypher = f"""
    MATCH (from:{from_label} {{id: $from_id}})
    MATCH (to:{to_label} {{id: $to_id}})
    MERGE (from)-[r:{rel_type}]->(to)
    """
    
    params = {
        "from_id": from_id,
        "to_id": to_id
    }
    
    # Add SET clause for relationship properties if provided
    if properties:
        cypher += "SET r = $props\n"
        params["props"] = properties
    
    cypher += "RETURN r"
    
    return cypher.strip(), params


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
    
    Returns:
        List of (cypher_query, parameters_dict) tuples, one per relationship type
    
    Example:
        >>> rels = [
        ...     {
        ...         'from_label': 'Contract',
        ...         'from_id': '123',
        ...         'to_label': 'Location',
        ...         'to_id': 'loc:portugal',
        ...         'rel_type': 'EXECUTED_AT_LOCATION'
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
