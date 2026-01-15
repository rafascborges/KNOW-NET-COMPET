# sources/graph_mappers/orbis_mapper.py
"""
Orbis mapper using declarative configuration.

Maps raw Orbis documents (person-company relationships) from orbis_gold to 
validated graph entities (nodes and relationships) for loading into Neo4j.
"""
from model import Person

from sources.graph_mappers.mapper_utils import (
    build_node_dict,
    build_relationships_one_to_one,
)


# =============================================================================
# FIELD MAPPINGS: target_field -> source_field
# =============================================================================

PERSON_FIELDS = {
    'person_name': 'name',
}


# =============================================================================
# MAIN MAPPER FUNCTION
# =============================================================================

def orbis_mapper(raw_doc: dict) -> dict:
    """
    Takes a raw Orbis document (person + company relationships)
    and returns a graph batch with validated dicts.
    
    Uses LinkML for validation - if validation passes, works with dicts directly.
    
    Input format:
        {
            "id": "C018582838",
            "name": "David Assuncao Dias",
            "associated": [{"nif": "501819398", "role": "dm"}, {"nif": "73473456", "role": "sh"}]
        }
    
    Returns:
        {
            'person': [dict],
            'relationships': [dict, ...]  # ASSOCIATED_WITH (with role property)
        }
    """
    
    # 1. CLEANING: Remove CouchDB metadata
    data = {k: v for k, v in raw_doc.items() if not k.startswith('_')}
    
    person_id = data.get('id')
    associated = data.get('associated') or []
    
    # Extract all entity IDs for LinkML validation
    entity_ids = [a['nif'] for a in associated if a.get('nif')]
    
    # -------------------------------------------------------------------------
    # STEP A: Build and Validate PERSON
    # -------------------------------------------------------------------------
    Person(
        id=person_id,
        person_name=data.get('name'),
        ASSOCIATED_WITH=entity_ids,
    )
    
    # Build person dict
    person_dict = build_node_dict(person_id, data, PERSON_FIELDS)
    
    # -------------------------------------------------------------------------
    # STEP B: Build RELATIONSHIPS with role property
    # -------------------------------------------------------------------------
    relationships = []
    
    # Person -> Entity (ASSOCIATED_WITH) with role property
    for assoc in associated:
        nif = assoc.get('nif')
        role = assoc.get('role')
        if nif and role:
            relationships.append(build_relationships_one_to_one(
                'Person', person_id, 'Entity', nif, 'ASSOCIATED_WITH',
                properties={'role': role}
            ))
    
    # -------------------------------------------------------------------------
    # RETURN
    # -------------------------------------------------------------------------
    return {
        'person': [person_dict],
        'relationships': relationships,
    }
