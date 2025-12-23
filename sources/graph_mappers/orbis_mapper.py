# sources/graph_mappers/orbis_mapper.py
"""
Orbis mapper using declarative configuration.

Maps raw Orbis documents (person-company relationships) from orbis_gold to 
validated graph entities (nodes and relationships) for loading into Neo4j.
"""
from model import Person

from sources.graph_mappers.mapper_utils import (
    build_node_dict,
    build_relationships_one_to_many,
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
    
    Returns:
        {
            'persons': [dict],
            'relationships': [dict, ...]  # DIRECTOR_OR_MANAGER_FOR, SHAREHOLDER_FOR
        }
    """
    
    # 1. CLEANING: Remove CouchDB metadata
    data = {k: v for k, v in raw_doc.items() if not k.startswith('_')}
    
    person_id = data.get('id')
    dm_entities = data.get('dm') or []  # Director/Manager entities
    sh_entities = data.get('sh') or []  # Shareholder entities
    
    # -------------------------------------------------------------------------
    # STEP A: Build and Validate PERSON
    # -------------------------------------------------------------------------
    Person(
        id=person_id,
        person_name=data.get('name'),
        DIRECTOR_OR_MANAGER_FOR=dm_entities,
        SHAREHOLDER_FOR=sh_entities,
    )
    
    # Build person dict
    person_dict = build_node_dict(person_id, data, PERSON_FIELDS)
    
    # -------------------------------------------------------------------------
    # STEP B: Build RELATIONSHIPS
    # -------------------------------------------------------------------------
    relationships = []
    
    # Person -> Entity (DIRECTOR_OR_MANAGER_FOR)
    relationships.extend(build_relationships_one_to_many(
        'Person', person_id, 'Entity', dm_entities, 'DIRECTOR_OR_MANAGER_FOR'
    ))
    
    # Person -> Entity (SHAREHOLDER_FOR)
    relationships.extend(build_relationships_one_to_many(
        'Person', person_id, 'Entity', sh_entities, 'SHAREHOLDER_FOR'
    ))
    
    # -------------------------------------------------------------------------
    # RETURN
    # -------------------------------------------------------------------------
    return {
        'persons': [person_dict],
        'relationships': relationships,
    }
