# sources/graph_mappers/entity_mapper.py
"""
Entity mapper using declarative configuration.

Maps raw entity documents from entities_gold (scraped NIF data) to validated 
graph entities (nodes and relationships) for loading into Neo4j.
"""
from model import Entity, Location

from sources.graph_mappers.mapper_utils import (
    get_location_id,
    build_node_dict,
    build_relationships_one_to_one,
)


# =============================================================================
# FIELD MAPPINGS: target_field -> source_field
# =============================================================================

ENTITY_FIELDS = {
    'entity_name': 'description',
    'valid_nif': 'valid_nif',
}


# =============================================================================
# MAIN MAPPER FUNCTION
# =============================================================================

def entities_mapper(raw_doc: dict) -> dict:
    """
    Takes a raw entity document from entities_gold 
    and returns a graph batch with validated dicts.
    
    Uses LinkML for validation - if validation passes, works with dicts directly.
    
    Returns:
        {
            'entity': [dict],
            'location': [dict, ...],
            'relationships': [dict, ...]
        }
    """
    
    # 1. CLEANING: Remove CouchDB metadata
    data = {k: v for k, v in raw_doc.items() if not k.startswith('_')}
    
    entity_id = data.get('nif')
    
    # -------------------------------------------------------------------------
    # STEP A: Process LOCATION (only if valid_nif is True)
    # -------------------------------------------------------------------------
    location_dicts = []
    location_id = None
    
    if data.get('valid_nif') and data.get('district'):
        location_id = get_location_id(
            country='Portugal',
            district=data.get('district'),
            municipality=data.get('municipality')
        )
        
        # Validate with LinkML
        Location(
            id=location_id,
            country='Portugal',
            district=data.get('district'),
            municipality=data.get('municipality'),
        )
        
        # Build location dict (only non-None values)
        location_dict = {
            k: v for k, v in {
                'id': location_id,
                'country': 'Portugal',
                'district': data.get('district'),
                'municipality': data.get('municipality'),
            }.items() if v is not None
        }
        location_dicts.append(location_dict)
    
    # -------------------------------------------------------------------------
    # STEP B: Build and Validate ENTITY
    # -------------------------------------------------------------------------
    # Validate with LinkML
    Entity(
        id=entity_id,
        entity_name=data.get('description'),
        valid_nif=data.get('valid_nif'),
        LOCATED_AT=location_dicts[0] if location_dicts else None,
    )
    
    # Build entity dict
    entity_dict = build_node_dict(entity_id, data, ENTITY_FIELDS)
    
    # -------------------------------------------------------------------------
    # STEP C: Build RELATIONSHIPS
    # -------------------------------------------------------------------------
    relationships = []
    
    # Entity -> Location (LOCATED_AT)
    if location_id:
        relationships.append(build_relationships_one_to_one(
            'Entity', entity_id, 'Location', location_id, 'LOCATED_AT'
        ))
    
    # -------------------------------------------------------------------------
    # RETURN
    # -------------------------------------------------------------------------
    return {
        'entity': [entity_dict],
        'location': location_dicts,
        'relationships': relationships,
    }