# sources/graph_mappers/municipal_entities_mapper.py
"""
Municipal entities mapper.

Maps municipal_entities_gold documents to graph entities:
- Entity nodes for municipalities and shareholding targets
- SHAREHOLDER_OF relationships with percentage property
- ADMINISTERED_BY relationships from Location to Entity
"""
from model import Entity, Location

from sources.graph_mappers.mapper_utils import (
    get_location_id,
    build_node_dict,
    build_relationships_one_to_one,
    build_relationships_one_to_many
)
from ..lookups.districts_municipalities import MUNICIPALITY_LOOKUP


# =============================================================================
# FIELD MAPPINGS
# =============================================================================

ENTITY_FIELDS = {
    'entity_name': 'description',
    'valid_nif': 'valid_nif',
}


def parse_percentage(value: str) -> float | None:
    """
    Parse percentage string to float.
    
    Examples:
        "100%" -> 100.0
        "56%" -> 56.0
        "12.5%" -> 12.5
    """
    if not value:
        return None
    try:
        return float(value.replace('%', '').strip())
    except (ValueError, TypeError):
        return None


# =============================================================================
# MAIN MAPPER FUNCTION
# =============================================================================

def municipal_entities_mapper(raw_doc: dict) -> dict:
    """
    Maps a municipal_entities_gold document to graph entities.
    
    Creates:
    - Entity node for the municipality
    - Entity nodes for shareholding targets (minimal, just ID)
    - SHAREHOLDER_OF relationships with percentage property
    - Location node for the administered municipality
    - ADMINISTERED_BY relationship from Location to Entity
    
    Returns:
        {
            'entity': [dict, ...],
            'location': [dict, ...],
            'relationships': [dict, ...]
        }
    """
    # 1. CLEANING: Remove CouchDB metadata
    data = {k: v for k, v in raw_doc.items() if not k.startswith('_')}
    
    entity_id:str = data.get('nif')
    administrates:str = data.get('administrates')
    holds_share_of:dict = data.get('holds_share_of') or {}
    
    entity_dicts = []
    location_dicts = []
    relationships = []
    
    # -------------------------------------------------------------------------
    # STEP A: Build Municipality Entity Node
    # -------------------------------------------------------------------------
    Entity(
        id=entity_id,
        entity_name=data.get('description'),
        valid_nif=data.get('valid_nif'),
    )
    entity_dict = build_node_dict(entity_id, data, ENTITY_FIELDS)
    entity_dicts.append(entity_dict)
    
    # -------------------------------------------------------------------------
    # STEP B: Build SHAREHOLDER_OF Relationships
    # -------------------------------------------------------------------------
    for target_nif, percentage_str in holds_share_of.items():
        # NOTE: We don't create stub nodes for targets here.
        # The relationship query uses MATCH to find existing nodes.
        # Creating {'id': target_nif} would cause SET n = item to wipe existing properties.
        
        # Parse percentage
        percentage = parse_percentage(percentage_str)
        
        # Build relationship with percentage property
        # Always include properties dict (even if empty) to avoid Neo4j NO_VALUE error
        rel_properties = {'percentage': percentage} if percentage is not None else {}
        relationships.append(build_relationships_one_to_one(
            'Entity', entity_id,
            'Entity', target_nif,
            'SHAREHOLDER_OF',
            properties=rel_properties
        ))
    
    # -------------------------------------------------------------------------
    # STEP C: Build Location and ADMINISTERED_BY Relationship
    # -------------------------------------------------------------------------
    if administrates and administrates in MUNICIPALITY_LOOKUP:
        country, district = MUNICIPALITY_LOOKUP[administrates]
        location_id = get_location_id(
            country=country,
            district=district,
            municipality=administrates
        )
        
        # Validate with LinkML
        Location(
            id=location_id,
            country=country,
            district=district,
            municipality=administrates,
        )
        
        location_dicts.append({
            'id': location_id,
            'country': country,
            'district': district,
            'municipality': administrates,
        })
        
        # Location -> ADMINISTERED_BY -> Entity
        relationships.append(build_relationships_one_to_one(
            'Location', location_id,
            'Entity', entity_id,
            'ADMINISTERED_BY'
        ))
    
    # -------------------------------------------------------------------------
    # RETURN
    # -------------------------------------------------------------------------
    return {
        'entity': entity_dicts,
        'location': location_dicts,
        'relationships': relationships,
    }
