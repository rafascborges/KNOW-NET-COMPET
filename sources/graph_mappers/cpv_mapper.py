# sources/graph_mappers/cpv_mapper.py
"""
CPV mapper using declarative configuration.

Maps raw CPV documents from cpv_structure_silver to validated 
graph entities (nodes and relationships) for loading into Neo4j.

Each CPV code has a BROADER relationship to its parent CPV code.
"""
from model import CPV

from sources.graph_mappers.mapper_utils import (
    build_node_dict,
    build_relationships_one_to_one,
)


# =============================================================================
# FIELD MAPPINGS: target_field -> source_field
# =============================================================================

CPV_FIELDS = {
    'label': 'labels',
    'level': 'level',
}


# =============================================================================
# MAIN MAPPER FUNCTION
# =============================================================================

def cpv_mapper(raw_doc: dict) -> dict:
    """
    Takes a raw CPV document from cpv_structure_silver
    and returns a graph batch with validated dicts.
    
    Uses LinkML for validation - if validation passes, works with dicts directly.
    
    Example input:
        {
            "_id": "03221000",
            "code": "03221000",
            "labels": "Produtos hortícolas",
            "emoji": "🥬",
            "level": "Class",
            "parent": "03220000"
        }
    
    Returns:
        {
            'cpvs': [dict],
            'relationships': [dict, ...]  # BROADER to parent CPV
        }
    """
    
    # 1. CLEANING: Remove CouchDB metadata
    data = {k: v for k, v in raw_doc.items() if not k.startswith('_')}
    
    cpv_id = data.get('code')
    parent_id = data.get('parent')
    
    # -------------------------------------------------------------------------
    # STEP A: Build and Validate CPV
    # -------------------------------------------------------------------------
    CPV(
        id=cpv_id,
        label=data.get('labels'),
        level=data.get('level'),
        BROADER=parent_id,  # Reference to parent CPV
    )
    
    # Build CPV dict
    cpv_dict = build_node_dict(cpv_id, data, CPV_FIELDS)
    
    # -------------------------------------------------------------------------
    # STEP B: Build RELATIONSHIPS
    # -------------------------------------------------------------------------
    relationships = []
    
    # CPV -> CPV (BROADER) - link to parent if exists
    if parent_id:
        relationships.append(build_relationships_one_to_one(
            'CPV', cpv_id, 'CPV', parent_id, 'BROADER'
        ))
    
    # -------------------------------------------------------------------------
    # RETURN
    # -------------------------------------------------------------------------
    return {
        'CPV': [cpv_dict],
        'relationships': relationships,
    }
