# sources/graph_mappers/pep_mapper.py
"""
PEP (Politically Exposed Persons) mapper.

Maps pep_gold documents to graph entities:
- Person nodes for politically exposed persons
- ASSOCIATED_WITH relationships to Entity nodes with role/equity/government/parliament properties
"""
from slugify import slugify
from model import Person

from sources.graph_mappers.mapper_utils import (
    build_relationships_one_to_one,
)


# =============================================================================
# MAIN MAPPER FUNCTION
# =============================================================================

def pep_mapper(raw_doc: dict) -> dict:
    """
    Takes a raw PEP document (person + entity associations)
    and returns a graph batch with validated dicts.
    
    Input format:
        {
            "_id": "ALBERTO JORGE TORRES DA SILVA FONSECA",
            "_rev": "1-75d5d9419848e96aed1e3f3777b52191",
            "associated": [
                {"nif": "501525882", "role": "Gestor", "equity_interest": null, "government": null, "parliament": 14},
                ...
            ]
        }
    
    Returns:
        {
            'person': [dict],
            'relationships': [dict, ...]  # ASSOCIATED_WITH with properties
        }
    """
    
    # 1. CLEANING: Get data including CouchDB _id (which is the person name)
    person_name = raw_doc.get('_id')
    associated = raw_doc.get('associated') or []
    
    # Generate slugified ID from person name
    person_id = f"pep:{slugify(person_name)}"
    
    # Extract entity IDs for LinkML validation
    entity_ids = [a['nif'] for a in associated if a.get('nif')]
    
    # -------------------------------------------------------------------------
    # STEP A: Build and Validate PERSON
    # -------------------------------------------------------------------------
    Person(
        id=person_id,
        person_name=person_name,
        ASSOCIATED_WITH=entity_ids,
    )
    
    # Build person dict
    person_dict = {
        'id': person_id,
        'person_name': person_name,
        'pep': True,
    }
    
    # -------------------------------------------------------------------------
    # STEP B: Build RELATIONSHIPS with properties
    # -------------------------------------------------------------------------
    relationships = []
    
    # Person -> Entity (ASSOCIATED_WITH) with all properties
    for assoc in associated:
        nif = assoc.get('nif')
        if not nif:
            continue
        
        # Build properties dict - gold data now has plural keys with lists
        # Filter out None values since Neo4j cannot store nulls in arrays
        properties = {}
        
        roles = [v for v in assoc.get('ri_roles', []) if v is not None]
        if roles:
            properties['ri_roles'] = roles
            
        equity_interests = [v for v in assoc.get('equity_interests', []) if v is not None]
        if equity_interests:
            properties['equity_interests'] = equity_interests
            
        governments = [int(v) for v in assoc.get('governments', []) if v is not None and v != '']
        if governments:
            properties['governments'] = governments
            
        parliaments = [int(v) for v in assoc.get('parliaments', []) if v is not None and v != '']
        if parliaments:
            properties['parliaments'] = parliaments
        
        relationships.append(build_relationships_one_to_one(
            'Person', person_id, 'Entity', nif, 'ASSOCIATED_WITH',
            properties=properties if properties else None
        ))
        
    # -------------------------------------------------------------------------
    # RETURN
    # -------------------------------------------------------------------------
    return {
        'person': [person_dict],
        'relationships': relationships,
    }
