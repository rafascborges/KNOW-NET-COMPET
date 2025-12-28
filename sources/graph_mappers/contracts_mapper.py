# sources/graph_mappers/contract_mapper.py
"""
Contract mapper using declarative configuration.

Maps raw contract documents from CouchDB to validated graph entities
(nodes and relationships) for loading into Neo4j.
"""
from model import Tender, Contract, Location, CPV, Document, Entity

from sources.graph_mappers.mapper_utils import (
    get_location_id,
    get_document_url,
    process_list_entities,
    process_simple_entities,
    build_node_dict,
    build_relationships_one_to_one,
    build_relationships_one_to_many,
    build_relationships_many_to_one,
)


# =============================================================================
# FIELD MAPPINGS: target_field -> source_field
# =============================================================================

CONTRACT_FIELDS = {
    'initial_value': 'initial_price',
    'final_value': 'final_price',
    'signing_date': 'signing_date',
    'execution_deadline': 'execution_deadline',
    'contract_type': 'contract_type',
    'causes_deadline_change': 'causes_deadline_change',
    'causes_price_change': 'causes_price_change',
}

TENDER_FIELDS = {
    'procedure_type': 'procedure_type',
    'procurement_method': 'procurement_method',
    'publication_date': 'publication_date',
    'close_date': 'close_date',
    'numberOfTenderers': 'numberOfTenderers',
    'environmental_criteria': 'environmental_criteria',
    'centralized_procedure': 'centralized_procedure',
}


# =============================================================================
# MAIN MAPPER FUNCTION
# =============================================================================

def contracts_mapper(raw_doc: dict) -> dict:
    """
    Takes a raw mixed JSON (Tender + Contract fields flat) 
    and returns a graph batch with validated dicts.
    
    Uses LinkML ONLY for validation - if validation passes, works with dicts directly.
    
    Returns:
        {
            'tenders': [dict],
            'contracts': [dict],
            'locations': [dict, ...],
            'documents': [dict, ...],
            'cpvs': [dict, ...],
            'contracted_entities': [dict, ...],  # Winners
            'contestant_entities': [dict, ...],  # Tenderers
            'procuring_entities': [dict, ...]    # Contracting agencies
            'relationships': [dict, ...]         # Explicit relationships
        }
    """
    
    # 1. CLEANING: Remove CouchDB metadata
    data = {k: v for k, v in raw_doc.items() if not k.startswith('_')}
    shared_id = str(data.get('contract_id'))

    # -------------------------------------------------------------------------
    # STEP A: Process LOCATIONS
    # -------------------------------------------------------------------------
    location_dicts, location_ids = process_list_entities(
        items=data.get('execution_location'),
        id_func=lambda loc: get_location_id(
            loc.get('country'), loc.get('district'), loc.get('municipality')
        ),
        linkml_class=Location,
        linkml_kwargs_func=lambda id, loc: {
            'id': id,
            'country': loc.get('country'),
            'district': loc.get('district'),
            'municipality': loc.get('municipality'),
        },
        dict_builder_func=lambda id, loc: {
            k: v for k, v in {
                'id': id,
                'country': loc.get('country'),
                'district': loc.get('district'),
                'municipality': loc.get('municipality'),
            }.items() if v is not None
        }
    )

    # -------------------------------------------------------------------------
    # STEP B: Process DOCUMENTS
    # -------------------------------------------------------------------------
    document_dicts, document_ids = process_list_entities(
        items=data.get('documents'),
        id_func=lambda doc: str(doc.get('id')) if doc.get('id') else None,
        linkml_class=Document,
        linkml_kwargs_func=lambda id, doc: {
            'id': id,
            'document_url': get_document_url(doc.get('id')),
            'document_description': doc.get('description'),
        },
        dict_builder_func=lambda id, doc: {
            k: v for k, v in {
                'id': id,
                'document_url': get_document_url(doc.get('id')),
                'document_description': doc.get('description'),
            }.items() if v is not None
        }
    )

    # -------------------------------------------------------------------------
    # STEP C: Process CPVs
    # -------------------------------------------------------------------------
    cpv_dicts, cpv_ids = process_list_entities(
        items=data.get('cpvs'),
        id_func=lambda cpv: str(cpv) if cpv else None,
        linkml_class=CPV,
        linkml_kwargs_func=lambda id, cpv: {
            'id': id,
            'label': f"CPV {id}",
            'level': "division",
        },
        dict_builder_func=lambda id, cpv: {
            'id': id,
            'label': f"CPV {id}",
            'level': "division",
        }
    )

    # -------------------------------------------------------------------------
    # STEP D: Process ENTITIES (separated by role)
    # -------------------------------------------------------------------------
    # Contracted entities (winners)
    contracted_entity_dicts, contracted_entity_ids = process_simple_entities(
        vats=data.get('contracted_vats'),
        linkml_class=Entity,
    )
    
    # Contestant entities (tenderers) - skip if already in contracted
    contestant_entity_dicts, contestant_entity_ids = process_simple_entities(
        vats=data.get('contestants_vats'),
        linkml_class=Entity,
        skip_if_in=set(contracted_entity_ids),
    )
    
    # Procuring entities (contracting agencies)
    procuring_entity_dicts, procuring_entity_ids = process_simple_entities(
        vats=data.get('contracting_agency_vats'),
        linkml_class=Entity,
    )

    # -------------------------------------------------------------------------
    # STEP E: Build and Validate CONTRACT
    # -------------------------------------------------------------------------
    Contract(
        id=shared_id,
        initial_value=data.get('initial_price'),
        final_value=data.get('final_price'),
        signing_date=data.get('signing_date'),
        execution_deadline=data.get('execution_deadline'),
        contract_type=data.get('contract_type'),
        causes_deadline_change=data.get('causes_deadline_change'),
        causes_price_change=data.get('causes_price_change'),
        EXECUTED_AT_LOCATION=location_dicts,
        HAS_CPV_CLASSIFICATION=cpv_ids,
        HAS_DOCUMENT=document_dicts,
    )
    contract_dict = build_node_dict(shared_id, data, CONTRACT_FIELDS)

    # -------------------------------------------------------------------------
    # STEP F: Build and Validate TENDER
    # -------------------------------------------------------------------------
    Tender(
        id=shared_id,
        procedure_type=data.get('procedure_type'),
        procurement_method=data.get('procurement_method'),
        publication_date=data.get('publication_date'),
        close_date=data.get('close_date'),
        numberOfTenderers=data.get('numberOfTenderers'),
        environmental_criteria=data.get('environmental_criteria'),
        centralized_procedure=data.get('centralized_procedure'),
        AWARDS_CONTRACT=shared_id,
    )
    tender_dict = build_node_dict(shared_id, data, TENDER_FIELDS)

    # -------------------------------------------------------------------------
    # STEP G: Build RELATIONSHIPS
    # -------------------------------------------------------------------------
    relationships = []
    
    # Tender -> Contract
    relationships.append(build_relationships_one_to_one(
        'Tender', shared_id, 'Contract', shared_id, 'AWARDS_CONTRACT'
    ))
    
    # Contract -> Locations
    relationships.extend(build_relationships_one_to_many(
        'Contract', shared_id, 'Location', location_ids, 'EXECUTED_AT_LOCATION'
    ))
    
    # Contract -> Documents
    relationships.extend(build_relationships_one_to_many(
        'Contract', shared_id, 'Document', document_ids, 'HAS_DOCUMENT'
    ))
    
    # Contract -> CPVs
    relationships.extend(build_relationships_one_to_many(
        'Contract', shared_id, 'CPV', cpv_ids, 'HAS_CLASSIFICATION'
    ))
    
    # Entity -> Tender (WON_TENDER)
    relationships.extend(build_relationships_many_to_one(
        'Entity', contracted_entity_ids, 'Tender', shared_id, 'WON_TENDER'
    ))
    
    # Entity -> Tender (IS_TENDERER_FOR)
    relationships.extend(build_relationships_many_to_one(
        'Entity', contestant_entity_ids, 'Tender', shared_id, 'IS_TENDERER_FOR'
    ))
    
    # Entity -> Tender (IS_PROCURING_ENTITY_FOR)
    relationships.extend(build_relationships_many_to_one(
        'Entity', procuring_entity_ids, 'Tender', shared_id, 'IS_PROCURING_ENTITY_FOR'
    ))
    
    # Entity -> Contract (SIGNED_CONTRACT)
    relationships.extend(build_relationships_many_to_one(
        'Entity', contracted_entity_ids, 'Contract', shared_id, 'SIGNED_CONTRACT'
    ))

    # -------------------------------------------------------------------------
    # RETURN
    # -------------------------------------------------------------------------
    return {
        "tenders": [tender_dict],
        "contracts": [contract_dict],
        "locations": location_dicts,
        "documents": document_dicts,
        "cpvs": cpv_dicts,
        "contracted_entities": contracted_entity_dicts,
        "contestant_entities": contestant_entity_dicts,
        "procuring_entities": procuring_entity_dicts,
        "relationships": relationships,
    }