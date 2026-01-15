# sources/graph_mappers/contract_mapper.py
"""
Contract mapper using declarative configuration.

Maps raw contract documents from CouchDB to validated graph entities
(nodes and relationships) for loading into Neo4j.
"""
from model import Tender, Contract, Location, CPV, Document

from sources.graph_mappers.mapper_utils import (
    get_location_id,
    get_document_url,
    parse_date,
    process_list_entities,
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
            'tender': [dict],
            'contract': [dict],
            'location': [dict, ...],
            'document': [dict, ...],
            'cpv': [dict, ...],
            'relationships': [dict, ...]         # Explicit relationships
        }
    """
    
    # 1. CLEANING: Remove CouchDB metadata
    data = {k: v for k, v in raw_doc.items() if not k.startswith('_')}
    shared_id = str(data.get('contract_id'))

    # -------------------------------------------------------------------------
    # STEP A: Process LOCATIONS (with BROADER hierarchy)
    # -------------------------------------------------------------------------
    # Build hierarchical location nodes and BROADER relationships
    # e.g., loc:Portugal/Lisboa/Lisboa -> BROADER -> loc:Portugal/Lisboa -> BROADER -> loc:Portugal
    
    location_dicts = []
    location_ids = []
    location_broader_rels = []
    seen_location_ids = set()
    
    for loc in (data.get('execution_location') or []):
        country = loc.get('country')
        district = loc.get('district')
        municipality = loc.get('municipality')
        
        if not country:
            continue
        
        # Build the hierarchy from most specific to least specific
        hierarchy = []
        
        # Level 1: Country only
        country_id = get_location_id(country)
        hierarchy.append({
            'id': country_id,
            'country': country,
        })
        
        # Level 2: Country + District
        if district:
            district_id = get_location_id(country, district)
            hierarchy.append({
                'id': district_id,
                'country': country,
                'district': district,
            })
        
        # Level 3: Country + District + Municipality
        if district and municipality:
            municipality_id = get_location_id(country, district, municipality)
            hierarchy.append({
                'id': municipality_id,
                'country': country,
                'district': district,
                'municipality': municipality,
            })
        
        # Add all hierarchy levels as location nodes (deduplicated)
        for loc_dict in hierarchy:
            loc_id = loc_dict['id']
            if loc_id not in seen_location_ids:
                seen_location_ids.add(loc_id)
                # Validate with LinkML
                Location(**loc_dict)
                location_dicts.append(loc_dict)
        
        # The most specific location is what we link to the contract
        most_specific_id = hierarchy[-1]['id']
        if most_specific_id not in location_ids:
            location_ids.append(most_specific_id)
        
        # Create BROADER relationships (child -> parent)
        for i in range(len(hierarchy) - 1, 0, -1):
            child_id = hierarchy[i]['id']
            parent_id = hierarchy[i - 1]['id']
            location_broader_rels.append(
                build_relationships_one_to_one('Location', child_id, 'Location', parent_id, 'BROADER')
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
    # STEP C: Process CPVs (IDs only - nodes come from cpv_mapper)
    # -------------------------------------------------------------------------
    # Only collect IDs for Contract -> CPV relationships
    # Actual CPV nodes with real labels/levels are created by cpv_mapper from cpv_structure_silver
    cpv_ids = [str(cpv) for cpv in (data.get('cpvs') or []) if cpv]
    cpv_dicts = []  # Don't create CPV nodes here

    # -------------------------------------------------------------------------
    # STEP D: Collect ENTITY IDs for relationships
    # -------------------------------------------------------------------------
    # NOTE: We only collect IDs here, NOT create Entity nodes.
    # Entity nodes with full properties are created by entities_mapper.
    # Creating {'id': vat} here would cause SET n = item to wipe existing properties.
    
    # Contracted entities (winners)
    contracted_entity_ids = [vat for vat in (data.get('contracted_vats') or []) if vat]
    
    # Contestant entities (tenderers) - deduplicate against contracted
    contracted_set = set(contracted_entity_ids)
    contestant_entity_ids = [vat for vat in (data.get('contestants_vats') or []) if vat and vat not in contracted_set]
    
    # Procuring entities (contracting agencies)
    procuring_entity_ids = [vat for vat in (data.get('contracting_agency_vats') or []) if vat]


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
    # Convert signing_date to native date
    contract_dict['signing_date'] = parse_date(data.get('signing_date'))

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
    # Convert date fields to native dates
    tender_dict['publication_date'] = parse_date(data.get('publication_date'))
    tender_dict['close_date'] = parse_date(data.get('close_date'))

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
    
    # Location -> Location (BROADER hierarchy)
    relationships.extend(location_broader_rels)
    
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
        "tender": [tender_dict],
        "contract": [contract_dict],
        "location": location_dicts,
        "document": document_dicts,
        "CPV": cpv_dicts,
        "relationships": relationships,
    }