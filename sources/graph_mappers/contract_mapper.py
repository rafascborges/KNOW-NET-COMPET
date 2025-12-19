# sources/graph_mappers.py
from model import Tender, Contract, Location, CPV, Document
from slugify import slugify

def get_location_id(country, district=None, municipality=None):
    # 1. Start with the country (Required)
    parts = [slugify(country)]
    
    # 2. Add District if it exists
    if district:
        parts.append(slugify(district))
        
    # 3. Add Municipality if it exists
    if municipality:
        parts.append(slugify(municipality))
        
    # 4. Join them with hyphens
    # Result examples: "loc:portugal", "loc:portugal-lisboa", "loc:portugal-lisboa-cascais"
    return f"loc:{'-'.join(parts)}"

def get_document_url(document_id):
    return f"schema:https://www.base.gov.pt/Base4/pt/resultados/?type=doc_documentos&id={document_id}&ext=.pdf"

def extract_nested_objects(parent_dict: dict, field_name: str, id_attr: str) -> list:
    """
    Extracts nested objects from a dictionary field, replaces them with IDs,
    and returns the list of extracted object dicts.
    """
    extracted_dicts = []
    if parent_dict.get(field_name):
        objs = parent_dict[field_name]
        if not isinstance(objs, list):
            objs = [objs]
        
        ids = []
        for obj in objs:
            extracted_dicts.append(obj.__dict__)
            ids.append(getattr(obj, id_attr))
        
        parent_dict[field_name] = ids
    return extracted_dicts

def procurement_mapper(raw_doc: dict):
    """
    Takes a raw mixed JSON (Tender + Contract fields flat) 
    and returns a graph batch {'tenders': [], 'contracts': []}.
    """
    
    # 1. CLEANING: Remove CouchDB metadata
    # We keep 'contract_id' because your JSON uses it as the main key
    data = {k: v for k, v in raw_doc.items() if not k.startswith('_')}
    
    # Shared ID for both entities
    shared_id = str(data.get('contract_id'))

    # --- STEP A: Build the CONTRACT (Inner Object) ---
    # Extract only the fields that belong to Contract
    
    # Handle Locations (List of dicts -> List of Location Objects)
    locs = []
    if data.get('execution_location'):
        for i, loc in enumerate(data['execution_location']):
            locs.append({
                "address_id": get_location_id(loc.get('country'), loc.get('district'), loc.get('municipality')),
                "country": loc.get('country'),
                "district": loc.get('district'),
                "municipality": loc.get('municipality')
            })

    documents = []
    if data.get('documents'):
        for i, doc in enumerate(data['documents']):
            documents.append({
                "document_id": doc.get('document_id'),
                "document_url": get_document_url(doc.get('document_id')),
                "document_type": doc.get('document_type')
            })

    contract_data = {
        "contract_id": shared_id,
        "initial_value": data.get('initial_price'), # Remapped
        "final_value": data.get('final_price'),
        "signing_date": data.get('signing_date'),
        "execution_deadline": data.get('execution_deadline'),
        "contract_type": data.get('contract_type'),
        "executedAtLocation": locs,
        "causes_deadline_change": data.get('causes_deadline_change'),
        "causes_price_change": data.get('causes_price_change'),
        "cpvs": data.get('cpvs'),
        "hasDocuments": documents
    }

    tender_data = {
        "tender_id": shared_id,
        "procedure_type": data.get('procedure_type'),
        "procurement_method": data.get('procurement_method'),
        "publication_date": data.get('publication_date'),
        "close_date": data.get('close_date'),
        "numberOfTenderers": data.get('numberOfTenderers'),
        "environmental_criteria": data.get('environmental_criteria'),
        "centralized_procedure": data.get('centralized_procedure'),
        "awardsContract": shared_id 
    }   
    
    # 2. VALIDATE CONTRACT
    # We instantiate it to ensure data is correct
    contract_obj = Contract(**contract_data)

    # 3. VALIDATE TENDER (and implicitly the hierarchy)
    tender_obj = Tender(**tender_data)

    # --- STEP C: FLATTEN FOR NEO4J ---
    # Now that validation passed, we break them apart again.
    
    # Convert back to dicts
    final_tender_dict = tender_obj.__dict__.copy()
    final_contract_dict = contract_obj.__dict__.copy()

    # REPLACEMENT: Swap the nested object for the ID string
    # Neo4j needs: (:Tender)-[:awardsContract]->(:Contract)
    final_tender_dict['awardsContract'] = shared_id
    
    # Extract nested objects and replace with IDs
    location_dicts = extract_nested_objects(final_contract_dict, 'executedAtLocation', 'address_id')
    document_dicts = extract_nested_objects(final_contract_dict, 'hasDocuments', 'document_id')

    return {
        "tenders": [final_tender_dict],
        "contracts": [final_contract_dict],
        "locations": location_dicts,
        "documents": document_dicts
    }