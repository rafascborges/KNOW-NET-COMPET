# sources/graph_mappers/mapper_utils.py
"""
Generic utilities for declarative graph mappers.

Provides reusable functions for:
- Processing lists of entities with deduplication and validation
- Building node dicts from field mappings
- Generating relationship dicts from declarative configs
"""
from typing import Any, Callable, Dict, List, Optional, Tuple, Type
from slugify import slugify


def get_location_id(country: str, district: Optional[str] = None, municipality: Optional[str] = None) -> str:
    """Generate a location ID from hierarchical location parts."""
    parts = [slugify(country)]
    if district:
        parts.append(slugify(district))
    if municipality:
        parts.append(slugify(municipality))
    return f"loc:{'/'.join(parts)}"


def get_document_url(document_id: str) -> str:
    """Generate document URL from document ID."""
    return f"https://www.base.gov.pt/Base4/pt/resultados/?type=doc_documentos&id={document_id}&ext=.pdf"


def process_list_entities(
    items: Optional[List[Any]],
    id_func: Callable[[Any], Optional[str]],
    linkml_class: Type,
    linkml_kwargs_func: Callable[[str, Any], Dict],
    dict_builder_func: Callable[[str, Any], Dict],
    skip_if_in: Optional[set] = None
) -> Tuple[List[Dict], List[str]]:
    """
    Process a list of items into validated dicts with deduplication.
    
    Args:
        items: List of raw items to process (can be None)
        id_func: Function to extract ID from an item
        linkml_class: LinkML class for validation
        linkml_kwargs_func: Function to build kwargs for LinkML validation
        dict_builder_func: Function to build output dict from (id, item)
        skip_if_in: Optional set of IDs to skip (for dedup across entity types)
        
    Returns:
        Tuple of (list of dicts, list of IDs)
    """
    result_dicts = []
    result_ids = []
    seen = set()
    
    for item in (items or []):
        item_id = id_func(item)
        if not item_id:
            continue
        if item_id in seen:
            continue
        if skip_if_in and item_id in skip_if_in:
            continue
            
        seen.add(item_id)
        
        # Validate with LinkML (raises on error)
        linkml_class(**linkml_kwargs_func(item_id, item))
        
        # Build output dict
        result_dicts.append(dict_builder_func(item_id, item))
        result_ids.append(item_id)
    
    return result_dicts, result_ids


def process_simple_entities(
    vats: Optional[List[str]],
    linkml_class: Type,
    skip_if_in: Optional[set] = None
) -> Tuple[List[Dict], List[str]]:
    """
    Process a simple list of IDs (like VATs) into validated dicts.
    
    Args:
        vats: List of VAT/ID strings
        linkml_class: LinkML class for validation
        skip_if_in: Optional set of IDs to skip
        
    Returns:
        Tuple of (list of dicts, list of IDs)
    """
    result_dicts = []
    result_ids = []
    
    for vat in (vats or []):
        if not vat:
            continue
        if skip_if_in and vat in skip_if_in:
            continue
            
        # Validate with LinkML
        linkml_class(id=vat)
        
        result_dicts.append({'id': vat})
        result_ids.append(vat)
    
    return result_dicts, result_ids


def build_node_dict(
    node_id: str,
    data: Dict,
    field_mapping: Dict[str, str]
) -> Dict:
    """
    Build a node dict with only non-None fields.
    
    Args:
        node_id: The ID for the node
        data: Source data dict
        field_mapping: Dict mapping target_field -> source_field
        
    Returns:
        Dict with 'id' and all non-None mapped fields
    """
    result = {'id': node_id}
    for target_field, source_field in field_mapping.items():
        value = data.get(source_field)
        if value is not None:
            result[target_field] = value
    return result


def build_relationships_one_to_one(
    from_label: str,
    from_id: str,
    to_label: str,
    to_id: str,
    rel_type: str
) -> Dict:
    """Build a single relationship dict."""
    return {
        'from_label': from_label,
        'from_id': from_id,
        'to_label': to_label,
        'to_id': to_id,
        'rel_type': rel_type
    }


def build_relationships_one_to_many(
    from_label: str,
    from_id: str,
    to_label: str,
    to_ids: List[str],
    rel_type: str
) -> List[Dict]:
    """Build relationship dicts from one node to many target nodes."""
    return [
        build_relationships_one_to_one(from_label, from_id, to_label, to_id, rel_type)
        for to_id in to_ids
    ]


def build_relationships_many_to_one(
    from_label: str,
    from_ids: List[str],
    to_label: str,
    to_id: str,
    rel_type: str
) -> List[Dict]:
    """Build relationship dicts from many nodes to one target node."""
    return [
        build_relationships_one_to_one(from_label, from_id, to_label, to_id, rel_type)
        for from_id in from_ids
    ]
