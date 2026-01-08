from typing import Dict, List, Any
from elt_core.base_source import BaseDataSource
import unicodedata
import re
from ..lookups.districts_municipalities import MUNICIPALITY_LOOKUP


# =============================================================================
# MODULE-LEVEL CONSTANTS
# =============================================================================

# Regex pattern to remove "Municipio de/da/do/das/dos" or "Câmara Municipal de/da/do/das/dos" prefix
MUNICIPAL_PREFIX_PATTERN = re.compile(
    r'^(Municipio|Camara Municipal)\s+(das|dos|da|de|do)?\s*', 
    re.IGNORECASE
)

# Map for municipality name corrections (non-standard -> standard)
MUNICIPALITY_DIFF_MAP = {
    'Idanha-a-nova': 'Idanha-a-Nova',
    'Montemor-o-velho': 'Montemor-o-Velho',
    'Condeixa-a-nova': 'Condeixa-a-Nova',
    'Figueira de Castelo Rodrigo': 'Fig. Castelo Rodrigo',
    'Montemor-o-novo': 'Montemor-o-Novo',
    'Albergaria-a-velha': 'Albergaria-a-Velha',
    'Santa Marta de Penaguiao': 'Sta Marta de Penaguiao',
    'Freixo de Espada A Cinta': 'Freixo Espada a Cinta',
    'Vila Real de Santo Antonio': 'Vila Real Sto Antonio',
    'Proenca-a-nova': 'Proenca-a-Nova',
}


MUNICIPALITY_MISS_NAMED = { 
    '503956546': 'Municipio de Borba',
    '506693651': 'Municipio de Cinfaes',
    '506556590': 'Municipio de Estremoz',
    '506215695': 'Municipio de Fundao',
    '506804240': 'Municipio de Lagoa - Faro',
    '512074410': 'Municipio de Lagoa - Acores',
    '512070946': 'Municipio de Madalena',
    '507012100': 'Municipio de Nazare',
    '506811913': 'Municipio de Ponte de Lima',
    '506806456': 'Municipio de Ponte de Sor',
    '505377802': 'Municipio de Proenca-a-Nova',
    '506173968': 'Municipio de Seixal',
    '506912833': 'Municipio de Vagos',
    '506697320': 'Municipio de Viseu',
    '511233639': 'Municipio de Calheta Madeira',
    '512074089': 'Municipio de Calheta Sao Jorge',
    '680011404': 'Antigo Dup. Municipio de Espinho',
    '680009060': 'Antigo Dup. Municipio de Moita',
    '680010793': 'Antigo Dup. Municipio de Ilhavo',
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def remove_accents(input_str: str) -> str:
    """Remove accents from unicode text."""
    if not input_str:
        return input_str
    nfkd_form = unicodedata.normalize('NFD', input_str)
    return "".join(c for c in nfkd_form if not unicodedata.combining(c))


def portuguese_title_case(text: str) -> str:
    """Title case with Portuguese exceptions (da, de, do, etc.)."""
    if not isinstance(text, str): 
        return text
    
    exceptions = {'da', 'de', 'do', 'das', 'dos', 'e'}
    words = text.split()
    
    return " ".join(
        word.lower() if word.lower() in exceptions else word.capitalize()
        for word in words
    )


def is_municipal_entity(description: str) -> bool:
    """Check if description represents a municipal entity."""
    normalized = remove_accents(description).lower()
    return normalized.startswith('municipio') or normalized.startswith('camara municipal')


def extract_municipality_name(description: str) -> str:
    """
    Extract and normalize municipality name from description.
    
    Examples:
        "Município de Lisboa, Portugal" -> "Lisboa"
        "Câmara Municipal de Braga" -> "Braga"
    """
    # Remove accents and take first part before comma
    name = remove_accents(description).split(',')[0]
    
    # Remove "Municipio/Câmara Municipal de/da/do" prefix using regex
    name = MUNICIPAL_PREFIX_PATTERN.sub('', name)
    
    # Additional normalizations
    name = name.replace('S. ', 'Sao ').strip()
    name = portuguese_title_case(name)
    
    # Apply known corrections
    return MUNICIPALITY_DIFF_MAP.get(name, name)


# =============================================================================
# MAIN CLASS
# =============================================================================

class MunicipalEntitiesGoldSource(BaseDataSource):
    source_name = "municipal_entities_gold"

    def transform(self, scraper_data: List[Dict[str, Any]], anuario_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        self.logger.info("Transforming Municipal Entities Gold Data...")

        #0. Fix municipalities with miss-named
        for doc in scraper_data:
            if doc.get('nif') in MUNICIPALITY_MISS_NAMED:
                doc['description'] = MUNICIPALITY_MISS_NAMED[doc.get('nif')]

        # 1. Filter entities where description starts with "Municipio" or "Câmara Municipal"
        filtered = [
            doc for doc in scraper_data 
            if is_municipal_entity(doc.get('description', ''))
        ]
        self.logger.info(f"Extracted {len(filtered)} municipal entities.")

        # 2. Process and validate against MUNICIPALITY_LOOKUP
        municipal_entities = []
        for doc in filtered:
            name = extract_municipality_name(doc['description'])
            
            if name not in MUNICIPALITY_LOOKUP:
                self.logger.warning(f"'{name}' not found in MUNICIPALITY_LOOKUP -- Description: {doc['description']}")
                continue
            
            doc['administrates'] = name
            municipal_entities.append(doc)

        self.logger.info(f"Processed {len(municipal_entities)} municipal entities.")

        # 2. Build Lookup for Municipal Entities (keyed by municipality NAME from 'administrates')
        municipal_by_name: Dict[str, Dict[str, Any]] = {
            doc.get('administrates'): doc 
            for doc in municipal_entities 
            if doc.get('administrates')
        }

        # 3. Enrich municipal entities with Anuario shareholding data
        # For each anuario doc, check if any municipality in municipalities_participation
        # matches a municipal entity, and add the shareholding relationship
        matched_count = 0
        for anuario_doc in anuario_data:
            anuario_nif = anuario_doc.get('nif') or anuario_doc.get('_id')
            participation = anuario_doc.get('municipalities_participation')
            
            if not anuario_nif or not participation:
                continue
            
            # Check each municipality in this anuario doc's participation
            has_match = False
            for municipality_name, percentage in participation.items():
                if municipality_name in municipal_by_name:
                    # Found match - add to holds_share_of
                    municipal_doc = municipal_by_name[municipality_name]
                    if 'holds_share_of' not in municipal_doc:
                        municipal_doc['holds_share_of'] = {}
                    municipal_doc['holds_share_of'][anuario_nif] = percentage
                    has_match = True
            
            if has_match:
                matched_count += 1
            else:
                self.logger.warning(f"Anuario doc '{anuario_nif}' ({anuario_doc.get('name', 'unknown')}) has no matching municipality")

        self.logger.info(f"Matched {matched_count}/{len(anuario_data)} anuario docs to municipal entities.")

        # 4. Build final gold docs (clean up _rev)
        gold_docs = []
        for doc in municipal_entities:
            if '_rev' in doc:
                del doc['_rev']
            gold_docs.append(doc)

        self.logger.info(f"Enriched {len(gold_docs)} entities.")
        return gold_docs

    def run(self):
        self.logger.info("Running Entities Gold Source...")

        # 1. Fetch Data
        # BaseDataSource.get_data prefixes with source_name, which is incorrect here as we want specific external DBs
        scraper_data = self.db_connector.get_all_documents("nifs_scrape_silver")
        self.logger.info(f"Loaded {len(scraper_data)} records from nifs_scrape_silver.")

        anuario_data = self.db_connector.get_all_documents("anuario_occ_silver")
        self.logger.info(f"Loaded {len(anuario_data)} records from anuario_occ_silver.")

        # 2. Transform
        gold_docs = self.transform(scraper_data, anuario_data)

        # 3. Save to Gold Database
        if gold_docs:
            self._save_in_batches(gold_docs, self.source_name, batch_size=5000)
        else:
            self.logger.warning("No valid gold records generated for Entities Gold.")
