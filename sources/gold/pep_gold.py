# sources/gold/pep_gold.py
"""
PEP (Politically Exposed Persons) Gold Source.

Merges data from social_careers_silver and societies_source_silver,
grouping by person name (Nome) and aggregating their associations.
"""
from typing import Dict, List, Any
from collections import defaultdict
from elt_core.base_source import BaseDataSource


class PEPGoldSource(BaseDataSource):
    source_name = "pep_gold"

    def transform(
        self, 
        social_careers_data: List[Dict[str, Any]], 
        societies_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Merge social careers and societies data by person name.
        
        Creates documents with structure:
        {
            "_id": <Nome>,
            "associated": [
                {
                    "nif": <NIPC>,
                    "roles": [<Cargo>, ...],
                    "equity_interests": [<Participação Social>, ...],
                    "governments": [<Governo>, ...],
                    "parliaments": [<Legislatura>, ...]
                },
                ...
            ]
        }
        
        Each nif appears once per person, with parallel arrays where index i
        corresponds to the same source record. None values are preserved
        to maintain index alignment.
        """
        self.logger.info("Transforming PEP Gold Data...")
        
        # Group all records by person name, then by nif
        # Key: Nome, Value: dict of {nif: {aggregated lists}}
        person_associations = defaultdict(dict)
        
        # Process social_careers_silver records
        for doc in social_careers_data:
            nome = doc.get('Nome')
            if not nome:
                continue
            
            nipc = doc.get('NIPC')
            if not nipc:
                continue
                
            government = doc.get('Governo')
            parliament = doc.get('Legislatura')
            role = doc.get('Cargo')
            
            # Initialize nif entry if not exists
            if nipc not in person_associations[nome]:
                person_associations[nome][nipc] = {
                    'nif': nipc,
                    'roles': [],
                    'equity_interests': [],
                    'governments': [],
                    'parliaments': [],
                }
            
            # Append all values (including None) to maintain index alignment
            agg = person_associations[nome][nipc]
            agg['roles'].append(role)
            agg['equity_interests'].append(None)  # Will be filled by societies data
            agg['governments'].append(government)
            agg['parliaments'].append(parliament)
        
        # Process societies_source_silver records
        for doc in societies_data:
            nome = doc.get('Nome')
            if not nome:
                continue
            
            nipc = doc.get('NIPC')
            if not nipc:
                continue
                
            government = doc.get('Governo')
            parliament = doc.get('Legislatura')
            equity_interest = doc.get('Participação Social')
            
            # Initialize nif entry if not exists
            if nipc not in person_associations[nome]:
                person_associations[nome][nipc] = {
                    'nif': nipc,
                    'roles': [],
                    'equity_interests': [],
                    'governments': [],
                    'parliaments': [],
                }
            
            # Append all values (including None) to maintain index alignment
            agg = person_associations[nome][nipc]
            agg['roles'].append(None)  # Societies don't have roles
            agg['equity_interests'].append(equity_interest)
            agg['governments'].append(government)
            agg['parliaments'].append(parliament)
        
        # Build final gold documents
        gold_docs = []
        for nome, nif_dict in person_associations.items():
            gold_doc = {
                '_id': nome,
                'associated': list(nif_dict.values())
            }
            gold_docs.append(gold_doc)
        
        self.logger.info(f"Created {len(gold_docs)} PEP gold records from {len(social_careers_data)} careers + {len(societies_data)} societies.")
        return gold_docs

    def run(self):
        self.logger.info("Running PEP Gold Source...")

        # 1. Fetch Data from both silver sources
        social_careers_data = self.db_connector.get_all_documents("social_careers_silver")
        self.logger.info(f"Loaded {len(social_careers_data)} records from social_careers_silver.")

        societies_data = self.db_connector.get_all_documents("societies_source_silver")
        self.logger.info(f"Loaded {len(societies_data)} records from societies_source_silver.")

        # 2. Transform
        gold_docs = self.transform(social_careers_data, societies_data)

        # 3. Save to Gold Database
        if gold_docs:
            self._save_in_batches(gold_docs, self.source_name, batch_size=5000)
        else:
            self.logger.warning("No valid gold records generated for PEP Gold.")
