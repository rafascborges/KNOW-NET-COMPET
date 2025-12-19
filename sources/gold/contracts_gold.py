from typing import Dict, List, Any
from elt_core.base_source import BaseDataSource

class ContractsGoldSource(BaseDataSource):
    source_name = "contracts_gold"

    def transform(self, contracts_silver: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        gold_docs = []
        fields_to_process = {
            'contracted': 'contracted_vats',
            'contracting_agency': 'contracting_agency_vats',
            'contestants': 'contestants_vats'
        }

        for doc in contracts_silver:
            new_doc = doc.copy()
            
            # Process specific fields
            for original_field, new_field in fields_to_process.items():
                # Remove original field
                entry_data = new_doc.pop(original_field, None)
                
                vats = []
                if entry_data:
                    # Normalize to list
                    items = entry_data if isinstance(entry_data, list) else [entry_data]
                    
                    for item in items:
                        if isinstance(item, dict):
                            nif = item.get('nif')
                            if nif:
                                vats.append(str(nif).strip())
                        # If it's already a string/int (though schema says dict), handle gracefully?
                        # The code in ContractsSource expects dicts with 'nif'.
                
                new_doc[new_field] = vats
            
            gold_docs.append(new_doc)
            
        return gold_docs

    def graph_mappers(self, validated_obj):
        """
        Takes a validated Contract and Tender object (tree) from model.py 
        and returns flat dictionaries for Neo4j.
        """

        
        

    def run(self):
        self.logger.info("Running Contracts Gold Source...")

        # 1. Fetch Silver Data
        contracts_silver = self.db_connector.get_all_documents("contracts_silver")
        self.logger.info(f"Loaded {len(contracts_silver)} records from contracts_silver.")

        # 2. Transform
        gold_docs = self.transform(contracts_silver)
        self.logger.info(f"Transformed {len(gold_docs)} records.")

        # 3. Save to Gold Database
        if gold_docs:
            self._save_in_batches(gold_docs, self.source_name, batch_size=5000)
        else:
            self.logger.warning("No valid gold records generated for Contracts Gold.")
