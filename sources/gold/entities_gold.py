from typing import Dict, List, Any
from elt_core.base_source import BaseDataSource

class EntitiesGoldSource(BaseDataSource):
    source_name = "entities_gold"

    def transform(self, scraper_data: List[Dict[str, Any]], anuario_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        self.logger.info("Transforming Entities Gold Data...")
        
        # 1. Build Lookup for Anuario Data
        # Keyed by _id (which is NIF)
        anuario_lookup: Dict[str, Dict[str, Any]] = {
            doc.get('_id'): doc 
            for doc in anuario_data 
            if doc.get('_id')
        }

        # 2. Merge Data
        gold_docs = []
        for doc in scraper_data:
            doc_id = doc.get('_id')
            if not doc_id:
                continue
        
            # Enrich with Anuario data if matches
            if '_rev' in doc:
                del doc['_rev']

            if doc_id in anuario_lookup:
                anuario_match = anuario_lookup[doc_id]
                # Extract specific field as requested
                participation = anuario_match.get('municipalities_participation')
                if participation:
                    doc['municipalities_participation'] = participation
            
            gold_docs.append(doc)

        self.logger.info(f"Enriched {len(gold_docs)} entities.")
        return gold_docs

    def run(self):
        self.logger.info("Running Entities Gold Source...")

        # 1. Fetch Data
        # BaseDataSource.get_data prefixes with source_name, which is incorrect here as we want specific external DBs
        scraper_data = self.db_connector.get_all_documents("nifs_scrape_bronze")
        self.logger.info(f"Loaded {len(scraper_data)} records from nifs_scrape_bronze.")

        anuario_data = self.db_connector.get_all_documents("anuario_occ_silver")
        self.logger.info(f"Loaded {len(anuario_data)} records from anuario_occ_silver.")

        # 2. Transform
        gold_docs = self.transform(scraper_data, anuario_data)

        # 3. Save to Gold Database
        if gold_docs:
            self._save_in_batches(gold_docs, self.source_name, batch_size=5000)
        else:
            self.logger.warning("No valid gold records generated for Entities Gold.")
