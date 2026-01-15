from typing import Dict, List, Any
from elt_core.base_source import BaseDataSource


class EntitiesGoldSource(BaseDataSource):
    source_name = "entities_gold"

    def transform(self, scraper_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        self.logger.info("Transforming Entities Gold Data...")
        
        # 2. Merge Data
        gold_docs = []
        for doc in scraper_data:
            doc_id = doc.get('_id')
            if not doc_id:
                continue

            gold_docs.append(doc)

        self.logger.info(f"Enriched {len(gold_docs)} entities.")
        return gold_docs

    def run(self):
        self.logger.info("Running Entities Gold Source...")

        # 1. Fetch Data
        # BaseDataSource.get_data prefixes with source_name, which is incorrect here as we want specific external DBs
        scraper_data = self.db_connector.get_all_documents("nifs_scrape_silver")
        self.logger.info(f"Loaded {len(scraper_data)} records from nifs_scrape_silver.")

        # 2. Transform
        gold_docs = self.transform(scraper_data)

        # 3. Save to Gold Database
        if gold_docs:
            self._save_in_batches(gold_docs, self.source_name, batch_size=5000)
        else:
            self.logger.warning("No valid gold records generated for Entities Gold.")
