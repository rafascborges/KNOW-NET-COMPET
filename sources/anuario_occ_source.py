import unicodedata
import pandas as pd
from typing import Dict, Optional, List, Any
from elt_core.base_source import BaseDataSource
from elt_core.transformations import to_dataframe, to_dict
from sources.lookups.occ_to_base_entity_map import OCC_TO_BASE_ENTITY_MAP

class AnuarioOCCSource(BaseDataSource):
    source_name = "anuario_occ"

    @staticmethod
    def _normalise_text(value: Optional[str]) -> str:
        """
        Remove accent marks and trim whitespace so ownership keys are easier to match downstream.
        """
        if not isinstance(value, str):
            return ""
        normalised = unicodedata.normalize("NFD", value.strip())
        return "".join(ch for ch in normalised if unicodedata.category(ch) != "Mn")

    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform raw Anuario OCC data dealing with implicit hierarchy (Municipality Header -> Company Rows).
        """
        self.logger.info("Starting Anuario OCC transformation...")
        df = to_dataframe(data)
        
        # 1. Build Ownership Lookup
        ownership: Dict[str, Dict[str, Optional[str]]] = {}
        last_municipality: Optional[str] = None
        
        for _, row in df.iterrows():
            marker = row.get("#")
            pmg_value = row.get("PMG")

            # Check if it's a marker row (Municipality header)
            if pd.notna(marker) and str(marker).strip():
                if pd.notna(pmg_value):
                    last_municipality = str(pmg_value).strip()
                continue

            # If no municipality set yet or no company name, skip
            if last_municipality is None or pd.isna(pmg_value):
                continue

            company_name = str(pmg_value).strip()
            participation = row.get("participacao_municipal")
            participation_value = (
                None if pd.isna(participation) else str(participation).strip()
            )

            companies = ownership.setdefault(company_name, {})
            companies[last_municipality] = participation_value

        self.logger.info(f"Built ownership lookup for {len(ownership)} companies.")

        # 2. Build Final Mapping using Lookup
        transformed_docs = []

        for occ_name, mapping in OCC_TO_BASE_ENTITY_MAP.items():
            if not mapping:
                continue

            base_name, nif = mapping
            owners = ownership.get(occ_name)
            
            if not owners:
                continue

            normalised_owners = {
                self._normalise_text(owner): participation 
                for owner, participation in owners.items()
            }

            doc = {
                "_id": str(nif), # Use NIF as ID
                "nif": str(nif),
                "name": base_name,
                "owners": normalised_owners,
                "original_occ_name": occ_name
            }
            transformed_docs.append(doc)

        self.logger.info(f"Transformation complete. Generated {len(transformed_docs)} records.")
        return transformed_docs

    def run(self, batch_size=10000):
        self.logger.info(f"Starting pipeline for {self.source_name}...")
        
        # Extract and Load Bronze
        for raw_batch in self.extract(batch_size=batch_size):
            self.load_bronze(raw_batch, batch_size=batch_size)

        # Transform and Load Silver
        bronze_data = self.get_data('bronze')
        self.logger.info(f"Fetched {len(bronze_data)} records. Applying transformations...")
        
        transformed_data = self.transform(bronze_data)
        self.load_silver(transformed_data)
        self.logger.info(f"Pipeline finished for {self.source_name}.")
        