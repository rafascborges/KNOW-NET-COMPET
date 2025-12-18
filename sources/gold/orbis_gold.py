from typing import Dict, List, Any, Set
from elt_core.gold_source import BaseGoldSource

class OrbisGoldSource(BaseGoldSource):
    def run(self):
        self.logger.info("Running Orbis Gold Source...")

        # 1. Fetch Silver Data
        dm_silver = self.get_data("orbis_dm_silver")
        self.logger.info(f"Loaded {len(dm_silver)} records from orbis_dm_silver.")

        sh_silver = self.get_data("orbis_sh_silver")
        self.logger.info(f"Loaded {len(sh_silver)} records from orbis_sh_silver.")

        # 2. Aggregation Structures
        # We need to track VATs and Names per UCI for both sources
        all_ucis: Set[str] = set()
        
        # Structure: UCI -> {'vats': Set[str], 'name': str}
        dm_data: Dict[str, Dict[str, Any]] = {}
        sh_data: Dict[str, Dict[str, Any]] = {}

        def process_source(records: List[Dict[str, Any]], target_dict: Dict, name_key: str):
            for record in records:
                uci_raw = record.get('UCI')
                if not uci_raw:
                    continue
                
                uci = str(uci_raw).strip()
                all_ucis.add(uci)
                
                if uci not in target_dict:
                    target_dict[uci] = {'vats': set(), 'name': None}
                
                # Add VAT
                vat_raw = record.get('VAT')
                if vat_raw:
                    target_dict[uci]['vats'].add(str(vat_raw).strip())
                
                # Capture Name (take the first non-null one we find for this UCI if not set)
                # Logic: If we haven't found a name for this UCI in this source yet, check this record.
                name_val = record.get(name_key)
                if name_val and not target_dict[uci]['name']:
                    target_dict[uci]['name'] = str(name_val).strip()

        # Process DM
        process_source(dm_silver, dm_data, 'DMFull name')
        
        # Process SH
        process_source(sh_silver, sh_data, 'SH - Name')

        # 3. Construct Gold Documents
        gold_docs = []
        for uci in all_ucis:
            dm_entry = dm_data.get(uci)
            sh_entry = sh_data.get(uci)

            # Determine Name
            # Priority: DM > SH
            final_name = None
            if dm_entry and dm_entry['name']:
                final_name = dm_entry['name']
            elif sh_entry and sh_entry['name']:
                final_name = sh_entry['name']

            # Get VAT lists
            dm_vats = list(dm_entry['vats']) if dm_entry else []
            sh_vats = list(sh_entry['vats']) if sh_entry else []

            doc = {
                "_id": uci,
                "name": final_name,
                "dm": dm_vats,
                "sh": sh_vats
            }
            gold_docs.append(doc)

        self.logger.info(f"Aggregated into {len(gold_docs)} UCI groups.")

        # 4. Save to Gold Database
        if gold_docs:
            self.save_gold(gold_docs, "orbis_gold")
        else:
            self.logger.warning("No valid gold records generated for Orbis Gold.")
