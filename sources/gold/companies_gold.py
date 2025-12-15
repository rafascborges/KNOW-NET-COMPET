from typing import Dict, List, Any
from elt_core.gold_source import BaseGoldSource
from sources.gold.schemas import CompanyGold
from pydantic import ValidationError

class CompaniesGoldSource(BaseGoldSource):
    def run(self):
        self.logger.info("Running Companies Gold Source...")
        
        # 1. Fetch Silver Data
        contracts = self.get_data("contracts_silver")
        marketing = self.get_data("marketing_silver") # Expecting empty or sample
        anuario = self.get_data("anuario_occ_silver") # Expecting empty or sample
        
        self.logger.info(f"Loaded {len(contracts)} contracts, {len(marketing)} marketing, {len(anuario)} anuario records.")
        
        # 2. Aggregation Layer (InMemory Dictionary)
        # NIF -> Partial Data Dict
        registry: Dict[str, Dict[str, Any]] = {}
        
        # Helper to get or create entry
        def get_entry(nif: str, name: str = None):
            if nif not in registry:
                registry[nif] = {
                    "nif": nif,
                    "name": name,
                    "total_contracts": 0,
                    "total_amount": 0.0,
                    "source_ids": [],
                    "sector": None,
                    "email": None,
                    "address": None
                }
            # Update name if we have a better one? For now keep first non-null
            if not registry[nif]["name"] and name:
                registry[nif]["name"] = name
            return registry[nif]

        # 3. Process Contracts
        for c in contracts:
            # We are aggregating SUPPLIERS ("contracted")
            # Structure: c['contracted'] is list of dicts with 'nif', 'name', etc.
            contracted_list = c.get('contracted', [])
            if isinstance(contracted_list, dict): # Handle edge case if it wasn't a list
                contracted_list = [contracted_list]
                
            price = c.get('initial_price', 0.0)
            if price is None: 
                price = 0.0
                
            # If multiple suppliers, we might attribute full price to both or split it?
            # Standard simplistic approach: Full price to all (as they are part of the deal) 
            # OR split equally. Let's attribute full price but note it. 
            # For now, let's just sum it up.
            
            for entity in contracted_list:
                if not isinstance(entity, dict): continue
                
                nif = entity.get('nif')
                name = entity.get('name') or entity.get('description') # sometimes name is desc
                
                if nif:
                    # Clean NIF just in case (though Silver should be clean)
                    nif = str(nif).strip()
                    entry = get_entry(nif, name)
                    entry["total_contracts"] += 1
                    try:
                        entry["total_amount"] += float(price)
                    except (ValueError, TypeError):
                        pass
                    
                    if '_id' in c:
                        entry["source_ids"].append(c['_id'])

        # 4. Enrich with Marketing (if available)
        for m in marketing:
            nif = m.get('nif')
            if nif and nif in registry:
                # Enrich existing found in contracts
                if 'sector' in m: registry[nif]['sector'] = m['sector']
                if 'email' in m: registry[nif]['email'] = m['email']
                if 'address' in m: registry[nif]['address'] = m['address']
            elif nif:
                # Add new company found only in marketing?
                # User prompted "join information", implying we want the intersection or union.
                # Usually Gold Companies should include ALL companies known.
                entry = get_entry(nif, m.get('name'))
                if 'sector' in m: entry['sector'] = m['sector']
                if 'email' in m: entry['email'] = m['email']
                if 'address' in m: entry['address'] = m['address']

        # 5. Convert to Pydantic and Validate
        valid_docs = []
        for nif, data in registry.items():
            try:
                model = CompanyGold(**data)
                # Model dump mode='json' handles serialization if needed, 
                # but we return dicts for save_documents_bulk
                doc = model.model_dump(mode='json') 
                # We need an _id for CouchDB
                doc['_id'] = f"company_{nif}"
                valid_docs.append(doc)
            except ValidationError as e:
                self.logger.warning(f"Validation failed for NIF {nif}: {e}")

        # 6. Save
        if valid_docs:
            self.save_gold(valid_docs, "companies_gold")
        else:
            self.logger.warning("No valid gold records generated.")
            
