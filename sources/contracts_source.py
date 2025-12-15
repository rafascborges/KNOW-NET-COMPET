from elt_core.base_source import BaseDataSource
from elt_core.transformations import (
    to_dataframe, 
    to_dict, 
    convert_dates_to_iso, 
    normalize_locations, 
    enrich_location_from_municipality, 
    enrich_location_from_district, 
    map_location_fixes,
    transform_contract_type,
    transform_cpvs,
    filter_dropna,
    filter_max_value,
    filter_price_anomalies,
    filter_date_sequence,
    add_column
)
from sources.lookups.countries_set import COUNTRIES_SET
from sources.lookups.districts_municipalities import DISTRICT_MUNICIPALITIES_DICT, MUNICIPALITY_LOOKUP
from sources.lookups.location_changes_maps import COUNTRY_CHANGES_MAP, DISTRICT_CHANGES_MAP, MUNICIPALITY_CHANGES_MAP



allowed_contract_types = {
    "Aquisição de bens móveis",
    "Aquisição de serviços",
    "Concessão de obras públicas",
    "Concessão de serviços públicos",
    "Empreitadas de obras públicas",
    "Locação de bens móveis",
    "Sociedade",
}

class Contracts2Source(BaseDataSource):
    source_name = "contracts"
    def transform(self, data):
        """
        Transform contracts data.
        - Convert dates to ISO format strings
        - Handle NaN values
        """
        self.logger.info("Starting transformation process...")
        df = to_dataframe(data)
        initial_count = len(df)
        self.logger.info(f"Initial row count: {initial_count}")
        
        # Step 1: Convert date columns to datetime objects then to string isoformat
        date_cols = ['publication_date', 'signing_date', 'close_date']
        df = convert_dates_to_iso(df, date_cols, logger=self.logger)

        # Step 2: Drop id column
        if 'id' in df.columns:
            df = df.drop(columns=['id'])

        # Step 3: Optimized Contract type transformation
        df = transform_contract_type(df, 'contract_type', allowed_contract_types, logger=self.logger)

        # Step 4: CPVs transformation
        df = transform_cpvs(df, 'cpvs', max_length=20, logger=self.logger)

        # Step 5: Drop rows that have missing values in mv_cols
        mv_cols = ["contracted", "contracting_agency"]
        df = filter_dropna(df, mv_cols, logger=self.logger)

        # Step 6: Remove rows where execution_deadline is greater than 11000
        df = filter_max_value(df, 'execution_deadline', 11000, logger=self.logger)

        # Step 7: Remove rows where initial_price is 0 and final_price is NaN or initial_price is less than 0   
        df = filter_price_anomalies(df, 'initial_price', 'final_price', logger=self.logger)

        # Step 8: Remove rows where signing_date is prior to publication_date
        # We want signing_date >= publication_date
        df = filter_date_sequence(df, 'publication_date', 'signing_date', logger=self.logger)

        # Step 9: Location transformations
        if 'execution_location' in df.columns:
            # Normalize country entries in execution_location
            df = normalize_locations(df, 'execution_location', set(COUNTRIES_SET), logger=self.logger)
            
            # Back propagate municipality to district and country
            df = enrich_location_from_municipality(df, 'execution_location', MUNICIPALITY_LOOKUP, logger=self.logger)

            # Back propagate district to country
            df = enrich_location_from_district(df, 'execution_location', DISTRICT_MUNICIPALITIES_DICT, logger=self.logger)

            # Fix location name entries 
            df = map_location_fixes(df, 'execution_location', 'country', COUNTRY_CHANGES_MAP, logger=self.logger)
            df = map_location_fixes(df, 'execution_location', 'district', DISTRICT_CHANGES_MAP, logger=self.logger)
            df = map_location_fixes(df, 'execution_location', 'municipality', MUNICIPALITY_CHANGES_MAP, logger=self.logger)

        # Step 10: Add number of tenderers by inspecting contestants column
        df = add_column(df, 'numberOfTenderers', df['contestants'].apply(lambda x: len(x) if isinstance(x, list) else 0))

        # Step 11.1: Ensure initial and final price existis by: 
        # if initial_price exists and final_price is missing, set final_price = initial_price
        df.loc[df['initial_price'].notna() & df['final_price'].isna(), 'final_price'] = df['initial_price']

        # Step 11.2: Ensure initial and final price existis by: 
        # if final_price exists and initial_price is missing, set initial_price = final_price
        df.loc[df['final_price'].notna() & df['initial_price'].isna(), 'initial_price'] = df['final_price']
        


        final_count = len(df)
        self.logger.info(f"Transformation complete. Final row count: {final_count}. Total dropped: {initial_count - final_count}")

        return to_dict(df)

    def extract_nifs(self, data, columns = ['contracted', 'contracting_agency', 'contestants']):
        """
        Extracts unique NIFs from specified columns and returns them.
        Expected column structure: list of dicts [{'nif': '...', ...}, ...]
        """
        # TODO: Check if nifs_scrape_queue exists, if so return

        self.logger.info(f"Extracting NIFs from columns: {columns}")
        unique_nifs = set()
        
        # Ensure data is a list of dicts
        if isinstance(data, dict):
            data = [data]
            
        for row in data:
            for col in columns:
                if col in row and row[col]:
                    val = row[col]
                    
                    if isinstance(val, list):
                        for item in val:
                            if isinstance(item, dict) and 'nif' in item:
                                nif = item['nif']
                                if nif:
                                    unique_nifs.add(str(nif))
                    elif isinstance(val, dict) and 'nif' in val:
                         nif = val['nif']
                         if nif:
                             unique_nifs.add(str(nif))

        self.logger.info(f"Found {len(unique_nifs)} unique NIFs.")
        
        if not unique_nifs:
            return

        # Prepare documents for queue
        docs = []
        for nif in unique_nifs:
            if nif.strip():
                docs.append({
                    "_id": nif,
                    "nif": nif,
                    "status": "pending",
                    "source": "contracts"
                })
        
        self._save_in_batches(docs, "nifs_scrape_queue", batch_size=5000)
        print(f"Saved {len(docs)} NIFs to queue.")

    def run(self, batch_size=5000):
        """
        Chains the steps together using Staged ELT.
        Phase 1: Ingest (Stream -> Bronze)
        Phase 2: Transform (Bronze -> Memory -> Silver)
        """
        # Phase 1: Ingestion
        self.logger.info(f"Starting pipeline for {self.source_name}...")
        
        for raw_batch in self.extract(batch_size=batch_size):
            self.load_bronze(raw_batch, batch_size=batch_size)
          
        self.logger.info(f"Ingestion complete. Records loaded to bronze.")

        # Phase 2: Transformation
        
        # Fetch ALL data from Bronze
        all_bronze_docs = self.get_data('bronze')
        
        self.logger.info(f"Fetched {len(all_bronze_docs)} records. Applying transformations...")
        
        # Transform
        clean_data = self.transform(all_bronze_docs)
        
        # Load Silver
        self.load_silver(clean_data, batch_size)
        
        self.logger.info(f"Transformation complete. {len(clean_data)} records loaded to silver.")

        # Phase 3: NIF Extraction
        
        self.extract_nifs(clean_data)
        
        self.logger.info(f"{self.source_name} finished successfully.")


