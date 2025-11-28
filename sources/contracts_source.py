import logging
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
    filter_date_sequence
)
from sources.lookups.countries_set import COUNTRIES_SET
from sources.lookups.districts_municipalities import DISTRICT_MUNICIPALITIES_DICT, MUNICIPALITY_LOOKUP
from sources.lookups.location_changes_maps import COUNTRY_CHANGES_MAP, DISTRICT_CHANGES_MAP, MUNICIPALITY_CHANGES_MAP

# Configure logger
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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
    def transform(self, data):
        """
        Transform contracts data.
        - Convert dates to ISO format strings
        - Handle NaN values
        """
        logger.info("Starting transformation process...")
        df = to_dataframe(data)
        initial_count = len(df)
        logger.info(f"Initial row count: {initial_count}")
        
        # Step 1: Convert date columns to datetime objects then to string isoformat
        date_cols = ['publication_date', 'signing_date', 'close_date']
        df = convert_dates_to_iso(df, date_cols, logger=logger)

        # Step 2: Drop id column
        if 'id' in df.columns:
            df = df.drop(columns=['id'])

        # Step 3: Optimized Contract type transformation
        df = transform_contract_type(df, 'contract_type', allowed_contract_types, logger=logger)

        # Step 4: CPVs transformation
        df = transform_cpvs(df, 'cpvs', max_length=20, logger=logger)

        # Step 5: Drop rows that have missing values in mv_cols
        mv_cols = ["contracted", "contracting_agency"]
        df = filter_dropna(df, mv_cols, logger=logger)

        # Step 6: Remove rows where execution_deadline is greater than 11000
        df = filter_max_value(df, 'execution_deadline', 11000, logger=logger)

        # Step 7: Remove rows where initial_price is 0 and final_price is NaN or initial_price is less than 0   
        df = filter_price_anomalies(df, 'initial_price', 'final_price', logger=logger)

        # Step 8: Remove rows where signing_date is prior to publication_date
        # We want signing_date >= publication_date
        df = filter_date_sequence(df, 'publication_date', 'signing_date', logger=logger)

        # Step 9: Location transformations
        if 'execution_location' in df.columns:
            # Normalize country entries in execution_location
            df = normalize_locations(df, 'execution_location', set(COUNTRIES_SET), logger=logger)
            
            # Back propagate municipality to district and country
            df = enrich_location_from_municipality(df, 'execution_location', MUNICIPALITY_LOOKUP, logger=logger)

            # Back propagate district to country
            df = enrich_location_from_district(df, 'execution_location', DISTRICT_MUNICIPALITIES_DICT, logger=logger)

            # Fix location name entries 
            df = map_location_fixes(df, 'execution_location', 'country', COUNTRY_CHANGES_MAP, logger=logger)
            df = map_location_fixes(df, 'execution_location', 'district', DISTRICT_CHANGES_MAP, logger=logger)
            df = map_location_fixes(df, 'execution_location', 'municipality', MUNICIPALITY_CHANGES_MAP, logger=logger)

        final_count = len(df)
        logger.info(f"Transformation complete. Final row count: {final_count}. Total dropped: {initial_count - final_count}")

        return to_dict(df)

    def extract_nifs(self, data, columns):
        """
        Extracts unique NIFs from specified columns and returns them.
        Expected column structure: list of dicts [{'nif': '...', ...}, ...]
        """
        logger.info(f"Extracting NIFs from columns: {columns}")
        unique_nifs = set()
        
        # Ensure data is a list of dicts
        if isinstance(data, dict):
            data = [data]
            
        for row in data:
            for col in columns:
                if col in row and row[col]:
                    val = row[col]
                    # Handle if it's a string representation of a list (though transform should have handled it, 
                    # but raw bronze data might be passed directly? No, we usually pass transformed data)
                    # The user said "In each we have a list of dictionaries".
                    # Let's assume it's already a python list of dicts if we pass transformed data,
                    # or we might need to parse it if it's coming from raw.
                    # Ideally we call this on transformed data.
                    
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

        logger.info(f"Found {len(unique_nifs)} unique NIFs.")
        
        if not unique_nifs:
            return

        # Prepare documents for queue
        docs = []
        for nif in unique_nifs:
            # Basic validation: NIFs are usually numeric and have 9 digits in Portugal, 
            # but let's just ensure it's not empty for now.
            if nif.strip():
                docs.append({
                    "_id": nif,
                    "nif": nif,
                    "status": "pending",
                    "source": "contracts"
                })
        
        return docs

    def run(self, batch_size=5000):
        """
        Chains the steps together using Staged ELT.
        Phase 1: Ingest (Stream -> Bronze)
        Phase 2: Transform (Bronze -> Memory -> Silver)
        """
        # print(f"Starting pipeline for {self.file_path}...")
        
        # # Phase 1: Ingestion
        # print("Phase 1: Ingestion (Stream -> Bronze)")
        # total_ingested = 0

        # raw_data = self.extract()
        
        # self.load_bronze(raw_data, batch_size)
          
        # print(f"Ingestion complete. {total_ingested} records loaded to bronze.")

        # # Phase 2: Transformation
        # print("Phase 2: Transformation (Bronze -> Silver)")
        
        # # Fetch ALL data from Bronze
        # all_bronze_docs = self.get_data('bronze')
        # print(f"Fetched {len(all_bronze_docs)} records. Applying transformations...")
        
        # # Transform
        # clean_data = self.transform(all_bronze_docs)
        
        # # Load Silver
        # self.load_silver(clean_data, batch_size)
        
        # print(f"Pipeline finished successfully.")


        ### TESTES 
        silver_data = self.get_data('silver')
        print(f"Fetched {len(silver_data)} records. Applying transformations...")

        unique_nifs = self.extract_nifs(silver_data, ['contracted', 'contracting_agency', 'contestants'])
        print(f"Found {len(unique_nifs)} unique NIFs.")

        self._save_in_batches(unique_nifs, "nifs_scrape_queue", batch_size=5000)
        print(f"Saved {len(unique_nifs)} NIFs to queue.")