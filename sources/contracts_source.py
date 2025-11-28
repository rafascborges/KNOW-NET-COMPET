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
