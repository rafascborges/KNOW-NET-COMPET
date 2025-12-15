import traceback
from pathlib import Path
from dotenv import load_dotenv
from elt_core.db_connector import DBConnector
from sources.marketing_source import MarketingSource
from sources.contracts_source import Contracts2Source    
from sources.anuario_occ_source import AnuarioOCCSource
from sources.nif_scraper_source import NifScraperSource
from sources.orbis_dm import OrbisDMSource
from sources.orbis_sh import OrbisSHSource
from sources.orbis_pt_companies_uci import OrbisPTCompaniesUCISource

MAX_WORKERS = 10

def main():
    load_dotenv()
    print("Initializing ELT Pipeline...")

    # Initialize DB Connector
    # Ensure you have CouchDB running. 
    # If using the provided docker-compose, it should be at localhost:5984
    try:
        db_connector = DBConnector()
    except Exception as e:
        print(f"Failed to connect to DB: {e}")
        traceback.print_exc()
        return

    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / 'data'

    # Configuration of sources: (SourceClass, id_column, filename)
    sources_config = [
        #(MarketingSource, 'marketing_sample.json', 'id'),
        (Contracts2Source, 'contracts_2009_2024.parquet', 'contract_id'),
        #(AnuarioOCCSource, 'anuario_occ_table.csv', None),
        #(OrbisDMSource, 'orbis_dm.csv', None),
        #(OrbisSHSource, 'orbis_sh.csv', None),
        #(OrbisPTCompaniesUCISource, 'orbis_pt_companies_uci.csv', None)
    ]

    for source_class, filename, id_column in sources_config:
        file_path = data_dir / filename

        if not file_path.exists():
            print(f"Skipping {filename}: File not found at {file_path}")
            continue

        # Instantiate Source
        print(f"Processing file: {file_path}")
        try:
            source_instance = source_class(file_path, db_connector, id_column)
            source_instance.run()
        except Exception as e:
            print(f"Pipeline failed for {filename}: {e}")
            traceback.print_exc()

    # # Run NIF Scraper
    # print("Running NIF Scraper...")
    # try:
    #     scraper = NifScraperSource(db_connector)
    #     scraper.run(max_workers=MAX_WORKERS)
    # except Exception as e:
    #     print(f"NIF Scraper failed: {e}")
    #     traceback.print_exc()


    # # Run Gold Layer
    # print("Running Gold Layer...")
    # try:
    #     companies_gold = CompaniesGoldSource(db_connector)
    #     companies_gold.run()
    # except Exception as e:
    #     print(f"Gold Layer failed: {e}")
    #     traceback.print_exc()

if __name__ == "__main__":
    main()
