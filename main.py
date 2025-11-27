import traceback
from pathlib import Path
from dotenv import load_dotenv
from elt_core.db_connector import DBConnector
from sources.marketing_source import MarketingSource
from sources.contracts_source import Contracts2Source

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
        #(MarketingSource, 'id', 'marketing_sample.json'),
        (Contracts2Source, 'contract_id', 'contracts_2009_2024.parquet'),
    ]

    for source_class, id_column, filename in sources_config:
        file_path = data_dir / filename

        if not file_path.exists():
            print(f"Skipping {filename}: File not found at {file_path}")
            continue

        # Instantiate Source
        print(f"Processing file: {file_path}")
        try:
            source_instance = source_class(file_path, db_connector, id_column)
            #source_instance.run()
            source_instance.get_data('bronze')
            source_instance.transform()
            source_instance.load_silver()


        except Exception as e:
            print(f"Pipeline failed for {filename}: {e}")
            traceback.print_exc()

if __name__ == "__main__":
    main()
