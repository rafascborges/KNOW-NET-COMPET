import traceback
from pathlib import Path
from dotenv import load_dotenv
from elt_core.db_connector import DBConnector
from sources.contracts_source import ContractsSource
from sources.anuario_occ_source import AnuarioOCCSource
from sources.nif_scraper_source import NifScraperSource
from sources.orbis_dm import OrbisDMSource
from sources.orbis_sh import OrbisSHSource
from sources.orbis_pt_companies_uci import OrbisPTCompaniesUCISource
from sources.gold.orbis_gold import OrbisGoldSource
from sources.gold.entities_gold import EntitiesGoldSource
from sources.gold.contracts_gold import ContractsGoldSource
from sources.cpv_structure_source import CPVStructureSource
from sources.graph_mappers.contract_mapper import contract_mapper
from elt_core.graph_loader import GraphLoader
import model 


MAX_WORKERS = 10
RUN_SCRAPER = False

# Configuration of sources: (SourceClass, id_column, filename)
SOURCES_CONFIG = [
    # (ContractsSource, 'contracts_2009_2024.parquet', 'contract_id'),
    #(AnuarioOCCSource, 'anuario_occ_table.csv', None),
    # (CPVStructureSource, 'cpv.json', None),
    # (OrbisDMSource, 'orbis_dm.csv', None),
    # (OrbisSHSource, 'orbis_sh.csv', None),
    # (OrbisPTCompaniesUCISource, 'orbis_pt_companies_uci.csv', None)
]

GOLD_SOURCES_CONFIG = [
    #ContractsGoldSource,
    #OrbisGoldSource,
    #EntitiesGoldSource,
]

def initialize_db_connector():
    """Initializes the database connector."""
    # Ensure you have CouchDB running. 
    # If using the provided docker-compose, it should be at localhost:5984
    try:
        return DBConnector()
    except Exception as e:
        print(f"Failed to connect to DB: {e}")
        traceback.print_exc()
        return None

def process_sources(db_connector, data_dir, sources_config):
    """Runs the standard data sources."""
    for source_class, filename, id_column in sources_config:
        file_path = data_dir / filename

        if not file_path.exists():
            print(f"Skipping {filename}: File not found at {file_path}")
            continue

        # Instantiate Source
        print(f"Processing file: {file_path}")
        try:
            source_instance = source_class(db_connector=db_connector, file_path=file_path, id_column=id_column)
            source_instance.run()
        except Exception as e:
            print(f"Pipeline failed for {filename}: {e}")
            traceback.print_exc()

def run_nif_scraper(db_connector):
    """Runs the NIF Scraper."""
    print("Running NIF Scraper...")
    try:
        scraper = NifScraperSource(db_connector)
        scraper.run(max_workers=MAX_WORKERS)
    except Exception as e:
        print(f"NIF Scraper failed: {e}")
        traceback.print_exc()

def run_gold_layer(db_connector, gold_sources_config):
    """Runs the Gold Layer sources."""
    print("Running Gold Layer...")
    try:
        for gold_source_class in gold_sources_config:
            gold_source_instance = gold_source_class(db_connector=db_connector)
            gold_source_instance.run()
    except Exception as e:
        print(f"Gold Layer failed: {e}")
        traceback.print_exc()

def run_graph_loader(db_connector):
    """Initialize and run the graph loader with Neo4j driver."""
    import os
    
    print("Starting Graph Loader...")
    
    # Neo4j connection details from environment variables
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
    
    # Initialize the GraphLoader with Neo4j driver
    loader = GraphLoader(
        db_connector=db_connector,
        neo4j_uri=NEO4J_URI,
        neo4j_auth=(NEO4J_USER, NEO4J_PASSWORD),
        model_module=model
    )
    
    try:
        # Run sync - no need to register collections anymore
        loader.sync_gold_db(
            couch_db_name="contracts_gold",
            doc_mapper_func=contract_mapper
        )
        
        print("Graph sync completed successfully!")
        
        # Log validation errors if any
        if loader.validation_errors:
            print(f"\n⚠️  {len(loader.validation_errors)} validation errors occurred.")
            print("Review with: loader.validation_errors")
            
    except Exception as e:
        print(f"Graph loader failed: {e}")
        traceback.print_exc()
    finally:
        # Close the Neo4j connection
        loader.close()


def main():
    load_dotenv()
    print("Initializing ELT Pipeline...")

    db_connector = initialize_db_connector()
    if not db_connector:
        return

    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / 'data'

    if SOURCES_CONFIG:
        process_sources(db_connector, data_dir, SOURCES_CONFIG)
    
    if RUN_SCRAPER:
        run_nif_scraper(db_connector)
        
    if GOLD_SOURCES_CONFIG:
        run_gold_layer(db_connector, GOLD_SOURCES_CONFIG)

    run_graph_loader(db_connector)

if __name__ == "__main__":
    main()
