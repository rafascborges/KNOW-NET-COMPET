import os
import traceback
from pathlib import Path

from dotenv import load_dotenv

from elt_core.db_connector import DBConnector
from elt_core.graph_loader import GraphLoader
from elt_core.graph_enrichment import run_all_enrichments
from sources.anuario_occ_source import AnuarioOCCSource
from sources.contracts_source import ContractsSource
from sources.cpv_structure_source import CPVStructureSource
from sources.gold.contracts_gold import ContractsGoldSource
from sources.gold.entities_gold import EntitiesGoldSource
from sources.gold.orbis_gold import OrbisGoldSource
from sources.gold.municipal_entities_gold import MunicipalEntitiesGoldSource
from sources.gold.pep_gold import PEPGoldSource
from sources.graph_mappers.contracts_mapper import contracts_mapper
from sources.graph_mappers.entities_mapper import entities_mapper
from sources.graph_mappers.orbis_mapper import orbis_mapper
from sources.graph_mappers.cpv_mapper import cpv_mapper
from sources.graph_mappers.municipal_entities_mapper import municipal_entities_mapper
from sources.graph_mappers.pep_mapper import pep_mapper
from sources.nif_scraper_source import NifScraperSource
from sources.orbis_dm import OrbisDMSource
from sources.orbis_pt_companies_uci import OrbisPTCompaniesUCISource
from sources.orbis_sh import OrbisSHSource
from sources.social_careers_source import SocialCareersSource
from sources.societies_source import SocietiesSource
from sources.people_area_source import PeopleAreaSource



MAX_WORKERS = 10
RUN_SCRAPER = False

# Configuration of sources: (SourceClass, filename, id_column)
SOURCES_CONFIG = [
    # (ContractsSource, 'contracts_2009_2024.parquet', 'contract_id'),
    # (AnuarioOCCSource, 'anuario_occ_table.csv', None),
    # (CPVStructureSource, 'cpv.json', None),
    # (OrbisDMSource, 'orbis_dm.csv', None),
    # (OrbisSHSource, 'orbis_sh.csv', None),
    # (OrbisPTCompaniesUCISource, 'orbis_pt_companies_uci.csv', None)
    # (SocialCareersSource, 'social_careers.json', None),
    # (SocietiesSource, 'societies.json', None),
    # (PeopleAreaSource, 'people.json', None),
]

GOLD_SOURCES_CONFIG = [
    # EntitiesGoldSource,
    # ContractsGoldSource,
    # OrbisGoldSource,
    # MunicipalEntitiesGoldSource,
    # PEPGoldSource,
]

GRAPH_LOADER_CONFIG = [
    # ("entities_gold", entities_mapper),
    # ("municipal_entities_gold", municipal_entities_mapper),
    # ("cpv_structure_silver", cpv_mapper),
    # ("contracts_gold", contracts_mapper),
    # ("orbis_gold", orbis_mapper),
    # ("pep_gold", pep_mapper),
]

GRAPH_ENRICHMENT_CONFIG = True

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
            source_instance.run(batch_size=10000)
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

def run_graph_loader(db_connector, graph_loader_config, graph_enrichment_config):
    """Initialize and run the graph loader with Neo4j driver."""
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
    )
    loader.init_neo4j_schema()
    
    try:
        for graph_source, graph_mapper in graph_loader_config:
    
            loader.sync_gold_db(
                couch_db_name=graph_source,
                doc_mapper_func=graph_mapper,
                batch_size=10000
            )
        

        # Graph enrichment: create derived relationships
        if graph_enrichment_config:
            run_all_enrichments(loader)
        
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

    if GRAPH_LOADER_CONFIG or GRAPH_ENRICHMENT_CONFIG:
        run_graph_loader(db_connector, GRAPH_LOADER_CONFIG, GRAPH_ENRICHMENT_CONFIG)

if __name__ == "__main__":
    main()
