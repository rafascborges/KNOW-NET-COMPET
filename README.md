# KNOW-NET-COMPET 🔍

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Neo4j](https://img.shields.io/badge/Neo4j-Graph%20Database-008CC1?logo=neo4j)](https://neo4j.com/)
[![CouchDB](https://img.shields.io/badge/CouchDB-Document%20Store-E42528?logo=apache-couchdb)](https://couchdb.apache.org/)
[![LinkML](https://img.shields.io/badge/LinkML-Data%20Modeling-purple)](https://linkml.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **A knowledge graph for understanding the Portuguese public procurement market**

KNOW-NET-COMPET is an ELT (Extract, Load, Transform) pipeline and knowledge graph system designed to model and analyze the Portuguese public procurement ecosystem. The project integrates multiple data sources—including public contracts, corporate ownership structures, and politically exposed persons—into a unified graph database for transparency and insight.

---

### Supported by

<p align="center">
  <img src="./assets/fct.png" alt="FCT - Fundação para a Ciência e a Tecnologia" height="80"/>
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
  <img src="./assets/magIC.png" alt="NOVA IMS MagIC - Information Management Research Center" height="80"/>
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
  <img src="./assets/lab-image.png" alt="NOVA Applied Economics & Analytics Lab" height="80"/>
</p>

<p align="center">
  <em>This investigation project is financed by <strong>FCT — Fundação para a Ciência e a Tecnologia</strong></em>
</p>

---

## 🎯 Objectives

- **Build a Knowledge Graph** connecting public tenders, contracts, entities, and people involved in Portuguese public procurement
- **Enable Transparency** by linking contracting agencies, suppliers, shareholders, and politically exposed persons
- **Support Research** into market dynamics, competition patterns, and procurement behavior

---

## 🏗️ Architecture

KNOW-NET-COMPET follows a **medallion architecture** (Bronze → Silver → Gold) for data processing, with a final graph layer in Neo4j:

<p align="center">
  <img src="./assets/Architecture_Proposal.drawio.png" alt="KNOW-NET-COMPET Architecture Diagram" width="900"/>
</p>

The pipeline flows through:
1. **Data Sources** — Portal BASE, ORBIS, Anuário OCC, and other registries
2. **CouchDB Staging** — Bronze (raw) → Silver (cleaned) → Gold (aggregated) layers
3. **LinkML Validation** — Schema-based data validation using `schema.yaml`
4. **Neo4j Graph** — Property graph storage for knowledge representation
5. **NeoDash** — Interactive dashboards for data visualization

---

## 📊 Data Sources

| Source | Description | Data Type |
|--------|-------------|-----------|
| **Portal BASE** | Portuguese public procurement contracts (2009-2024) | Tenders, Contracts, Entities |
| **ORBIS** | Corporate ownership and director/shareholder data | Company relationships |
| **CPV** | Common Procurement Vocabulary classification codes | Hierarchical taxonomy |
| **NIF Registry** | Portuguese tax identification number validation | Entity verification |
| **Municipal Entities** | Local government administration data | Location governance |
| **PEP Data** | Politically Exposed Persons information | Public x Private connection |

---

## 🧩 Graph Schema

The knowledge graph models the following entities and relationships:

<p align="center">
  <img src="./assets/diagram-schema.png" alt="KNOW-NET-COMPET Graph Schema" width="900"/>
</p>

### Nodes
- **Tender** — Procurement procedures with method, dates, and criteria
- **Contract** — Signed agreements with values, deadlines, and types
- **Entity** — Organizations (buyers and suppliers)
- **Person** — Individuals associated with entities
- **Location** — Hierarchical geography (Country → District → Municipality)
- **CPV** — Product/service classification codes
- **Document** — Contract-related documentation

### Key Relationships
- `AWARDS_CONTRACT` — Tender → Contract
- `IS_PROCURING_ENTITY_FOR` — Entity → Tender
- `SIGNED_CONTRACT` — Entity → Contract
- `SHAREHOLDER_OF` — Entity → Entity (Currently focused on municipal entities participation)
- `ASSOCIATED_WITH` — Person → Entity
- `LOCATED_AT` — Entity → Location
- `BROADER` — Hierarchical relationships (CPV, Location)

---

## 🚀 Getting Started

### Prerequisites

- **Python** 3.12 or higher
- **Docker** and **Docker Compose**
- **uv** (recommended for dependency management)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd KNOW-NET-COMPET
   ```

2. **Configure environment variables**
   
   Create a `.env` file in the root directory:
   ```env
   # CouchDB Configuration
   COUCHDB_USER=admin
   COUCHDB_PASSWORD=your_secure_password
   COUCHDB_SECRET=your_secret_key
   COUCHDB_URL=http://admin:your_secure_password@localhost:5984/
   
   # Neo4j Configuration
   NEO4J_URI=bolt://localhost:7687
   NEO4J_USER=neo4j
   NEO4J_PASSWORD=password
   
   # Data Storage
   DATA_ROOT=/path/to/your/data/storage
   ```

3. **Start the databases**
   ```bash
   docker-compose up -d
   ```
   
   This will start:
   - **CouchDB** on port `5984`
   - **Neo4j** on ports `7474` (HTTP) and `7687` (Bolt)
   - **NeoDash** on port `5005` for graph visualization

4. **Install dependencies**
   ```bash
   uv sync
   ```

---

## 💻 Usage

### Running the Pipeline

Execute the main pipeline script:

```bash
uv run python main.py
```

The pipeline orchestrates:
1. **Source Processing** — Extract and transform raw data files
2. **Gold Layer Generation** — Aggregate and validate entities
3. **Graph Loading** — Sync validated data to Neo4j
4. **Graph Enrichment** — Create derived relationships

### Configuration

Edit `main.py` to enable/disable specific pipeline stages:

```python
# Enable data sources to process
SOURCES_CONFIG = [
    (ContractsSource, 'contracts_2009_2024.parquet', 'contract_id'),
    (CPVStructureSource, 'cpv.json', None),
    # ... add more sources
]

# Enable gold layer aggregations
GOLD_SOURCES_CONFIG = [
    ContractsGoldSource,
    EntitiesGoldSource,
    OrbisGoldSource,
    # ... add more gold sources
]

# Enable graph sync
GRAPH_LOADER_CONFIG = [
    ("contracts_gold", contracts_mapper),
    ("entities_gold", entities_mapper),
    # ... add more mappers
]
```

---

## 📁 Project Structure

```
KNOW-NET-COMPET/
├── elt_core/                    # Core ELT framework
│   ├── base_source.py           # Abstract base class for data sources
│   ├── db_connector.py          # CouchDB connection and operations
│   ├── graph_loader.py          # Neo4j graph sync engine
│   ├── graph_enrichment.py      # Derived relationship creation
│   └── transformations.py       # Common data transformations
│
├── sources/                     # Data source implementations
│   ├── contracts_source.py      # Portal BASE contracts processor
│   ├── nif_scraper_source.py    # NIF web scraper
│   ├── orbis_*.py               # ORBIS data processors
│   │
│   ├── gold/                    # Gold layer aggregators
│   │   ├── contracts_gold.py
│   │   ├── entities_gold.py
│   │   └── ...
│   │
│   ├── graph_mappers/           # Document → Graph mappers
│   │   ├── contracts_mapper.py
│   │   ├── entities_mapper.py
│   │   └── ...
│   │
│   └── lookups/                 # Reference data & mappings
│       ├── districts_municipalities.py
│       └── ...
│
├── assets/                      # Images and diagrams
├── schema.yaml                  # LinkML schema definition
├── model.py                     # Generated Python classes from LinkML
├── constraints.cypher           # Neo4j constraint definitions
├── main.py                      # Pipeline entry point
├── docker-compose.yml           # Database infrastructure
└── pyproject.toml               # Project dependencies
```

---

## 🛠️ Extending KNOW-NET-COMPET

### Adding a New Data Source

1. Create a new class in `sources/` inheriting from `BaseDataSource`:
   ```python
   from elt_core.base_source import BaseDataSource
   
   class MyNewSource(BaseDataSource):
       @property
       def source_name(self):
           return "my_new_source"
       
       def transform(self, data):
           # Implement transformation logic
           return transformed_data
       
       def run(self, batch_size=5000):
           # Implement pipeline logic
           pass
   ```

2. Register in `main.py`'s `SOURCES_CONFIG`

### Adding a Graph Mapper

1. Create a mapper function in `sources/graph_mappers/`:
   ```python
   def my_mapper(doc: dict) -> dict:
       return {
           "entity": [...],        # List of entity dicts
           "relationships": [...]  # List of relationship dicts
       }
   ```

2. Register in `main.py`'s `GRAPH_LOADER_CONFIG`

---

## 🔧 Development

### Generate Schema Artifacts

```bash
# Generate Python classes from LinkML schema
gen-python schema.yaml > model.py

# Generate PlantUML diagram
gen-plantuml schema.yaml > schema.puml

# Generate Neo4j constraints
uv run python gen_neo4j_constraints.py schema.yaml > constraints.cypher
```

---

## 📜 License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.
