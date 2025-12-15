# KNOWNET

KNOWNET is a robust ELT (Extract, Load, Transform) pipeline designed to ingest, process, and store data from various formats (JSON, Parquet, CSV) into a CouchDB database. It features a modular architecture that allows for easy integration of new data sources and includes comprehensive transformation logic.

## Features

- **Modular Source Architecture**: Easily extensible design for adding new data sources by inheriting from `BaseSource`.
- **Robust Database Integration**: Built-in `DBConnector` for CouchDB with automatic retries, connection pooling, and bulk document operations.
- **Data Transformation**: Core transformation utilities to clean, map, and enrich data before loading.
- **Dockerized Infrastructure**: Simple setup using Docker Compose for the CouchDB backend.
- **Type-Safe & Tested**: Includes unit tests and utilizes modern Python features.

## Project Structure

```
KNOWNET/
├── elt_core/           # Core ELT logic
│   ├── base_source.py  # Base class for all data sources
│   ├── db_connector.py # CouchDB connection and operations
│   └── transformations.py # Data transformation utilities
├── sources/            # Source-specific implementations
│   ├── contracts_source.py
│   ├── marketing_source.py
│   └── ...
├── data/               # Input data directory
├── tests/              # Unit tests
├── main.py             # Pipeline entry point
├── docker-compose.yml  # Database infrastructure
└── pyproject.toml      # Project configuration and dependencies
```

## Prerequisites

- **Python**: Version 3.12 or higher
- **Docker**: For running the CouchDB database
- **uv**: Recommended for fast dependency management (or standard `pip`)

## Installation & Setup

1.  **Clone the Repository**
    ```bash
    git clone <repository-url>
    cd KNOWNET
    ```

2.  **Configure Environment**
    Create a `.env` file in the root directory. You can use the following template:
    ```env
    COUCHDB_USER=admin
    COUCHDB_PASSWORD=password
    COUCHDB_SECRET=your_secret_key
    DATA_ROOT=./couchdb_storage
    COUCHDB_URL=http://admin:password@localhost:5984/
    ```

3.  **Start the Database**
    Run the CouchDB container using Docker Compose:
    ```bash
    docker-compose up -d
    ```
    Wait a few moments for the database to initialize. You can check the status at `http://localhost:5984/_up`.

4.  **Install Dependencies**
    Using `uv` (recommended):
    ```bash
    uv sync
    ```
    Or using `pip`:
    ```bash
    pip install -e .
    ```

## Usage

To run the ELT pipeline, execute the `main.py` script:

```bash
python main.py
```

The pipeline will:
1.  Initialize the database connection.
2.  Scan the `data/` directory for configured source files.
3.  Process each source: extract data, apply transformations, and load it into CouchDB.

## Adding a New Source

1.  Create a new class in `sources/` inheriting from `elt_core.base_source.BaseSource`.
2.  Implement the `transform` method to define your specific data logic.
3.  Register the new source in `main.py` within the `sources_config` list.

## Testing

Run the test suite using `pytest`:

```bash
pytest
```
