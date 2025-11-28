from abc import ABC, abstractmethod
import json
import csv
import datetime
from pathlib import Path
import pandas as pd
import pyarrow.dataset as ds

class BaseDataSource(ABC):
    def __init__(self, file_path, db_connector, id_column=None):
        self.file_path = Path(file_path)
        self.db_connector = db_connector
        self.id_column = id_column

    @property
    def source_name(self):
        """
        Derives source name from class name.
        e.g. MarketingSource -> marketing
        """
        return self.__class__.__name__.lower().replace('source', '')

    def extract(self, batch_size=5000):
        """
        Yields batches of data from the file.
        Supports JSON, CSV, and Parquet.
        """
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")

        ext = self.file_path.suffix.lower()
        
        if ext == '.json':
            with self.file_path.open('r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    for i in range(0, len(data), batch_size):
                        yield data[i:i + batch_size]
                else:
                    yield [data]
        elif ext == '.csv':
            for chunk in pd.read_csv(self.file_path, chunksize=batch_size):
                yield chunk.to_dict(orient='records')
        elif ext == '.parquet':
            dataset = ds.dataset(self.file_path)
            for batch in dataset.to_batches(batch_size=batch_size):
                # pyarrow batch to python list of dicts
                yield batch.to_pylist()
        else:
            raise ValueError(f"Unsupported file extension: {ext}")

    def load_bronze(self, batch_data):
        """
        Dumps a batch of raw data into the 'bronze' database.
        """
        db_name = f"{self.source_name}_bronze"
        
        docs_batch = []
        for item in batch_data:
            doc = item.copy()
            if self.id_column and self.id_column in doc:
                doc['_id'] = str(doc[self.id_column])
            docs_batch.append(doc)
        
        self.db_connector.save_documents_bulk(db_name, docs_batch)
        print(f"Saved batch of {len(docs_batch)} docs to '{db_name}'")

    @abstractmethod
    def transform(self, data):
        """
        Abstract method to transform the raw data.
        Must return a cleaned dictionary or list of dictionaries.
        """
        pass

    def load_silver(self, transformed_batch_data):
        """
        Saves a batch of transformed data to the 'silver' database.
        """
        db_name = f"{self.source_name}_silver"
        
        # Ensure data is a list
        if isinstance(transformed_batch_data, dict):
            items = [transformed_batch_data]
        elif isinstance(transformed_batch_data, list):
            items = transformed_batch_data
        else:
            raise ValueError("Transformed data must be a dict or list")

        docs_batch = []
        for item in items:
            doc = item.copy()
            if self.id_column and self.id_column in doc:
                doc['_id'] = str(doc[self.id_column])
            
            # Remove _rev if present to avoid conflicts when saving to a new database
            if '_rev' in doc:
                del doc['_rev']
                
            docs_batch.append(doc)
        
        self.db_connector.save_documents_bulk(db_name, docs_batch)
        print(f"Saved batch of {len(docs_batch)} docs to '{db_name}'")

    def get_data(self, stage):
        """
        Fetches all documents from the specified stage (bronze, silver, gold).
        """
        db_name = f"{self.source_name}_{stage}"
        print(f"Fetching all documents from {db_name}...")
        return self.db_connector.get_all_documents(db_name)

    def run(self):
        """
        Chains the steps together using Staged ELT.
        Phase 1: Ingest (Stream -> Bronze)
        Phase 2: Transform (Bronze -> Memory -> Silver)
        """
        print(f"Starting pipeline for {self.file_path}...")
        
        # Phase 1: Ingestion
        print("Phase 1: Ingestion (Stream -> Bronze)")
        total_ingested = 0
        for i, raw_batch in enumerate(self.extract()):
            self.load_bronze(raw_batch)
            print(f"Batch {i+1} ingested.")
            total_ingested += len(raw_batch)
        print(f"Ingestion complete. {total_ingested} records loaded to bronze.")

        # Phase 2: Transformation
        print("Phase 2: Transformation (Bronze -> Silver)")
        
        # Fetch ALL data from Bronze
        all_bronze_docs = self.get_data('bronze')
        
        print(f"Fetched {len(all_bronze_docs)} records. Applying transformations...")
        
        # Transform
        clean_data = self.transform(all_bronze_docs)
        
        # Load Silver (batching handled inside load_silver if we pass a list)
        if isinstance(clean_data, list):
            batch_size = 5000 # Use a reasonable batch size for silver loading
            total_transformed = len(clean_data)
            for i in range(0, total_transformed, batch_size):
                batch = clean_data[i:i + batch_size]
                self.load_silver(batch)
        else:
            # If transform returns a dict (single item), just load it
            self.load_silver(clean_data)
            
        print(f"Pipeline finished successfully.")
