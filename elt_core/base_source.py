from abc import ABC, abstractmethod
import logging
import os
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
        self.logger = self._setup_logger()

    def _setup_logger(self):
        """
        Configures a logger for the source.
        """
        log_path = os.getenv("LOG_PATH", "logs")
        log_dir = Path(log_path)
        log_dir.mkdir(parents=True, exist_ok=True)

        logger = logging.getLogger(self.source_name)
        
        log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
        log_level = getattr(logging, log_level_str, logging.INFO)
        logger.setLevel(log_level)

        # Avoid adding handlers if they already exist
        if not logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

            # File Handler
            file_handler = logging.FileHandler(log_dir / f"{self.source_name}.log")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            # Stream Handler
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)
        
        return logger

    @property
    @abstractmethod
    def source_name(self):
        """
        Abstract property for the source name.
        Must be implemented by subclasses.
        """
        pass

    def _prepare_documents(self, items):
        """
        Prepares a list of items for saving to the database.
        - Sets _id from id_column if present.
        - Removes _rev to avoid conflicts.
        """
        docs_batch = []
        for item in items:
            doc = item.copy()
            if self.id_column and self.id_column in doc:
                doc['_id'] = str(doc[self.id_column])
            
            # Remove _rev if present to avoid conflicts when saving to a new database
            if '_rev' in doc:
                del doc['_rev']
                
            docs_batch.append(doc)
        return docs_batch

    def _save_in_batches(self, items, db_name, batch_size=5000):
        """
        Prepares and saves items to the database in batches.
        """
        if isinstance(items, dict):
            items = [items]
            
        total = len(items)
        for i in range(0, total, batch_size):
            batch = items[i:i + batch_size]
            docs_batch = self._prepare_documents(batch)
            self.db_connector.save_documents_bulk(db_name, docs_batch)
            print(f"Saved batch of {len(docs_batch)} docs to '{db_name}'")

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

    def load_bronze(self, batch_data, batch_size=5000):
        """
        Dumps a batch of raw data into the 'bronze' database.
        """
        db_name = f"{self.source_name}_bronze"
        self._save_in_batches(batch_data, db_name, batch_size)

    @abstractmethod
    def transform(self, data):
        """
        Abstract method to transform the raw data.
        Must return a cleaned dictionary or list of dictionaries.
        """
        pass

    def load_silver(self, transformed_batch_data, batch_size=5000):
        """
        Saves a batch of transformed data to the 'silver' database.
        """
        db_name = f"{self.source_name}_silver"
        self._save_in_batches(transformed_batch_data, db_name, batch_size)

    def get_data(self, stage):
        """
        Fetches all documents from the specified stage (bronze, silver, gold).
        """
        db_name = f"{self.source_name}_{stage}"
        print(f"Fetching all documents from {db_name}...")
        return self.db_connector.get_all_documents(db_name)

    @abstractmethod
    def run(self):
        """
        Abstract method to run the pipeline.
        """
        pass