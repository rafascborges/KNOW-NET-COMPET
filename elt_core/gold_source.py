from typing import List, Dict, Any
from abc import ABC, abstractmethod
import logging

class BaseGoldSource(ABC):
    def __init__(self, db_connector):
        self.db_connector = db_connector
        self.logger = logging.getLogger(self.__class__.__name__)
        # Ensure logging is configured (basic config might be needed if not main)
        if not self.logger.handlers:
             logging.basicConfig(level=logging.INFO)

    def get_data(self, source_name: str) -> List[Dict[str, Any]]:
        """
        Fetches all documents from the specified Silver database.
        """
        self.logger.info(f"Fetching data from {source_name}...")
        return self.db_connector.get_all_documents(source_name)

    def save_gold(self, data: List[Dict[str, Any]], gold_db_name: str, batch_size=5000):
        """
        Saves the validated Gold data to the database in batches.
        """
        self.logger.info(f"Saving {len(data)} documents to {gold_db_name}...")
        
        # Batch saving logic
        total = len(data)
        for i in range(0, total, batch_size):
            batch = data[i:i + batch_size]
            # No _prepare_documents needed if we assume Pydantic output is clean, 
            # but db_connector.save_documents_bulk expects a list of dicts.
            # We might want to ensure _id is present if not already.
            # But usually Gold entities should have a stable ID.
            
            self.db_connector.save_documents_bulk(gold_db_name, batch)
            self.logger.info(f"Saved batch of {batch_size} documents to {gold_db_name}")

    @abstractmethod
    def run(self):
        """
        Main execution method for the Gold Source.
        """
        pass
