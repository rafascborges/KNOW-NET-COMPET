from elt_core.base_source import BaseDataSource
from elt_core.transformations import to_dataframe, to_dict


class RIParliamentSource(BaseDataSource):

    source_name = "ri_parliament"
    
    def transform(self, data):
        """
        Normalize input: 
        """
        df = to_dataframe(data)
        #TODO: Enrich Entidade with NIF by cross matching with nif nifs_scrape_bronze 

        #TODO: Cross match names with Orbis 
            
        return to_dict(df)

    def run(self, batch_size=5000):
        """
        Runs the pipeline.
        """
        print(f"Starting pipeline for {self.file_path}...")
        for raw_batch in self.extract(batch_size=batch_size):
            self.load_bronze(raw_batch, batch_size=batch_size)

        db_name = f"{self.source_name}_bronze"
        print(f"Fetching all documents from {db_name}...")
        bronze_data = self.get_data('bronze')

        print(f"Fetched {len(bronze_data)} records. Applying transformations...")
        transformed_data = self.transform(bronze_data)
        self.load_silver(transformed_data)
