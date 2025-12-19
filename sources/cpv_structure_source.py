from elt_core.base_source import BaseDataSource
from elt_core.transformations import to_dataframe, to_dict, extract_dict_key, map_values

LEVEL_NAMES = {
    1: "Division",
    2: "Group",
    3: "Class",
    4: "Category",
}

class CPVStructureSource(BaseDataSource):

    source_name = "cpv_structure"
    
    def transform(self, data):
        """
        Normalize input: lowercase email addresses, rename keys.
        """
        df = to_dataframe(data)

        # Extract 'pt' label from 'labels' dictionary
        df = extract_dict_key(df, "labels", "pt", self.logger)
        
        # Map 'level' integer to string description
        df = map_values(df, "level", LEVEL_NAMES, self.logger)
            
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
