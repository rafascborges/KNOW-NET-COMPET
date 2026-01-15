from elt_core.base_source import BaseDataSource
from elt_core.transformations import to_dataframe, to_dict
from elt_core.transformations import normalize_name


class PeopleAreaSource(BaseDataSource):

    source_name = "people_area"
    
    def transform(self, data):
        """
        Normalize input: 
        """
        df = to_dataframe(data)

        self.logger.info(f"Transforming {len(df)} records.")
        # NORMALIZE NAME (some entries have 'Nome Completo' instead of 'Nome')
        df['Nome'] = df['Nome'].fillna(df.get('Nome Completo', ''))
        df['Nome'] = normalize_name(df['Nome'])

        # Transform Governo float into string (via int to remove .0)
        # Handle NaN values by filling with empty string
        df['Governo'] = df['Governo'].fillna(0).astype(int).astype(str).replace('0', '')

        # Transform Legislatura float into string (via int to remove .0)
        # Handle NaN values by filling with empty string
        df['Legislatura'] = df['Legislatura'].fillna(0).astype(int).astype(str).replace('0', '')

        self.logger.info(f"Transformed {len(df)} records.")


        return to_dict(df)

    def run(self, batch_size=5000):
        """
        Runs the pipeline.
        """
        self.logger.info(f"Starting pipeline for {self.file_path}...")
        for raw_batch in self.extract(batch_size=batch_size):
            self.load_bronze(raw_batch, batch_size=batch_size)

        db_name = f"{self.source_name}_bronze"
        self.logger.info(f"Fetching all documents from {db_name}...")
        bronze_data = self.get_data('bronze')

        print(f"Fetched {len(bronze_data)} records. Applying transformations...")
        transformed_data = self.transform(bronze_data)
        self.load_silver(transformed_data)
