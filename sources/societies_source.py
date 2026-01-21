from elt_core.base_source import BaseDataSource
from elt_core.transformations import to_dataframe, to_dict
from elt_core.transformations import normalize_name
from elt_core.transformations import roman_to_int


class SocietiesSource(BaseDataSource):

    source_name = "societies_source"
    
    def transform(self, data):
        """
        Normalize input: 
        """
        df = to_dataframe(data)

        # NORMALIZE NAME 
        df['Nome'] = normalize_name(df['Nome'])
        
        # Strip NIPC 
        df['NIPC'] = df['NIPC'].str.strip()

        # Load nifs_scrape_silver and get nif set
        nifs_scrape_silver = self.db_connector.get_all_documents('nifs_scrape_silver')
        nif_set = {doc.get('_id') for doc in nifs_scrape_silver if doc.get('_id')}
        
        # Add BASE_MATCHING field: True if NIPC exists in nifs_scrape_silver
        df['BASE_MATCHING'] = df['NIPC'].isin(nif_set)

        # Transform Roman Algorism into int
        df['Governo'] = df['Governo'].apply(roman_to_int)

        # Transform Parliament float into string (via int to remove .0)
        # Handle NaN values by filling with empty string
        df['Legislatura'] = df['Legislatura'].fillna(0).astype(int).astype(str).replace('0', '')

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

        self.logger.info(f"Fetched {len(bronze_data)} records. Applying transformations...")
        transformed_data = self.transform(bronze_data)
        self.load_silver(transformed_data)
