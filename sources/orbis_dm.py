from elt_core.base_source import BaseDataSource
from elt_core.transformations import to_dataframe, to_dict, propagate_company_vat, clean_vat, rename_columns
import pandas as pd

class OrbisDMSource(BaseDataSource):
    source_name = "orbis_dm"
    GROUP_COLUMN = "Company name Latin alphabet"
    VAT_COLUMN = "VAT/Tax number"
    UCI_COLUMN = "DMUCI (Unique Contact Identifier)"

    def transform(self, data):
        """
        Applies VAT propagation and cleaning.
        """
        df = to_dataframe(data)


        df = rename_columns(df, {self.GROUP_COLUMN: "company_name", 
                                 self.VAT_COLUMN: "VAT", 
                                 self.UCI_COLUMN: "UCI"})
        self.logger.info(f"Renamed columns for {len(df)} records.")
        
        self.GROUP_COLUMN = "company_name"
        self.VAT_COLUMN = "VAT" 
        self.UCI_COLUMN = "UCI"
        
        # Propagate VAT
        df = propagate_company_vat(
            df, 
            group_col=self.GROUP_COLUMN, 
            vat_col=self.VAT_COLUMN, 
            logger=self.logger
        )
        self.logger.info(f"Propagated VAT for {len(df)} records.")
        
        # Clean VAT
        df = clean_vat(
            df, 
            vat_col=self.VAT_COLUMN, 
            logger=self.logger
        )
        self.logger.info(f"Cleaned VAT for {len(df)} records.")

        # Fix datetime columns using excel dates
        df['DMAppointment date'] = pd.to_datetime(df['DMAppointment date'], unit='D', origin='1899-12-30').dt.strftime('%Y-%m-%d')
        df['DMResignation date'] = pd.to_datetime(df['DMResignation date'], unit='D', origin='1899-12-30').dt.strftime('%Y-%m-%d')
        self.logger.info(f"Fixed datetime columns for {len(df)} records.")
        
        return to_dict(df)

    def run(self, batch_size=10000):
        """
        Runs the pipeline.
        """
        self.logger.info(f"Starting pipeline for {self.source_name}...")
        
        # Extract and Load Bronze
        for raw_batch in self.extract(batch_size=batch_size):
            self.load_bronze(raw_batch, batch_size=batch_size)

        # Transform and Load Silver
        bronze_data = self.get_data('bronze')
        self.logger.info(f"Fetched {len(bronze_data)} records. Applying transformations...")
        
        transformed_data = self.transform(bronze_data)
        self.load_silver(transformed_data)
        self.logger.info(f"Pipeline finished for {self.source_name}.")
