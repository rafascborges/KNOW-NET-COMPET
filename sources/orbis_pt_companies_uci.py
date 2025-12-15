from elt_core.base_source import BaseDataSource
from elt_core.transformations import to_dataframe, to_dict, propagate_company_vat, clean_vat

class OrbisPTCompaniesUCISource(BaseDataSource):
    GROUP_COLUMN = "Company name Latin alphabet"
    VAT_COLUMN = "VAT/Tax number"
    
    # Columns to check for filtering
    DM_UCI_COLUMN = "DMUCI (Unique Contact Identifier)"
    SH_UCI_COLUMN = "SH - UCI"

    def transform(self, data):
        """
        Applies VAT propagation, cleaning, and filtering based on UCIs from DM and SH sources.
        """
        df = to_dataframe(data)
        
        # 1. Propagate VAT
        df = propagate_company_vat(
            df, 
            group_col=self.GROUP_COLUMN, 
            vat_col=self.VAT_COLUMN, 
            logger=self.logger
        )
        
        # 2. Clean VAT
        df = clean_vat(
            df, 
            vat_col=self.VAT_COLUMN, 
            logger=self.logger
        )
        
        # 3. Filter by UCIs from other sources
        # We need to fetch the UCIs from the silver collections of the other sources
        try:
            dm_docs = self.db_connector.get_all_documents("orbisdm_silver")
            sh_docs = self.db_connector.get_all_documents("orbissh_silver")
            
            dm_uci_set = {str(d.get(self.DM_UCI_COLUMN)) for d in dm_docs if d.get(self.DM_UCI_COLUMN)}
            sh_uci_set = {str(d.get(self.SH_UCI_COLUMN)) for d in sh_docs if d.get(self.SH_UCI_COLUMN)}
            
            self.logger.info(f"Loaded {len(dm_uci_set)} DM UCIs and {len(sh_uci_set)} SH UCIs for filtering.")
            
            initial_count = len(df)
            
            # Create mask
            mask = pd.Series(False, index=df.index)
            
            if self.DM_UCI_COLUMN in df.columns and dm_uci_set:
                mask |= df[self.DM_UCI_COLUMN].astype(str).isin(dm_uci_set)
                
            if self.SH_UCI_COLUMN in df.columns and sh_uci_set:
                mask |= df[self.SH_UCI_COLUMN].astype(str).isin(sh_uci_set)
            
            # Apply filter strictly
            df = df.loc[mask].copy()

            self.logger.info(f"Filtered rows based on UCI: {initial_count} -> {len(df)}")

        except Exception as e:
            self.logger.warning(f"Could not fetch/filter by UCIs from other sources. Error: {e}")

        return to_dict(df)

    def run(self, batch_size=5000):
        """
        Runs the pipeline.
        """
        self.logger.info(f"Starting pipeline for {self.source_name}...")
        
        # Extract and Load Bronze
        # Note: The user mentioned "low_memory=False" for this file in pandas read_csv.
        # Our extract method uses chunking for CSVs, so memory shouldn't be an issue.
        for raw_batch in self.extract(batch_size=batch_size):
            self.load_bronze(raw_batch, batch_size=batch_size)

        # Transform and Load Silver
        bronze_data = self.get_data('bronze')
        self.logger.info(f"Fetched {len(bronze_data)} records. Applying transformations...")
        
        transformed_data = self.transform(bronze_data)
        self.load_silver(transformed_data)
        self.logger.info(f"Pipeline finished for {self.source_name}.")
