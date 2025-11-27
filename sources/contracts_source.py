from elt_core.base_source import BaseDataSource
from elt_core.transformations import to_dataframe, to_dict, convert_dates_to_iso
import pandas as pd

allowed_contract_types = {
    "Aquisição de bens móveis",
    "Aquisição de serviços",
    "Concessão de obras públicas",
    "Concessão de serviços públicos",
    "Empreitadas de obras públicas",
    "Locação de bens móveis",
    "Sociedade",
}

class Contracts2Source(BaseDataSource):
    def transform(self, data):
        """
        Transform contracts data.
        - Convert dates to ISO format strings
        - Handle NaN values
        """
        df = to_dataframe(data)
        
        # Convert date columns to datetime objects then to string isoformat
        date_cols = ['publication_date', 'signing_date', 'close_date']
        df = convert_dates_to_iso(df, date_cols)

        # Drop id column
        df = df.drop(columns=['id'], errors='ignore')

        # Contract type transformation
        df["contract_type"] = (
            df["contract_type"]
            .astype(str)
            .str.split("<br/>")
            .apply(lambda x: [i if i in allowed_contract_types else "Outros Tipos" for i in x])
            .apply(lambda x: list(set(x)))
        )

        def _split_unique(val):
            if isinstance(val, str):
                return list({p for p in val.split("|") if p})
            if isinstance(val, list):
                return list({str(v) for v in val if pd.notna(v)})
            return []

        df = df.copy()
        df["cpvs"] = df["cpvs"].apply(_split_unique)

        return to_dict(df)
