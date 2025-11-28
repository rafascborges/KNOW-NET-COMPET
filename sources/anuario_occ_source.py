from elt_core.base_source import BaseDataSource
from elt_core.transformations import to_dataframe, to_dict

class AnuarioOCCSource(BaseDataSource):
    def transform(self, data):
        """
        Normalize input: lowercase email addresses, rename keys.
        """
        df = to_dataframe(data)
            
        return to_dict(df)

