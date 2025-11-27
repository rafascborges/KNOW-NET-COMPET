import pandas as pd
from typing import List, Dict, Any, Union

def to_dataframe(data: Union[List[Dict[str, Any]], Dict[str, Any]]) -> pd.DataFrame:
    """
    Converts a list of dictionaries or a single dictionary to a pandas DataFrame.
    """
    if isinstance(data, dict):
        data = [data]
    return pd.DataFrame(data)

def to_dict(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Converts a pandas DataFrame back to a list of dictionaries.
    Also handles NaN values by replacing them with None.
    """
    df = df.astype(object).where(pd.notnull(df), None)
    return df.to_dict(orient='records')

def filter_rows(df: pd.DataFrame, column: str, value: Any) -> pd.DataFrame:
    """
    Filters rows where the column value matches the given value.
    """
    if column not in df.columns:
        return df
    return df[df[column] == value]

def rename_columns(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Renames columns based on the provided mapping.
    """
    return df.rename(columns=mapping)

def drop_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Drops the specified columns from the DataFrame.
    """
    return df.drop(columns=[c for c in columns if c in df.columns], errors='ignore')

def add_column(df: pd.DataFrame, column_name: str, value: Any) -> pd.DataFrame:
    """
    Adds a new column with a constant value.
    """
    df[column_name] = value
    return df

def drop_duplicates(df: pd.DataFrame, subset: List[str] = None) -> pd.DataFrame:
    """
    Drops duplicate rows, keeping the first occurrence.
    If subset is provided, considers only those columns for identifying duplicates.
    """
    return df.drop_duplicates(subset=subset, keep='first')

def convert_dates_to_iso(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Converts specified columns to datetime objects and then to ISO format strings (YYYY-MM-DD).
    Handles errors by coercing to NaT (which will be handled by to_dict later).
    """
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
    return df
