import pandas as pd
from typing import List, Dict, Any, Union, Optional
import logging

def _log_step(logger: Optional[logging.Logger], step_name: str, initial_count: int, final_count: int):
    """
    Helper to log row counts and dropped rows.
    """
    if logger:
        dropped = initial_count - final_count
        logger.info(f"{step_name}: Dropped {dropped} rows. Remaining: {final_count} rows.")

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

def filter_rows(df: pd.DataFrame, column: str, value: Any, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Filters rows where the column value matches the given value.
    """
    initial_count = len(df)
    if column not in df.columns:
        return df
    df = df[df[column] == value]
    _log_step(logger, f"Filter {column} == {value}", initial_count, len(df))
    return df

def rename_columns(df: pd.DataFrame, mapping: Dict[str, str], logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Renames columns based on the provided mapping.
    """
    df = df.rename(columns=mapping)
    if logger:
        logger.info(f"Renamed columns with mapping: {mapping}")
    return df

def drop_columns(df: pd.DataFrame, columns: List[str], logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Drops the specified columns from the DataFrame.
    """
    df = df.drop(columns=[c for c in columns if c in df.columns], errors='ignore')
    if logger:
        logger.info(f"Dropped columns: {columns}")
    return df

def add_column(df: pd.DataFrame, column_name: str, value: Any, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Adds a new column with a constant value.
    """
    df[column_name] = value
    if logger:
        logger.info(f"Added column '{column_name}' with value: {value}")
    return df

def drop_duplicates(df: pd.DataFrame, subset: List[str] = None, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Drops duplicate rows, keeping the first occurrence.
    If subset is provided, considers only those columns for identifying duplicates.
    """
    initial_count = len(df)
    df = df.drop_duplicates(subset=subset, keep='first')
    _log_step(logger, "Drop Duplicates", initial_count, len(df))
    return df

def convert_dates_to_iso(df: pd.DataFrame, columns: List[str], logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Converts specified columns to datetime objects and then to ISO format strings (YYYY-MM-DD).
    Handles errors by coercing to NaT (which will be handled by to_dict later).
    """
    # This transformation doesn't drop rows, but we can log that it happened
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
    
    if logger:
        logger.info(f"Converted dates to ISO for columns: {columns}")
    return df

def normalize_locations(df: pd.DataFrame, column: str, countries_set: set, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Normalizes location entries in the specified column.
    Moves country names from 'district' or 'municipality' to 'country'.
    """
    if column not in df.columns:
        return df

    def _fix_one(locations_list):
        if not isinstance(locations_list, list):
            return locations_list
        corrected_list = []
        for loc in locations_list:
            new_loc = loc.copy() if isinstance(loc, dict) else loc
            if isinstance(new_loc, dict):
                found_country = None
                district_val = new_loc.get("district")
                municipality_val = new_loc.get("municipality")
                
                if district_val and district_val in countries_set:
                    found_country = district_val
                elif municipality_val and municipality_val in countries_set:
                    found_country = municipality_val
                
                if found_country:
                    new_loc["country"] = found_country
                    new_loc["district"] = None
                    new_loc["municipality"] = None
            corrected_list.append(new_loc)
        return corrected_list

    # Use list comprehension for better performance than apply
    df[column] = [_fix_one(x) for x in df[column]]
    
    if logger:
        logger.info(f"Normalized locations in column: {column}")
    return df

def enrich_location_from_municipality(df: pd.DataFrame, column: str, lookup: Dict[str, Any], logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Back-propagates municipality information to district and country fields using a lookup dictionary.
    Lookup format: {municipality: (country, district)}
    """
    if column not in df.columns:
        return df

    def _fix_one(locations_list):
        if not isinstance(locations_list, list):
            return locations_list
        corrected_list = []
        for loc in locations_list:
            new_loc = loc.copy() if isinstance(loc, dict) else loc
            if isinstance(new_loc, dict) and "municipality" in new_loc:
                municipality = new_loc.get("municipality")
                match = lookup.get(municipality)
                if match and match != "AMBIGUOUS":
                    country, district = match
                    new_loc["country"] = country
                    new_loc["district"] = district
            corrected_list.append(new_loc)
        return corrected_list

    # Use list comprehension for better performance
    df[column] = [_fix_one(x) for x in df[column]]
    
    if logger:
        logger.info(f"Enriched locations from municipality in column: {column}")
    return df

def enrich_location_from_district(df: pd.DataFrame, column: str, lookup: Dict[str, Any], logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    !!! ONLY FOR PORTUGAL DISTRICTS!!!
    Back-propagates district information to country fields using a lookup dictionary. 
    Lookup format: {district: [municipality1, municipality2, ...]}
    """
    if column not in df.columns:
        return df

    def _fix_one(locations_list):
        if not isinstance(locations_list, list):
            return locations_list
        corrected_list = []
        for loc in locations_list:
            new_loc = loc.copy() if isinstance(loc, dict) else loc
            if isinstance(new_loc, dict) and "district" in new_loc:
                district = new_loc.get("district")
                if district in lookup:
                    new_loc["country"] = "Portugal"
            corrected_list.append(new_loc)
        return corrected_list

    # Use list comprehension for better performance
    df[column] = [_fix_one(x) for x in df[column]]
    
    if logger:
        logger.info(f"Enriched locations from district in column: {column}")
    return df


def map_location_fixes(df: pd.DataFrame, column: str, level: str, lookup: Dict[str, str], logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Maps values of a specific level (e.g. 'country', 'district', 'municipality') in the location list
    using a lookup dictionary.
    """
    if column not in df.columns:
        return df

    def _fix_one(locations_list):
        if not isinstance(locations_list, list):
            return locations_list
        corrected_list = []
        for loc in locations_list:
            new_loc = loc.copy() if isinstance(loc, dict) else loc
            if isinstance(new_loc, dict) and level in new_loc:
                old_val = new_loc.get(level)
                if old_val in lookup:
                    new_loc[level] = lookup[old_val]
            corrected_list.append(new_loc)
        return corrected_list

    # Use list comprehension for better performance
    df[column] = [_fix_one(x) for x in df[column]]
    
    if logger:
        logger.info(f"Mapped location fixes for {level} in column: {column}")
    return df

# --- New Transformations ---

def transform_contract_type(df: pd.DataFrame, column: str, allowed_types: set, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Transforms contract types, normalizing them against an allowed set.
    """
    if column in df.columns:
        df[column] = [
            list(set([t if t in allowed_types else "Outros Tipos" for t in types]))
            for types in df[column].astype(str).str.split('<br/>')
        ]
        if logger:
            logger.info(f"Transformed contract types in column: {column}")
    return df

def transform_cpvs(df: pd.DataFrame, column: str, max_length: int = 20, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Transforms CPVs by splitting and deduplicating, then filters out rows with too many CPVs.
    """
    initial_count = len(df)
    if column in df.columns:
        # Split by pipe, then split by hyphen and take first part, then deduplicate
        df[column] = [
            list({p.split("-", 1)[0] for p in val.split("|") if p}) if isinstance(val, str) else []
            for val in df[column]
        ]
        # Remove rows where cpvs length is greater than max_length
        df = df[df[column].apply(len) <= max_length]
    
    _log_step(logger, f"Transform CPVs (max_len={max_length})", initial_count, len(df))
    return df

def filter_dropna(df: pd.DataFrame, subset: List[str], logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Drops rows with missing values in the specified subset of columns.
    """
    initial_count = len(df)
    if all(col in df.columns for col in subset):
        df = df.dropna(subset=subset)
    _log_step(logger, f"DropNA subset={subset}", initial_count, len(df))
    return df

def filter_max_value(df: pd.DataFrame, column: str, max_value: float, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Filters rows where the column value exceeds the max_value.
    """
    initial_count = len(df)
    if column in df.columns:
        df = df[df[column] <= max_value]
    _log_step(logger, f"Filter Max Value {column} <= {max_value}", initial_count, len(df))
    return df

def filter_price_anomalies(df: pd.DataFrame, initial_price_col: str, final_price_col: str, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Removes rows where initial_price is 0 and final_price is NaN, or initial_price is less than 0.
    """
    initial_count = len(df)
    if initial_price_col in df.columns and final_price_col in df.columns:
    
        condition = ~((df[initial_price_col] == 0) & (df[final_price_col].isna()) | (df[initial_price_col] < 0))
        df = df[condition]
        
    _log_step(logger, "Filter Price Anomalies", initial_count, len(df))
    return df

def filter_date_sequence(df: pd.DataFrame, start_date_col: str, end_date_col: str, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Filters rows where the start_date is after the end_date (e.g. signing_date < publication_date).
    Wait, usually signing >= publication.
    Original: df = df[df['signing_date'] >= df['publication_date']]
    So we keep rows where signing is after or equal to publication.
    """
    initial_count = len(df)
    if start_date_col in df.columns and end_date_col in df.columns:
        # Ensure we are comparing dates, assuming they are already ISO strings or datetime
        # If they are strings, direct comparison works for ISO format.
        df = df[df[start_date_col] >= df[end_date_col]]
        
    _log_step(logger, f"Filter Date Sequence {start_date_col} >= {end_date_col}", initial_count, len(df))
    return df
def extract_dict_key(df: pd.DataFrame, column: str, key: str, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Extracts a specific key from a column containing dictionaries.
    If the value is not a dict or key is missing, returns None.
    """
    if column in df.columns:
        def _extract(val):
            if isinstance(val, dict):
                return val.get(key)
            return None
        
        df[column] = df[column].apply(_extract)
        
        if logger:
            logger.info(f"Extracted key '{key}' from column: {column}")
    return df

def map_values(df: pd.DataFrame, column: str, mapping: Dict[Any, Any], logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Maps values in a column using a lookup dictionary.
    Values not found in the mapping are left unchanged.
    """
    if column in df.columns:
        df[column] = df[column].map(lambda x: mapping.get(x, x))
        
        if logger:
            logger.info(f"Mapped values in column: {column}")
    return df
def propagate_company_vat(
    df: pd.DataFrame,
    group_col: str,
    vat_col: str,
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Propagates VAT numbers within groups defined by group_col.
    Fills missing VATs by forward and backward filling within each group.
    """
    initial_count = len(df)
    if group_col not in df.columns or vat_col not in df.columns:
        if logger:
            logger.warning(f"Missing columns for propagation: {group_col}, {vat_col}")
        return df

    # Ensure VAT column is string for consistent filling
    # We use a temporary column to avoid modifying the original until we assign back
    # But here we want to modify the dataframe.
    
    # Logic from user:
    # df[vat_col] = (
    #     df[vat_col].astype("string")
    #     .groupby(df[group_col])
    #     .transform(lambda x: x.ffill().bfill())
    # )
    
    # We need to be careful with types.
    df[vat_col] = (
        df[vat_col].astype("string")
        .groupby(df[group_col])
        .transform(lambda x: x.ffill().bfill())
    )
    
    if logger:
        logger.info(f"Propagated VAT in column {vat_col} grouped by {group_col}")
    return df

def clean_vat(df: pd.DataFrame, vat_col: str, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Normalizes VAT numbers: strip decimals and enforce 9-digit numeric identifiers.
    Drops rows where VAT is missing or invalid.
    """
    initial_count = len(df)
    if vat_col not in df.columns:
        if logger:
            logger.warning(f"Missing VAT column '{vat_col}'")
        return df

    # Strip decimals (e.g. "123456789.0" -> "123456789")
    df[vat_col] = df[vat_col].astype(str).str.split(".").str[0]
    
    # Drop NaNs
    df = df.dropna(subset=[vat_col])
    
    # Keep only 9-digit numeric
    mask = df[vat_col].str.fullmatch(r"\d{9}")
    df = df.loc[mask].copy()
    
    _log_step(logger, f"Clean VAT {vat_col}", initial_count, len(df))
    return df
