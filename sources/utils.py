"""
Utility functions for data sources.
"""
import pandas as pd


def normalize_name(series: pd.Series) -> pd.Series:
    """
    Normalize a name series by:
    1. Converting to uppercase
    2. Removing common prefixes (MR, MRS, MS, DR)

    Args:
        series: A pandas Series containing names

    Returns:
        A pandas Series with normalized names
    """
    result = series.str.upper()
    result = (
        result.str.replace('MR ', '', regex=False)
        .str.replace('MRS ', '', regex=False)
        .str.replace('MS ', '', regex=False)
        .str.replace('DR ', '', regex=False)
    )
    return result
