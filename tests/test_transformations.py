import pytest
import pandas as pd
from elt_core.transformations import (
    to_dataframe,
    to_dict,
    filter_rows,
    rename_columns,
    drop_columns,
    add_column,
    drop_duplicates,
    convert_dates_to_iso
)

def test_to_dataframe_list():
    data = [{"a": 1}, {"a": 2}]
    df = to_dataframe(data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.iloc[0]["a"] == 1

def test_to_dataframe_dict():
    data = {"a": 1}
    df = to_dataframe(data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["a"] == 1

def test_to_dict():
    df = pd.DataFrame([{"a": 1}, {"a": 2}])
    data = to_dict(df)
    assert isinstance(data, list)
    assert len(data) == 2
    assert data[0]["a"] == 1

def test_filter_rows():
    df = pd.DataFrame([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
    filtered = filter_rows(df, "a", 1)
    assert len(filtered) == 1
    assert filtered.iloc[0]["b"] == "x"

def test_filter_rows_missing_column():
    df = pd.DataFrame([{"a": 1}])
    filtered = filter_rows(df, "z", 1)
    assert len(filtered) == 1  # Should return original df

def test_rename_columns():
    df = pd.DataFrame([{"a": 1}])
    renamed = rename_columns(df, {"a": "b"})
    assert "b" in renamed.columns
    assert "a" not in renamed.columns

def test_drop_columns():
    df = pd.DataFrame([{"a": 1, "b": 2}])
    dropped = drop_columns(df, ["a"])
    assert "a" not in dropped.columns
    assert "b" in dropped.columns

def test_add_column():
    df = pd.DataFrame([{"a": 1}])
    added = add_column(df, "b", 2)
    assert "b" in added.columns
    assert added.iloc[0]["b"] == 2

def test_drop_duplicates():
    df = pd.DataFrame([{"a": 1}, {"a": 1}, {"a": 2}])
    deduped = drop_duplicates(df)
    assert len(deduped) == 2
    assert deduped.iloc[0]["a"] == 1
    assert deduped.iloc[1]["a"] == 2

def test_drop_duplicates_subset():
    df = pd.DataFrame([{"a": 1, "b": 1}, {"a": 1, "b": 2}])
    deduped = drop_duplicates(df, subset=["a"])
    assert len(deduped) == 1
    assert deduped.iloc[0]["b"] == 1

def test_convert_dates_to_iso():
    df = pd.DataFrame([
        {"date": "2023-01-01 10:00:00", "other": "val"},
        {"date": "invalid", "other": "val2"}
    ])
    converted = convert_dates_to_iso(df, ["date"])
    assert converted.iloc[0]["date"] == "2023-01-01"
    # Invalid date becomes NaT, which is handled by to_dict later, 
    # but here it stays as NaT in the DataFrame
    assert pd.isna(converted.iloc[1]["date"])
