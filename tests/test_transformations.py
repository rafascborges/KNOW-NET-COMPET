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
    convert_dates_to_iso,
    normalize_locations,
    enrich_location_from_municipality,
    enrich_location_from_district,
    map_location_values
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

def test_normalize_locations():
    countries = {"Portugal", "Spain"}
    df = pd.DataFrame([
        {
            "loc": [
                {"country": None, "district": "Portugal", "municipality": "Lisbon"},
                {"country": "Spain", "district": "Madrid", "municipality": "Madrid"}
            ]
        },
        {
            "loc": [
                {"country": None, "district": "Unknown", "municipality": "Spain"}
            ]
        }
    ])
    
    normalized = normalize_locations(df, "loc", countries)
    
    # Row 1, Item 1: Portugal moved from district to country
    locs1 = normalized.iloc[0]["loc"]
    assert locs1[0]["country"] == "Portugal"
    assert locs1[0]["district"] is None
    
    # Row 1, Item 2: Already correct, no change
    assert locs1[1]["country"] == "Spain"
    
    # Row 2, Item 1: Spain moved from municipality to country
    locs2 = normalized.iloc[1]["loc"]
    assert locs2[0]["country"] == "Spain"
    assert locs2[0]["municipality"] is None

def test_enrich_location_from_municipality():
    lookup = {"Lisbon": ("Portugal", "Lisbon District"), "Madrid": ("Spain", "Madrid District")}
    df = pd.DataFrame([
        {
            "loc": [
                {"country": None, "district": None, "municipality": "Lisbon"},
                {"country": None, "district": None, "municipality": "Unknown"}
            ]
        }
    ])
    
    enriched = enrich_location_from_municipality(df, "loc", lookup)
    
    locs = enriched.iloc[0]["loc"]
    
    # Item 1: Enriched
    assert locs[0]["country"] == "Portugal"
    assert locs[0]["district"] == "Lisbon District"
    
    # Item 2: Not enriched (unknown municipality)
    assert locs[1]["country"] is None
    assert locs[1]["district"] is None

def test_enrich_location_from_district():
    lookup = {"Lisbon District": ["Lisbon"], "Porto District": ["Porto"]}
    df = pd.DataFrame([
        {
            "loc": [
                {"country": None, "district": "Lisbon District"},
                {"country": None, "district": "Unknown District"}
            ]
        }
    ])
    
    enriched = enrich_location_from_district(df, "loc", lookup)
    
    locs = enriched.iloc[0]["loc"]
    
    # Item 1: Enriched (district in lookup)
    assert locs[0]["country"] == "Portugal"
    
    # Item 2: Not enriched (district not in lookup)
    assert locs[1]["country"] is None

def test_map_location_values():
    lookup = {"OldName": "NewName"}
    df = pd.DataFrame([
        {
            "loc": [
                {"country": "OldName", "district": "Dist1"},
                {"country": "CorrectName", "district": "Dist2"}
            ]
        }
    ])
    
    mapped = map_location_values(df, "loc", "country", lookup)
    
    locs = mapped.iloc[0]["loc"]
    
    # Item 1: Mapped
    assert locs[0]["country"] == "NewName"
    
    # Item 2: Unchanged
    assert locs[1]["country"] == "CorrectName"
