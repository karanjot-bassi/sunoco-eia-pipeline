"""
Unit tests for silver layer transformation logic.
Uses in-memory data only — no API calls, no file I/O.
"""
import pandas as pd
import pytest
from src.silver import clean_series


def make_raw_df(data: list) -> pd.DataFrame:
    """
    Helper to create a raw DataFrame in the shape that
    load_bronze_series produces before clean_series is called.
    """
    return pd.DataFrame(data, columns=["period", "value_mbbl_d", "series_name"])


class TestCleanSeries:

    def test_period_parsed_to_datetime(self):
        """Period column must be converted from string to datetime."""
        raw = make_raw_df([["2020-01", 100.0, "Exports"]])
        result = clean_series(raw)
        assert pd.api.types.is_datetime64_any_dtype(result["period"])

    def test_value_cast_to_float(self):
        """Value column must be numeric float."""
        raw = make_raw_df([["2020-01", "100.5", "Exports"]])
        result = clean_series(raw)
        assert result["value_mbbl_d"].dtype == float

    def test_null_values_dropped(self):
        """Rows with null values must be removed."""
        raw = make_raw_df([
            ["2020-01", 100.0,  "Exports"],
            ["2020-02", None,   "Exports"],
            ["2020-03", 200.0,  "Exports"],
        ])
        result = clean_series(raw)
        assert len(result) == 2
        assert result["value_mbbl_d"].isna().sum() == 0

    def test_schema_correct(self):
        """Output must have exactly these three columns in this order."""
        raw = make_raw_df([["2020-01", 100.0, "Exports"]])
        result = clean_series(raw)
        assert list(result.columns) == ["period", "series_name", "value_mbbl_d"]

    def test_sorted_by_period(self):
        """Rows must be sorted oldest first."""
        raw = make_raw_df([
            ["2020-03", 300.0, "Exports"],
            ["2020-01", 100.0, "Exports"],
            ["2020-02", 200.0, "Exports"],
        ])
        result = clean_series(raw)
        periods = result["period"].tolist()
        assert periods == sorted(periods)

    def test_non_numeric_value_coerced_to_null_and_dropped(self):
        """Non-numeric values must be coerced to NaN and dropped."""
        raw = make_raw_df([
            ["2020-01", "bad_data", "Exports"],
            ["2020-02", 200.0,      "Exports"],
        ])
        result = clean_series(raw)
        assert len(result) == 1
        assert result.iloc[0]["value_mbbl_d"] == 200.0