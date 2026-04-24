"""
Validation tests against the actual gold layer output.
These check that the real data is reasonable, not just that the code works.
Run after gold.py has been executed.
"""
import pandas as pd
import pytest
from src.config import GOLD_DIR, SUPPLY_COLS, DISPOSITION_COLS

GOLD_PATH = GOLD_DIR / "distillate_padd3_annual.csv"

# Columns that must never be null in the gold layer
# Adjustments is excluded — it only has data through 2020
REQUIRED_COLS = [
    "Exports",
    "Net Receipts",
    "Products Supplied",
    "Refinery and Blender Net Production",
    "Stock Change",
]


@pytest.fixture(scope="module")
def gold():
    """Load gold layer once and share across all tests in this module."""
    return pd.read_csv(GOLD_PATH)


def test_gold_file_exists():
    """Gold CSV must exist before any other validation can run."""
    assert GOLD_PATH.exists(), f"Gold file not found at {GOLD_PATH}"


def test_minimum_year_coverage(gold):
    """Must have at least 10 complete years of data (2015-2024)."""
    complete_years = gold[gold["year"] < 2026]
    assert len(complete_years) >= 10, (
        f"Expected at least 10 complete years, got {len(complete_years)}"
    )


def test_no_nulls_in_required_columns(gold):
    """Core series must have no null values for complete years."""
    complete = gold[gold["year"] < 2026]
    for col in REQUIRED_COLS:
        null_count = complete[col].isna().sum()
        assert null_count == 0, (
            f"Column '{col}' has {null_count} null values in complete years"
        )


def test_no_negative_production(gold):
    """Refinery production must always be positive."""
    assert (gold["Refinery and Blender Net Production"] > 0).all(), (
        "Negative refinery production values found"
    )


def test_no_negative_exports(gold):
    """Exports must always be positive."""
    assert (gold["Exports"] > 0).all(), (
        "Negative export values found"
    )


def test_net_receipts_always_negative(gold):
    """
    Net Receipts must always be negative for PADD 3.
    PADD 3 is always a net sender to other regions, never a net receiver.
    """
    assert (gold["Net Receipts"] < 0).all(), (
        "Positive Net Receipts found — PADD 3 should always be a net sender"
    )


def test_production_exceeds_exports(gold):
    """
    Refinery production must exceed exports in every year.
    PADD 3 cannot export more than it produces.
    """
    assert (
        gold["Refinery and Blender Net Production"] > gold["Exports"]
    ).all(), "Exports exceed refinery production in some years"


def test_reasonable_production_range(gold):
    """
    Annual average refinery production must be between 1500 and 4000 MBBL/D.
    Values outside this range indicate a data error.
    """
    assert gold["Refinery and Blender Net Production"].between(1500, 4000).all(), (
        "Refinery production values outside expected range of 1500-4000 MBBL/D"
    )


def test_supply_disposition_balance(gold):
    """
    Sum of supply columns should approximately equal sum of disposition columns.
    This is a physical identity — barrels must go somewhere.
    Tolerance: 20% to account for Adjustments series gap post-2020.
    """
    supply_cols = [c for c in SUPPLY_COLS if c in gold.columns]
    disp_cols   = [c for c in DISPOSITION_COLS if c in gold.columns]

    # Only test years where Adjustments data exists (2015-2020)
    complete = gold[gold["year"] <= 2020].copy()
    complete = complete.fillna(0)

    supply_total = complete[supply_cols].sum(axis=1)
    disp_total   = complete[disp_cols].sum(axis=1)

    imbalance    = (supply_total - disp_total).abs()
    tolerance    = supply_total.abs() * 0.20

    failures = imbalance[imbalance > tolerance]
    assert len(failures) == 0, (
        f"Supply/disposition imbalance exceeds 20% in years: "
        f"{complete.loc[failures.index, 'year'].tolist()}"
    )