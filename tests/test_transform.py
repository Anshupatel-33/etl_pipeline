"""
tests/test_transform.py
────────────────────────
Unit tests for the transform layer.
Run with:  pytest tests/ -v
"""

import pytest
import pandas as pd
from datetime import datetime

from etl.transform import (
    transform_sales,
    clean_nulls,
    cast_types,
    deduplicate,
    derive_columns,
    REQUIRED_COLUMNS,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def valid_raw_df() -> pd.DataFrame:
    """Minimal valid DataFrame that should pass all transforms."""
    return pd.DataFrame({
        "order_id"        : ["ORD-001", "ORD-002", "ORD-003"],
        "customer_id"     : ["CUST-1",  "CUST-2",  "CUST-3"],
        "order_date"      : ["2024-01-01", "2024-02-15", "2024-03-20"],
        "amount"          : ["100.00",     "250.50",      "75.00"],
        "currency"        : ["USD",        "EUR",         "GBP"],
        "product_category": ["Electronics", "Clothing",   "Books"],
    })


@pytest.fixture
def df_with_nulls(valid_raw_df) -> pd.DataFrame:
    """Valid DataFrame with some intentional nulls."""
    df = valid_raw_df.copy()
    df.loc[1, "order_id"] = None     # should be dropped
    df.loc[2, "amount"]   = ""       # should be dropped
    return df


@pytest.fixture
def df_with_dupes(valid_raw_df) -> pd.DataFrame:
    """Valid DataFrame with a duplicate order_id."""
    df = valid_raw_df.copy()
    df.loc[2, "order_id"] = "ORD-001"   # duplicate of row 0
    return df


# ── Schema validation tests ───────────────────────────────────────────────────

def test_missing_required_column_raises():
    """transform_sales should raise ValueError if a required column is absent."""
    bad_df = pd.DataFrame({"order_id": ["x"], "amount": ["10"]})
    with pytest.raises(ValueError, match="missing required columns"):
        transform_sales(bad_df)


# ── clean_nulls tests ─────────────────────────────────────────────────────────

def test_clean_nulls_drops_null_order_id(df_with_nulls):
    result = clean_nulls(df_with_nulls)
    assert "ORD-001" in result["order_id"].values
    assert result["order_id"].isna().sum() == 0


def test_clean_nulls_drops_empty_string_amount(df_with_nulls):
    result = clean_nulls(df_with_nulls)
    # Row 2 had empty amount — should be gone
    assert len(result) < len(df_with_nulls)


# ── cast_types tests ──────────────────────────────────────────────────────────

def test_cast_types_converts_amount_to_float(valid_raw_df):
    result = cast_types(valid_raw_df)
    assert result["amount"].dtype == float


def test_cast_types_converts_order_date_to_datetime(valid_raw_df):
    result = cast_types(valid_raw_df)
    assert pd.api.types.is_datetime64_any_dtype(result["order_date"])


def test_cast_types_uppercases_currency(valid_raw_df):
    df = valid_raw_df.copy()
    df["currency"] = ["usd", "eur", "gbp"]
    result = cast_types(df)
    assert all(result["currency"] == result["currency"].str.upper())


# ── deduplicate tests ─────────────────────────────────────────────────────────

def test_dedup_removes_duplicate_order_ids(df_with_dupes):
    df    = cast_types(df_with_dupes)
    result = deduplicate(df)
    assert result["order_id"].nunique() == len(result)


def test_dedup_keeps_first_occurrence(df_with_dupes):
    df     = cast_types(df_with_dupes)
    result = deduplicate(df)
    # ORD-001 first row had amount 100.00
    row = result[result["order_id"] == "ORD-001"].iloc[0]
    assert float(row["amount"]) == 100.00


# ── derive_columns tests ──────────────────────────────────────────────────────

def test_derive_revenue_usd_for_usd_is_same_as_amount(valid_raw_df):
    df = cast_types(valid_raw_df)
    df = df[df["currency"] == "USD"].reset_index(drop=True)
    result = derive_columns(df)
    # USD rate = 1.0, so revenue_usd == amount
    pd.testing.assert_series_equal(
        result["revenue_usd"], result["amount"], check_names=False, rtol=1e-4
    )


def test_derive_year_month_format(valid_raw_df):
    df = cast_types(valid_raw_df)
    result = derive_columns(df)
    # Should look like "2024-01"
    assert result["year_month"].str.match(r"\d{4}-\d{2}").all()


def test_derive_order_size_categories(valid_raw_df):
    df = cast_types(valid_raw_df)
    result = derive_columns(df)
    valid_sizes = {"small", "medium", "large", "enterprise"}
    assert set(result["order_size"].astype(str).unique()) <= valid_sizes


def test_derive_etl_loaded_at_is_timestamp(valid_raw_df):
    df = cast_types(valid_raw_df)
    result = derive_columns(df)
    assert pd.api.types.is_datetime64_any_dtype(result["etl_loaded_at"])


# ── End-to-end transform test ─────────────────────────────────────────────────

def test_full_transform_returns_expected_columns(valid_raw_df):
    result = transform_sales(valid_raw_df)
    expected_cols = ["order_id", "revenue_usd", "year_month", "order_size", "etl_loaded_at"]
    for col in expected_cols:
        assert col in result.columns, f"Missing expected column: {col}"


def test_full_transform_no_nulls_in_critical_cols(valid_raw_df):
    result = transform_sales(valid_raw_df)
    for col in ["order_id", "amount", "revenue_usd"]:
        assert result[col].isna().sum() == 0, f"Unexpected nulls in {col}"
