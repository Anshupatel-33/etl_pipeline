"""
etl/transform.py — Transform layer: clean, validate and enrich raw sales data.
Compatible with Python 3.8+
"""

import sys, os
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from typing import Dict
import pandas as pd
from etl.logger import logger

REQUIRED_COLUMNS = ["order_id", "customer_id", "order_date", "amount", "currency"]

# Python 3.8 compatible — use Dict from typing instead of dict[str, float]
FX_RATES: Dict[str, float] = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "INR": 0.012,
    "CAD": 0.74,
}


def transform_sales(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Starting transform on {len(df):,} rows")
    initial_count = len(df)

    df = _validate_schema(df)
    df = clean_nulls(df)
    df = cast_types(df)
    df = deduplicate(df)
    df = derive_columns(df)

    dropped = initial_count - len(df)
    logger.success(f"Transform complete — {len(df):,} rows retained, {dropped:,} dropped")
    return df


def _validate_schema(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Raw data is missing required columns: {missing}")
    logger.debug("Schema validation passed")
    return df


def clean_nulls(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.replace("", pd.NA)
    df = df.dropna(subset=["order_id", "amount", "order_date"])
    dropped = before - len(df)
    if dropped:
        logger.warning(f"Dropped {dropped:,} rows with null critical fields")
    return df.reset_index(drop=True)


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["order_id"]   = df["order_id"].astype(str).str.strip()
    df["order_date"] = pd.to_datetime(df["order_date"], infer_datetime_format=True, errors="coerce")
    df["amount"]     = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["amount", "order_date"])

    if "currency" in df.columns:
        df["currency"] = df["currency"].str.upper().str.strip().fillna("USD")
    if "customer_id" in df.columns:
        df["customer_id"] = df["customer_id"].astype(str).str.strip()
    if "product_category" in df.columns:
        df["product_category"] = df["product_category"].str.title().str.strip()

    logger.debug("Type casting complete")
    return df.reset_index(drop=True)


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset="order_id", keep="first")
    dupes = before - len(df)
    if dupes:
        logger.warning(f"Removed {dupes:,} duplicate order_id rows")
    return df.reset_index(drop=True)


def derive_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["exchange_rate"] = df["currency"].map(FX_RATES).fillna(1.0)
    df["revenue_usd"]   = (df["amount"] * df["exchange_rate"]).round(4)
    df["year_month"]    = df["order_date"].dt.to_period("M").astype(str)
    df["day_of_week"]   = df["order_date"].dt.day_name()
    df["quarter"]       = df["order_date"].dt.to_period("Q").astype(str)
    df["order_size"]    = pd.cut(
        df["amount"],
        bins=[0, 50, 200, 1000, float("inf")],
        labels=["small", "medium", "large", "enterprise"],
        right=True,
    )
    df["etl_loaded_at"] = pd.Timestamp.utcnow()
    logger.debug("Derived columns added")
    
    return df