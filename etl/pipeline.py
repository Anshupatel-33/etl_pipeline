"""
etl/pipeline.py
───────────────
Orchestrates the full ETL run:  Extract → Transform → Load → Summary

Usage
─────
  python -m etl.pipeline                      # runs with default settings
  python -m etl.pipeline --source csv         # CSV only
  python -m etl.pipeline --source api         # API only
  python -m etl.pipeline --table sales_clean  # custom target table
  python -m etl.pipeline --mode upsert        # upsert instead of append
"""

import sys, os
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import argparse
import time

from etl.logger    import logger
from etl.extract   import extract_all_csvs, extract_from_api
from etl.transform import transform_sales
from etl.load      import load_to_postgres, load_summary_table, get_engine
from config.settings import RAW_DATA_PATH


def run_pipeline(
    source: str = "csv",
    table:  str = "sales_clean",
    mode:   str = "upsert",
) -> dict:
    """
    Execute the full ETL pipeline.

    Parameters
    ----------
    source : "csv" | "api" | "both"
    table  : PostgreSQL target table name
    mode   : "append" | "replace" | "upsert"

    Returns
    -------
    dict
        Run metadata: rows_extracted, rows_loaded, duration_seconds, status
    """
    start = time.perf_counter()
    logger.info("=" * 60)
    logger.info(f"ETL PIPELINE STARTED  source={source}  table={table}  mode={mode}")
    logger.info("=" * 60)

    # ── 1. EXTRACT ────────────────────────────────────────────────────────────
    logger.info("Step 1/3 — EXTRACT")
    try:
        if source == "csv":
            raw_df = extract_all_csvs(RAW_DATA_PATH)
        elif source == "api":
            raw_df = extract_from_api()
        else:                                            # "both"
            import pandas as pd
            csv_df = extract_all_csvs(RAW_DATA_PATH)
            api_df = extract_from_api()
            raw_df = pd.concat([csv_df, api_df], ignore_index=True)

        rows_extracted = len(raw_df)
        logger.info(f"Extracted {rows_extracted:,} raw rows")

    except Exception as exc:
        logger.error(f"EXTRACT failed: {exc}")
        return {"status": "failed", "step": "extract", "error": str(exc)}

    # ── 2. TRANSFORM ──────────────────────────────────────────────────────────
    logger.info("Step 2/3 — TRANSFORM")
    try:
        clean_df = transform_sales(raw_df)
    except Exception as exc:
        logger.error(f"TRANSFORM failed: {exc}")
        return {"status": "failed", "step": "transform", "error": str(exc)}

    # ── 3. LOAD ───────────────────────────────────────────────────────────────
    logger.info("Step 3/3 — LOAD")
    try:
        rows_loaded = load_to_postgres(clean_df, table=table, mode=mode)
        load_summary_table(clean_df, engine=get_engine())
    except Exception as exc:
        logger.error(f"LOAD failed: {exc}")
        return {"status": "failed", "step": "load", "error": str(exc)}

    # ── Done ──────────────────────────────────────────────────────────────────
    duration = round(time.perf_counter() - start, 2)
    logger.info("=" * 60)
    logger.success(
        f"PIPELINE COMPLETE — {rows_loaded:,} rows loaded in {duration}s"
    )
    logger.info("=" * 60)

    return {
        "status":           "success",
        "rows_extracted":   rows_extracted,
        "rows_loaded":      rows_loaded,
        "duration_seconds": duration,
    }


# ── CLI entry point ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Sales ETL pipeline")
    parser.add_argument("--source", default="csv",        choices=["csv", "api", "both"])
    parser.add_argument("--table",  default="sales_clean")
    parser.add_argument("--mode",   default="upsert",     choices=["append", "replace", "upsert"])
    args = parser.parse_args()

    result = run_pipeline(source=args.source, table=args.table, mode=args.mode)
    print(result)
