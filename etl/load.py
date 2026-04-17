"""
etl/load.py — Load layer: write cleaned DataFrame to PostgreSQL.
Uses psycopg (v3) driver via SQLAlchemy — works on Windows without DLL issues.
"""

import sys, os
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pandas as pd
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl.logger import logger
from config.settings import DATABASE_URL, CHUNK_SIZE

_engine = None  # type: Engine

def get_engine():
    global _engine
    if _engine is None:
        logger.info(f"Creating engine → {DATABASE_URL.split('@')[-1]}")
        _engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    return _engine

CREATE_SALES_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    order_id          TEXT         PRIMARY KEY,
    customer_id       TEXT,
    order_date        TIMESTAMPTZ,
    amount            NUMERIC(14,4),
    currency          TEXT,
    exchange_rate     NUMERIC(10,6),
    revenue_usd       NUMERIC(14,4),
    year_month        TEXT,
    quarter           TEXT,
    day_of_week       TEXT,
    order_size        TEXT,
    product_category  TEXT,
    source_file       TEXT,
    etl_loaded_at     TIMESTAMPTZ  DEFAULT NOW()
);
"""

def create_table_if_not_exists(engine, table="sales_clean"):
    with engine.begin() as conn:
        conn.execute(text(CREATE_SALES_DDL.format(table=table)))
    logger.info(f"Table '{table}' ensured")

def load_to_postgres(df: pd.DataFrame, table="sales_clean", mode="upsert"):
    if df.empty:
        logger.warning("Empty DataFrame — skipping load")
        return 0

    engine = get_engine()
    create_table_if_not_exists(engine, table)
    rows = len(df)
    logger.info(f"Loading {rows:,} rows → '{table}'  mode={mode}")

    try:
        if mode == "upsert":
            _upsert(df, table, engine)
        else:
            df.to_sql(
                name=table, con=engine,
                if_exists="append" if mode == "append" else "replace",
                index=False, chunksize=CHUNK_SIZE, method="multi",
            )
    except SQLAlchemyError as exc:
        logger.error(f"DB write failed: {exc}")
        raise

    logger.success(f"Loaded {rows:,} rows into '{table}'")
    return rows

def _upsert(df: pd.DataFrame, table: str, engine) -> None:
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from sqlalchemy import Table, MetaData

    meta = MetaData()
    meta.reflect(bind=engine, only=[table])
    tbl = meta.tables[table]
    records = df.to_dict(orient="records")

    with engine.begin() as conn:
        for i in range(0, len(records), CHUNK_SIZE):
            chunk = records[i: i + CHUNK_SIZE]
            stmt  = pg_insert(tbl).values(chunk)
            stmt  = stmt.on_conflict_do_update(
                index_elements=["order_id"],
                set_={c.name: stmt.excluded[c.name] for c in tbl.columns if c.name != "order_id"},
            )
            conn.execute(stmt)

def load_summary_table(df: pd.DataFrame, engine=None) -> None:
    engine = engine or get_engine()
    summary = (
        df.groupby("year_month")
        .agg(
            total_revenue_usd=("revenue_usd", "sum"),
            order_count      =("order_id",    "count"),
            avg_order_value  =("revenue_usd", "mean"),
            unique_customers =("customer_id", "nunique"),
        )
        .round(4).reset_index()
    )
    summary["etl_loaded_at"] = pd.Timestamp.utcnow()
    summary.to_sql("sales_monthly_summary", con=engine, if_exists="replace",
                   index=False, chunksize=CHUNK_SIZE)
    logger.success(f"Summary table refreshed — {len(summary)} months")