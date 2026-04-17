"""
scripts/setup_db.py
────────────────────
Run once before first pipeline execution to verify the DB connection.

Usage (always run from the project root folder)
─────
  python scripts/setup_db.py
"""

import sys
import os

# ── Add project root to sys.path so 'config' and 'etl' are importable ────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from sqlalchemy import create_engine, text
from config.settings import DATABASE_URL, DB_NAME


def setup():
    print(f"Connecting to: {DATABASE_URL.split('@')[-1]}")

    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"Connected! PostgreSQL version: {version}")

            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
            conn.commit()
            print(f"Extensions ready")

        print(f"\nDatabase '{DB_NAME}' is set up and ready.")
        print("Next step: python -m etl.pipeline")

    except Exception as e:
        print(f"Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Is PostgreSQL running?")
        print("  2. Does the database exist? Run:  createdb sales_db")
        print("  3. Check credentials in your .env file")
        sys.exit(1)


if __name__ == "__main__":
    setup()
