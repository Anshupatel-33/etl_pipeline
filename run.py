"""
run.py  —  Project root launcher (Windows-friendly)
────────────────────────────────────────────────────
Run this from the etl_pipeline folder. It sets up sys.path
correctly before importing anything, so no ModuleNotFoundError.

Usage
─────
  python run.py pipeline          # run full ETL
  python run.py pipeline --source csv --mode upsert
  python run.py setup             # verify DB connection
  python run.py test              # run pytest
"""

import sys
import os

# Always add project root first
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse

def main():
    parser = argparse.ArgumentParser(description="ETL Pipeline launcher")
    parser.add_argument("command", choices=["pipeline", "setup", "test"],
                        help="What to run")
    parser.add_argument("--source", default="csv",  choices=["csv","api","both"])
    parser.add_argument("--table",  default="sales_clean")
    parser.add_argument("--mode",   default="upsert", choices=["append","replace","upsert"])
    args = parser.parse_args()

    if args.command == "pipeline":
        from etl.pipeline import run_pipeline
        result = run_pipeline(source=args.source, table=args.table, mode=args.mode)
        print(result)

    elif args.command == "setup":
        import scripts.setup_db as s
        s.setup()

    elif args.command == "test":
        import pytest
        sys.exit(pytest.main(["tests/", "-v"]))

if __name__ == "__main__":
    main()
