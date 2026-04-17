"""
config/settings.py — Centralised configuration loaded from environment variables.
"""

import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "sales_db")
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")

# psycopg3 dialect: postgresql+psycopg  (works on Windows without DLL issues)
DATABASE_URL = (
    f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

BASE_DIR            = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH       = os.path.join(BASE_DIR, os.getenv("RAW_DATA_PATH",       "data/raw/"))
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, os.getenv("PROCESSED_DATA_PATH", "data/processed/"))
LOG_DIR             = os.path.join(BASE_DIR, "logs")

API_BASE_URL = os.getenv("API_BASE_URL", "https://api.example.com/v1")
API_KEY      = os.getenv("API_KEY",      "")

LOG_LEVEL  = os.getenv("LOG_LEVEL",  "INFO")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1000"))