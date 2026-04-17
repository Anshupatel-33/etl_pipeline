"""
etl/extract.py
──────────────
Extract layer — pull raw data from CSV files or a REST API.

Exported functions
──────────────────
  extract_from_csv(file_path)    → pd.DataFrame
  extract_from_api(endpoint)     → pd.DataFrame
  extract_all()                  → pd.DataFrame   (merges both sources)
"""

import os
import requests
import pandas as pd
from typing import Optional

from etl.logger import logger
from config.settings import RAW_DATA_PATH, API_BASE_URL, API_KEY


# ── CSV extraction ────────────────────────────────────────────────────────────

def extract_from_csv(file_path: str) -> pd.DataFrame:
    """
    Read a CSV file into a DataFrame.

    Parameters
    ----------
    file_path : str
        Absolute or relative path to the CSV file.

    Returns
    -------
    pd.DataFrame
        Raw (untransformed) data.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    """
    if not os.path.exists(file_path):
        logger.error(f"CSV file not found: {file_path}")
        raise FileNotFoundError(f"CSV not found: {file_path}")

    logger.info(f"Extracting CSV → {file_path}")
    df = pd.read_csv(file_path, dtype=str)          # read everything as str first
    logger.success(f"Extracted {len(df):,} rows, {len(df.columns)} columns from CSV")
    return df


def extract_all_csvs(directory: Optional[str] = None) -> pd.DataFrame:
    """
    Read every *.csv inside *directory* and concatenate into one DataFrame.
    Adds a 'source_file' column so you can trace each row back to its file.
    """
    directory = directory or RAW_DATA_PATH
    csv_files = [f for f in os.listdir(directory) if f.endswith(".csv")]

    if not csv_files:
        logger.warning(f"No CSV files found in {directory}")
        return pd.DataFrame()

    frames = []
    for fname in csv_files:
        path = os.path.join(directory, fname)
        df   = extract_from_csv(path)
        df["source_file"] = fname
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    logger.info(f"Combined {len(csv_files)} CSV files → {len(combined):,} total rows")
    return combined


# ── API extraction ────────────────────────────────────────────────────────────

def extract_from_api(
    endpoint: str = "/sales",
    params:   Optional[dict] = None,
    timeout:  int = 30,
) -> pd.DataFrame:
    """
    Fetch data from a REST API endpoint that returns a JSON array.

    Parameters
    ----------
    endpoint : str
        Path appended to API_BASE_URL (e.g. "/sales").
    params : dict, optional
        Query-string parameters (e.g. {"date": "2024-01-01"}).
    timeout : int
        Request timeout in seconds.

    Returns
    -------
    pd.DataFrame
        Normalised JSON payload.

    Raises
    ------
    requests.HTTPError
        On non-2xx responses.
    """
    url     = f"{API_BASE_URL}{endpoint}"
    headers = {"Authorization": f"Bearer {API_KEY}"} if API_KEY else {}

    logger.info(f"Extracting API → {url}  params={params}")

    response = requests.get(url, headers=headers, params=params, timeout=timeout)
    response.raise_for_status()                      # raises on 4xx / 5xx

    payload = response.json()

    # Handle both   {"data": [...]}   and   [...]   responses
    if isinstance(payload, dict) and "data" in payload:
        payload = payload["data"]

    df = pd.json_normalize(payload)
    logger.success(f"Extracted {len(df):,} rows from API")
    return df
