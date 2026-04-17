"""
tests/test_extract.py
──────────────────────
Unit tests for the extract layer.
Run with:  pytest tests/ -v
"""

import os
import pytest
import pandas as pd

from etl.extract import extract_from_csv, extract_all_csvs


SAMPLE_CSV = os.path.join(
    os.path.dirname(__file__), "..", "data", "raw", "sales_sample.csv"
)


def test_extract_from_csv_returns_dataframe():
    df = extract_from_csv(SAMPLE_CSV)
    assert isinstance(df, pd.DataFrame)


def test_extract_from_csv_has_rows():
    df = extract_from_csv(SAMPLE_CSV)
    assert len(df) > 0


def test_extract_from_csv_has_expected_columns():
    df = extract_from_csv(SAMPLE_CSV)
    for col in ["order_id", "customer_id", "order_date", "amount", "currency"]:
        assert col in df.columns


def test_extract_from_csv_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        extract_from_csv("/nonexistent/path/file.csv")


def test_extract_all_csvs_returns_dataframe(tmp_path):
    """extract_all_csvs should concatenate all CSVs in a directory."""
    # Write two tiny CSV files to a temp directory
    for i in range(2):
        content = "order_id,amount\nORD-00{i},100\n".format(i=i)
        (tmp_path / f"part_{i}.csv").write_text(content)

    df = extract_all_csvs(str(tmp_path))
    assert isinstance(df, pd.DataFrame)
    assert "source_file" in df.columns      # should tag each row with its file
