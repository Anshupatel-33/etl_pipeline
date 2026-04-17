# ETL Pipeline — E-Commerce Sales

A beginner-friendly, production-structured ETL pipeline that extracts sales data from CSV files or a REST API, transforms it with Pandas, loads it into PostgreSQL, and orchestrates daily runs with Apache Airflow. Optional dbt models sit on top for analytics.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Project Structure](#3-project-structure)
4. [Prerequisites](#4-prerequisites)
5. [Quick Start](#5-quick-start)
6. [Configuration](#6-configuration)
7. [Running the Pipeline](#7-running-the-pipeline)
8. [Airflow Setup](#8-airflow-setup)
9. [dbt Models](#9-dbt-models)
10. [Running Tests](#10-running-tests)
11. [Database Tables](#11-database-tables)
12. [Key Concepts Explained](#12-key-concepts-explained)
13. [Extending the Project](#13-extending-the-project)
14. [Troubleshooting](#14-troubleshooting)

---

## 1. Project Overview

| Item | Detail |
|---|---|
| **Goal** | Daily batch ETL for e-commerce sales data |
| **Extract** | CSV files from `data/raw/` or a REST API |
| **Transform** | Pandas — clean nulls, cast types, deduplicate, derive columns |
| **Load** | PostgreSQL via SQLAlchemy (upsert by `order_id`) |
| **Orchestrate** | Apache Airflow DAG — runs at 06:00 UTC every day |
| **Analytics** | dbt models for monthly revenue and product breakdown |
| **Tech stack** | Python 3.11, Pandas, SQLAlchemy, Airflow 2.8, dbt-postgres |

---

## 2. Architecture

```
┌─────────────────┐     raw rows      ┌──────────────────────┐     clean rows     ┌──────────────────┐
│    EXTRACT       │  ──────────────►  │     TRANSFORM         │  ───────────────►  │      LOAD         │
│                  │                   │                        │                    │                   │
│  CSV files       │                   │  • Drop nulls          │                    │  PostgreSQL       │
│  REST API        │                   │  • Cast types          │                    │  sales_clean      │
└─────────────────┘                   │  • Deduplicate         │                    │  (upsert)         │
                                       │  • Derive columns      │                    └──────────────────┘
                                       │    revenue_usd          │                            │
                                       │    year_month           │                            ▼
                                       │    order_size           │                    ┌──────────────────┐
                                       └──────────────────────┘                    │  dbt models       │
                                                                                    │  monthly_revenue  │
                                       ┌──────────────────────┐                    │  top_products     │
                                       │    ORCHESTRATE         │                    └──────────────────┘
                                       │                        │
                                       │  Airflow DAG           │
                                       │  @daily  06:00 UTC     │
                                       │  retries=2             │
                                       └──────────────────────┘
```

### Data flow through the transform step

| Raw column | Action | Output column |
|---|---|---|
| `order_id` | strip whitespace, cast to str | `order_id` |
| `order_date` | parse various formats → datetime | `order_date` |
| `amount` | cast to float, drop unparseable | `amount` |
| `currency` | upper-case, fill missing → USD | `currency` |
| *(derived)* | `amount × FX rate` | `revenue_usd` |
| *(derived)* | `order_date.dt.to_period('M')` | `year_month` |
| *(derived)* | cut into 4 tiers | `order_size` |
| *(derived)* | UTC now | `etl_loaded_at` |

---

## 3. Project Structure

```
etl_pipeline/
│
├── etl/                        # Core ETL modules
│   ├── __init__.py
│   ├── extract.py              # Pull data from CSV / API
│   ├── transform.py            # Clean and enrich data
│   ├── load.py                 # Write to PostgreSQL
│   ├── pipeline.py             # Wires E→T→L together (CLI entry point)
│   └── logger.py               # Shared loguru logger
│
├── dags/
│   └── etl_dag.py              # Airflow DAG (runs daily @ 06:00 UTC)
│
├── dbt_models/
│   ├── monthly_revenue.sql     # Revenue aggregated by month
│   └── top_products.sql        # Revenue by product category
│
├── data/
│   └── raw/
│       └── sales_sample.csv    # 20-row sample dataset to test with
│
├── tests/
│   ├── __init__.py
│   ├── test_extract.py         # Unit tests — extract layer
│   └── test_transform.py       # Unit tests — transform layer
│
├── scripts/
│   └── setup_db.py             # One-time DB connection check & setup
│
├── config/
│   ├── __init__.py
│   └── settings.py             # Centralised config (reads from .env)
│
├── logs/                       # Auto-created at runtime
├── .env.example                # Copy this to .env and fill in your values
└── requirements.txt
```

---

## 4. Prerequisites

| Tool | Minimum version | Install |
|---|---|---|
| Python | 3.11 | https://python.org |
| PostgreSQL | 14 | https://postgresql.org |
| pip | 23 | bundled with Python |

> **Optional:** Apache Airflow (for scheduling) and dbt-postgres (for analytics models). The pipeline runs perfectly without them for learning purposes.

---

## 5. Quick Start

Follow these steps in order. Each command is copy-pasteable.

### Step 1 — Clone / unzip and enter the directory

```bash
cd etl_pipeline
```

### Step 2 — Create a virtual environment

```bash
python -m venv venv

# macOS / Linux
source venv/bin/activate

# Windows
venv\Scripts\activate
```

### Step 3 — Install dependencies

```bash
pip install -r requirements.txt
```

### Step 4 — Configure environment variables

```bash
cp .env.example .env
```

Open `.env` in any text editor and set your PostgreSQL credentials:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=sales_db
DB_USER=postgres
DB_PASSWORD=your_password_here
```

### Step 5 — Create the database

```bash
# In psql or pgAdmin run:
CREATE DATABASE sales_db;

# Or from the terminal:
createdb sales_db
```

### Step 6 — Verify connection

```bash
python scripts/setup_db.py
```

Expected output:
```
✅ Connected! PostgreSQL version: PostgreSQL 16.x ...
✅ Extensions ready
Database 'sales_db' is set up and ready.
```

### Step 7 — Run the pipeline

```bash
python -m etl.pipeline
```

Expected output:
```
2024-01-20 10:00:01 | INFO     | ETL PIPELINE STARTED  source=csv  table=sales_clean  mode=upsert
2024-01-20 10:00:01 | INFO     | Step 1/3 — EXTRACT
2024-01-20 10:00:01 | SUCCESS  | Extracted 20 rows from CSV
2024-01-20 10:00:01 | INFO     | Step 2/3 — TRANSFORM
2024-01-20 10:00:01 | SUCCESS  | Transform complete — 20 rows retained, 0 dropped
2024-01-20 10:00:02 | INFO     | Step 3/3 — LOAD
2024-01-20 10:00:02 | SUCCESS  | Loaded 20 rows into 'sales_clean'
2024-01-20 10:00:02 | SUCCESS  | PIPELINE COMPLETE — 20 rows loaded in 1.42s
```

### Step 8 — Verify in PostgreSQL

```sql
SELECT year_month, COUNT(*) AS orders, ROUND(SUM(revenue_usd), 2) AS revenue
FROM sales_clean
GROUP BY year_month
ORDER BY year_month;
```

---

## 6. Configuration

All settings live in `config/settings.py` and are loaded from your `.env` file.

| Variable | Default | Description |
|---|---|---|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `sales_db` | Target database name |
| `DB_USER` | `postgres` | DB username |
| `DB_PASSWORD` | *(required)* | DB password |
| `RAW_DATA_PATH` | `data/raw/` | Folder scanned for CSV files |
| `PROCESSED_DATA_PATH` | `data/processed/` | Output folder for processed files |
| `CHUNK_SIZE` | `1000` | Rows per DB write batch |
| `LOG_LEVEL` | `INFO` | Logging verbosity (DEBUG, INFO, WARNING) |
| `API_BASE_URL` | — | Base URL for REST API extraction |
| `API_KEY` | — | Bearer token for API auth |

---

## 7. Running the Pipeline

### Basic run (CSV source, default table, upsert mode)

```bash
python -m etl.pipeline
```

### CLI options

```bash
# Extract from API instead of CSV
python -m etl.pipeline --source api

# Extract from both CSV and API
python -m etl.pipeline --source both

# Write to a different table
python -m etl.pipeline --table sales_staging

# Use append mode instead of upsert (allows duplicates)
python -m etl.pipeline --mode append

# Full replace (drops and recreates the table)
python -m etl.pipeline --mode replace
```

### Run individual layers for debugging

```python
from etl.extract   import extract_from_csv
from etl.transform import transform_sales
from etl.load      import load_to_postgres

# Extract
df = extract_from_csv("data/raw/sales_sample.csv")
print(df.head())

# Transform
clean = transform_sales(df)
print(clean.dtypes)

# Load
rows = load_to_postgres(clean, table="sales_test", mode="replace")
print(f"Loaded {rows} rows")
```

---

## 8. Airflow Setup

### Install and initialise Airflow

```bash
export AIRFLOW_HOME=~/airflow

pip install apache-airflow==2.8.1

airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

### Point Airflow at the DAGs folder

In `~/airflow/airflow.cfg` set:

```
dags_folder = /path/to/etl_pipeline/dags
```

### Start Airflow

```bash
# Terminal 1 — web server
airflow webserver --port 8080

# Terminal 2 — scheduler
airflow scheduler
```

Open http://localhost:8080, log in with `admin / admin`, and enable the `sales_etl_pipeline` DAG.

### DAG summary

| Property | Value |
|---|---|
| DAG ID | `sales_etl_pipeline` |
| Schedule | `0 6 * * *` — 06:00 UTC daily |
| Retries | 2 (5-minute back-off) |
| Catchup | Disabled |
| Tasks | `extract → transform → load → summary → notify` |

### Trigger a manual run

```bash
airflow dags trigger sales_etl_pipeline
```

---

## 9. dbt Models

### Install dbt

```bash
pip install dbt-postgres
```

### Set up a profile

Create `~/.dbt/profiles.yml`:

```yaml
etl_pipeline:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      password: your_password_here
      dbname: sales_db
      schema: public
      threads: 4
```

### Run models

```bash
cd etl_pipeline

# Run all models
dbt run --profiles-dir ~/.dbt --project-dir dbt_models

# Run a specific model
dbt run --select monthly_revenue

# Test models
dbt test
```

### Available models

| Model | Type | Description |
|---|---|---|
| `monthly_revenue` | table | Total revenue, order count, avg order value per month |
| `top_products` | view | Revenue breakdown by product category |

---

## 10. Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run only transform tests
pytest tests/test_transform.py -v

# Run with coverage report
pytest tests/ --cov=etl --cov-report=term-missing
```

### What is tested

| Test file | What it covers |
|---|---|
| `test_extract.py` | CSV file reading, missing file error, multi-file concat |
| `test_transform.py` | Schema validation, null dropping, type casting, deduplication, derived columns, end-to-end transform |

---

## 11. Database Tables

### `sales_clean` — main fact table

| Column | Type | Description |
|---|---|---|
| `order_id` | TEXT (PK) | Unique order identifier |
| `customer_id` | TEXT | Customer identifier |
| `order_date` | TIMESTAMPTZ | Order timestamp |
| `amount` | NUMERIC(14,4) | Original order amount |
| `currency` | TEXT | ISO currency code |
| `exchange_rate` | NUMERIC(10,6) | FX rate to USD |
| `revenue_usd` | NUMERIC(14,4) | Amount converted to USD |
| `year_month` | TEXT | e.g. `2024-01` |
| `quarter` | TEXT | e.g. `2024Q1` |
| `day_of_week` | TEXT | e.g. `Monday` |
| `order_size` | TEXT | `small / medium / large / enterprise` |
| `product_category` | TEXT | Product category (title-cased) |
| `source_file` | TEXT | Which CSV this row came from |
| `etl_loaded_at` | TIMESTAMPTZ | When the ETL wrote this row |

### `sales_monthly_summary` — aggregation table

| Column | Type | Description |
|---|---|---|
| `year_month` | TEXT | Month period |
| `total_revenue_usd` | NUMERIC | Sum of revenue |
| `order_count` | BIGINT | Number of orders |
| `avg_order_value` | NUMERIC | Average revenue per order |
| `unique_customers` | BIGINT | Distinct customers |
| `etl_loaded_at` | TIMESTAMPTZ | Refresh timestamp |

---

## 12. Key Concepts Explained

### Idempotency
Running the pipeline twice must produce the same result — no duplicate rows. This is achieved with **upsert** (`INSERT … ON CONFLICT (order_id) DO UPDATE`). If the same order arrives again, it updates in place rather than inserting a second row.

### Schema validation
Before any transformation, `transform_sales()` checks that all required columns exist. If a column is missing (e.g. the upstream CSV adds/removes a field), the pipeline fails immediately with a clear error instead of silently writing wrong data.

### Chunked writes
`load_to_postgres()` writes in batches of 1,000 rows (`CHUNK_SIZE`). This prevents memory exhaustion on large files and keeps individual DB transactions short.

### Audit column (`etl_loaded_at`)
Every row receives a UTC timestamp recording when the ETL wrote it. This lets you query "what was loaded in the last run?" and supports incremental strategies.

### FX normalisation
All revenue is normalised to USD using a static rate table (`FX_RATES` in `transform.py`). In production, replace this with a live rates API (e.g. Open Exchange Rates).

---

## 13. Extending the Project

### Add a new data source

1. Add an extract function in `etl/extract.py`
2. Ensure the returned DataFrame has the required columns (see `REQUIRED_COLUMNS` in `transform.py`)
3. Call it from `etl/pipeline.py` in the extract step

### Add a new derived column

Open `etl/transform.py` → `derive_columns()` and add a new line:

```python
df["is_high_value"] = df["revenue_usd"] > 500
```

### Add email/Slack alerts

In `dags/etl_dag.py` → `_notify()`, add:

```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
# or
from airflow.utils.email import send_email
```

### Write to Snowflake instead of PostgreSQL

Replace `DATABASE_URL` in `.env` with a Snowflake connection string and install `snowflake-sqlalchemy`:

```
DATABASE_URL=snowflake://user:pass@account/database/schema
```

No code changes needed — SQLAlchemy handles the dialect.

### Add data quality checks with Great Expectations

```bash
pip install great-expectations
great_expectations init
```

Then add an expectation suite to validate row counts, column ranges, and null rates after each extract.

---

## 14. Troubleshooting

### `psycopg2.OperationalError: could not connect to server`

- Is PostgreSQL running? Run `pg_isready`
- Check `DB_HOST`, `DB_PORT`, `DB_PASSWORD` in your `.env`
- If using Docker: ensure the container port is mapped (`-p 5432:5432`)

### `ModuleNotFoundError: No module named 'etl'`

Run the pipeline from the project root directory (where `etl/` folder is visible):

```bash
cd etl_pipeline
python -m etl.pipeline      # correct — uses module syntax
```

### `ValueError: Raw data is missing required columns`

Your CSV is missing one of: `order_id`, `customer_id`, `order_date`, `amount`, `currency`. Check the column names in your file match exactly (case-sensitive).

### Airflow DAG not appearing in the UI

- Check `dags_folder` in `~/airflow/airflow.cfg` points to the `dags/` directory
- Look for syntax errors: `python dags/etl_dag.py`
- Restart the scheduler: `airflow scheduler`

### `KeyError` on FX rate

Your CSV contains a currency code not in the `FX_RATES` dictionary in `transform.py`. Add it:

```python
FX_RATES["SGD"] = 0.74
```

---

## Licence

MIT — free to use, modify, and distribute for learning and commercial projects.
