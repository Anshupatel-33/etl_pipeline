"""
dags/etl_dag.py
───────────────
Airflow DAG — schedules the Sales ETL pipeline to run daily at 06:00 UTC.

Dag ID  : sales_etl_pipeline
Schedule: 06:00 UTC every day
Retries : 2  (5-minute back-off between attempts)

Tasks
─────
  1. extract_task   — pulls raw CSV / API data
  2. transform_task — cleans and enriches the data
  3. load_task      — writes to PostgreSQL
  4. summary_task   — refreshes the monthly summary table
  5. notify_task    — logs a success message (extend to Slack/email)

Dependencies
────────────
  extract_task >> transform_task >> load_task >> summary_task >> notify_task
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow                            import DAG
from airflow.operators.python           import PythonOperator
from airflow.utils.dates                import days_ago

# ── Default arguments applied to all tasks ───────────────────────────────────
DEFAULT_ARGS = {
    "owner"          : "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry" : False,
    "retries"        : 2,
    "retry_delay"    : timedelta(minutes=5),
}


# ═════════════════════════════════════════════════════════════════════════════
# Task callables
# ═════════════════════════════════════════════════════════════════════════════

def _extract(**context) -> None:
    """Extract raw data and push to XCom so downstream tasks can use it."""
    import pandas as pd
    from etl.extract import extract_all_csvs
    from config.settings import RAW_DATA_PATH

    df = extract_all_csvs(RAW_DATA_PATH)

    # Serialise to JSON for XCom (keep it small — push only what downstream needs)
    context["ti"].xcom_push(key="raw_row_count", value=len(df))
    context["ti"].xcom_push(key="raw_data_json", value=df.to_json(orient="records", date_format="iso"))


def _transform(**context) -> None:
    """Pull raw data from XCom, transform, push clean data back."""
    import pandas as pd
    from etl.transform import transform_sales

    raw_json = context["ti"].xcom_pull(task_ids="extract_task", key="raw_data_json")
    raw_df   = pd.read_json(raw_json, orient="records")

    clean_df = transform_sales(raw_df)
    context["ti"].xcom_push(key="clean_data_json",  value=clean_df.to_json(orient="records", date_format="iso"))
    context["ti"].xcom_push(key="clean_row_count",  value=len(clean_df))


def _load(**context) -> None:
    """Pull clean data from XCom and load to PostgreSQL."""
    import pandas as pd
    from etl.load import load_to_postgres

    clean_json = context["ti"].xcom_pull(task_ids="transform_task", key="clean_data_json")
    clean_df   = pd.read_json(clean_json, orient="records")

    rows = load_to_postgres(clean_df, table="sales_clean", mode="upsert")
    context["ti"].xcom_push(key="rows_loaded", value=rows)


def _load_summary(**context) -> None:
    """Refresh the monthly summary aggregation table."""
    import pandas as pd
    from etl.load import load_summary_table, get_engine

    clean_json = context["ti"].xcom_pull(task_ids="transform_task", key="clean_data_json")
    clean_df   = pd.read_json(clean_json, orient="records")

    load_summary_table(clean_df, engine=get_engine())


def _notify(**context) -> None:
    """Log a final summary. Extend this to send Slack / email notifications."""
    raw_count   = context["ti"].xcom_pull(task_ids="extract_task",   key="raw_row_count")
    clean_count = context["ti"].xcom_pull(task_ids="transform_task", key="clean_row_count")
    rows_loaded = context["ti"].xcom_pull(task_ids="load_task",      key="rows_loaded")

    summary = {
        "dag_run_id"  : context["run_id"],
        "execution_dt": str(context["execution_date"]),
        "raw_rows"    : raw_count,
        "clean_rows"  : clean_count,
        "rows_loaded" : rows_loaded,
    }
    print(f"✅ ETL run complete: {json.dumps(summary, indent=2)}")
    # TODO: add Slack / email alert here
    #   from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


# ═════════════════════════════════════════════════════════════════════════════
# DAG definition
# ═════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id          = "sales_etl_pipeline",
    description     = "Daily ETL: Extract sales CSV/API → Transform → Load PostgreSQL",
    default_args    = DEFAULT_ARGS,
    schedule_interval = "0 6 * * *",      # 06:00 UTC every day
    start_date      = days_ago(1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["etl", "sales", "beginner"],
) as dag:

    extract_task = PythonOperator(
        task_id         = "extract_task",
        python_callable = _extract,
    )

    transform_task = PythonOperator(
        task_id         = "transform_task",
        python_callable = _transform,
    )

    load_task = PythonOperator(
        task_id         = "load_task",
        python_callable = _load,
    )

    summary_task = PythonOperator(
        task_id         = "summary_task",
        python_callable = _load_summary,
    )

    notify_task = PythonOperator(
        task_id         = "notify_task",
        python_callable = _notify,
    )

    # ── Task dependency chain ──────────────────────────────────────────────
    extract_task >> transform_task >> load_task >> summary_task >> notify_task
