"""Settlement reconciliation DAG.

Linear, sequential pipeline: ingest -> dbt run -> dbt test. Sequential by design
because DuckDB is single-writer — only one task may hold the warehouse file at a
time (see docs/scalability.md). The DAG is parametrized by `reference_date`
(defaults to the DAG run's logical date) so backfills and retries are idempotent:
each stage re-runs safely thanks to delete+insert on the natural key.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = os.environ.get("PIPELINE_REPO", "/opt/airflow/repo")

# reference_date resolves at render time from the run's logical date, or an
# operator-supplied dag_run.conf override for manual backfills.
REFERENCE_DATE = (
    "{{ dag_run.conf.get('reference_date', ds) if dag_run else ds }}"
)

COMMON_ENV = {
    "DATA_PATH": os.environ.get("DATA_PATH", f"{REPO}/docs/sample-data"),
    "DUCKDB_PATH": os.environ.get("DUCKDB_PATH", f"{REPO}/warehouse/settlement.duckdb"),
    "PYTHONPATH": f"{REPO}/src",
}

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="settlement_reconciliation",
    description="Ingest -> recompute reconciliation -> publish data products",
    schedule="0 3 * * *",  # daily, after the PaySettler file lands
    start_date=datetime(2025, 3, 1),
    catchup=False,
    default_args=default_args,
    tags=["settlement", "reconciliation", "duckdb", "dbt"],
) as dag:

    ingest = BashOperator(
        task_id="ingest",
        bash_command=(
            f"cd {REPO} && python -m pipeline ingest "
            f"--reference-date {REFERENCE_DATE} --run-id {{{{ run_id }}}}"
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {REPO} && dbt run --project-dir dbt --profiles-dir dbt "
            f'--vars \'{{"reference_date": "{REFERENCE_DATE}", "run_id": "{{{{ run_id }}}}"}}\''
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    # Integrity tests (severity=error) fail the task and stop the pipeline;
    # anomaly tests (severity=warn) surface in logs / dq_check_result but pass.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {REPO} && dbt test --project-dir dbt --profiles-dir dbt "
            f'--vars \'{{"reference_date": "{REFERENCE_DATE}", "run_id": "{{{{ run_id }}}}"}}\''
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    # Publish Gold as consumable Parquet products (read-only warehouse access).
    publish = BashOperator(
        task_id="publish",
        bash_command=(
            f"cd {REPO} && python -m pipeline publish "
            f"--reference-date {REFERENCE_DATE} --run-id {{{{ run_id }}}}"
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    ingest >> dbt_run >> dbt_test >> publish
