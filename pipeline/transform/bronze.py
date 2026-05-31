"""Bronze layer — raw ingestion with metadata columns, no business rules."""
from __future__ import annotations

from datetime import datetime, timezone

import duckdb
from pipeline.config import PipelineConfig
from pipeline.ingest.parquet_reader import read_parquet_batches
from pipeline.ingest.csv_reader import read_paysettler_csv
from pipeline.observability.logging import get_logger

logger = get_logger(__name__)


def create_bronze_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")


def load_transactions(con: duckdb.DuckDBPyConnection, cfg: PipelineConfig, reference_date: str) -> tuple[list[str], list[str]]:
    """Load both transaction parquet batches into bronze.transactions_raw.

    Returns (batch1_cols, batch2_cols) for schema drift quality checks.
    """
    now = datetime.now(timezone.utc).isoformat()
    b1_cols, b2_cols = read_parquet_batches(con, cfg)

    con.execute(f"""
        CREATE OR REPLACE TABLE bronze.transactions_raw AS
        SELECT *, '{now}' AS _ingested_at, 'transactions_batch_*.parquet' AS _source_file,
               '{reference_date}' AS _reference_date
        FROM raw_transactions
    """)
    count = con.execute("SELECT count(*) FROM bronze.transactions_raw").fetchone()[0]
    logger.info(f"bronze.transactions_raw: {count} rows loaded")
    return b1_cols, b2_cols


def load_reconciliation_runs(con: duckdb.DuckDBPyConnection, cfg: PipelineConfig, reference_date: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    path = str(cfg.reconciliation_runs)
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze.reconciliation_runs_raw AS
        SELECT *, '{now}' AS _ingested_at, 'reconciliation_runs.parquet' AS _source_file,
               '{reference_date}' AS _reference_date
        FROM read_parquet('{path}')
    """)
    count = con.execute("SELECT count(*) FROM bronze.reconciliation_runs_raw").fetchone()[0]
    logger.info(f"bronze.reconciliation_runs_raw: {count} rows loaded")


def load_reconciliation_results(con: duckdb.DuckDBPyConnection, cfg: PipelineConfig, reference_date: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    path = str(cfg.reconciliation_results)
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze.reconciliation_results_raw AS
        SELECT *, '{now}' AS _ingested_at, 'reconciliation_results.parquet' AS _source_file,
               '{reference_date}' AS _reference_date
        FROM read_parquet('{path}')
    """)
    count = con.execute("SELECT count(*) FROM bronze.reconciliation_results_raw").fetchone()[0]
    logger.info(f"bronze.reconciliation_results_raw: {count} rows loaded")


def load_paysettler(con: duckdb.DuckDBPyConnection, cfg: PipelineConfig, reference_date: str) -> int:
    """Load PaySettler CSV into bronze.paysettler_raw. Returns row count."""
    now = datetime.now(timezone.utc).isoformat()
    row_count = read_paysettler_csv(con, cfg)

    con.execute(f"""
        CREATE OR REPLACE TABLE bronze.paysettler_raw AS
        SELECT *, '{now}' AS _ingested_at, 'settlement_paysettler.csv' AS _source_file,
               '{reference_date}' AS _reference_date
        FROM raw_paysettler
    """)
    logger.info(f"bronze.paysettler_raw: {row_count} rows loaded")
    return row_count


def load_enterprise_company(con: duckdb.DuckDBPyConnection, cfg: PipelineConfig, reference_date: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    path = str(cfg.enterprise_company)
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze.enterprise_company_raw AS
        SELECT *, '{now}' AS _ingested_at, 'enterprise_company.parquet' AS _source_file,
               '{reference_date}' AS _reference_date
        FROM read_parquet('{path}')
    """)
    count = con.execute("SELECT count(*) FROM bronze.enterprise_company_raw").fetchone()[0]
    logger.info(f"bronze.enterprise_company_raw: {count} rows loaded")


def run_bronze(con: duckdb.DuckDBPyConnection, cfg: PipelineConfig, reference_date: str) -> tuple[list[str], list[str], int]:
    """Execute full bronze layer. Returns (batch1_cols, batch2_cols, csv_row_count)."""
    create_bronze_schema(con)
    b1_cols, b2_cols = load_transactions(con, cfg, reference_date)
    load_reconciliation_runs(con, cfg, reference_date)
    load_reconciliation_results(con, cfg, reference_date)
    csv_row_count = load_paysettler(con, cfg, reference_date)
    load_enterprise_company(con, cfg, reference_date)
    return b1_cols, b2_cols, csv_row_count
