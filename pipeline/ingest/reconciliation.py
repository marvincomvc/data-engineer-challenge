from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from pipeline.db import transaction

logger = logging.getLogger(__name__)


def ingest_reconciliation_runs(conn: duckdb.DuckDBPyConnection, data_path: Path) -> int:
    """Load reconciliation runs CDC into bronze.reconciliation_runs.

    Each run_id has two CDC rows (Op=I for IN_PROGRESS, Op=U for COMPLETED).
    Deduplication to latest state happens in the silver layer.
    """
    path = str(data_path / "reconciliation_runs.parquet")
    with transaction(conn):
        conn.execute("DELETE FROM bronze.reconciliation_runs")
        conn.execute(f"""
            INSERT INTO bronze.reconciliation_runs (
                id, reference_date, file_name, status, total_transactions,
                started_at, completed_at, created_at, op, _timestamp
            )
            SELECT
                id,
                reference_date,
                file_name,
                status,
                total_transactions,
                started_at,
                completed_at,
                created_at,
                "Op",
                _timestamp
            FROM read_parquet('{path}')
        """)
    count: int = conn.execute("SELECT COUNT(*) FROM bronze.reconciliation_runs").fetchone()[0]
    logger.info("bronze.reconciliation_runs loaded", extra={"counts": {"rows": count}})
    return count


def ingest_reconciliation_results(conn: duckdb.DuckDBPyConnection, data_path: Path) -> int:
    """Load reconciliation results CDC into bronze.reconciliation_results."""
    path = str(data_path / "reconciliation_results.parquet")
    with transaction(conn):
        conn.execute("DELETE FROM bronze.reconciliation_results")
        conn.execute(f"""
            INSERT INTO bronze.reconciliation_results (
                id, run_id, transaction_id, merchant_id, category,
                internal_amount, processor_amount, difference,
                created_at, op, _timestamp
            )
            SELECT
                id,
                run_id,
                transaction_id,
                merchant_id,
                category,
                internal_amount,
                processor_amount,
                difference,
                created_at,
                "Op",
                _timestamp
            FROM read_parquet('{path}')
        """)
    count: int = conn.execute("SELECT COUNT(*) FROM bronze.reconciliation_results").fetchone()[0]
    logger.info("bronze.reconciliation_results loaded", extra={"counts": {"rows": count}})
    return count
