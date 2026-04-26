from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from pipeline.db import transaction

logger = logging.getLogger(__name__)


def ingest_transactions(conn: duckdb.DuckDBPyConnection, data_path: Path) -> int:
    """Load all transaction batches into bronze.transactions.

    Uses union_by_name so that the payment_method column added in batch_2
    is filled with NULL for batch_1 rows instead of causing a schema error.
    Full DELETE + INSERT makes this idempotent across multiple runs.
    """
    glob = str(data_path / "transactions_batch_*.parquet")
    with transaction(conn):
        conn.execute("DELETE FROM bronze.transactions")
        conn.execute(f"""
            INSERT INTO bronze.transactions (
                id, transaction_id, merchant_id, amount, currency, status, description,
                payment_method, created_at, updated_at, op, _timestamp
            )
            SELECT
                id,
                transaction_id,
                merchant_id,
                amount,
                currency,
                status,
                description,
                payment_method,
                created_at,
                updated_at,
                "Op",
                _timestamp
            FROM read_parquet('{glob}', union_by_name = true)
        """)
    count: int = conn.execute("SELECT COUNT(*) FROM bronze.transactions").fetchone()[0]
    logger.info("bronze.transactions loaded", extra={"counts": {"rows": count}})
    return count
