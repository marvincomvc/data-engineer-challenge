from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from pipeline.db import transaction

logger = logging.getLogger(__name__)


def ingest_merchants(conn: duckdb.DuckDBPyConnection, data_path: Path) -> int:
    """Load merchant master data into bronze.merchants.

    Full DELETE + INSERT on every run because this is reference data with
    no reference_date scope — always reflect the latest source state.
    """
    path = str(data_path / "enterprise_company.parquet")
    with transaction(conn):
        conn.execute("DELETE FROM bronze.merchants")
        conn.execute(f"""
            INSERT INTO bronze.merchants (
                id, merchant_id, legal_name, trade_name, document,
                primary_cnae, created_at, updated_at, op, _timestamp
            )
            SELECT
                id,
                merchant_id,
                legal_name,
                trade_name,
                document,
                primary_cnae,
                created_at,
                updated_at,
                "Op",
                _timestamp
            FROM read_parquet('{path}')
        """)
    count: int = conn.execute("SELECT COUNT(*) FROM bronze.merchants").fetchone()[0]
    logger.info("bronze.merchants loaded", extra={"counts": {"rows": count}})
    return count
