"""Silver transformations for internal transactions."""
from __future__ import annotations

import duckdb
from pipeline.observability.logging import get_logger

logger = get_logger(__name__)


def build_silver_transactions(con: duckdb.DuckDBPyConnection) -> int:
    """Deduplicate transactions by transaction_id (keep latest updated_at),
    normalize types (dates to UTC, consistent types).
    Reject rows with null transaction_id (HARD check).

    Returns count of rejected rows (null transaction_id).
    """
    # Count rows that will be rejected
    rejected = con.execute("""
        SELECT count(*) FROM bronze.transactions_raw
        WHERE transaction_id IS NULL
    """).fetchone()[0]

    if rejected > 0:
        logger.warning(f"Rejecting {rejected} transactions with null transaction_id")

    # Dedup: keep the row with the latest _timestamp (CDC semantics — later timestamp wins)
    con.execute("""
        CREATE OR REPLACE TABLE silver.transactions AS
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY transaction_id
                       ORDER BY _timestamp DESC, updated_at DESC
                   ) AS rn
            FROM bronze.transactions_raw
            WHERE transaction_id IS NOT NULL
        )
        SELECT
            id,
            transaction_id,
            merchant_id,
            amount,
            currency,
            status,
            description,
            COALESCE(payment_method, NULL) AS payment_method,
            created_at::TIMESTAMPTZ AS created_at,
            updated_at::TIMESTAMPTZ AS updated_at,
            Op,
            _timestamp::TIMESTAMPTZ AS _timestamp,
            _ingested_at,
            _source_file,
            _reference_date
        FROM ranked
        WHERE rn = 1
    """)

    count = con.execute("SELECT count(*) FROM silver.transactions").fetchone()[0]
    logger.info(f"silver.transactions: {count} rows (deduped)")
    return rejected
