"""Silver transformations for merchant (enterprise_company) data."""
from __future__ import annotations

import duckdb
from pipeline.observability.logging import get_logger

logger = get_logger(__name__)


def build_silver_merchants(con: duckdb.DuckDBPyConnection) -> None:
    """Deduplicate enterprise_company by merchant_id, keep latest CDC row."""
    # CDC dedup: keep the latest _timestamp per merchant_id
    con.execute("""
        CREATE OR REPLACE TABLE silver.merchants AS
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY merchant_id
                       ORDER BY _timestamp DESC
                   ) AS rn
            FROM bronze.enterprise_company_raw
            WHERE merchant_id IS NOT NULL
        )
        SELECT
            id,
            merchant_id,
            legal_name,
            trade_name,
            document,
            primary_cnae,
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

    count = con.execute("SELECT count(*) FROM silver.merchants").fetchone()[0]
    logger.info(f"silver.merchants: {count} rows")
