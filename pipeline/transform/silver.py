"""Silver layer — cleaned, deduplicated, integrated data."""
from __future__ import annotations

import duckdb
from pipeline.config import PipelineConfig
from pipeline.observability.logging import get_logger
from pipeline.observability.metrics import RunMetrics
from pipeline.transform.transactions import build_silver_transactions
from pipeline.transform.merchants import build_silver_merchants
from pipeline.transform.reconciliation import reconcile

logger = get_logger(__name__)


def create_silver_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")


def build_silver_reconciliation_runs(con: duckdb.DuckDBPyConnection) -> None:
    """Deduplicate reconciliation runs (CDC: keep latest Op per run id)."""
    con.execute("""
        CREATE OR REPLACE TABLE silver.reconciliation_runs AS
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY id
                       ORDER BY _timestamp DESC
                   ) AS rn
            FROM bronze.reconciliation_runs_raw
        )
        SELECT
            id AS run_id,
            reference_date,
            file_name,
            status,
            total_transactions,
            started_at::TIMESTAMPTZ AS started_at,
            completed_at::TIMESTAMPTZ AS completed_at,
            created_at::TIMESTAMPTZ AS created_at,
            Op,
            _timestamp::TIMESTAMPTZ AS _timestamp,
            _ingested_at,
            _source_file,
            _reference_date
        FROM ranked
        WHERE rn = 1
    """)

    count = con.execute("SELECT count(*) FROM silver.reconciliation_runs").fetchone()[0]
    logger.info(f"silver.reconciliation_runs: {count} rows")


def build_silver_reconciliation_results(con: duckdb.DuckDBPyConnection) -> None:
    """Create silver.reconciliation_results from CDC source data.

    This loads the pre-existing reconciliation results from the source.
    The pipeline's own reconciliation (reconcile()) will also write to this
    table for new reference_dates.
    """
    # Build from CDC source — dedup and join with runs
    con.execute("""
        CREATE OR REPLACE TABLE silver.reconciliation_results AS
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY id
                       ORDER BY _timestamp DESC
                   ) AS rn
            FROM bronze.reconciliation_results_raw
        ),
        deduped AS (
            SELECT
                id,
                run_id,
                transaction_id,
                merchant_id,
                category,
                internal_amount,
                processor_amount,
                difference,
                created_at::TIMESTAMPTZ AS created_at,
                Op,
                _timestamp::TIMESTAMPTZ AS _timestamp
            FROM ranked
            WHERE rn = 1
        )
        SELECT
            d.id,
            d.run_id,
            d.transaction_id,
            d.merchant_id,
            d.category,
            d.internal_amount,
            d.processor_amount,
            d.difference,
            d.created_at,
            r.reference_date,
            d.Op,
            d._timestamp
        FROM deduped d
        LEFT JOIN silver.reconciliation_runs r ON d.run_id = r.run_id
    """)

    count = con.execute("SELECT count(*) FROM silver.reconciliation_results").fetchone()[0]
    logger.info(f"silver.reconciliation_results: {count} rows (with reference_date)")


def build_silver_paysettler_settlements(con: duckdb.DuckDBPyConnection) -> None:
    """Silver PaySettler: cleaned settlements."""
    con.execute("""
        CREATE OR REPLACE TABLE silver.paysettler_settlements AS
        SELECT
            transaction_id,
            merchant_id,
            amount,
            currency,
            settled_at,
            processor_reference,
            status,
            _ingested_at,
            _source_file,
            _reference_date
        FROM bronze.paysettler_raw
    """)

    count = con.execute("SELECT count(*) FROM silver.paysettler_settlements").fetchone()[0]
    logger.info(f"silver.paysettler_settlements: {count} rows")


def run_silver(
    con: duckdb.DuckDBPyConnection,
    cfg: PipelineConfig,
    reference_date: str,
    metrics: RunMetrics,
) -> None:
    """Execute full silver layer."""
    create_silver_schema(con)
    build_silver_transactions(con)
    build_silver_merchants(con)
    build_silver_reconciliation_runs(con)
    build_silver_reconciliation_results(con)
    build_silver_paysettler_settlements(con)

    # Run reconciliation for the reference_date
    reconcile(con, reference_date, cfg)

    # Populate metrics from reconciliation
    counts = con.execute(f"""
        SELECT category, count(*)
        FROM silver.reconciliation_results
        WHERE reference_date = '{reference_date}'
        GROUP BY category
    """).fetchall()

    cat_map = dict(counts)
    metrics.matched = cat_map.get("MATCHED", 0)
    metrics.mismatched = cat_map.get("MISMATCHED", 0)
    metrics.unreconciled_processor = cat_map.get("UNRECONCILED_PROCESSOR", 0)
    metrics.unreconciled_internal = cat_map.get("UNRECONCILED_INTERNAL", 0)
    metrics.total_processed = sum(cat_map.values())

    volumes = con.execute(f"""
        SELECT
            COALESCE(SUM(internal_amount), 0),
            COALESCE(SUM(processor_amount), 0),
            COALESCE(SUM(difference), 0)
        FROM silver.reconciliation_results
        WHERE reference_date = '{reference_date}'
    """).fetchone()
    metrics.total_internal_volume = volumes[0]
    metrics.total_processor_volume = volumes[1]
    metrics.total_difference = volumes[2]
