"""Reconciliation logic — categorize transactions as MATCHED, MISMATCHED, etc.

Compares internal transactions (silver.transactions) against PaySettler
settlements (silver.paysettler_settlements) for a given reference_date.

Matching rules:
- Match by transaction_id
- MATCHED: found on both sides, |difference| <= tolerance (R$ 0.01)
- MISMATCHED: found on both sides, |difference| > tolerance
- UNRECONCILED_PROCESSOR: in PaySettler but not internal (within window)
- UNRECONCILED_INTERNAL: in internal but not PaySettler (within window)
"""
from __future__ import annotations

import duckdb
from pipeline.config import PipelineConfig
from pipeline.observability.logging import get_logger

logger = get_logger(__name__)


def reconcile(
    con: duckdb.DuckDBPyConnection,
    reference_date: str,
    cfg: PipelineConfig,
) -> None:
    """Run reconciliation for a reference_date, inserting results into
    silver.reconciliation_results.

    The reconciliation_results table is appended to (not replaced), with
    each reference_date being idempotently handled via DELETE+INSERT.
    """
    window_days = cfg.reconciliation_window_days
    tolerance = cfg.mismatch_tolerance

    # Ensure the reconciliation_results table exists with the extended schema
    # that includes reference_date from our pipeline (not just from the CDC source)
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver.reconciliation_results (
            id INTEGER,
            run_id BIGINT,
            transaction_id VARCHAR,
            merchant_id VARCHAR,
            category VARCHAR,
            internal_amount DOUBLE,
            processor_amount DOUBLE,
            difference DOUBLE,
            created_at TIMESTAMPTZ,
            reference_date VARCHAR,
            Op VARCHAR,
            _timestamp TIMESTAMPTZ
        )
    """)

    # Idempotent: delete existing results for this reference_date before re-inserting
    con.execute(f"""
        DELETE FROM silver.reconciliation_results
        WHERE reference_date = '{reference_date}'
    """)

    # Insert reconciliation results using SQL directly
    con.execute(f"""
        WITH internal_window AS (
            SELECT transaction_id, merchant_id, amount, created_at
            FROM silver.transactions
            WHERE created_at::DATE BETWEEN
                  ('{reference_date}'::DATE - INTERVAL '{window_days} days') AND
                  '{reference_date}'::DATE
        ),
        processor AS (
            SELECT transaction_id, merchant_id, amount, settled_at, status
            FROM silver.paysettler_settlements
        ),
        matched AS (
            SELECT
                i.transaction_id,
                COALESCE(i.merchant_id, p.merchant_id) AS merchant_id,
                CASE
                    WHEN ABS(i.amount - p.amount) <= {tolerance} THEN 'MATCHED'
                    ELSE 'MISMATCHED'
                END AS category,
                i.amount AS internal_amount,
                p.amount AS processor_amount,
                ABS(i.amount - p.amount) AS difference
            FROM internal_window i
            JOIN processor p ON i.transaction_id = p.transaction_id
        ),
        unreconciled_processor AS (
            SELECT
                p.transaction_id,
                p.merchant_id,
                'UNRECONCILED_PROCESSOR' AS category,
                NULL::DOUBLE AS internal_amount,
                p.amount AS processor_amount,
                NULL::DOUBLE AS difference
            FROM processor p
            WHERE p.transaction_id NOT IN (SELECT transaction_id FROM internal_window)
        ),
        unreconciled_internal AS (
            SELECT
                i.transaction_id,
                i.merchant_id,
                'UNRECONCILED_INTERNAL' AS category,
                i.amount AS internal_amount,
                NULL::DOUBLE AS processor_amount,
                NULL::DOUBLE AS difference
            FROM internal_window i
            WHERE i.transaction_id NOT IN (SELECT transaction_id FROM processor)
        ),
        all_results AS (
            SELECT * FROM matched
            UNION ALL
            SELECT * FROM unreconciled_processor
            UNION ALL
            SELECT * FROM unreconciled_internal
        )
        INSERT INTO silver.reconciliation_results
            (transaction_id, merchant_id, category, internal_amount,
             processor_amount, difference, reference_date)
        SELECT
            transaction_id,
            merchant_id,
            category,
            internal_amount,
            processor_amount,
            difference,
            '{reference_date}' AS reference_date
        FROM all_results
    """)

    counts = con.execute(f"""
        SELECT category, count(*)
        FROM silver.reconciliation_results
        WHERE reference_date = '{reference_date}'
        GROUP BY category
    """).fetchall()

    for cat, cnt in counts:
        logger.info(f"Reconciliation {reference_date}: {cat} = {cnt}")
