"""Gold layer — business-consumable tables for Operations, CFO, and Compliance."""
from __future__ import annotations

import duckdb
from pipeline.observability.logging import get_logger

logger = get_logger(__name__)


def create_gold_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")


def build_daily_reconciliation_health(con: duckdb.DuckDBPyConnection) -> None:
    """-- consumer: operations
    Daily reconciliation health: discrepancy rate, unreconciled volumes, processing time.
    """
    con.execute("""
        CREATE OR REPLACE TABLE gold.daily_reconciliation_health AS
        WITH run_stats AS (
            SELECT
                r.reference_date,
                r.run_id,
                r.status AS run_status,
                r.started_at,
                r.completed_at,
                r.total_transactions,
                EXTRACT(EPOCH FROM (r.completed_at - r.started_at)) / 60.0 AS processing_time_minutes
            FROM silver.reconciliation_runs r
            WHERE r.status = 'COMPLETED'
        ),
        result_stats AS (
            SELECT
                reference_date,
                count(*) AS total_results,
                SUM(CASE WHEN category = 'MATCHED' THEN 1 ELSE 0 END) AS matched_count,
                SUM(CASE WHEN category = 'MISMATCHED' THEN 1 ELSE 0 END) AS mismatched_count,
                SUM(CASE WHEN category = 'UNRECONCILED_PROCESSOR' THEN 1 ELSE 0 END) AS unreconciled_processor_count,
                SUM(CASE WHEN category = 'UNRECONCILED_INTERNAL' THEN 1 ELSE 0 END) AS unreconciled_internal_count
            FROM silver.reconciliation_results
            GROUP BY reference_date
        )
        SELECT
            rs.reference_date,
            rs.total_results,
            rs.matched_count,
            rs.mismatched_count,
            rs.unreconciled_processor_count,
            rs.unreconciled_internal_count,
            CASE WHEN rs.total_results > 0
                 THEN rs.mismatched_count::DOUBLE / rs.total_results
                 ELSE 0 END AS discrepancy_rate,
            run_s.run_status,
            run_s.processing_time_minutes,
            run_s.started_at,
            run_s.completed_at
        FROM result_stats rs
        LEFT JOIN run_stats run_s ON rs.reference_date = run_s.reference_date
        ORDER BY rs.reference_date
    """)

    count = con.execute("SELECT count(*) FROM gold.daily_reconciliation_health").fetchone()[0]
    logger.info(f"gold.daily_reconciliation_health: {count} rows")


def build_daily_financial_volume(con: duckdb.DuckDBPyConnection) -> None:
    """-- consumer: cfo
    Daily financial volume: internal, processor, and accumulated difference.
    """
    con.execute("""
        CREATE OR REPLACE TABLE gold.daily_financial_volume AS
        SELECT
            reference_date,
            SUM(COALESCE(internal_amount, 0)) AS internal_volume_brl,
            SUM(COALESCE(processor_amount, 0)) AS processor_volume_brl,
            SUM(COALESCE(internal_amount, 0)) - SUM(COALESCE(processor_amount, 0)) AS daily_difference,
            SUM(COALESCE(difference, 0)) AS total_abs_difference,
            count(*) AS transaction_count
        FROM silver.reconciliation_results
        GROUP BY reference_date
        ORDER BY reference_date
    """)

    count = con.execute("SELECT count(*) FROM gold.daily_financial_volume").fetchone()[0]
    logger.info(f"gold.daily_financial_volume: {count} rows")


def build_merchant_volume_summary(con: duckdb.DuckDBPyConnection) -> None:
    """-- consumer: cfo
    Merchant-level volume summary for top merchants.
    """
    con.execute("""
        CREATE OR REPLACE TABLE gold.merchant_volume_summary AS
        SELECT
            r.merchant_id,
            m.legal_name,
            m.trade_name,
            m.document,
            count(*) AS total_transactions,
            SUM(COALESCE(r.internal_amount, 0)) AS total_internal_volume,
            SUM(COALESCE(r.processor_amount, 0)) AS total_processor_volume,
            SUM(COALESCE(r.difference, 0)) AS total_difference,
            SUM(CASE WHEN r.category = 'MISMATCHED' THEN 1 ELSE 0 END) AS mismatched_count
        FROM silver.reconciliation_results r
        LEFT JOIN silver.merchants m ON r.merchant_id = m.merchant_id
        GROUP BY r.merchant_id, m.legal_name, m.trade_name, m.document
        ORDER BY total_internal_volume DESC
    """)

    count = con.execute("SELECT count(*) FROM gold.merchant_volume_summary").fetchone()[0]
    logger.info(f"gold.merchant_volume_summary: {count} rows")


def build_audit_discrepancies(con: duckdb.DuckDBPyConnection) -> None:
    """-- consumer: compliance
    All MISMATCHED transactions with both sides' values for audit trail.
    """
    con.execute("""
        CREATE OR REPLACE TABLE gold.audit_discrepancies AS
        SELECT
            r.reference_date,
            r.transaction_id,
            r.merchant_id,
            m.legal_name,
            m.trade_name,
            m.document,
            r.internal_amount,
            r.processor_amount,
            r.difference,
            r.created_at
        FROM silver.reconciliation_results r
        LEFT JOIN silver.merchants m ON r.merchant_id = m.merchant_id
        WHERE r.category = 'MISMATCHED'
        ORDER BY r.difference DESC
    """)

    count = con.execute("SELECT count(*) FROM gold.audit_discrepancies").fetchone()[0]
    logger.info(f"gold.audit_discrepancies: {count} rows")


def build_audit_unreconciled(con: duckdb.DuckDBPyConnection) -> None:
    """-- consumer: compliance
    All UNRECONCILED transactions with merchant and reference date.
    """
    con.execute("""
        CREATE OR REPLACE TABLE gold.audit_unreconciled AS
        SELECT
            r.reference_date,
            r.transaction_id,
            r.merchant_id,
            m.legal_name,
            m.trade_name,
            m.document,
            r.category,
            COALESCE(r.internal_amount, r.processor_amount) AS amount,
            r.created_at
        FROM silver.reconciliation_results r
        LEFT JOIN silver.merchants m ON r.merchant_id = m.merchant_id
        WHERE r.category IN ('UNRECONCILED_PROCESSOR', 'UNRECONCILED_INTERNAL')
        ORDER BY r.reference_date, r.category
    """)

    count = con.execute("SELECT count(*) FROM gold.audit_unreconciled").fetchone()[0]
    logger.info(f"gold.audit_unreconciled: {count} rows")


def build_audit_reconciliation_runs_history(con: duckdb.DuckDBPyConnection) -> None:
    """-- consumer: compliance
    Complete history of all reconciliation runs with metadata.
    """
    con.execute("""
        CREATE OR REPLACE TABLE gold.audit_reconciliation_runs_history AS
        SELECT
            run_id,
            reference_date,
            file_name,
            status,
            total_transactions,
            started_at,
            completed_at,
            EXTRACT(EPOCH FROM (completed_at - started_at)) / 60.0 AS processing_time_minutes
        FROM silver.reconciliation_runs
        ORDER BY reference_date
    """)

    count = con.execute("SELECT count(*) FROM gold.audit_reconciliation_runs_history").fetchone()[0]
    logger.info(f"gold.audit_reconciliation_runs_history: {count} rows")


def run_gold(con: duckdb.DuckDBPyConnection) -> None:
    """Execute full gold layer."""
    create_gold_schema(con)
    build_daily_reconciliation_health(con)
    build_daily_financial_volume(con)
    build_merchant_volume_summary(con)
    build_audit_discrepancies(con)
    build_audit_unreconciled(con)
    build_audit_reconciliation_runs_history(con)
