"""Gold layer: dimensional model (Kimball) — dimensions, facts, marts.

- fact_reconciliation_result: transaction grain (1 row per transaction per run) — the audit trail.
- fact_settlement_daily: periodic snapshot (merchant x currency x day) — serves Ops + CFO.
- dim_merchant (SCD2), dim_date (role-playing), dim_transaction_profile (junk).
Facts are written with a transactional delete+insert per reference_date (idempotent).
"""
from __future__ import annotations

from pipeline.db import transaction

# Reusable expressions so the junk dimension and the fact join stay in sync.
_WITHIN_TOL = "(r.difference IS NOT NULL AND r.difference <= 0.01)"
_SETTLE_STATUS = "COALESCE(s.status, 'N/A')"
_HAS_INTERNAL = "(r.internal_amount IS NOT NULL)"
_HAS_PROCESSOR = "(r.processor_amount IS NOT NULL)"


def build_dimensions(con) -> None:
    # dim_date — covers every event date (run reference dates, settlement dates,
    # internal created dates); role-played as reference_date and settlement_date.
    con.execute("""
        CREATE OR REPLACE TABLE dim_date AS
        WITH all_dates AS (
            SELECT CAST(reference_date AS DATE) AS d FROM silver_runs
            UNION ALL SELECT CAST(settled_at AS DATE) FROM silver_settlement
            UNION ALL SELECT CAST(substr(created_at, 1, 10) AS DATE) FROM silver_transactions
        ),
        span AS (SELECT min(d) AS lo, max(d) AS hi FROM all_dates),
        days AS (
            SELECT CAST(unnest(generate_series(
                (SELECT lo FROM span)::TIMESTAMP,
                (SELECT hi FROM span)::TIMESTAMP,
                INTERVAL 1 DAY)) AS DATE) AS date
        )
        SELECT
            CAST(strftime(date, '%Y%m%d') AS INTEGER) AS date_key,
            date,
            year(date) AS year, quarter(date) AS quarter, month(date) AS month,
            monthname(date) AS month_name, day(date) AS day,
            dayofweek(date) AS day_of_week,
            (dayofweek(date) IN (0, 6)) AS is_weekend,
            week(date) AS iso_week
        FROM days
    """)

    # dim_merchant — SCD Type 2 (fixture has one version per merchant: is_current, open valid_to).
    con.execute("CREATE OR REPLACE SEQUENCE merchant_key_seq START 1")
    con.execute("""
        CREATE OR REPLACE TABLE dim_merchant AS
        SELECT nextval('merchant_key_seq') AS merchant_key,
               merchant_id, legal_name, trade_name, document, primary_cnae,
               CAST(created_at AS TIMESTAMP) AS valid_from,
               CAST(NULL AS TIMESTAMP) AS valid_to,
               TRUE AS is_current
        FROM silver_merchants
    """)

    # dim_transaction_profile — junk dimension: only the flag combinations that occur.
    con.execute("CREATE OR REPLACE SEQUENCE profile_key_seq START 1")
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_transaction_profile AS
        SELECT nextval('profile_key_seq') AS profile_key,
               category, within_tolerance, settlement_status,
               has_internal_record, has_processor_record
        FROM (
            SELECT DISTINCT
                r.category AS category,
                {_WITHIN_TOL} AS within_tolerance,
                {_SETTLE_STATUS} AS settlement_status,
                {_HAS_INTERNAL} AS has_internal_record,
                {_HAS_PROCESSOR} AS has_processor_record
            FROM silver_results r
            LEFT JOIN silver_settlement s ON s.transaction_id = r.transaction_id
        )
    """)


def init_facts(con) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_reconciliation_result (
            result_id BIGINT, transaction_id VARCHAR, reconciliation_run_id BIGINT, processor_reference VARCHAR,
            merchant_key BIGINT, reference_date DATE, settlement_date DATE, profile_key BIGINT,
            category VARCHAR, internal_amount DECIMAL(18,2), processor_amount DECIMAL(18,2),
            settled_amount DECIMAL(18,2), difference DECIMAL(18,2),
            is_matched INTEGER, is_mismatched INTEGER, is_unreconciled INTEGER
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_settlement_daily (
            reference_date DATE, merchant_key BIGINT, currency VARCHAR,
            total_transactions BIGINT, matched_count BIGINT, mismatched_count BIGINT,
            unreconciled_processor_count BIGINT, unreconciled_internal_count BIGINT,
            discrepancy_rate DOUBLE, total_settled_amount DECIMAL(18,2),
            total_reversed_amount DECIMAL(18,2), total_difference DECIMAL(18,2)
        )
    """)


def build_facts(con, reference_date: str) -> None:
    """Idempotently (re)build both facts for one reference_date in a single transaction."""
    with transaction(con):
        con.execute(
            "DELETE FROM fact_reconciliation_result WHERE reference_date = CAST(? AS DATE)",
            [reference_date],
        )
        con.execute(f"""
            INSERT INTO fact_reconciliation_result
            SELECT
                r.id AS result_id, r.transaction_id, r.run_id, s.processor_reference,
                m.merchant_key, CAST(ru.reference_date AS DATE), CAST(s.settled_at AS DATE),
                p.profile_key, r.category, r.internal_amount, r.processor_amount,
                s.settled_amount, r.difference,
                (r.category = 'MATCHED')::INTEGER,
                (r.category = 'MISMATCHED')::INTEGER,
                (r.category LIKE 'UNRECONCILED%')::INTEGER
            FROM silver_results r
            JOIN silver_runs ru ON ru.id = r.run_id
            LEFT JOIN dim_merchant m ON m.merchant_id = r.merchant_id AND m.is_current
            LEFT JOIN silver_settlement_current s ON s.transaction_id = r.transaction_id
            LEFT JOIN dim_transaction_profile p
                   ON p.category = r.category
                  AND p.within_tolerance = {_WITHIN_TOL}
                  AND p.settlement_status = {_SETTLE_STATUS}
                  AND p.has_internal_record = {_HAS_INTERNAL}
                  AND p.has_processor_record = {_HAS_PROCESSOR}
            WHERE ru.reference_date = ?
        """, [reference_date])

        con.execute(
            "DELETE FROM fact_settlement_daily WHERE reference_date = CAST(? AS DATE)",
            [reference_date],
        )
        con.execute("""
            INSERT INTO fact_settlement_daily
            WITH base AS (
                SELECT
                    CAST(ru.reference_date AS DATE) AS reference_date,
                    m.merchant_key,
                    COALESCE(s.currency, tx.currency, 'BRL') AS currency,
                    r.category, r.difference, s.settled_amount, s.status AS settle_status
                FROM silver_results r
                JOIN silver_runs ru ON ru.id = r.run_id
                LEFT JOIN dim_merchant m ON m.merchant_id = r.merchant_id AND m.is_current
                LEFT JOIN silver_settlement_current s ON s.transaction_id = r.transaction_id
                LEFT JOIN silver_transactions tx ON tx.transaction_id = r.transaction_id
                WHERE ru.reference_date = ?
            )
            SELECT
                reference_date, merchant_key, currency,
                count(*) AS total_transactions,
                count(*) FILTER (WHERE category = 'MATCHED') AS matched_count,
                count(*) FILTER (WHERE category = 'MISMATCHED') AS mismatched_count,
                count(*) FILTER (WHERE category = 'UNRECONCILED_PROCESSOR') AS unreconciled_processor_count,
                count(*) FILTER (WHERE category = 'UNRECONCILED_INTERNAL') AS unreconciled_internal_count,
                1.0 - (count(*) FILTER (WHERE category = 'MATCHED'))::DOUBLE / count(*) AS discrepancy_rate,
                sum(settled_amount) FILTER (WHERE settle_status = 'SETTLED') AS total_settled_amount,
                sum(settled_amount) FILTER (WHERE settle_status = 'REVERSED') AS total_reversed_amount,
                sum(difference) AS total_difference
            FROM base
            GROUP BY reference_date, merchant_key, currency
        """, [reference_date])


def build_marts(con) -> None:
    con.execute("""
        CREATE OR REPLACE TABLE mart_ops_reconciliation_health AS
        SELECT reference_date,
               sum(total_transactions) AS total_transactions,
               sum(matched_count) AS matched,
               sum(mismatched_count) AS mismatched,
               sum(unreconciled_processor_count) AS unreconciled_processor,
               sum(unreconciled_internal_count) AS unreconciled_internal,
               1.0 - sum(matched_count)::DOUBLE / nullif(sum(total_transactions), 0) AS discrepancy_rate
        FROM fact_settlement_daily
        GROUP BY reference_date ORDER BY reference_date
    """)
    con.execute("""
        CREATE OR REPLACE TABLE mart_cfo_settled_volume AS
        SELECT f.reference_date, f.currency, m.primary_cnae,
               sum(f.total_settled_amount) AS total_settled_amount,
               sum(f.total_reversed_amount) AS total_reversed_amount,
               sum(f.total_transactions) AS transactions
        FROM fact_settlement_daily f
        LEFT JOIN dim_merchant m ON m.merchant_key = f.merchant_key
        GROUP BY f.reference_date, f.currency, m.primary_cnae
        ORDER BY f.reference_date
    """)
    con.execute("""
        CREATE OR REPLACE TABLE mart_compliance_audit_trail AS
        SELECT f.transaction_id, f.reconciliation_run_id, f.reference_date, f.settlement_date,
               m.merchant_id, m.legal_name, m.document, f.category,
               f.internal_amount, f.processor_amount, f.settled_amount, f.difference,
               f.processor_reference
        FROM fact_reconciliation_result f
        LEFT JOIN dim_merchant m ON m.merchant_key = f.merchant_key
    """)
