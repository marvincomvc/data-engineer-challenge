"""Silver layer assembly: CDC dedup + type normalization from bronze tables.

Each builder is idempotent:
  - Master data (transactions, merchants): full DELETE + INSERT — bronze always
    has a complete snapshot.
  - Date-scoped data (runs, results, settlement): DELETE WHERE reference_date/
    source_file = current run, then INSERT matching rows only.
"""
from __future__ import annotations

import logging

import duckdb

from pipeline.db import transaction
from pipeline.transform.cdc import dedup_latest

logger = logging.getLogger(__name__)

# SQL expression to normalize a raw settled_at VARCHAR to TIMESTAMPTZ.
# Appends +00:00 for bare datetimes (no tz suffix) — assumes UTC, matching
# how the generator treats timestamps without explicit timezone.
_SETTLED_AT_EXPR = """
    TRY_CAST(
        CASE
            WHEN settled_at LIKE '%Z' OR settled_at LIKE '%+%' OR settled_at LIKE '%-__:__'
                THEN settled_at
            WHEN settled_at IS NOT NULL AND len(settled_at) > 0
                THEN settled_at || '+00:00'
            ELSE NULL
        END
    AS TIMESTAMPTZ)
"""

# SQL expression to normalize a raw amount VARCHAR to DECIMAL(18,2).
# Handles formats found in the source CSV:
#   "13266.42"     — standard float
#   "13266,42"     — comma as decimal separator
#   "23.696,77"    — BRL format (dot=thousands, comma=decimal)
#   "R$ 23.696,77" — BRL prefix + thousands dot + comma decimal
#   "R$ 873,16"    — BRL prefix + comma decimal only
# Rule: if a comma is present, it is the decimal separator;
# any dot before it is a thousands separator and is removed.
_AMOUNT_EXPR = """
    TRY_CAST(
        CASE
            WHEN trim(amount) LIKE 'R$%'
                THEN replace(replace(trim(substr(trim(amount), 3)), '.', ''), ',', '.')
            WHEN trim(amount) LIKE '%,%'
                THEN replace(replace(trim(amount), '.', ''), ',', '.')
            ELSE trim(amount)
        END
    AS DECIMAL(18, 2))
"""


def build_silver_transactions(conn: duckdb.DuckDBPyConnection) -> int:
    """Deduplicate bronze.transactions → silver.transactions.

    Takes the latest CDC row per transaction_id. Marks rows with op='D' as
    is_deleted=true so downstream joins can exclude them.
    Full replace because bronze always holds a complete snapshot.
    """
    with transaction(conn):
        conn.execute("DELETE FROM silver.transactions")
        conn.execute(f"""
            INSERT INTO silver.transactions (
                transaction_id, merchant_id, amount, currency, status,
                payment_method, created_at, updated_at, is_deleted
            )
            SELECT
                transaction_id,
                UPPER(TRIM(merchant_id))                     AS merchant_id,
                TRY_CAST(amount AS DECIMAL(18, 2))           AS amount,
                currency,
                status,
                payment_method,
                TRY_CAST(created_at AS TIMESTAMPTZ)          AS created_at,
                TRY_CAST(updated_at AS TIMESTAMPTZ)          AS updated_at,
                (op = 'D')                                   AS is_deleted
            FROM ({dedup_latest("bronze.transactions", "transaction_id")})
        """)
    count: int = conn.execute("SELECT COUNT(*) FROM silver.transactions").fetchone()[0]
    deleted = conn.execute(
        "SELECT COUNT(*) FROM silver.transactions WHERE is_deleted"
    ).fetchone()[0]
    logger.info(
        "silver.transactions built",
        extra={"counts": {"rows": count, "deleted": deleted}},
    )
    return count


def build_silver_merchants(conn: duckdb.DuckDBPyConnection) -> int:
    """Deduplicate bronze.merchants → silver.merchants.

    Full replace — merchant master is reference data with no date scope.
    """
    with transaction(conn):
        conn.execute("DELETE FROM silver.merchants")
        conn.execute(f"""
            INSERT INTO silver.merchants (
                merchant_id, legal_name, trade_name, document,
                primary_cnae, created_at, updated_at
            )
            SELECT
                merchant_id,
                legal_name,
                trade_name,
                document,
                primary_cnae,
                TRY_CAST(created_at AS TIMESTAMPTZ) AS created_at,
                TRY_CAST(updated_at AS TIMESTAMPTZ) AS updated_at
            FROM ({dedup_latest("bronze.merchants", "merchant_id")})
        """)
    count: int = conn.execute("SELECT COUNT(*) FROM silver.merchants").fetchone()[0]
    logger.info("silver.merchants built", extra={"counts": {"rows": count}})
    return count


def build_silver_reconciliation_runs(
    conn: duckdb.DuckDBPyConnection,
    reference_date: str,
) -> int:
    """Deduplicate bronze.reconciliation_runs → silver.reconciliation_runs.

    Scoped to the given reference_date. Each run_id has an I (IN_PROGRESS) and
    a U (COMPLETED) CDC row; dedup_latest keeps the COMPLETED state.
    """
    with transaction(conn):
        conn.execute(
            "DELETE FROM silver.reconciliation_runs WHERE reference_date = ?::DATE",
            [reference_date],
        )
        conn.execute(
            f"""
            INSERT INTO silver.reconciliation_runs (
                run_id, reference_date, file_name, status, total_transactions,
                started_at, completed_at, created_at
            )
            SELECT
                id                                          AS run_id,
                reference_date::DATE                       AS reference_date,
                file_name,
                status,
                total_transactions,
                TRY_CAST(started_at   AS TIMESTAMPTZ)      AS started_at,
                TRY_CAST(completed_at AS TIMESTAMPTZ)      AS completed_at,
                TRY_CAST(created_at   AS TIMESTAMPTZ)      AS created_at
            FROM ({dedup_latest("bronze.reconciliation_runs", "id")})
            WHERE reference_date::DATE = ?::DATE
            """,
            [reference_date],
        )
    count: int = conn.execute(
        "SELECT COUNT(*) FROM silver.reconciliation_runs WHERE reference_date = ?::DATE",
        [reference_date],
    ).fetchone()[0]
    logger.info(
        "silver.reconciliation_runs built",
        extra={"counts": {"rows": count}, "reference_date": reference_date},
    )
    return count


def build_silver_reconciliation_results(
    conn: duckdb.DuckDBPyConnection,
    reference_date: str,
) -> int:
    """Load bronze.reconciliation_results → silver.reconciliation_results.

    Joins to silver.reconciliation_runs (already built for this reference_date)
    to populate the reference_date column. Results whose run_id has no matching
    run in silver are excluded — the WHERE on rn.reference_date acts as an
    implicit INNER JOIN filter, so orphan results never reach the silver table.
    """
    with transaction(conn):
        conn.execute(
            "DELETE FROM silver.reconciliation_results WHERE reference_date = ?::DATE",
            [reference_date],
        )
        conn.execute(
            """
            INSERT INTO silver.reconciliation_results (
                result_id, run_id, transaction_id, merchant_id, category,
                internal_amount, processor_amount, difference,
                reference_date, created_at
            )
            -- Deduplicate by (transaction_id, run_id) before joining to runs.
            -- CDC replay can produce duplicate result rows with the same business key;
            -- keep the latest row by _timestamp to get the final reconciled state.
            WITH deduped AS (
                SELECT * EXCLUDE (_rn)
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY transaction_id, run_id
                            ORDER BY _timestamp DESC
                        ) AS _rn
                    FROM bronze.reconciliation_results
                )
                WHERE _rn = 1
            )
            SELECT
                r.id                                            AS result_id,
                r.run_id,
                r.transaction_id,
                r.merchant_id,
                r.category,
                TRY_CAST(r.internal_amount  AS DECIMAL(18, 2)) AS internal_amount,
                TRY_CAST(r.processor_amount AS DECIMAL(18, 2)) AS processor_amount,
                TRY_CAST(r.difference       AS DECIMAL(18, 2)) AS difference,
                rn.reference_date                              AS reference_date,
                TRY_CAST(r.created_at       AS TIMESTAMPTZ)    AS created_at
            FROM deduped r
            LEFT JOIN silver.reconciliation_runs rn USING (run_id)
            WHERE rn.reference_date = ?::DATE
            """,
            [reference_date],
        )
    count: int = conn.execute(
        "SELECT COUNT(*) FROM silver.reconciliation_results WHERE reference_date = ?::DATE",
        [reference_date],
    ).fetchone()[0]
    logger.info(
        "silver.reconciliation_results built",
        extra={"counts": {"rows": count}, "reference_date": reference_date},
    )
    return count


def build_silver_settlement(
    conn: duckdb.DuckDBPyConnection,
    source_file: str,
) -> int:
    """Normalize bronze.settlement → silver.settlement.

    Scoped by source_file for idempotency. Normalization applied:
      - amount: all BRL formats → DECIMAL ("R$ 23.696,77" → 23696.77)
      - settled_at: bare ISO 8601 or Z-suffix → TIMESTAMPTZ UTC
      - merchant_id: lowercased ids uppercased
    """
    with transaction(conn):
        conn.execute(
            "DELETE FROM silver.settlement WHERE _source_file = ?",
            [source_file],
        )
        conn.execute(
            f"""
            INSERT INTO silver.settlement (
                transaction_id, merchant_id, amount, currency,
                settled_at, processor_reference, status, _source_file
            )
            SELECT
                transaction_id,
                UPPER(TRIM(merchant_id))  AS merchant_id,
                {_AMOUNT_EXPR}            AS amount,
                currency,
                {_SETTLED_AT_EXPR}                                  AS settled_at,
                processor_reference,
                status,
                _source_file
            FROM bronze.settlement
            WHERE _source_file = ?
            """,
            [source_file],
        )
    count: int = conn.execute(
        "SELECT COUNT(*) FROM silver.settlement WHERE _source_file = ?",
        [source_file],
    ).fetchone()[0]
    logger.info(
        "silver.settlement built",
        extra={"counts": {"rows": count, "source_file": source_file}},
    )
    return count


def build_silver(
    conn: duckdb.DuckDBPyConnection,
    reference_date: str,
    settlement_source_file: str | None = None,
) -> dict[str, int]:
    """Orchestrate all silver builders for a given reference_date.

    settlement_source_file must be the exact _source_file value used during
    ingest (i.e. the CSV filename). Passing it explicitly avoids a non-
    deterministic LIMIT 1 query when bronze.settlement holds multiple dates.

    Returns a dict with row counts per table.
    """
    counts = {
        "transactions": build_silver_transactions(conn),
        "merchants": build_silver_merchants(conn),
        "reconciliation_runs": build_silver_reconciliation_runs(conn, reference_date),
        "reconciliation_results": build_silver_reconciliation_results(conn, reference_date),
        "settlement": (
            build_silver_settlement(conn, settlement_source_file)
            if settlement_source_file
            else 0
        ),
    }
    return counts
