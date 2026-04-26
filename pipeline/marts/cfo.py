"""gold.mart_cfo — consolidated financial volume by day and merchant.

Consumer: CFO / Finance team.

Questions answered:
  - What is the total settled volume per merchant today?
  - Which merchants moved the most money this week?
  - What is the financial exposure from mismatched settlements?
  - How does settled volume compare to internal transaction volume?

Granularity: one row per (reference_date, merchant_id).

total_settled_amount: sum of processor_amount where the processor has
settled the transaction (category IN ('MATCHED', 'MISMATCHED')). This
represents cash that actually moved through PaySettler.

Idempotency: DELETE WHERE reference_date = ? then INSERT.
"""
from __future__ import annotations

import logging

import duckdb

from pipeline.db import transaction

logger = logging.getLogger(__name__)


def build_mart_cfo(
    conn: duckdb.DuckDBPyConnection,
    reference_date: str,
    pipeline_run_id: str,
) -> int:
    with transaction(conn):
        conn.execute(
            "DELETE FROM gold.mart_cfo WHERE reference_date = ?::DATE",
            [reference_date],
        )
        conn.execute(
            """
        INSERT INTO gold.mart_cfo (
            reference_date,
            merchant_id,
            merchant_legal_name,
            merchant_trade_name,
            document,
            primary_cnae,
            total_transactions,
            total_settled_amount,
            total_discrepancy_amount,
            matched_count,
            mismatched_count,
            unreconciled_internal_count,
            unreconciled_processor_count,
            _pipeline_run_id
        )
        SELECT
            rr.reference_date,
            rr.merchant_id,
            m.legal_name                                                                        AS merchant_legal_name,
            m.trade_name                                                                        AS merchant_trade_name,
            m.document,
            m.primary_cnae,
            COUNT(rr.result_id)                                                                 AS total_transactions,
            COALESCE(
                SUM(CASE WHEN rr.category IN ('MATCHED', 'MISMATCHED')
                         THEN rr.processor_amount ELSE 0 END),
            0)                                                                                  AS total_settled_amount,
            COALESCE(
                SUM(CASE WHEN rr.category = 'MISMATCHED' THEN rr.difference ELSE 0 END),
            0)                                                                                  AS total_discrepancy_amount,
            SUM(CASE WHEN rr.category = 'MATCHED'                THEN 1 ELSE 0 END)            AS matched_count,
            SUM(CASE WHEN rr.category = 'MISMATCHED'             THEN 1 ELSE 0 END)            AS mismatched_count,
            SUM(CASE WHEN rr.category = 'UNRECONCILED_INTERNAL'  THEN 1 ELSE 0 END)            AS unreconciled_internal_count,
            SUM(CASE WHEN rr.category = 'UNRECONCILED_PROCESSOR' THEN 1 ELSE 0 END)            AS unreconciled_processor_count,
            ?                                                                                   AS _pipeline_run_id
        FROM silver.reconciliation_results rr
        LEFT JOIN silver.merchants m USING (merchant_id)
        WHERE rr.reference_date = ?::DATE
          AND rr.merchant_id IS NOT NULL
        GROUP BY
            rr.reference_date,
            rr.merchant_id,
            m.legal_name,
            m.trade_name,
            m.document,
            m.primary_cnae
        """,
        [pipeline_run_id, reference_date],
    )
    count: int = conn.execute(
        "SELECT COUNT(*) FROM gold.mart_cfo WHERE reference_date = ?::DATE",
        [reference_date],
    ).fetchone()[0]
    logger.info(
        "gold.mart_cfo built",
        extra={"counts": {"rows": count}, "reference_date": reference_date},
    )
    return count
