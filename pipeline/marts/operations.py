"""gold.mart_operations — daily reconciliation health per run.

Consumer: Operations team.

Questions answered:
  - Did today's reconciliation complete successfully?
  - What is the match rate trend over the last N days?
  - Which runs have the highest discrepancy volume?
  - How long are runs taking over time?

Granularity: one row per (reference_date, run_id).
Idempotency: DELETE WHERE reference_date = ? then INSERT.
"""
from __future__ import annotations

import logging

import duckdb

from pipeline.db import transaction

logger = logging.getLogger(__name__)


def build_mart_operations(
    conn: duckdb.DuckDBPyConnection,
    reference_date: str,
    pipeline_run_id: str,
) -> int:
    with transaction(conn):
        conn.execute(
            "DELETE FROM gold.mart_operations WHERE reference_date = ?::DATE",
            [reference_date],
        )
        conn.execute(
            """
        INSERT INTO gold.mart_operations (
            reference_date,
            run_id,
            file_name,
            run_status,
            run_duration_seconds,
            total_transactions,
            matched_count,
            mismatched_count,
            unreconciled_processor_count,
            unreconciled_internal_count,
            match_rate_pct,
            total_discrepancy_amount,
            _pipeline_run_id
        )
        SELECT
            rn.reference_date,
            rn.run_id,
            rn.file_name,
            rn.status                                                                   AS run_status,
            CASE
                WHEN rn.completed_at IS NOT NULL
                THEN datediff('second', rn.started_at, rn.completed_at)::INTEGER
                ELSE NULL
            END                                                                         AS run_duration_seconds,
            COUNT(rr.result_id)                                                         AS total_transactions,
            SUM(CASE WHEN rr.category = 'MATCHED'                THEN 1 ELSE 0 END)    AS matched_count,
            SUM(CASE WHEN rr.category = 'MISMATCHED'             THEN 1 ELSE 0 END)    AS mismatched_count,
            SUM(CASE WHEN rr.category = 'UNRECONCILED_PROCESSOR' THEN 1 ELSE 0 END)    AS unreconciled_processor_count,
            SUM(CASE WHEN rr.category = 'UNRECONCILED_INTERNAL'  THEN 1 ELSE 0 END)    AS unreconciled_internal_count,
            ROUND(
                SUM(CASE WHEN rr.category = 'MATCHED' THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(rr.result_id), 0),
            2)::DECIMAL(5, 2)                                                           AS match_rate_pct,
            COALESCE(
                SUM(CASE WHEN rr.category = 'MISMATCHED' THEN rr.difference ELSE 0 END),
            0)                                                                          AS total_discrepancy_amount,
            ?                                                                           AS _pipeline_run_id
        FROM silver.reconciliation_runs rn
        LEFT JOIN silver.reconciliation_results rr ON rn.run_id = rr.run_id
        WHERE rn.reference_date = ?::DATE
        GROUP BY
            rn.reference_date,
            rn.run_id,
            rn.file_name,
            rn.status,
            rn.started_at,
            rn.completed_at
        """,
        [pipeline_run_id, reference_date],
    )
    count: int = conn.execute(
        "SELECT COUNT(*) FROM gold.mart_operations WHERE reference_date = ?::DATE",
        [reference_date],
    ).fetchone()[0]
    logger.info(
        "gold.mart_operations built",
        extra={"counts": {"rows": count}, "reference_date": reference_date},
    )
    return count
