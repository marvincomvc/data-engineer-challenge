"""gold.mart_compliance — full audit trail per transaction per run.

Consumer: Compliance / Audit team.

Questions answered:
  - What is the complete history of a specific transaction_id across all runs?
  - Which transactions were categorized differently on reruns?
  - Full lineage from CSV file_name to final reconciliation category.
  - Which runs produced mismatches for merchant X?
  - State of every transaction at the time each run completed.

Granularity: one row per (transaction_id, run_id).

This mart is rebuilt per reference_date (DELETE + INSERT) rather than
append-only. The audit semantic is preserved because:
  - Each row captures the state at run completion time (run_started_at,
    run_completed_at, result_created_at).
  - Running the pipeline twice for the same date replaces stale data with
    the current re-processed state — which is the correct audit outcome
    for a re-run scenario.

Idempotency: DELETE WHERE reference_date = ? then INSERT.
"""
from __future__ import annotations

import logging

import duckdb

from pipeline.db import transaction

logger = logging.getLogger(__name__)


def build_mart_compliance(
    conn: duckdb.DuckDBPyConnection,
    reference_date: str,
    pipeline_run_id: str,
) -> int:
    with transaction(conn):
        conn.execute(
            "DELETE FROM gold.mart_compliance WHERE reference_date = ?::DATE",
            [reference_date],
        )
        conn.execute(
            """
        INSERT INTO gold.mart_compliance (
            transaction_id,
            run_id,
            reference_date,
            file_name,
            merchant_id,
            category,
            internal_amount,
            processor_amount,
            difference,
            internal_status,
            transaction_created_at,
            result_created_at,
            run_started_at,
            run_completed_at,
            _pipeline_run_id
        )
        SELECT
            rr.transaction_id,
            rr.run_id,
            rr.reference_date,
            rn.file_name,
            rr.merchant_id,
            rr.category,
            rr.internal_amount,
            rr.processor_amount,
            rr.difference,
            t.status        AS internal_status,
            t.created_at    AS transaction_created_at,
            rr.created_at   AS result_created_at,
            rn.started_at   AS run_started_at,
            rn.completed_at AS run_completed_at,
            ?               AS _pipeline_run_id
        FROM silver.reconciliation_results rr
        LEFT JOIN silver.reconciliation_runs rn USING (run_id)
        LEFT JOIN silver.transactions t
               ON rr.transaction_id = t.transaction_id
              AND NOT t.is_deleted
        WHERE rr.reference_date = ?::DATE
        """,
        [pipeline_run_id, reference_date],
    )
    count: int = conn.execute(
        "SELECT COUNT(*) FROM gold.mart_compliance WHERE reference_date = ?::DATE",
        [reference_date],
    ).fetchone()[0]
    logger.info(
        "gold.mart_compliance built",
        extra={"counts": {"rows": count}, "reference_date": reference_date},
    )
    return count
