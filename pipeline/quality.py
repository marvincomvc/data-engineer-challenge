"""Data-quality checks — tiered policy: FIX (in adapter/silver) / STOP / QUARANTINE.

STOP       = circuit breakers that abort the run (integrity threats on the requested date).
QUARANTINE = isolate bad-but-non-corrupting rows into side tables and continue (counted + recorded).

Quarantine tables are recreated each run (CREATE OR REPLACE) so re-running a date is idempotent
and the tables always reflect the current state instead of accumulating duplicates.
"""
from __future__ import annotations


class DataQualityError(Exception):
    """Raised by a circuit breaker to abort the run (the transaction rolls back)."""


def init_quarantine(con) -> None:
    con.execute("CREATE OR REPLACE TABLE quarantine_settlement (transaction_id VARCHAR, reason VARCHAR)")
    con.execute("CREATE OR REPLACE TABLE quarantine_results (result_id BIGINT, transaction_id VARCHAR, reason VARCHAR)")


def assert_circuit_breakers(con, reference_date: str) -> None:
    """STOP-tier checks for the requested reference_date."""
    # STOP 1: the upstream reconciliation run for the requested date must be COMPLETED.
    runs = con.execute(
        "SELECT status FROM silver_runs WHERE reference_date = ?", [reference_date]
    ).fetchall()
    if any(status != "COMPLETED" for (status,) in runs):
        raise DataQualityError(
            f"upstream run for {reference_date} is not COMPLETED: {[r[0] for r in runs]}"
        )
    # STOP 2: no settlement may have a NULL join key.
    nulls = con.sql("SELECT count(*) FROM silver_settlement WHERE transaction_id IS NULL").fetchone()[0]
    if nulls:
        raise DataQualityError(f"{nulls} settlement rows with NULL transaction_id")


def quarantine_settlements(con) -> int:
    """QUARANTINE orphan REVERSED settlements (no internal pair). Returns count moved."""
    moved = con.execute("""
        INSERT INTO quarantine_settlement
        SELECT transaction_id, 'ORPHAN_REVERSED'
        FROM silver_settlement
        WHERE status = 'REVERSED' AND has_internal = false
        RETURNING transaction_id
    """).fetchall()
    con.execute("DELETE FROM silver_settlement WHERE status = 'REVERSED' AND has_internal = false")
    return len(moved)


def quarantine_results(con) -> int:
    """QUARANTINE results that cannot be safely modelled: a run_id with no run (orphan FK),
    or a NULL merchant_id. Recorded with a reason and removed from silver_results so the
    audit trail row count reconciles to (kept results), instead of silently shrinking on a join."""
    moved = con.execute("""
        INSERT INTO quarantine_results
        SELECT id, transaction_id,
               CASE WHEN run_id NOT IN (SELECT id FROM silver_runs) THEN 'ORPHAN_RUN'
                    ELSE 'NULL_MERCHANT' END AS reason
        FROM silver_results
        WHERE run_id NOT IN (SELECT id FROM silver_runs) OR merchant_id IS NULL
        RETURNING result_id
    """).fetchall()
    con.execute("""
        DELETE FROM silver_results
        WHERE run_id NOT IN (SELECT id FROM silver_runs) OR merchant_id IS NULL
    """)
    return len(moved)
