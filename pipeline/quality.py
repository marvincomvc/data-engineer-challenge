"""Data-quality checks — tiered policy: FIX (in adapter/silver) / STOP / QUARANTINE.

STOP  = circuit breakers that abort the run (integrity threats).
QUARANTINE = isolate bad-but-non-corrupting rows into a side table and continue.
"""
from __future__ import annotations


class DataQualityError(Exception):
    """Raised by a circuit breaker to abort the run (the transaction rolls back)."""


def assert_circuit_breakers(con, reference_date: str) -> None:
    # STOP 1: the upstream reconciliation run for this date must be COMPLETED.
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
    """Move orphan REVERSED settlements (no internal pair) to quarantine; return count."""
    moved = con.execute("""
        INSERT INTO quarantine_settlement
        SELECT transaction_id, 'ORPHAN_REVERSED'
        FROM silver_settlement
        WHERE status = 'REVERSED' AND has_internal = false
        RETURNING transaction_id
    """).fetchall()
    con.execute("DELETE FROM silver_settlement WHERE status = 'REVERSED' AND has_internal = false")
    return len(moved)
