"""Idempotency: re-running a reference_date must not duplicate or corrupt data."""
from __future__ import annotations

import duckdb

from conftest import REFERENCE_DATE, run_pipeline


def _count(warehouse) -> int:
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        return con.execute("select count(*) from main.fct_reconciliation_result").fetchone()[0]
    finally:
        con.close()


def test_rerun_same_date_is_stable(controlled_warehouse):
    warehouse, data_dir = controlled_warehouse
    run_pipeline(warehouse, data_dir, REFERENCE_DATE, command="run")
    first = _count(warehouse)
    assert first > 0
    # second identical run must not add rows
    run_pipeline(warehouse, data_dir, REFERENCE_DATE, command="run")
    assert _count(warehouse) == first


def test_rerun_preserves_grain(controlled_warehouse):
    warehouse, data_dir = controlled_warehouse
    run_pipeline(warehouse, data_dir, REFERENCE_DATE, command="run")
    run_pipeline(warehouse, data_dir, REFERENCE_DATE, command="run")
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        total, distinct = con.execute(
            "select count(*), count(distinct (reference_date, transaction_id)) "
            "from main.fct_reconciliation_result"
        ).fetchone()
    finally:
        con.close()
    assert total == distinct, "re-run introduced duplicate grain"
