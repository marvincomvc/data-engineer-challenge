"""Correctness of the reconciliation engine against controlled inputs.

Each transaction has a known expected category; we run the real pipeline
(ingest -> dbt run) and assert the categorization, tolerance boundary, window
boundary, and CDC collapse all behave per the domain rules.
"""
from __future__ import annotations

import duckdb
import pytest

from conftest import REFERENCE_DATE, run_pipeline


@pytest.fixture
def built(controlled_warehouse):
    warehouse, data_dir = controlled_warehouse
    run_pipeline(warehouse, data_dir, REFERENCE_DATE, command="run")
    return warehouse


def _categories(warehouse) -> dict[str, str]:
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        rows = con.execute(
            "select transaction_id, category from main.fct_reconciliation_result"
        ).fetchall()
    finally:
        con.close()
    return dict(rows)


def test_all_categories_correct(built, expected_categories):
    got = _categories(built)
    for tid, expected in expected_categories.items():
        assert got.get(tid) == expected, f"{tid}: expected {expected}, got {got.get(tid)}"


def test_tolerance_boundary_is_matched(built):
    # diff of exactly R$0.01 must be MATCHED, not MISMATCHED.
    assert _categories(built)["t2-tolerance"] == "MATCHED"


def test_out_of_window_internal_is_processor_orphan(built):
    # internal created before the 7-day window -> processor row is an orphan.
    assert _categories(built)["t6-outofwindow"] == "UNRECONCILED_PROCESSOR"


def test_cdc_collapse_uses_latest_amount(built):
    # t1 was ingested twice (stale 999.99 then correct 100.00); latest wins so
    # it reconciles as MATCHED against the processor's 100.00.
    assert _categories(built)["t1-match"] == "MATCHED"


def test_grain_is_unique(built):
    con = duckdb.connect(str(built), read_only=True)
    try:
        total, distinct = con.execute(
            "select count(*), count(distinct (reference_date, transaction_id)) "
            "from main.fct_reconciliation_result"
        ).fetchone()
    finally:
        con.close()
    assert total == distinct
