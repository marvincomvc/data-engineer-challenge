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


def test_other_day_settlement_is_scoped_out(built):
    # a processor row settled the day BEFORE reference_date must not be reconciled
    # on this date at all (not even UNRECONCILED_PROCESSOR) — date-isolation regression.
    assert "t10-otherday" not in _categories(built)


def test_out_of_window_internal_is_processor_orphan(built):
    # internal created before the 7-day window -> processor row is an orphan.
    assert _categories(built)["t6-outofwindow"] == "UNRECONCILED_PROCESSOR"


def test_cdc_collapse_uses_latest_amount(built):
    # t1 was ingested twice (stale 999.99 then correct 100.00); latest wins so
    # it reconciles as MATCHED against the processor's 100.00.
    assert _categories(built)["t1-match"] == "MATCHED"


def test_cross_currency_pair_is_mismatched(built):
    # same transaction_id, BRL internal vs USD processor -> cannot MATCH.
    assert _categories(built)["t9-crossccy"] == "MISMATCHED"


def test_cfo_volume_has_per_currency_grain(built):
    # a USD transaction must appear as its own (reference_date, currency) row,
    # never summed into BRL.
    con = duckdb.connect(str(built), read_only=True)
    try:
        currencies = {
            r[0] for r in con.execute("select distinct currency from main.mart_cfo_volume").fetchall()
        }
    finally:
        con.close()
    assert {"BRL", "USD"} <= currencies


def test_cfo_volume_excludes_cross_currency_amount(built):
    # t9 is BRL internal / USD processor: its USD processor_amount (100) must NOT
    # be summed into the BRL processor_volume. BRL processor rows are
    # t1+t2+t3+t5+t6 = 100 + 100.01 + 100.02 + 75 + 200 = 575.03.
    con = duckdb.connect(str(built), read_only=True)
    try:
        brl, usd = (
            con.execute(
                "select "
                "  sum(processor_volume) filter (where currency='BRL'), "
                "  sum(processor_volume) filter (where currency='USD') "
                "from main.mart_cfo_volume"
            ).fetchone()
        )
    finally:
        con.close()
    assert float(brl) == 575.03   # cross-currency USD 100 excluded
    assert float(usd) == 200.00   # only t8-usd


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
