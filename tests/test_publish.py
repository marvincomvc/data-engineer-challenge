"""Publish stage: Gold marts exported as consumable, partitioned Parquet products."""
from __future__ import annotations

import duckdb

from conftest import REFERENCE_DATE, run_pipeline


def _gold_dir(warehouse):
    # conftest sets GOLD_PATH to <warehouse_parent>/gold
    return warehouse.parent / "gold"


def test_products_written_as_partitioned_parquet(controlled_warehouse):
    warehouse, data_dir = controlled_warehouse
    run_pipeline(warehouse, data_dir, REFERENCE_DATE, command="run")
    gold = _gold_dir(warehouse)
    # partitioned marts land under reference_date=<date>/
    part = gold / "mart_ops_health" / f"reference_date={REFERENCE_DATE}"
    assert part.is_dir()
    assert list(part.glob("*.parquet")), "no parquet written for mart_ops_health partition"
    # dimensions land as a directory with a file (so serving globs match)
    assert list((gold / "dim_merchant").glob("*.parquet"))


def test_products_readable_without_warehouse(controlled_warehouse):
    # A consumer reads the Parquet products directly — no warehouse access at all.
    warehouse, data_dir = controlled_warehouse
    run_pipeline(warehouse, data_dir, REFERENCE_DATE, command="run")
    gold = _gold_dir(warehouse)
    con = duckdb.connect()
    glob = (gold / "mart_ops_health" / "**/*.parquet").as_posix()
    rows = con.execute(
        f"select total_transactions from read_parquet('{glob}', hive_partitioning=true)"
    ).fetchall()
    # 6 scenario txns + t7-drift (batch_2, in-window, unreconciled_internal) = 7
    assert rows and rows[0][0] == 7


def test_serving_catalog_exposes_all_products(controlled_warehouse):
    warehouse, data_dir = controlled_warehouse
    run_pipeline(warehouse, data_dir, REFERENCE_DATE, command="run")
    serving = _gold_dir(warehouse) / "serving.duckdb"
    assert serving.exists()
    con = duckdb.connect(str(serving), read_only=True)
    try:
        for view in ("mart_ops_health", "mart_cfo_volume", "mart_compliance_audit",
                     "dim_merchant", "dim_category", "dim_date"):
            n = con.execute(f"select count(*) from {view}").fetchone()[0]
            assert n >= 0  # view resolves against the parquet glob
    finally:
        con.close()


def test_republish_is_idempotent(controlled_warehouse):
    warehouse, data_dir = controlled_warehouse
    run_pipeline(warehouse, data_dir, REFERENCE_DATE, command="run")
    run_pipeline(warehouse, data_dir, REFERENCE_DATE, command="run")
    part = _gold_dir(warehouse) / "mart_ops_health" / f"reference_date={REFERENCE_DATE}"
    # re-publishing overwrites the partition, not appends
    assert len(list(part.glob("*.parquet"))) == 1
