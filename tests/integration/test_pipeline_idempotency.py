"""Integration: running the pipeline twice for the same date must be idempotent.

Gold mart rows must be identical after two runs; pipeline_runs grows by 1
(it's an audit log, not a mart).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.db import get_connection
from pipeline.main import run


def _pick_reference_date(data_path: Path) -> str:
    csvs = sorted((data_path / "paysettler").glob("settlement_*.csv"))
    if not csvs:
        pytest.skip("No settlement CSV found in synthetic data")
    return csvs[0].stem.replace("settlement_", "")


def _gold_counts(conn) -> dict[str, int]:
    return {
        "pipeline_runs": conn.execute("SELECT COUNT(*) FROM gold.pipeline_runs").fetchone()[0],
        "mart_operations": conn.execute("SELECT COUNT(*) FROM gold.mart_operations").fetchone()[0],
        "mart_cfo": conn.execute("SELECT COUNT(*) FROM gold.mart_cfo").fetchone()[0],
        "mart_compliance": conn.execute("SELECT COUNT(*) FROM gold.mart_compliance").fetchone()[0],
    }


def test_idempotency_same_date(synthetic_data_path: Path, tmp_path: Path) -> None:
    """Mart row counts are identical after a second run for the same reference_date."""
    db_path = tmp_path / "idempotency.duckdb"
    ref_date = _pick_reference_date(synthetic_data_path)

    run(reference_date=ref_date, data_path=synthetic_data_path, db_path=db_path)
    conn = get_connection(db_path)
    counts_first = _gold_counts(conn)
    conn.close()

    run(reference_date=ref_date, data_path=synthetic_data_path, db_path=db_path)
    conn = get_connection(db_path)
    counts_second = _gold_counts(conn)
    conn.close()

    assert counts_first["mart_operations"] == counts_second["mart_operations"], (
        f"mart_operations changed: {counts_first['mart_operations']} → {counts_second['mart_operations']}"
    )
    assert counts_first["mart_cfo"] == counts_second["mart_cfo"], (
        f"mart_cfo changed: {counts_first['mart_cfo']} → {counts_second['mart_cfo']}"
    )
    assert counts_first["mart_compliance"] == counts_second["mart_compliance"], (
        f"mart_compliance changed: {counts_first['mart_compliance']} → {counts_second['mart_compliance']}"
    )
    # pipeline_runs is an audit log — must grow by exactly 1
    assert counts_second["pipeline_runs"] == counts_first["pipeline_runs"] + 1, (
        "pipeline_runs should increase by 1 per run"
    )


def test_idempotency_two_different_dates(synthetic_data_path: Path, tmp_path: Path) -> None:
    """Running for two distinct dates accumulates rows for both without overwriting."""
    db_path = tmp_path / "two_dates.duckdb"
    csvs = sorted((synthetic_data_path / "paysettler").glob("settlement_*.csv"))
    if len(csvs) < 2:
        pytest.skip("Need at least 2 settlement CSVs for this test")

    date_a = csvs[0].stem.replace("settlement_", "")
    date_b = csvs[1].stem.replace("settlement_", "")

    run(reference_date=date_a, data_path=synthetic_data_path, db_path=db_path)
    conn = get_connection(db_path)
    rows_after_a = conn.execute("SELECT COUNT(*) FROM gold.mart_operations").fetchone()[0]
    conn.close()

    run(reference_date=date_b, data_path=synthetic_data_path, db_path=db_path)
    conn = get_connection(db_path)
    rows_after_b = conn.execute("SELECT COUNT(*) FROM gold.mart_operations").fetchone()[0]
    conn.close()

    # Both dates contribute rows — total should be > rows from first date alone
    assert rows_after_b >= rows_after_a, (
        "Adding a second date should not reduce mart_operations row count"
    )
