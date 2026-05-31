"""Pytest fixtures for pipeline tests — uses docs/sample-data/ as deterministic data."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from pipeline.config import PipelineConfig

REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DATA = REPO_ROOT / "docs" / "sample-data"


@pytest.fixture
def cfg(tmp_path: Path) -> PipelineConfig:
    """Pipeline config pointing at sample data and a temp warehouse."""
    return PipelineConfig(
        data_path=SAMPLE_DATA,
        warehouse_path=tmp_path / "test_warehouse.duckdb",
    )


@pytest.fixture
def con(cfg: PipelineConfig) -> duckdb.DuckDBPyConnection:
    """DuckDB connection with the warehouse initialized."""
    conn = duckdb.connect(str(cfg.warehouse_path))
    yield conn
    conn.close()


@pytest.fixture
def populated_con(cfg: PipelineConfig, con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """DuckDB connection with bronze+silver layers populated."""
    from pipeline.transform.bronze import run_bronze
    from pipeline.transform.silver import run_silver, create_silver_schema
    from pipeline.transform.silver import (
        build_silver_transactions,
        build_silver_merchants,
        build_silver_reconciliation_runs,
        build_silver_reconciliation_results,
        build_silver_paysettler_settlements,
    )
    from pipeline.observability.metrics import RunMetrics

    b1_cols, b2_cols, csv_count = run_bronze(con, cfg, "2025-03-16")
    metrics = RunMetrics(reference_date="2025-03-16")
    run_silver(con, cfg, "2025-03-16", metrics)
    return con
