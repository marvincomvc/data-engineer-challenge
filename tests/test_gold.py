"""Tests for the gold layer."""
from __future__ import annotations

import duckdb
import pytest

from pipeline.config import PipelineConfig
from pipeline.observability.metrics import RunMetrics
from pipeline.transform.bronze import run_bronze
from pipeline.transform.silver import run_silver
from pipeline.transform.gold import run_gold


class TestGoldTables:
    def _setup(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        run_bronze(con, cfg, "2025-03-16")
        metrics = RunMetrics(reference_date="2025-03-16")
        run_silver(con, cfg, "2025-03-16", metrics)
        run_gold(con)

    def test_daily_reconciliation_health(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        self._setup(con, cfg)
        count = con.execute("SELECT count(*) FROM gold.daily_reconciliation_health").fetchone()[0]
        assert count > 0

    def test_daily_financial_volume(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        self._setup(con, cfg)
        count = con.execute("SELECT count(*) FROM gold.daily_financial_volume").fetchone()[0]
        assert count > 0

    def test_merchant_volume_summary(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        self._setup(con, cfg)
        count = con.execute("SELECT count(*) FROM gold.merchant_volume_summary").fetchone()[0]
        assert count > 0

    def test_audit_discrepancies(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        self._setup(con, cfg)
        count = con.execute("SELECT count(*) FROM gold.audit_discrepancies").fetchone()[0]
        assert count > 0

    def test_audit_unreconciled(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        self._setup(con, cfg)
        count = con.execute("SELECT count(*) FROM gold.audit_unreconciled").fetchone()[0]
        assert count > 0

    def test_audit_reconciliation_runs_history(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        self._setup(con, cfg)
        count = con.execute("SELECT count(*) FROM gold.audit_reconciliation_runs_history").fetchone()[0]
        assert count > 0

    def test_discrepancy_rate_between_0_and_1(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        self._setup(con, cfg)
        rates = con.execute("SELECT discrepancy_rate FROM gold.daily_reconciliation_health").fetchall()
        for (rate,) in rates:
            assert 0.0 <= rate <= 1.0
