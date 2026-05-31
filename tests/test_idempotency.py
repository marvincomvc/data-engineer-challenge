"""Integration test — idempotency: running the pipeline twice must not duplicate records."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from pipeline.cli import run_pipeline
from pipeline.config import PipelineConfig, DEFAULT_DATA_PATH


class TestIdempotency:
    def test_second_run_same_counts(self, tmp_path) -> None:
        """Running the pipeline twice for the same date must produce identical row counts."""
        cfg = PipelineConfig(
            data_path=DEFAULT_DATA_PATH,
            warehouse_path=tmp_path / "idempotency.duckdb",
        )

        # First run
        rc1 = run_pipeline("2025-03-16", force=True, cfg=cfg)
        assert rc1 == 0

        con = duckdb.connect(str(cfg.warehouse_path))
        tables_to_check = [
            "bronze.transactions_raw",
            "silver.transactions",
            "silver.reconciliation_results",
            "gold.daily_reconciliation_health",
        ]
        counts_run1 = {}
        for t in tables_to_check:
            counts_run1[t] = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        con.close()

        # Second run (with force since first run marked as COMPLETED)
        rc2 = run_pipeline("2025-03-16", force=True, cfg=cfg)
        assert rc2 == 0

        con = duckdb.connect(str(cfg.warehouse_path))
        for t in tables_to_check:
            count = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            assert count == counts_run1[t], f"{t} changed after second run: {counts_run1[t]} -> {count}"
        con.close()

    def test_pipeline_runs_history_grows(self, tmp_path) -> None:
        """Pipeline runs table should record both runs (not be idempotent on metadata)."""
        cfg = PipelineConfig(
            data_path=DEFAULT_DATA_PATH,
            warehouse_path=tmp_path / "idempotency_runs.duckdb",
        )

        run_pipeline("2025-03-16", force=True, cfg=cfg)
        con = duckdb.connect(str(cfg.warehouse_path))
        runs_after_first = con.execute("SELECT count(*) FROM control.pipeline_runs").fetchone()[0]
        con.close()

        run_pipeline("2025-03-16", force=True, cfg=cfg)
        con = duckdb.connect(str(cfg.warehouse_path))
        runs_after_second = con.execute("SELECT count(*) FROM control.pipeline_runs").fetchone()[0]
        con.close()

        assert runs_after_second > runs_after_first
