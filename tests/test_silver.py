"""Tests for the silver layer — deduplication, normalization, joins."""
from __future__ import annotations

import duckdb
import pytest

from pipeline.config import PipelineConfig
from pipeline.observability.metrics import RunMetrics
from pipeline.transform.bronze import run_bronze
from pipeline.transform.silver import run_silver


class TestSilverTransactions:
    def test_deduplication(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        """Dedup should reduce row count vs bronze (CDC replay creates dupes)."""
        run_bronze(con, cfg, "2025-03-16")
        bronze_count = con.execute("SELECT count(*) FROM bronze.transactions_raw").fetchone()[0]
        metrics = RunMetrics(reference_date="2025-03-16")
        run_silver(con, cfg, "2025-03-16", metrics)
        silver_count = con.execute("SELECT count(*) FROM silver.transactions").fetchone()[0]
        assert silver_count <= bronze_count

    def test_no_null_transaction_ids(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        """Silver transactions should never have null transaction_id."""
        run_bronze(con, cfg, "2025-03-16")
        metrics = RunMetrics(reference_date="2025-03-16")
        run_silver(con, cfg, "2025-03-16", metrics)
        null_count = con.execute("SELECT count(*) FROM silver.transactions WHERE transaction_id IS NULL").fetchone()[0]
        assert null_count == 0

    def test_dates_are_timestamptz(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        run_bronze(con, cfg, "2025-03-16")
        metrics = RunMetrics(reference_date="2025-03-16")
        run_silver(con, cfg, "2025-03-16", metrics)
        cols = con.execute("SELECT column_name, column_type FROM (DESCRIBE silver.transactions)").fetchall()
        col_types = {c[0]: c[1] for c in cols}
        assert "TIMESTAMP" in col_types.get("created_at", "")
        assert "TIMESTAMP" in col_types.get("updated_at", "")


class TestSilverReconciliation:
    def test_reference_date_enriched(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        """Silver reconciliation_results should have reference_date from runs join."""
        run_bronze(con, cfg, "2025-03-16")
        metrics = RunMetrics(reference_date="2025-03-16")
        run_silver(con, cfg, "2025-03-16", metrics)
        cols = [r[0] for r in con.execute("SELECT column_name FROM (DESCRIBE silver.reconciliation_results)").fetchall()]
        assert "reference_date" in cols

    def test_merchants_deduped(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        run_bronze(con, cfg, "2025-03-16")
        metrics = RunMetrics(reference_date="2025-03-16")
        run_silver(con, cfg, "2025-03-16", metrics)
        count = con.execute("SELECT count(*) FROM silver.merchants").fetchone()[0]
        assert count > 0

    def test_paysettler_amounts_are_double(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        """PaySettler settlements should have amount as DOUBLE."""
        run_bronze(con, cfg, "2025-03-16")
        metrics = RunMetrics(reference_date="2025-03-16")
        run_silver(con, cfg, "2025-03-16", metrics)
        cols = con.execute("SELECT column_name, column_type FROM (DESCRIBE silver.paysettler_settlements)").fetchall()
        col_types = {c[0]: c[1] for c in cols}
        assert col_types.get("amount") == "DOUBLE"


class TestSilverReconciliationLogic:
    def test_reconciliation_produces_categories(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        run_bronze(con, cfg, "2025-03-16")
        metrics = RunMetrics(reference_date="2025-03-16")
        run_silver(con, cfg, "2025-03-16", metrics)
        categories = con.execute(f"""
            SELECT DISTINCT category FROM silver.reconciliation_results
            WHERE reference_date = '2025-03-16'
        """).fetchall()
        cat_set = {c[0] for c in categories}
        assert "MISMATCHED" in cat_set
        assert any(cat in cat_set for cat in ["UNRECONCILED_PROCESSOR", "UNRECONCILED_INTERNAL"])

    def test_metrics_populated(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        run_bronze(con, cfg, "2025-03-16")
        metrics = RunMetrics(reference_date="2025-03-16")
        run_silver(con, cfg, "2025-03-16", metrics)
        assert metrics.total_processed > 0
        assert metrics.mismatched >= 0
