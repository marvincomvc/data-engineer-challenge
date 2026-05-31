"""Tests for the bronze layer."""
from __future__ import annotations

import duckdb
import pytest

from pipeline.config import PipelineConfig
from pipeline.transform.bronze import run_bronze


class TestBronzeLoad:
    def test_transactions_raw_loaded(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        run_bronze(con, cfg, "2025-03-16")
        count = con.execute("SELECT count(*) FROM bronze.transactions_raw").fetchone()[0]
        # Sample data has 742 + 318 = 1060 rows
        assert count > 0

    def test_paysettler_raw_loaded(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        _, _, csv_count = run_bronze(con, cfg, "2025-03-16")
        assert csv_count > 0
        count = con.execute("SELECT count(*) FROM bronze.paysettler_raw").fetchone()[0]
        assert count == csv_count

    def test_reconciliation_runs_loaded(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        run_bronze(con, cfg, "2025-03-16")
        count = con.execute("SELECT count(*) FROM bronze.reconciliation_runs_raw").fetchone()[0]
        assert count > 0

    def test_reconciliation_results_loaded(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        run_bronze(con, cfg, "2025-03-16")
        count = con.execute("SELECT count(*) FROM bronze.reconciliation_results_raw").fetchone()[0]
        assert count > 0

    def test_enterprise_company_loaded(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        run_bronze(con, cfg, "2025-03-16")
        count = con.execute("SELECT count(*) FROM bronze.enterprise_company_raw").fetchone()[0]
        assert count > 0

    def test_metadata_columns_present(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        run_bronze(con, cfg, "2025-03-16")
        cols = [r[0] for r in con.execute("SELECT column_name FROM (DESCRIBE bronze.transactions_raw)").fetchall()]
        assert "_ingested_at" in cols
        assert "_source_file" in cols
        assert "_reference_date" in cols

    def test_schema_drift_columns_preserved(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        """payment_method from batch_2 should be present in bronze."""
        run_bronze(con, cfg, "2025-03-16")
        cols = [r[0] for r in con.execute("SELECT column_name FROM (DESCRIBE bronze.transactions_raw)").fetchall()]
        assert "payment_method" in cols
