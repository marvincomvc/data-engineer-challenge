"""Tests for data quality checks."""
from __future__ import annotations

import duckdb
import pytest

from pipeline.config import PipelineConfig
from pipeline.observability.metrics import RunMetrics
from pipeline.quality.checks import (
    CheckResult,
    check_null_transaction_id_in_results,
    check_negative_amount_in_transactions,
    check_empty_csv,
    check_duplicate_transaction_id_in_run,
    check_mismatch_rate,
    check_orphan_merchants,
    check_schema_drift_logged,
)
from pipeline.transform.bronze import run_bronze
from pipeline.transform.silver import run_silver


def _setup(con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
    run_bronze(con, cfg, "2025-03-16")
    metrics = RunMetrics(reference_date="2025-03-16")
    run_silver(con, cfg, "2025-03-16", metrics)


class TestHardChecks:
    def test_null_transaction_id_in_results(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        _setup(con, cfg)
        result = check_null_transaction_id_in_results(con)
        assert isinstance(result, CheckResult)
        assert result.check_type == "HARD"

    def test_negative_amount_in_transactions(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        _setup(con, cfg)
        result = check_negative_amount_in_transactions(con)
        assert isinstance(result, CheckResult)
        assert result.check_type == "HARD"

    def test_empty_csv_passes(self) -> None:
        result = check_empty_csv(100)
        assert result.passed

    def test_empty_csv_fails(self) -> None:
        result = check_empty_csv(0)
        assert not result.passed


class TestSoftChecks:
    def test_mismatch_rate(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        _setup(con, cfg)
        result = check_mismatch_rate(con, threshold=0.99)
        assert result.check_type == "SOFT"
        assert result.passed  # 99% threshold should always pass with sample data

    def test_mismatch_rate_fails(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        _setup(con, cfg)
        result = check_mismatch_rate(con, threshold=0.001)
        assert result.check_type == "SOFT"
        assert not result.passed  # 0.1% threshold should fail with sample data

    def test_orphan_merchants(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        _setup(con, cfg)
        result = check_orphan_merchants(con)
        assert result.check_type == "SOFT"

    def test_duplicate_transaction_id_in_run(self, con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> None:
        _setup(con, cfg)
        result = check_duplicate_transaction_id_in_run(con, "2025-03-16")
        assert result.check_type == "SOFT"

    def test_schema_drift_logged(self) -> None:
        result = check_schema_drift_logged(
            ["id", "name", "amount"],
            ["id", "name", "amount", "payment_method"],
        )
        assert result.passed
        assert "payment_method" in result.message


class TestCheckResultFormat:
    def test_str_pass(self) -> None:
        r = CheckResult(name="test", check_type="HARD", passed=True, message="ok")
        assert "PASS" in str(r)
        assert "HARD" in str(r)

    def test_str_fail(self) -> None:
        r = CheckResult(name="test", check_type="SOFT", passed=False, message="bad", failed_count=5)
        assert "FAIL" in str(r)
        assert "SOFT" in str(r)
