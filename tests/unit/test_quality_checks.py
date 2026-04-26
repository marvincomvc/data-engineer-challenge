"""Unit tests for quality checks — STOP raises, WARN logs and returns."""
from __future__ import annotations

import pytest

from pipeline.quality.checks import (
    BRONZE_STOP_CHECKS,
    SILVER_WARN_CHECKS,
    QualityError,
    Severity,
    make_settlement_stop_checks,
    run_stop_checks,
    run_warn_checks,
)

_SOURCE_FILE = "test.csv"


# ---------------------------------------------------------------------------
# Helpers — minimal valid rows so only the targeted check can fire
# ---------------------------------------------------------------------------

def _add_valid_transaction(conn, txn_id: str = "txn-001") -> None:
    conn.execute("""
        INSERT INTO bronze.transactions (
            id, transaction_id, merchant_id, amount, currency, status,
            created_at, updated_at, op, _timestamp
        ) VALUES (1, ?, 'MERCH_001', 100.0, 'BRL', 'COMPLETED',
                  '2025-03-04T10:00:00', '2025-03-04T10:00:00', 'I', '2025-03-04T10:00:00')
    """, [txn_id])


def _add_valid_settlement(conn, amount: str = "100.00", status: str = "SETTLED") -> None:
    conn.execute("""
        INSERT INTO bronze.settlement (
            transaction_id, merchant_id, amount, currency,
            settled_at, processor_reference, status, _source_file
        ) VALUES ('txn-001', 'MERCH_001', ?, 'BRL',
                  '2025-03-04T10:00:00Z', 'ref-001', ?, ?)
    """, [amount, status, _SOURCE_FILE])


def _add_valid_result(conn, run_id: int = 1) -> None:
    conn.execute("""
        INSERT INTO bronze.reconciliation_results (
            id, run_id, transaction_id, merchant_id, category,
            internal_amount, processor_amount, difference,
            created_at, op, _timestamp
        ) VALUES (1, ?, 'txn-001', 'MERCH_001', 'MATCHED',
                  100.0, 100.0, 0.0,
                  '2025-03-04T10:01:00', 'I', '2025-03-04T10:01:00')
    """, [run_id])


# ---------------------------------------------------------------------------
# STOP checks
# ---------------------------------------------------------------------------

class TestStopChecks:
    def test_all_pass_with_clean_data(self, tmp_duckdb) -> None:
        """All STOP checks return zero violations when data is valid."""
        _add_valid_transaction(tmp_duckdb)
        _add_valid_settlement(tmp_duckdb)
        _add_valid_result(tmp_duckdb)
        results = run_stop_checks(tmp_duckdb, _SOURCE_FILE)
        assert all(r.passed for r in results)

    def test_null_transaction_id_raises(self, tmp_duckdb) -> None:
        """A NULL transaction_id is a STOP — rows cannot be reconciled."""
        tmp_duckdb.execute("""
            INSERT INTO bronze.transactions (
                id, transaction_id, merchant_id, amount, currency, status,
                created_at, updated_at, op, _timestamp
            ) VALUES (1, NULL, 'MERCH_001', 100.0, 'BRL', 'COMPLETED',
                      '2025-03-04T10:00:00', '2025-03-04T10:00:00', 'I', '2025-03-04T10:00:00')
        """)
        with pytest.raises(QualityError) as exc_info:
            run_stop_checks(tmp_duckdb, _SOURCE_FILE)
        assert "null_transaction_id" in str(exc_info.value)

    def test_unparseable_settlement_amount_raises(self, tmp_duckdb) -> None:
        """An amount that cannot be parsed to decimal is a STOP."""
        _add_valid_transaction(tmp_duckdb)
        _add_valid_settlement(tmp_duckdb, amount="N/A")
        _add_valid_result(tmp_duckdb)
        with pytest.raises(QualityError) as exc_info:
            run_stop_checks(tmp_duckdb, _SOURCE_FILE)
        assert "unparseable_settlement_amount" in str(exc_info.value)

    def test_non_positive_settlement_amount_raises(self, tmp_duckdb) -> None:
        """A zero or negative amount is a STOP — can't represent a payment."""
        _add_valid_transaction(tmp_duckdb)
        _add_valid_settlement(tmp_duckdb, amount="-50.00")
        _add_valid_result(tmp_duckdb)
        with pytest.raises(QualityError) as exc_info:
            run_stop_checks(tmp_duckdb, _SOURCE_FILE)
        assert "non_positive_settlement_amount" in str(exc_info.value)

    def test_invalid_settlement_status_raises(self, tmp_duckdb) -> None:
        """An unknown settlement status is a STOP."""
        _add_valid_transaction(tmp_duckdb)
        _add_valid_settlement(tmp_duckdb, status="PENDING")
        _add_valid_result(tmp_duckdb)
        with pytest.raises(QualityError) as exc_info:
            run_stop_checks(tmp_duckdb, _SOURCE_FILE)
        assert "invalid_settlement_status" in str(exc_info.value)

    def test_null_run_id_in_results_raises(self, tmp_duckdb) -> None:
        """A reconciliation result with NULL run_id cannot be attributed to any run."""
        _add_valid_transaction(tmp_duckdb)
        _add_valid_settlement(tmp_duckdb)
        # Deliberately pass NULL run_id
        tmp_duckdb.execute("""
            INSERT INTO bronze.reconciliation_results (
                id, run_id, transaction_id, merchant_id, category,
                internal_amount, processor_amount, difference,
                created_at, op, _timestamp
            ) VALUES (1, NULL, 'txn-001', 'MERCH_001', 'MATCHED',
                      100.0, 100.0, 0.0,
                      '2025-03-04T10:01:00', 'I', '2025-03-04T10:01:00')
        """)
        with pytest.raises(QualityError) as exc_info:
            run_stop_checks(tmp_duckdb, _SOURCE_FILE)
        assert "null_run_id_in_results" in str(exc_info.value)

    def test_quality_error_names_all_failures(self, tmp_duckdb) -> None:
        """QualityError message lists every failing check name."""
        # Both settlement checks fire: unparseable AND invalid status
        _add_valid_transaction(tmp_duckdb)
        _add_valid_settlement(tmp_duckdb, amount="??", status="UNKNOWN")
        _add_valid_result(tmp_duckdb)
        with pytest.raises(QualityError) as exc_info:
            run_stop_checks(tmp_duckdb, _SOURCE_FILE)
        error_msg = str(exc_info.value)
        assert "unparseable_settlement_amount" in error_msg or "invalid_settlement_status" in error_msg

    def test_quality_error_exposes_failures_list(self, tmp_duckdb) -> None:
        """QualityError.failures contains CheckResult objects."""
        tmp_duckdb.execute("""
            INSERT INTO bronze.transactions (
                id, transaction_id, merchant_id, amount, currency, status,
                created_at, updated_at, op, _timestamp
            ) VALUES (1, NULL, 'MERCH_001', 100.0, 'BRL', 'COMPLETED',
                      '2025-03-04', '2025-03-04', 'I', '2025-03-04')
        """)
        with pytest.raises(QualityError) as exc_info:
            run_stop_checks(tmp_duckdb, _SOURCE_FILE)
        assert exc_info.value.failures
        assert exc_info.value.failures[0].violations > 0

    def test_settlement_checks_ignore_other_files(self, tmp_duckdb) -> None:
        """Bad rows from a different source_file must not block the current run."""
        _add_valid_transaction(tmp_duckdb)
        _add_valid_result(tmp_duckdb)
        # Bad row from an old file — different _source_file
        tmp_duckdb.execute("""
            INSERT INTO bronze.settlement (
                transaction_id, merchant_id, amount, currency,
                settled_at, processor_reference, status, _source_file
            ) VALUES ('txn-old', 'MERCH_001', 'N/A', 'BRL',
                      '2025-03-01T10:00:00Z', 'ref-old', 'SETTLED', 'old.csv')
        """)
        # Valid row for the current file
        _add_valid_settlement(tmp_duckdb)
        # Should pass — bad row is scoped to 'old.csv', not 'test.csv'
        results = run_stop_checks(tmp_duckdb, _SOURCE_FILE)
        assert all(r.passed for r in results)

    def test_all_stop_checks_have_stop_severity(self) -> None:
        """Sanity: every STOP check entry must have Severity.STOP."""
        all_checks = BRONZE_STOP_CHECKS + make_settlement_stop_checks("x.csv")
        for check in all_checks:
            assert check.severity == Severity.STOP


# ---------------------------------------------------------------------------
# WARN checks
# ---------------------------------------------------------------------------

class TestWarnChecks:
    def test_warn_never_raises(self, tmp_duckdb) -> None:
        """WARN checks must return results regardless of violations."""
        results = run_warn_checks(tmp_duckdb)
        assert isinstance(results, list)

    def test_warn_returns_all_checks(self, tmp_duckdb) -> None:
        """run_warn_checks returns one result per SILVER_WARN_CHECKS entry."""
        results = run_warn_checks(tmp_duckdb)
        assert len(results) == len(SILVER_WARN_CHECKS)

    def test_all_warn_checks_have_warn_severity(self) -> None:
        """Sanity: every SILVER_WARN_CHECKS entry must be Severity.WARN."""
        for check in SILVER_WARN_CHECKS:
            assert check.severity == Severity.WARN

    def test_check_result_passed_property(self, tmp_duckdb) -> None:
        """CheckResult.passed is True when violations == 0."""
        results = run_warn_checks(tmp_duckdb)
        for r in results:
            # With empty silver tables, all WARN checks should pass (0 violations)
            assert r.passed is True
            assert r.violations == 0
