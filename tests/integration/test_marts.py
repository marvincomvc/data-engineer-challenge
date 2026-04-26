"""Integration: mart formula correctness — categories, aggregations, lineage."""
from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from pipeline.db import get_connection
from pipeline.ingest.merchants import ingest_merchants
from pipeline.ingest.reconciliation import (
    ingest_reconciliation_results,
    ingest_reconciliation_runs,
)
from pipeline.ingest.settlement import ingest_settlement
from pipeline.ingest.transactions import ingest_transactions
from pipeline.marts.cfo import build_mart_cfo
from pipeline.marts.compliance import build_mart_compliance
from pipeline.marts.operations import build_mart_operations
from pipeline.transform.silver import build_silver


def _pick_reference_date(data_path: Path) -> str:
    csvs = sorted((data_path / "paysettler").glob("settlement_*.csv"))
    if not csvs:
        pytest.skip("No settlement CSV found in synthetic data")
    return csvs[0].stem.replace("settlement_", "")


@pytest.fixture(scope="module")
def mart_conn(
    synthetic_data_path: Path,
    tmp_path_factory: pytest.TempPathFactory,
):
    """Fully populated DuckDB with all bronze, silver, and gold layers built."""
    db_path = tmp_path_factory.mktemp("marts") / "marts.duckdb"
    conn = get_connection(db_path)
    ref_date = _pick_reference_date(synthetic_data_path)

    ingest_transactions(conn, synthetic_data_path)
    ingest_reconciliation_runs(conn, synthetic_data_path)
    ingest_reconciliation_results(conn, synthetic_data_path)
    ingest_merchants(conn, synthetic_data_path)
    _, settlement_file = ingest_settlement(conn, synthetic_data_path, ref_date)
    build_silver(conn, ref_date, settlement_file)

    run_id = str(uuid.uuid4())
    build_mart_operations(conn, ref_date, run_id)
    build_mart_cfo(conn, ref_date, run_id)
    build_mart_compliance(conn, ref_date, run_id)

    yield conn, ref_date
    conn.close()


class TestMartOperations:
    def test_has_rows(self, mart_conn) -> None:
        conn, ref_date = mart_conn
        count = conn.execute(
            "SELECT COUNT(*) FROM gold.mart_operations WHERE reference_date = ?::DATE",
            [ref_date],
        ).fetchone()[0]
        assert count > 0

    def test_category_counts_sum_to_total(self, mart_conn) -> None:
        """matched + mismatched + unreconciled_processor + unreconciled_internal = total."""
        conn, ref_date = mart_conn
        rows = conn.execute("""
            SELECT
                total_transactions,
                matched_count
                    + mismatched_count
                    + unreconciled_processor_count
                    + unreconciled_internal_count AS sum_cats
            FROM gold.mart_operations
            WHERE reference_date = ?::DATE
        """, [ref_date]).fetchall()
        assert rows, "No mart_operations rows for this reference_date"
        for total, sum_cats in rows:
            assert total == sum_cats, (
                f"Category sum ({sum_cats}) != total_transactions ({total})"
            )

    def test_match_rate_within_bounds(self, mart_conn) -> None:
        conn, ref_date = mart_conn
        rows = conn.execute("""
            SELECT match_rate_pct FROM gold.mart_operations
            WHERE reference_date = ?::DATE AND match_rate_pct IS NOT NULL
        """, [ref_date]).fetchall()
        for (rate,) in rows:
            assert 0.0 <= float(rate) <= 100.0

    def test_discrepancy_amount_non_negative(self, mart_conn) -> None:
        conn, ref_date = mart_conn
        neg = conn.execute("""
            SELECT COUNT(*) FROM gold.mart_operations
            WHERE reference_date = ?::DATE AND total_discrepancy_amount < 0
        """, [ref_date]).fetchone()[0]
        assert neg == 0

    def test_nullif_safe_on_empty_date(self, mart_conn) -> None:
        """Building mart for a date with no runs must not raise a division error."""
        conn, _ = mart_conn
        empty_date = "1900-01-01"
        run_id = str(uuid.uuid4())
        build_mart_operations(conn, empty_date, run_id)
        count = conn.execute(
            "SELECT COUNT(*) FROM gold.mart_operations WHERE reference_date = '1900-01-01'::DATE"
        ).fetchone()[0]
        assert count == 0  # no runs → no rows, and no divide-by-zero error


class TestMartCfo:
    def test_no_null_merchant_id(self, mart_conn) -> None:
        """mart_cfo PK includes merchant_id — it must never be NULL."""
        conn, _ = mart_conn
        count = conn.execute(
            "SELECT COUNT(*) FROM gold.mart_cfo WHERE merchant_id IS NULL"
        ).fetchone()[0]
        assert count == 0

    def test_total_settled_amount_non_negative(self, mart_conn) -> None:
        conn, _ = mart_conn
        neg = conn.execute(
            "SELECT COUNT(*) FROM gold.mart_cfo WHERE total_settled_amount < 0"
        ).fetchone()[0]
        assert neg == 0

    def test_matched_plus_mismatched_le_total(self, mart_conn) -> None:
        """Settled rows (MATCHED + MISMATCHED) can't exceed total reconciled rows."""
        conn, ref_date = mart_conn
        violations = conn.execute("""
            SELECT COUNT(*) FROM gold.mart_cfo
            WHERE reference_date = ?::DATE
              AND matched_count + mismatched_count > total_transactions
        """, [ref_date]).fetchone()[0]
        assert violations == 0

    def test_has_merchant_name_for_known_merchants(self, mart_conn) -> None:
        """Merchants present in silver.merchants should have legal_name populated."""
        conn, ref_date = mart_conn
        unnamed = conn.execute("""
            SELECT COUNT(*) FROM gold.mart_cfo cfo
            JOIN silver.merchants m USING (merchant_id)
            WHERE cfo.reference_date = ?::DATE
              AND cfo.merchant_legal_name IS NULL
        """, [ref_date]).fetchone()[0]
        assert unnamed == 0


class TestMartCompliance:
    def test_no_null_category(self, mart_conn) -> None:
        conn, _ = mart_conn
        count = conn.execute(
            "SELECT COUNT(*) FROM gold.mart_compliance WHERE category IS NULL"
        ).fetchone()[0]
        assert count == 0

    def test_categories_are_valid_values(self, mart_conn) -> None:
        conn, _ = mart_conn
        rows = conn.execute("SELECT DISTINCT category FROM gold.mart_compliance").fetchall()
        valid = {"MATCHED", "MISMATCHED", "UNRECONCILED_INTERNAL", "UNRECONCILED_PROCESSOR"}
        actual = {r[0] for r in rows}
        assert actual.issubset(valid), f"Unexpected categories: {actual - valid}"

    def test_no_duplicate_pk(self, mart_conn) -> None:
        """(transaction_id, run_id) is the PK — duplicates violate the schema."""
        conn, ref_date = mart_conn
        dup_count = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT transaction_id, run_id, COUNT(*) AS cnt
                FROM gold.mart_compliance
                WHERE reference_date = ?::DATE
                GROUP BY transaction_id, run_id
                HAVING cnt > 1
            )
        """, [ref_date]).fetchone()[0]
        assert dup_count == 0

    def test_lineage_columns_present(self, mart_conn) -> None:
        """All audit-trail columns must exist in mart_compliance."""
        conn, _ = mart_conn
        cols = {
            r[0]
            for r in conn.execute("SELECT column_name FROM (DESCRIBE gold.mart_compliance)").fetchall()
        }
        required = {
            "transaction_id", "run_id", "reference_date", "file_name",
            "merchant_id", "category", "internal_amount", "processor_amount",
            "difference", "internal_status", "transaction_created_at",
            "result_created_at", "run_started_at", "run_completed_at",
            "_pipeline_run_id",
        }
        missing = required - cols
        assert not missing, f"Missing compliance lineage columns: {missing}"

    def test_matched_rows_have_both_amounts(self, mart_conn) -> None:
        """MATCHED rows must have both internal_amount and processor_amount."""
        conn, ref_date = mart_conn
        incomplete = conn.execute("""
            SELECT COUNT(*) FROM gold.mart_compliance
            WHERE reference_date = ?::DATE
              AND category = 'MATCHED'
              AND (internal_amount IS NULL OR processor_amount IS NULL)
        """, [ref_date]).fetchone()[0]
        assert incomplete == 0
