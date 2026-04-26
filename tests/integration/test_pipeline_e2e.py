"""Integration: end-to-end pipeline output shapes, value ranges, and correctness."""
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


@pytest.fixture(scope="module")
def pipeline_output(
    synthetic_data_path: Path,
    tmp_path_factory: pytest.TempPathFactory,
):
    """Run pipeline once and yield (conn, reference_date) for assertions."""
    db_path = tmp_path_factory.mktemp("e2e") / "e2e.duckdb"
    ref_date = _pick_reference_date(synthetic_data_path)
    run(reference_date=ref_date, data_path=synthetic_data_path, db_path=db_path)
    conn = get_connection(db_path)
    yield conn, ref_date
    conn.close()


class TestPipelineStatus:
    def test_pipeline_run_recorded_as_completed(self, pipeline_output) -> None:
        conn, _ = pipeline_output
        status = conn.execute(
            "SELECT status FROM gold.pipeline_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        assert status is not None
        assert status[0] == "COMPLETED"

    def test_pipeline_run_has_counts(self, pipeline_output) -> None:
        conn, _ = pipeline_output
        row = conn.execute(
            "SELECT counts FROM gold.pipeline_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        assert row is not None
        assert row[0] is not None  # counts JSON is populated


class TestBronzeLayer:
    def test_transactions_loaded(self, pipeline_output) -> None:
        conn, _ = pipeline_output
        count = conn.execute("SELECT COUNT(*) FROM bronze.transactions").fetchone()[0]
        assert count > 0

    def test_settlement_loaded(self, pipeline_output) -> None:
        conn, _ = pipeline_output
        count = conn.execute("SELECT COUNT(*) FROM bronze.settlement").fetchone()[0]
        assert count > 0

    def test_merchants_loaded(self, pipeline_output) -> None:
        conn, _ = pipeline_output
        count = conn.execute("SELECT COUNT(*) FROM bronze.merchants").fetchone()[0]
        assert count > 0


class TestSilverLayer:
    def test_transactions_deduplicated(self, pipeline_output) -> None:
        """silver.transactions has no duplicate transaction_ids for active rows."""
        conn, _ = pipeline_output
        dup_count = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT transaction_id, COUNT(*) AS cnt
                FROM silver.transactions
                WHERE NOT is_deleted
                GROUP BY transaction_id
                HAVING cnt > 1
            )
        """).fetchone()[0]
        assert dup_count == 0

    def test_settlement_amounts_are_positive(self, pipeline_output) -> None:
        """All parsed settlement amounts in silver must be > 0."""
        conn, _ = pipeline_output
        neg = conn.execute(
            "SELECT COUNT(*) FROM silver.settlement WHERE amount IS NOT NULL AND amount <= 0"
        ).fetchone()[0]
        assert neg == 0

    def test_settlement_amounts_not_null(self, pipeline_output) -> None:
        conn, _ = pipeline_output
        nulls = conn.execute(
            "SELECT COUNT(*) FROM silver.settlement WHERE amount IS NULL"
        ).fetchone()[0]
        assert nulls == 0


class TestGoldMarts:
    def test_mart_operations_has_rows(self, pipeline_output) -> None:
        conn, _ = pipeline_output
        assert conn.execute("SELECT COUNT(*) FROM gold.mart_operations").fetchone()[0] > 0

    def test_mart_cfo_has_rows(self, pipeline_output) -> None:
        conn, _ = pipeline_output
        assert conn.execute("SELECT COUNT(*) FROM gold.mart_cfo").fetchone()[0] > 0

    def test_mart_compliance_has_rows(self, pipeline_output) -> None:
        conn, _ = pipeline_output
        assert conn.execute("SELECT COUNT(*) FROM gold.mart_compliance").fetchone()[0] > 0

    def test_match_rate_in_valid_range(self, pipeline_output) -> None:
        """match_rate_pct must be between 0 and 100 for every run."""
        conn, _ = pipeline_output
        rows = conn.execute("""
            SELECT match_rate_pct FROM gold.mart_operations
            WHERE match_rate_pct IS NOT NULL
        """).fetchall()
        for (rate,) in rows:
            assert 0.0 <= float(rate) <= 100.0, f"match_rate_pct out of range: {rate}"

    def test_mart_cfo_no_null_merchant_id(self, pipeline_output) -> None:
        """PK column merchant_id must never be NULL in mart_cfo."""
        conn, _ = pipeline_output
        count = conn.execute(
            "SELECT COUNT(*) FROM gold.mart_cfo WHERE merchant_id IS NULL"
        ).fetchone()[0]
        assert count == 0

    def test_mart_compliance_no_null_category(self, pipeline_output) -> None:
        """Every compliance row must carry a category — it's NOT NULL in schema."""
        conn, _ = pipeline_output
        count = conn.execute(
            "SELECT COUNT(*) FROM gold.mart_compliance WHERE category IS NULL"
        ).fetchone()[0]
        assert count == 0

    def test_mart_compliance_categories_are_valid(self, pipeline_output) -> None:
        conn, _ = pipeline_output
        rows = conn.execute(
            "SELECT DISTINCT category FROM gold.mart_compliance"
        ).fetchall()
        valid = {"MATCHED", "MISMATCHED", "UNRECONCILED_INTERNAL", "UNRECONCILED_PROCESSOR"}
        actual = {r[0] for r in rows}
        assert actual.issubset(valid), f"Unexpected categories: {actual - valid}"

    def test_total_settled_amount_non_negative(self, pipeline_output) -> None:
        conn, _ = pipeline_output
        neg = conn.execute(
            "SELECT COUNT(*) FROM gold.mart_cfo WHERE total_settled_amount < 0"
        ).fetchone()[0]
        assert neg == 0
