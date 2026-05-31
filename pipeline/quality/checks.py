"""Data quality checks — HARD (pipeline-blocking) and SOFT (log-and-continue)."""
from __future__ import annotations

from dataclasses import dataclass

import duckdb


@dataclass
class CheckResult:
    name: str
    check_type: str  # "HARD" | "SOFT"
    passed: bool
    message: str
    failed_count: int = 0

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{self.check_type}] {self.name}: {status} — {self.message}"


# --- HARD checks ---


def check_null_transaction_id_in_results(con: duckdb.DuckDBPyConnection) -> CheckResult:
    count = con.execute(
        "SELECT count(*) FROM silver.reconciliation_results WHERE transaction_id IS NULL"
    ).fetchone()[0]
    return CheckResult(
        name="null_transaction_id_in_results",
        check_type="HARD",
        passed=count == 0,
        message=f"{count} rows with null transaction_id in reconciliation_results",
        failed_count=count,
    )


def check_negative_amount_in_transactions(con: duckdb.DuckDBPyConnection) -> CheckResult:
    count = con.execute(
        "SELECT count(*) FROM silver.transactions WHERE amount < 0"
    ).fetchone()[0]
    return CheckResult(
        name="negative_amount_in_transactions",
        check_type="HARD",
        passed=count == 0,
        message=f"{count} rows with negative amount in transactions",
        failed_count=count,
    )


def check_empty_csv(csv_row_count: int) -> CheckResult:
    return CheckResult(
        name="empty_csv",
        check_type="HARD",
        passed=csv_row_count > 0,
        message=f"CSV has {csv_row_count} data rows",
        failed_count=0 if csv_row_count > 0 else 1,
    )


def check_duplicate_transaction_id_in_run(con: duckdb.DuckDBPyConnection, reference_date: str = "") -> CheckResult:
    """Check for duplicate transaction_ids within the same run/date.

    Note: SOFT because CDC source data may contain pre-existing duplicates
    that the pipeline already deduplicates during silver processing.
    If a reference_date is provided, only check that date's pipeline results.
    """
    if reference_date:
        count = con.execute(f"""
            SELECT count(*) FROM (
                SELECT transaction_id, count(*) as cnt
                FROM silver.reconciliation_results
                WHERE reference_date = '{reference_date}'
                  AND run_id IS NULL
                GROUP BY transaction_id
                HAVING cnt > 1
            )
        """).fetchone()[0]
        msg = f"{count} duplicate transaction_ids in pipeline results for {reference_date}"
    else:
        count = con.execute("""
            SELECT count(*) FROM (
                SELECT transaction_id, reference_date, count(*) as cnt
                FROM silver.reconciliation_results
                GROUP BY transaction_id, reference_date
                HAVING cnt > 1
            )
        """).fetchone()[0]
        msg = f"{count} duplicate transaction_ids within the same run (includes CDC source data)"
    return CheckResult(
        name="duplicate_transaction_id_in_run",
        check_type="SOFT",
        passed=count == 0,
        message=msg,
        failed_count=count,
    )


# --- SOFT checks ---


def check_mismatch_rate(con: duckdb.DuckDBPyConnection, threshold: float = 0.20) -> CheckResult:
    row = con.execute("""
        SELECT
            count(*) AS total,
            COALESCE(SUM(CASE WHEN category = 'MISMATCHED' THEN 1 ELSE 0 END), 0) AS mismatched
        FROM silver.reconciliation_results
    """).fetchone()
    total, mismatched = row[0], row[1]
    rate = mismatched / total if total > 0 else 0.0
    return CheckResult(
        name="mismatch_rate_above_threshold",
        check_type="SOFT",
        passed=rate <= threshold,
        message=f"Mismatch rate: {rate:.2%} (threshold: {threshold:.0%})",
        failed_count=mismatched if rate > threshold else 0,
    )


def check_orphan_merchants(con: duckdb.DuckDBPyConnection) -> CheckResult:
    count = con.execute("""
        SELECT count(DISTINCT merchant_id)
        FROM silver.reconciliation_results
        WHERE merchant_id IS NOT NULL
          AND merchant_id NOT IN (SELECT merchant_id FROM silver.merchants)
    """).fetchone()[0]
    return CheckResult(
        name="orphan_merchants",
        check_type="SOFT",
        passed=count == 0,
        message=f"{count} merchants in results not found in enterprise_company",
        failed_count=count,
    )


def check_schema_drift_logged(
    batch1_cols: list[str], batch2_cols: list[str]
) -> CheckResult:
    extra_in_b2 = set(batch2_cols) - set(batch1_cols)
    extra_in_b1 = set(batch1_cols) - set(batch2_cols)
    # Schema drift is expected and handled — always passes, but logs details
    return CheckResult(
        name="schema_drift_logged",
        check_type="SOFT",
        passed=True,
        message=f"Schema drift: batch2 extra={extra_in_b2 or 'none'}, batch1 extra={extra_in_b1 or 'none'}",
        failed_count=0,
    )


def run_all_checks(
    con: duckdb.DuckDBPyConnection,
    csv_row_count: int = 0,
    batch1_cols: list[str] | None = None,
    batch2_cols: list[str] | None = None,
    mismatch_threshold: float = 0.20,
    reference_date: str = "",
) -> list[CheckResult]:
    results = []
    results.append(check_null_transaction_id_in_results(con))
    results.append(check_negative_amount_in_transactions(con))
    results.append(check_empty_csv(csv_row_count))
    results.append(check_duplicate_transaction_id_in_run(con, reference_date))
    results.append(check_mismatch_rate(con, mismatch_threshold))
    results.append(check_orphan_merchants(con))
    if batch1_cols is not None and batch2_cols is not None:
        results.append(check_schema_drift_logged(batch1_cols, batch2_cols))
    return results
