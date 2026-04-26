"""Data quality checks — STOP vs WARN.

STOP checks run on bronze right after ingest. Any violation aborts the
pipeline by raising QualityError. These protect downstream silver/gold from
structurally invalid data (unparseable amounts, missing primary keys, etc.).

WARN checks run on silver after transformation. Violations are logged as
warnings but never abort the pipeline. These signal business-level anomalies
(non-BRL currencies, orphan merchants, low match rate) that Ops should
investigate but that don't make the data unusable.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Sequence

import duckdb

logger = logging.getLogger(__name__)

# SQL fragment — amount parsing expression mirrored from silver.py.
# Used here to detect which rows will silently become NULL after normalization.
_AMOUNT_PARSE = """
    TRY_CAST(
        CASE
            WHEN trim(amount) LIKE 'R$%'
                THEN replace(replace(trim(substr(trim(amount), 3)), '.', ''), ',', '.')
            WHEN trim(amount) LIKE '%,%'
                THEN replace(replace(trim(amount), '.', ''), ',', '.')
            ELSE trim(amount)
        END
    AS DECIMAL(18, 2))
"""


class Severity(str, Enum):
    STOP = "STOP"
    WARN = "WARN"


@dataclass(frozen=True)
class QualityCheck:
    name: str
    severity: Severity
    sql: str
    description: str
    params: tuple = ()


@dataclass
class CheckResult:
    check: QualityCheck
    violations: int

    @property
    def passed(self) -> bool:
        return self.violations == 0


class QualityError(Exception):
    """Raised when one or more STOP checks find violations."""

    def __init__(self, failures: list[CheckResult]) -> None:
        self.failures = failures
        names = ", ".join(f.check.name for f in failures)
        counts = ", ".join(str(f.violations) for f in failures)
        super().__init__(f"Quality STOP failures [{names}] with violation counts [{counts}]")


# ---------------------------------------------------------------------------
# STOP checks — tables that are fully replaced each run (no scoping needed)
# ---------------------------------------------------------------------------

BRONZE_STOP_CHECKS: list[QualityCheck] = [
    QualityCheck(
        name="null_transaction_id",
        severity=Severity.STOP,
        sql="SELECT COUNT(*) FROM bronze.transactions WHERE transaction_id IS NULL",
        description="Transactions with NULL transaction_id cannot be reconciled",
    ),
    QualityCheck(
        name="null_run_id_in_results",
        severity=Severity.STOP,
        sql="SELECT COUNT(*) FROM bronze.reconciliation_results WHERE run_id IS NULL",
        description="Reconciliation results with no run_id cannot be attributed to a run",
    ),
]


def make_settlement_stop_checks(source_file: str) -> list[QualityCheck]:
    """Return STOP checks scoped to the current settlement file.

    bronze.settlement accumulates rows from multiple files (one per date).
    Filtering by _source_file ensures a bad row from a previous run cannot
    block the current run.
    """
    return [
        QualityCheck(
            name="unparseable_settlement_amount",
            severity=Severity.STOP,
            sql=f"""
                SELECT COUNT(*) FROM bronze.settlement
                WHERE _source_file = ?
                  AND (amount IS NULL
                       OR trim(amount) = ''
                       OR {_AMOUNT_PARSE} IS NULL)
            """,
            description="Settlement amounts that cannot be parsed to a numeric value",
            params=(source_file,),
        ),
        QualityCheck(
            name="non_positive_settlement_amount",
            severity=Severity.STOP,
            sql=f"""
                SELECT COUNT(*) FROM bronze.settlement
                WHERE _source_file = ?
                  AND {_AMOUNT_PARSE} IS NOT NULL
                  AND {_AMOUNT_PARSE} <= 0
            """,
            description="Settlement amounts that are zero or negative",
            params=(source_file,),
        ),
        QualityCheck(
            name="invalid_settlement_status",
            severity=Severity.STOP,
            sql="""
                SELECT COUNT(*) FROM bronze.settlement
                WHERE _source_file = ?
                  AND status NOT IN ('SETTLED', 'REVERSED')
            """,
            description="Settlement rows with an unknown status value",
            params=(source_file,),
        ),
    ]


# ---------------------------------------------------------------------------
# WARN checks — run on silver after transformation
# ---------------------------------------------------------------------------

SILVER_WARN_CHECKS: list[QualityCheck] = [
    QualityCheck(
        name="non_brl_currency",
        severity=Severity.WARN,
        sql="SELECT COUNT(*) FROM silver.transactions WHERE currency != 'BRL'",
        description="Transactions in currencies other than BRL",
    ),
    QualityCheck(
        name="null_merchant_id",
        severity=Severity.WARN,
        sql="""
            SELECT COUNT(*) FROM silver.transactions
            WHERE merchant_id IS NULL AND NOT is_deleted
        """,
        description="Non-deleted transactions with a NULL merchant_id",
    ),
    QualityCheck(
        name="orphan_merchant_in_results",
        severity=Severity.WARN,
        sql="""
            SELECT COUNT(*)
            FROM silver.reconciliation_results rr
            LEFT JOIN silver.merchants m USING (merchant_id)
            WHERE m.merchant_id IS NULL
              AND rr.reference_date IS NOT NULL
        """,
        description="Reconciliation results referencing a merchant not in silver.merchants",
    ),
    QualityCheck(
        name="low_match_rate",
        severity=Severity.WARN,
        sql="""
            SELECT COUNT(*) FROM (
                SELECT
                    run_id,
                    SUM(CASE WHEN category = 'MATCHED' THEN 1 ELSE 0 END)::DOUBLE
                        / NULLIF(COUNT(*), 0) AS match_rate
                FROM silver.reconciliation_results
                WHERE reference_date IS NOT NULL
                GROUP BY run_id
                HAVING match_rate < 0.5
            )
        """,
        description="Reconciliation runs where fewer than 50% of transactions matched",
    ),
    QualityCheck(
        name="orphan_merchant_in_settlement",
        severity=Severity.WARN,
        sql="""
            SELECT COUNT(*)
            FROM silver.settlement s
            LEFT JOIN silver.merchants m USING (merchant_id)
            WHERE m.merchant_id IS NULL
        """,
        description="Settlement rows referencing a merchant not in silver.merchants",
    ),
]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_checks(
    conn: duckdb.DuckDBPyConnection,
    checks: Sequence[QualityCheck],
) -> list[CheckResult]:
    """Execute every check and return results."""
    results = []
    for check in checks:
        violations: int = conn.execute(check.sql, list(check.params)).fetchone()[0]
        result = CheckResult(check=check, violations=violations)
        results.append(result)
        if result.passed:
            logger.debug("check passed", extra={"step": f"quality.{check.name}"})
        else:
            logger.warning(
                "check failed",
                extra={
                    "step": f"quality.{check.name}",
                    "severity": check.severity.value,
                    "violations": violations,
                    "description": check.description,
                },
            )
    return results


def run_stop_checks(conn: duckdb.DuckDBPyConnection, source_file: str) -> list[CheckResult]:
    """Run all STOP checks on bronze. Raises QualityError on any failure."""
    all_checks = BRONZE_STOP_CHECKS + make_settlement_stop_checks(source_file)
    results = run_checks(conn, all_checks)
    failures = [r for r in results if not r.passed]
    if failures:
        raise QualityError(failures)
    return results


def run_warn_checks(conn: duckdb.DuckDBPyConnection) -> list[CheckResult]:
    """Run all WARN checks on silver. Logs failures but never raises."""
    return run_checks(conn, SILVER_WARN_CHECKS)
