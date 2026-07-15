from datetime import datetime

import duckdb

from app.config import settings

TABLES = [
    ("bronze", "transactions"),
    ("silver", "transactions"),
    ("silver", "reconciliation_runs"),
    ("silver", "reconciliation_results"),
    ("silver", "enterprise_company"),
    ("gold", "dim_merchant"),
    ("gold", "fact_reconciliation"),
    ("diamond", "cfo_daily_metrics"),
]


def _result(
    name: str,
    status: str,
    message: str,
    rows: int | None = None,
):
    return {
        "check_name": name,
        "status": status,
        "message": message,
        "rows": rows,
        "checked_at": datetime.utcnow(),
    }


# -------------------------------------------------------
# Pipeline
# -------------------------------------------------------


def check_table_exists(conn, schema, table):

    exists = conn.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema='{schema}'
          AND table_name='{table}'
    """).fetchone()[0]

    return _result(
        f"{schema}.{table}",
        "PASS" if exists else "FAIL",
        "Table exists" if exists else "Table not found",
    )


def check_row_count(conn, schema, table):

    rows = conn.execute(f"""
        SELECT COUNT(*)
        FROM {schema}.{table}
    """).fetchone()[0]

    return _result(
        f"{schema}.{table}_rows",
        "PASS" if rows else "FAIL",
        f"{rows} rows",
        rows,
    )


# -------------------------------------------------------
# Silver
# -------------------------------------------------------


def check_duplicate_transactions(conn):

    rows = conn.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT transaction_id
            FROM silver.transactions
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    return _result(
        "duplicate_transactions",
        "PASS" if rows == 0 else "FAIL",
        f"{rows} duplicated transactions",
        rows,
    )


def check_null_transaction_id(conn):

    rows = conn.execute("""
        SELECT COUNT(*)
        FROM silver.transactions
        WHERE transaction_id IS NULL
    """).fetchone()[0]

    return _result(
        "null_transaction_id",
        "PASS" if rows == 0 else "FAIL",
        f"{rows} null ids",
        rows,
    )


def check_null_amount(conn):

    rows = conn.execute("""
        SELECT COUNT(*)
        FROM silver.transactions
        WHERE amount IS NULL
    """).fetchone()[0]

    return _result(
        "null_amount",
        "PASS" if rows == 0 else "FAIL",
        f"{rows} null amounts",
        rows,
    )


def check_negative_amount(conn):

    rows = conn.execute("""
        SELECT COUNT(*)
        FROM silver.transactions
        WHERE amount <= 0
    """).fetchone()[0]

    return _result(
        "negative_amount",
        "PASS" if rows == 0 else "FAIL",
        f"{rows} invalid amounts",
        rows,
    )


def check_future_created_at(conn):

    rows = conn.execute("""
        SELECT COUNT(*)
        FROM silver.transactions
        WHERE created_at > CURRENT_TIMESTAMP
    """).fetchone()[0]

    return _result(
        "future_created_at",
        "PASS" if rows == 0 else "FAIL",
        f"{rows} future rows",
        rows,
    )


def check_valid_currency(conn):

    rows = conn.execute("""
        SELECT COUNT(*)
        FROM silver.transactions
        WHERE currency <> 'BRL'
    """).fetchone()[0]

    return _result(
        "currency_domain",
        "PASS" if rows == 0 else "FAIL",
        f"{rows} invalid currencies",
        rows,
    )


# -------------------------------------------------------
# Business Rules
# -------------------------------------------------------


def check_missing_company(conn):

    rows = conn.execute("""
        SELECT COUNT(*)
        FROM silver.transactions t
        LEFT JOIN silver.enterprise_company c
            ON t.merchant_id = c.merchant_id
        WHERE c.merchant_id IS NULL
    """).fetchone()[0]

    return _result(
        "merchant_lookup",
        "PASS" if rows == 0 else "FAIL",
        f"{rows} merchants not found",
        rows,
    )


def check_reconciliation_reference(conn):

    rows = conn.execute("""
        SELECT COUNT(*)
        FROM silver.reconciliation_results r
        LEFT JOIN silver.transactions t
            ON r.transaction_id=t.transaction_id
        WHERE t.transaction_id IS NULL
    """).fetchone()[0]

    return _result(
        "reconciliation_reference",
        "PASS" if rows == 0 else "FAIL",
        f"{rows} orphan reconciliations",
        rows,
    )

# -------------------------------------------------------
# Runner
# -------------------------------------------------------


def run_all_checks():

    conn = duckdb.connect(settings.DUCKDB_PATH)

    results = []

    for schema, table in TABLES:
        result = check_table_exists(conn, schema, table)

        results.append(result)

        if result["status"] == "PASS":
            results.append(
                check_row_count(
                    conn,
                    schema,
                    table,
                )
            )

    results.extend(
        [
            check_duplicate_transactions(conn),
            check_null_transaction_id(conn),
            check_null_amount(conn),
            check_negative_amount(conn),
            check_future_created_at(conn),
            check_valid_currency(conn),
            check_missing_company(conn),
            check_reconciliation_reference(conn),
        ]
    )

    conn.close()

    return results
