from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from pipeline.db import transaction

logger = logging.getLogger(__name__)

_EXPECTED_COLUMNS = {
    "transaction_id", "merchant_id", "amount", "currency",
    "settled_at", "processor_reference", "status",
}


def find_settlement_file(data_path: Path, reference_date: str) -> Path:
    """Return the settlement CSV path for the given reference_date.

    Prefers a per-day file under paysettler/ (generated data layout).
    Falls back to the consolidated settlement_paysettler.csv (sample-data layout).
    """
    per_day = data_path / "paysettler" / f"settlement_{reference_date}.csv"
    if per_day.exists():
        return per_day
    fallback = data_path / "settlement_paysettler.csv"
    if fallback.exists():
        logger.warning(
            "per-day CSV not found, using fallback",
            extra={"step": "ingest.settlement", "reference_date": reference_date},
        )
        return fallback
    raise FileNotFoundError(
        f"No settlement CSV found for {reference_date} in {data_path}. "
        "Expected paysettler/settlement_{date}.csv or settlement_paysettler.csv."
    )


def ingest_settlement(
    conn: duckdb.DuckDBPyConnection,
    data_path: Path,
    reference_date: str,
) -> tuple[int, str]:
    """Load settlement CSV into bronze.settlement.

    Reads all columns as VARCHAR (all_varchar=true) to preserve raw dirty-data
    patterns (comma decimals, missing timezone). Parsing and normalization
    happen in the silver transform layer.

    Idempotency: deletes existing rows for this source_file before inserting.

    Returns (row_count, source_file_name) so callers can pass the exact file
    to build_silver without querying bronze non-deterministically.
    """
    csv_path = find_settlement_file(data_path, reference_date)
    source_file = csv_path.name

    _validate_csv_header(conn, csv_path)

    with transaction(conn):
        conn.execute("DELETE FROM bronze.settlement WHERE _source_file = ?", [source_file])
        conn.execute(f"""
            INSERT INTO bronze.settlement (
                transaction_id, merchant_id, amount, currency,
                settled_at, processor_reference, status, _source_file
            )
            SELECT
                transaction_id,
                merchant_id,
                amount,
                currency,
                settled_at,
                processor_reference,
                status,
                ?
            FROM read_csv(
                '{csv_path}',
                all_varchar = true,
                header = true
            )
        """, [source_file])
    count: int = conn.execute(
        "SELECT COUNT(*) FROM bronze.settlement WHERE _source_file = ?", [source_file]
    ).fetchone()[0]
    logger.info(
        "bronze.settlement loaded",
        extra={"counts": {"rows": count, "source_file": source_file}},
    )
    return count, source_file


def _validate_csv_header(conn: duckdb.DuckDBPyConnection, csv_path: Path) -> None:
    """STOP check: raise if required columns are missing from the CSV header."""
    rows = conn.execute(
        f"SELECT column_name FROM (DESCRIBE SELECT * FROM read_csv('{csv_path}', header=true, all_varchar=true))"
    ).fetchall()
    actual = {r[0] for r in rows}
    missing = _EXPECTED_COLUMNS - actual
    if missing:
        raise ValueError(
            f"Settlement CSV {csv_path.name} is missing required columns: {missing}"
        )
