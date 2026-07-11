"""PaySettler CSV helpers.

The raw file is landed *as strings* so dirty values (comma decimals, missing
timezone suffix, lowercased merchant ids) survive into Bronze untouched and are
repaired deterministically in the dbt staging layer, where the logic is unit
tested. This module only owns robust raw reading and reference-date derivation.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb


def read_raw_relation(con: duckdb.DuckDBPyConnection, csv_path: Path) -> str:
    """Return a SQL snippet that reads the CSV with every column as VARCHAR.

    all_varchar=true preserves malformed numerics/timestamps so the integrity
    gate in staging can decide what to repair vs. reject.
    """
    safe_path = csv_path.as_posix().replace("'", "''")  # escape for the SQL literal
    return (
        f"read_csv('{safe_path}', "
        "header=true, all_varchar=true, quote='\"', escape='\"', "
        "ignore_errors=false)"
    )


def infer_reference_date(con: duckdb.DuckDBPyConnection, csv_path: Path) -> date:
    """Reference date = the business day the settlement file represents.

    Derived from the max settlement date in the file. In production the value
    comes from the filename (settlement_YYYYMMDD_*.csv); we fall back to the
    data itself so the sample generic filename still works.
    """
    rel = read_raw_relation(con, csv_path)
    result = con.execute(
        f"SELECT max(try_cast(settled_at AS TIMESTAMP)::DATE) FROM {rel}"
    ).fetchone()
    if not result or result[0] is None:
        raise ValueError(f"Could not infer reference_date from {csv_path}")
    return result[0]
