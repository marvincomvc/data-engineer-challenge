"""DuckDB connection factory.

DuckDB is single-writer: only one process may hold a read/write handle to the
file at a time. The whole pipeline is therefore designed around a single
short-lived connection per stage (open -> do work -> close), and the Airflow
DAG runs stages sequentially. See docs/scalability.md for where this breaks.
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb


@contextmanager
def connect(duckdb_path: Path, read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(duckdb_path), read_only=read_only)
    try:
        yield con
    finally:
        con.close()


def ensure_schemas(con: duckdb.DuckDBPyConnection) -> None:
    """Bronze (Python-owned raw landing) and meta (observability) schemas.

    Silver/Gold schemas are created and owned by dbt.
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS meta")
