from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


@contextmanager
def transaction(conn: duckdb.DuckDBPyConnection) -> Generator[None, None, None]:
    """Wrap a block of SQL in an explicit BEGIN/COMMIT, rolling back on error."""
    conn.begin()
    try:
        yield
        conn.commit()
    except Exception:
        conn.rollback()
        raise

# Resolves to <repo_root>/sql/ — works for editable installs and in-container at /app/
_DEFAULT_SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def get_connection(db_path: str | Path = ":memory:") -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(db_path))
    _bootstrap_schema(conn)
    return conn


def _bootstrap_schema(conn: duckdb.DuckDBPyConnection) -> None:
    sql_dir = _resolve_sql_dir()
    for layer in ("bronze", "silver", "gold"):
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {layer}")
        sql_file = sql_dir / "schema" / f"{layer}.sql"
        if sql_file.exists():
            conn.execute(sql_file.read_text())
        else:
            logger.warning("schema file not found: %s", sql_file)


def _resolve_sql_dir() -> Path:
    if _DEFAULT_SQL_DIR.exists():
        return _DEFAULT_SQL_DIR
    fallback = Path("/app/sql")
    if fallback.exists():
        return fallback
    raise FileNotFoundError(
        f"sql/ directory not found at {_DEFAULT_SQL_DIR} or {fallback}. "
        "Set SQL_DIR env var or ensure the project is correctly installed."
    )
