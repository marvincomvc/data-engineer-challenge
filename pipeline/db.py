"""DuckDB connection and transaction helpers."""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import duckdb


def connect(database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection. Defaults to an in-memory database (used by tests)."""
    return duckdb.connect(database)


@contextmanager
def transaction(con: duckdb.DuckDBPyConnection) -> Iterator[None]:
    """Run a block inside a single transaction.

    Commits on success; rolls back and re-raises on any exception. This is what
    makes the partition-replace (DELETE + INSERT) idempotent and crash-safe: a
    failure mid-write leaves the target table untouched instead of half-deleted.
    """
    con.execute("BEGIN TRANSACTION")
    try:
        yield
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
