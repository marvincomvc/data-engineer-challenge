"""Read internal Parquet files with schema drift handling between batches.

Uses DuckDB's union_by_name=true to automatically coalesce divergent columns
across transactions_batch_*.parquet files.
"""
from __future__ import annotations

import glob
from pathlib import Path

import duckdb
from pipeline.config import PipelineConfig
from pipeline.observability.logging import get_logger

logger = get_logger(__name__)


def get_parquet_columns(con: duckdb.DuckDBPyConnection, path: str) -> list[str]:
    """Get column names from a parquet file."""
    cols = con.execute(f"""
    SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{path}'))
    """).fetchall()
    return [c[0] for c in cols]


def _discover_transaction_batches(data_path: Path) -> list[str]:
    """Find all transactions_batch_*.parquet files sorted by name."""
    pattern = str(data_path / "transactions_batch_*.parquet")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No transactions_batch_*.parquet files found in {data_path}")
    return files


def read_parquet_batches(
    con: duckdb.DuckDBPyConnection, cfg: PipelineConfig
) -> tuple[list[str], list[str]]:
    """Read all transaction batches using union_by_name for schema drift handling.

    Creates a view 'raw_transactions' in the connection that unions all batches
    with automatic NULL coalescing for divergent columns.

    Returns (first_batch_columns, last_batch_columns) for quality checks.
    """
    batch_files = _discover_transaction_batches(cfg.data_path)

    if len(batch_files) == 1:
        glob_path = batch_files[0]
    else:
        glob_path = str(cfg.data_path / "transactions_batch_*.parquet")

    # Detect schema drift for logging
    batch_cols: list[list[str]] = []
    for path in batch_files:
        cols = get_parquet_columns(con, path)
        batch_cols.append(cols)

    for i, cols in enumerate(batch_cols):
        logger.info(f"Discovered batch: {Path(batch_files[i]).name} ({len(cols)} columns)")

    for i in range(1, len(batch_cols)):
        extra = set(batch_cols[i]) - set(batch_cols[0])
        if extra:
            logger.info(f"Schema drift: columns {extra} missing in {Path(batch_files[0]).name}")
        missing = set(batch_cols[0]) - set(batch_cols[i])
        if missing:
            logger.info(f"Schema drift: columns {missing} missing in {Path(batch_files[i]).name}")

    con.execute(f"""
    CREATE OR REPLACE VIEW raw_transactions AS
    SELECT * FROM read_parquet('{glob_path}', union_by_name=true)
    """)

    return batch_cols[0], batch_cols[-1]
