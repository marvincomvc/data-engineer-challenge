"""Bronze layer: land raw sources into the DuckDB warehouse.

Design choices:
- Raw values are preserved as-is (strings for the CSV) so downstream staging
  owns all cleaning; Bronze is an auditable copy of what arrived.
- Lineage columns (_source_file, _ingested_at, _run_id) are stamped on every row.
- Loads are transactional and full-replace, which makes Bronze trivially
  idempotent for the challenge's snapshot-style CDC extracts. At production
  scale you would land only the new partition (see docs/scalability.md).
- Memory-awareness comes from DuckDB's streaming parquet reader plus an
  explicit memory_limit, so a 5M-row file never has to fit in RAM.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from ..config import Config
from ..logging import get_logger
from .csv_parser import read_raw_relation

log = get_logger("pipeline.bronze")

# transactions ships in two CDC batches; batch_2 has an extra payment_method
# column (schema drift). union_by_name + filename give us drift tolerance and
# per-row lineage in one native read.
TRANSACTION_BATCHES = ["transactions_batch_1", "transactions_batch_2"]
SINGLE_PARQUET_SOURCES = {
    "raw_reconciliation_runs": "reconciliation_runs",
    "raw_reconciliation_results": "reconciliation_results",
    "raw_enterprise_company": "enterprise_company",
}

# Processor settlement feeds (CSV). Adding a second processor is one entry here
# plus one dbt staging adapter (stg_<name>.sql) unioned into stg_settlements.
# See docs/PR.md > Extensibilidade.
PROCESSOR_SOURCES = {
    "raw_paysettler": "settlement_paysettler.csv",
}


def _apply_memory_limit(con: duckdb.DuckDBPyConnection, limit: str = "2GB") -> None:
    con.execute(f"PRAGMA memory_limit='{limit}'")
    con.execute("PRAGMA temp_directory='warehouse/.duckdb_spill'")


def load_bronze(con: duckdb.DuckDBPyConnection, cfg: Config, run_id: str) -> dict[str, int]:
    """Full-replace load of every source into the bronze schema. Returns row counts."""
    _apply_memory_limit(con)
    counts: dict[str, int] = {}

    con.execute("BEGIN TRANSACTION")
    try:
        # --- internal transactions (2 batches, schema drift) --------------
        paths = [str(cfg.parquet(b).as_posix()) for b in TRANSACTION_BATCHES]
        path_list = ", ".join(f"'{p}'" for p in paths)
        con.execute(
            f"""
            CREATE OR REPLACE TABLE bronze.raw_transactions AS
            SELECT *,
                   parse_filename(filename) AS _source_file,
                   now()                     AS _ingested_at,
                   '{run_id}'                AS _run_id
            FROM read_parquet([{path_list}], union_by_name=true, filename=true)
            """
        )
        counts["raw_transactions"] = _count(con, "bronze.raw_transactions")

        # --- other CDC parquet sources ------------------------------------
        for table, src in SINGLE_PARQUET_SOURCES.items():
            path = cfg.parquet(src).as_posix()
            con.execute(
                f"""
                CREATE OR REPLACE TABLE bronze.{table} AS
                SELECT *,
                       '{Path(path).name}' AS _source_file,
                       now()                AS _ingested_at,
                       '{run_id}'           AS _run_id
                FROM read_parquet('{path}')
                """
            )
            counts[table] = _count(con, f"bronze.{table}")

        # --- processor settlement feeds (CSV, landed as raw strings) ------
        for table, filename in PROCESSOR_SOURCES.items():
            csv_path = cfg.data_path / filename
            rel = read_raw_relation(con, csv_path)
            con.execute(
                f"""
                CREATE OR REPLACE TABLE bronze.{table} AS
                SELECT *,
                       '{filename}' AS _source_file,
                       now()        AS _ingested_at,
                       '{run_id}'   AS _run_id
                FROM {rel}
                """
            )
            counts[table] = _count(con, f"bronze.{table}")

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        log.error("bronze.load_failed", extra={"json": {"run_id": run_id}})
        raise

    log.info("bronze.loaded", extra={"json": {"run_id": run_id, "counts": counts}})
    return counts


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
