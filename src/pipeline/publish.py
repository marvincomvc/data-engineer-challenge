"""Publish Gold marts as consumable data products.

Consumption model: business consumers must NOT read the live warehouse file
(DuckDB is single-writer — a BI tool holding it open would block the pipeline).
Instead we publish immutable, partitioned Parquet to a `gold/` zone:

    gold/
      mart_ops_health/reference_date=2025-03-16/*.parquet
      mart_cfo_volume/reference_date=2025-03-16/*.parquet   (+ .csv extract)
      mart_compliance_audit/reference_date=2025-03-16/*.parquet
      fct_reconciliation_result/reference_date=2025-03-16/*.parquet
      dim_merchant/*.parquet  dim_category/*.parquet  dim_date/*.parquet
      serving.duckdb           (thin catalog: VIEWS over the globs, for BI)

These files are the data products: portable (pandas/Spark/BI/warehouse external
tables all read Parquet), versioned by reference_date, and decoupled from the
writer. Re-publishing a reference_date overwrites just that partition -> idempotent.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from .config import Config
from .logging import get_logger

log = get_logger("pipeline.publish")

# marts/facts partitioned by reference_date (published per-run, one partition)
PARTITIONED = ["mart_ops_health", "mart_cfo_volume", "mart_compliance_audit",
               "fct_reconciliation_result"]
# dimensions have no date grain -> published whole
DIMENSIONS = ["dim_merchant", "dim_category", "dim_date"]
# marts also handy as a flat CSV extract for spreadsheet consumers (CFO)
CSV_EXTRACTS = ["mart_cfo_volume"]


def publish_gold(con: duckdb.DuckDBPyConnection, cfg: Config, reference_date: str) -> dict[str, int]:
    """Export Gold to the gold/ Parquet zone. Returns rows written per product."""
    gold_dir = cfg.gold_path()
    gold_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, int] = {}

    for mart in PARTITIONED:
        target = (gold_dir / mart).as_posix()
        con.execute(
            f"""
            COPY (SELECT * FROM main.{mart} WHERE reference_date = DATE '{reference_date}')
            TO '{target}'
            (FORMAT parquet, PARTITION_BY (reference_date), OVERWRITE_OR_IGNORE)
            """
        )
        written[mart] = con.execute(
            f"SELECT count(*) FROM main.{mart} WHERE reference_date = DATE '{reference_date}'"
        ).fetchone()[0]

    for dim in DIMENSIONS:
        # write into a directory (not a bare file) so the serving glob matches
        dim_dir = gold_dir / dim
        dim_dir.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"COPY (SELECT * FROM main.{dim}) TO '{(dim_dir / 'data.parquet').as_posix()}' (FORMAT parquet)"
        )
        written[dim] = con.execute(f"SELECT count(*) FROM main.{dim}").fetchone()[0]

    for mart in CSV_EXTRACTS:
        target = (gold_dir / f"{mart}.csv").as_posix()
        con.execute(f"COPY (SELECT * FROM main.{mart}) TO '{target}' (FORMAT csv, HEADER)")

    log.info("publish.done", extra={"json": {"reference_date": reference_date, "written": written,
                                             "gold_dir": gold_dir.as_posix()}})
    _refresh_serving_catalog(cfg, gold_dir)
    return written


def _refresh_serving_catalog(cfg: Config, gold_dir: Path) -> None:
    """Create serving.duckdb with views over the Gold Parquet globs (for BI).

    Best-effort: the views reference file globs, so they never need updating once
    created — new partitions are picked up automatically. If a BI tool currently
    holds the file open (read lock), we skip silently; the existing views already
    serve the fresh Parquet.
    """
    serving_path = gold_dir / "serving.duckdb"
    products = PARTITIONED + DIMENSIONS
    try:
        con = duckdb.connect(str(serving_path))
    except duckdb.IOException:
        log.info("publish.serving_locked_skip", extra={"json": {"path": serving_path.as_posix()}})
        return
    try:
        for name in products:
            glob = (gold_dir / name / "**/*.parquet").as_posix()
            con.execute(
                f"CREATE OR REPLACE VIEW {name} AS "
                f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true, union_by_name=true)"
            )
        log.info("publish.serving_ready", extra={"json": {"path": serving_path.as_posix(),
                                                          "views": products}})
    finally:
        con.close()
