"""Pipeline CLI.

    python -m pipeline ingest --reference-date 2025-03-16
    python -m pipeline run    --reference-date 2025-03-16     # ingest + dbt build + publish
    python -m pipeline run                                    # infer date from CSV

`run` is the local one-shot path (used by `make run`); Airflow calls the
individual stages so each gets its own retry/observability. A single `run_id`
(passed with --run-id under Airflow, generated otherwise) ties logs,
pipeline_run rows, dq_check_result, and dbt provenance together.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date
from pathlib import Path

from .config import CONFIG, Config
from .db import connect, ensure_schemas
from .ingest.bronze import load_bronze
from .ingest.csv_parser import infer_reference_date
from .logging import get_logger
from .publish import publish_gold
from .run_registry import (
    init as init_registry,
    load_dq_results,
    new_run_id,
    record_stage_standalone,
    stage_span,
)

log = get_logger("pipeline.cli", CONFIG.log_level)

DBT_DIR = "dbt"


def _resolve_reference_date(cfg: Config, given: str | None) -> str:
    if given:
        date.fromisoformat(given)  # validate format early
        return given
    with connect(cfg.duckdb_path, read_only=False) as con:
        return infer_reference_date(con, cfg.csv_path()).isoformat()


def _dbt_vars(cfg: Config, reference_date: str, run_id: str) -> str:
    """Single source of business rules: env-backed config -> dbt vars.

    Forwarding these makes RECONCILIATION_TOLERANCE / _WINDOW_DAYS / warn
    thresholds actually take effect (they previously only lived in dbt_project.yml).
    json.dumps keeps the values safely quoted.
    """
    return json.dumps(
        {
            "reference_date": reference_date,
            "run_id": run_id,
            "reconciliation_tolerance": float(cfg.tolerance),
            "reconciliation_window_days": cfg.window_days,
            "mismatch_rate_warn": cfg.mismatch_rate_warn,
            "orphan_rate_warn": cfg.orphan_rate_warn,
        }
    )


def cmd_ingest(cfg: Config, reference_date: str, run_id: str) -> None:
    with connect(cfg.duckdb_path) as con:
        ensure_schemas(con)
        init_registry(con)
        with stage_span(con, run_id, reference_date, "ingest") as span:
            counts = load_bronze(con, cfg, run_id)
            span.rows_out = sum(counts.values())


def cmd_publish(cfg: Config, reference_date: str, run_id: str) -> None:
    # Read the warehouse read-only; publish only writes Parquet to the gold/ zone.
    with record_stage_standalone(cfg.duckdb_path, run_id, reference_date, "publish") as span:
        with connect(cfg.duckdb_path, read_only=True) as con:
            written = publish_gold(con, cfg, reference_date)
        span.rows_out = sum(written.values())


def cmd_dbt(cfg: Config, reference_date: str, run_id: str, command: str) -> None:
    """Invoke dbt (`run` / `test` / `build`) with shared vars, then load DQ results."""
    vars_json = _dbt_vars(cfg, reference_date, run_id)
    cmd = ["dbt", command, "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR, "--vars", vars_json]
    log.info("dbt.invoke", extra={"json": {"cmd": " ".join(cmd[:3]), "run_id": run_id}})
    # DuckDB is single-writer: do NOT hold a connection while dbt runs, or dbt
    # cannot open the warehouse file. record_stage_standalone releases the lock
    # around the subprocess.
    with record_stage_standalone(cfg.duckdb_path, run_id, reference_date, f"dbt_{command}"):
        result = subprocess.run(cmd, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"dbt {command} failed with exit code {result.returncode}")
        # dbt has exited (released the file lock); surface test results into DQ observability.
        if command in ("test", "build"):
            load_dq_results(
                cfg.duckdb_path, run_id, reference_date, Path(DBT_DIR) / "target" / "run_results.json"
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("ingest", "run", "dbt-run", "dbt-test", "publish"):
        p = sub.add_parser(name)
        p.add_argument("--reference-date", default=None, help="YYYY-MM-DD; inferred from CSV if omitted")
        p.add_argument("--run-id", default=None, help="correlation id; generated if omitted")

    args = parser.parse_args(argv)
    cfg = CONFIG
    run_id = args.run_id or new_run_id()
    reference_date = _resolve_reference_date(cfg, args.reference_date)
    log.info(
        "pipeline.start",
        extra={"json": {"command": args.command, "reference_date": reference_date, "run_id": run_id}},
    )

    try:
        if args.command == "ingest":
            cmd_ingest(cfg, reference_date, run_id)
        elif args.command == "dbt-run":
            cmd_dbt(cfg, reference_date, run_id, "run")
        elif args.command == "dbt-test":
            cmd_dbt(cfg, reference_date, run_id, "test")
        elif args.command == "publish":
            cmd_publish(cfg, reference_date, run_id)
        elif args.command == "run":
            cmd_ingest(cfg, reference_date, run_id)
            cmd_dbt(cfg, reference_date, run_id, "build")  # run + test together
            cmd_publish(cfg, reference_date, run_id)       # export consumable data products
    except Exception as exc:  # noqa: BLE001 - top-level boundary logs and exits non-zero
        log.error("pipeline.failed", extra={"json": {"run_id": run_id, "error": str(exc)}})
        return 1

    log.info("pipeline.complete", extra={"json": {"run_id": run_id, "reference_date": reference_date}})
    return 0


if __name__ == "__main__":
    sys.exit(main())
