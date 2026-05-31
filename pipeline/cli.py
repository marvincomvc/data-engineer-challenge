"""CLI entrypoint for the Settlement Reconciliation pipeline.

Usage:
    python -m pipeline.cli run --reference-date 2025-03-15
    python -m pipeline.cli run --reference-date 2025-03-15 --force
    python -m pipeline.cli status --reference-date 2025-03-15
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from pipeline.config import PipelineConfig
from pipeline.observability.logging import get_logger, log_with_context
from pipeline.observability.metrics import RunMetrics
from pipeline.quality.checks import run_all_checks
from pipeline.transform.bronze import run_bronze
from pipeline.transform.silver import run_silver
from pipeline.transform.gold import run_gold
from pipeline.load.writer import (
    create_control_schema,
    ensure_pipeline_runs_table,
    start_run,
    complete_run,
    fail_run,
)


def _file_hash(path: Path) -> str:
    if not path.exists():
        return ""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()[:16]


def run_pipeline(reference_date: str, force: bool = False, cfg: PipelineConfig | None = None) -> int:
    """Execute the full pipeline for a given reference date.

    Returns 0 on success, 1 on failure.
    """
    if cfg is None:
        cfg = PipelineConfig()

    logger = get_logger("pipeline", cfg.log_level)
    con = duckdb.connect(str(cfg.warehouse_path))
    metrics = RunMetrics(reference_date=reference_date)

    try:
        # Initialize control schema and table
        create_control_schema(con)
        ensure_pipeline_runs_table(con)

        # Compute source file hash for idempotency check
        source_hash = _file_hash(cfg.paysettler_csv)

        # Check for conflicting runs
        existing = con.execute(f"""
            SELECT run_id, status, started_at
            FROM control.pipeline_runs
            WHERE reference_date = '{reference_date}'
              AND status = 'IN_PROGRESS'
            ORDER BY run_id DESC LIMIT 1
        """).fetchall()

        if existing and not force:
            run_id, status, started_at = existing[0]
            start_dt = datetime.fromisoformat(str(started_at))
            elapsed_hours = (datetime.now(timezone.utc) - start_dt).total_seconds() / 3600
            if elapsed_hours < cfg.stale_run_hours:
                logger.error(f"Run {run_id} for {reference_date} is IN_PROGRESS (started {elapsed_hours:.1f}h ago). Use --force.")
                return 1

        # Start run
        run_id = start_run(con, reference_date, source_hash)
        metrics.run_id = run_id
        metrics.started_at = datetime.now(timezone.utc).isoformat()

        # Bronze layer
        log_with_context(logger, 20, "Starting bronze layer", phase="bronze", reference_date=reference_date)
        b1_cols, b2_cols, csv_row_count = run_bronze(con, cfg, reference_date)

        # Silver layer
        log_with_context(logger, 20, "Starting silver layer", phase="silver", reference_date=reference_date)
        run_silver(con, cfg, reference_date, metrics)

        # Quality checks (after silver — where HARD checks are applied)
        log_with_context(logger, 20, "Running quality checks", phase="quality", reference_date=reference_date)
        check_results = run_all_checks(con, csv_row_count, b1_cols, b2_cols, reference_date=reference_date)

        for cr in check_results:
            if cr.passed:
                logger.info(str(cr))
            elif cr.check_type == "HARD":
                logger.error(str(cr))
                metrics.hard_checks_failed.append(cr.name)
            else:
                logger.warning(str(cr))
                metrics.soft_checks_failed.append(cr.name)

        # If HARD checks failed, abort
        if metrics.hard_checks_failed:
            fail_run(con, run_id, f"HARD checks failed: {metrics.hard_checks_failed}")
            logger.error("Pipeline aborted due to HARD check failures")
            return 1

        # Gold layer
        log_with_context(logger, 20, "Starting gold layer", phase="gold", reference_date=reference_date)
        run_gold(con)

        # Complete run
        metrics.status = "COMPLETED"
        metrics.completed_at = datetime.now(timezone.utc).isoformat()
        complete_run(con, run_id, metrics.to_dict())

        log_with_context(logger, 20, "Pipeline completed", **metrics.to_dict())
        return 0

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        if metrics.run_id:
            fail_run(con, metrics.run_id, str(e))
        return 1

    finally:
        con.close()


def show_status(reference_date: str, cfg: PipelineConfig | None = None) -> int:
    """Show the status of pipeline runs for a reference date."""
    if cfg is None:
        cfg = PipelineConfig()

    con = duckdb.connect(str(cfg.warehouse_path))
    try:
        create_control_schema(con)
        ensure_pipeline_runs_table(con)
        rows = con.execute(f"""
            SELECT run_id, status, started_at, completed_at,
                   total_processed, matched, mismatched,
                   unreconciled_processor, unreconciled_internal,
                   hard_checks_failed, soft_checks_failed
            FROM control.pipeline_runs
            WHERE reference_date = '{reference_date}'
            ORDER BY run_id DESC
        """).fetchall()

        if not rows:
            print(f"No pipeline runs found for {reference_date}")
            return 0

        for row in rows:
            run_id, status, started_at, completed_at, total, matched, mismatched, ur_proc, ur_int, hard, soft = row
            print(f"Run {run_id}: {status}")
            print(f"  Started:    {started_at}")
            print(f"  Completed:  {completed_at or 'N/A'}")
            print(f"  Total:      {total}")
            print(f"  Matched:    {matched}")
            print(f"  Mismatched: {mismatched}")
            print(f"  Unrec Proc: {ur_proc}")
            print(f"  Unrec Int:  {ur_int}")
            if hard:
                print(f"  HARD fails: {hard}")
            if soft:
                print(f"  SOFT fails: {soft}")
            print()

        return 0

    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Settlement Reconciliation Pipeline")
    sub = parser.add_subparsers(dest="command")

    run_parser = sub.add_parser("run", help="Execute pipeline for a reference date")
    run_parser.add_argument("--reference-date", required=True, help="YYYY-MM-DD")
    run_parser.add_argument("--force", action="store_true", help="Force re-run even if IN_PROGRESS")

    status_parser = sub.add_parser("status", help="Check pipeline run status")
    status_parser.add_argument("--reference-date", required=True, help="YYYY-MM-DD")

    args = parser.parse_args()

    if args.command == "run":
        sys.exit(run_pipeline(args.reference_date, force=args.force))
    elif args.command == "status":
        sys.exit(show_status(args.reference_date))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
