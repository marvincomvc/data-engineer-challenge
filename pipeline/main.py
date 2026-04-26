"""Settlement reconciliation analytical pipeline — entry point.

Processes CDC parquets + PaySettler CSV for one reference_date and
populates three analytical marts in the DuckDB warehouse.

Usage:
    python -m pipeline.main --reference-date 2025-03-04
    python -m pipeline.main --reference-date 2025-03-04 --data-path /tmp/data
    python -m pipeline.main --reference-date 2025-03-04 --db-path /tmp/test.duckdb
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import Config
from pipeline.db import get_connection
from pipeline.ingest.merchants import ingest_merchants
from pipeline.ingest.reconciliation import (
    ingest_reconciliation_results,
    ingest_reconciliation_runs,
)
from pipeline.ingest.settlement import ingest_settlement
from pipeline.ingest.transactions import ingest_transactions
from pipeline.marts.cfo import build_mart_cfo
from pipeline.marts.compliance import build_mart_compliance
from pipeline.marts.operations import build_mart_operations
from pipeline.observability.logger import JsonFormatter, get_logger, timed_step
from pipeline.quality.checks import QualityError, run_stop_checks, run_warn_checks
from pipeline.transform.silver import build_silver

logger = get_logger("pipeline.main")


def _configure_logging(log_level: str) -> None:
    """Set up the root logger with JSON output so all module loggers inherit it."""
    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        root.addHandler(handler)


def run(
    reference_date: str,
    data_path: Path,
    db_path: Path,
) -> None:
    """Execute the full pipeline for one reference_date.

    Sequence:
        1. Bootstrap DB schema
        2. Record pipeline start in gold.pipeline_runs
        3. Ingest all sources → bronze
        4. STOP quality checks (aborts on failure)
        5. Transform bronze → silver
        6. WARN quality checks (logs, never aborts)
        7. Build gold marts (operations, cfo, compliance)
        8. Mark pipeline as COMPLETED
    """
    run_id = str(uuid.uuid4())
    started_at = datetime.now(tz=timezone.utc)
    counts: dict[str, int] = {}

    logger.info(
        "pipeline starting",
        extra={"step": "init", "reference_date": reference_date},
    )

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection(db_path)

    conn.execute(
        """
        INSERT INTO gold.pipeline_runs (run_id, reference_date, status, started_at)
        VALUES (?, ?::DATE, 'RUNNING', current_timestamp)
        """,
        [run_id, reference_date],
    )

    try:
        with timed_step(logger, "ingest.transactions", reference_date):
            counts["transactions"] = ingest_transactions(conn, data_path)

        with timed_step(logger, "ingest.reconciliation_runs", reference_date):
            counts["reconciliation_runs"] = ingest_reconciliation_runs(conn, data_path)

        with timed_step(logger, "ingest.reconciliation_results", reference_date):
            counts["reconciliation_results"] = ingest_reconciliation_results(conn, data_path)

        with timed_step(logger, "ingest.merchants", reference_date):
            counts["merchants"] = ingest_merchants(conn, data_path)

        with timed_step(logger, "ingest.settlement", reference_date):
            counts["settlement"], settlement_source_file = ingest_settlement(
                conn, data_path, reference_date
            )

        with timed_step(logger, "quality.stop_checks", reference_date):
            run_stop_checks(conn, settlement_source_file)

        with timed_step(logger, "transform.silver", reference_date):
            silver_counts = build_silver(conn, reference_date, settlement_source_file)
            counts.update({f"silver_{k}": v for k, v in silver_counts.items()})

        with timed_step(logger, "quality.warn_checks", reference_date):
            run_warn_checks(conn)

        with timed_step(logger, "marts.operations", reference_date):
            counts["mart_operations"] = build_mart_operations(conn, reference_date, run_id)

        with timed_step(logger, "marts.cfo", reference_date):
            counts["mart_cfo"] = build_mart_cfo(conn, reference_date, run_id)

        with timed_step(logger, "marts.compliance", reference_date):
            counts["mart_compliance"] = build_mart_compliance(conn, reference_date, run_id)

        duration = int((datetime.now(tz=timezone.utc) - started_at).total_seconds())
        conn.execute(
            """
            UPDATE gold.pipeline_runs
            SET status = 'COMPLETED',
                completed_at = current_timestamp,
                duration_seconds = ?,
                counts = ?
            WHERE run_id = ?
            """,
            [duration, json.dumps(counts), run_id],
        )
        logger.info(
            "pipeline completed",
            extra={
                "step": "done",
                "reference_date": reference_date,
                "duration_s": duration,
                "counts": counts,
            },
        )

    except QualityError as exc:
        _mark_failed(conn, run_id, str(exc))
        logger.error(
            "pipeline aborted: STOP quality check failed",
            extra={"step": "quality.stop_checks", "reference_date": reference_date},
        )
        sys.exit(1)

    except Exception as exc:
        _mark_failed(conn, run_id, repr(exc))
        logger.exception(
            "pipeline failed with unexpected error",
            extra={"step": "error", "reference_date": reference_date},
        )
        sys.exit(1)

    finally:
        conn.close()


def _mark_failed(conn, run_id: str, error_message: str) -> None:
    try:
        conn.execute(
            """
            UPDATE gold.pipeline_runs
            SET status = 'FAILED',
                completed_at = current_timestamp,
                error_message = ?
            WHERE run_id = ?
            """,
            [error_message[:2000], run_id],
        )
    except Exception:
        pass  # best-effort; don't mask the original exception


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run the settlement reconciliation analytical pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--reference-date",
        required=True,
        metavar="YYYY-MM-DD",
        help="Date to process. Determines which settlement CSV to load.",
    )
    p.add_argument(
        "--data-path",
        metavar="PATH",
        help="Override DATA_PATH env var. Directory containing source parquet/CSV files.",
    )
    p.add_argument(
        "--db-path",
        metavar="PATH",
        help="Override DB_PATH env var. Path to DuckDB warehouse file.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    cfg = Config.from_env()

    _configure_logging(cfg.log_level)

    data_path = Path(args.data_path) if args.data_path else cfg.data_path
    db_path = Path(args.db_path) if args.db_path else cfg.db_path

    run(
        reference_date=args.reference_date,
        data_path=data_path,
        db_path=db_path,
    )


if __name__ == "__main__":
    main()
