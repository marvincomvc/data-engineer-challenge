"""Idempotent writer — staging → promote pattern for DuckDB tables.

Strategy: write to a staging schema, validate, then atomically replace
the production table with DELETE+INSERT scoped to reference_date.
"""
from __future__ import annotations

import duckdb
from pipeline.observability.logging import get_logger

logger = get_logger(__name__)


def create_control_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS control")


def ensure_pipeline_runs_table(con: duckdb.DuckDBPyConnection) -> None:
    """Create control.pipeline_runs if not exists — immutable audit log."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS control.pipeline_runs (
            run_id           INTEGER PRIMARY KEY,
            reference_date   VARCHAR NOT NULL,
            status           VARCHAR NOT NULL DEFAULT 'IN_PROGRESS',
            started_at       TIMESTAMPTZ NOT NULL,
            completed_at     TIMESTAMPTZ,
            source_file_hash VARCHAR,
            total_processed  INTEGER DEFAULT 0,
            matched          INTEGER DEFAULT 0,
            mismatched       INTEGER DEFAULT 0,
            unreconciled_processor INTEGER DEFAULT 0,
            unreconciled_internal  INTEGER DEFAULT 0,
            hard_checks_failed     VARCHAR DEFAULT '',
            soft_checks_failed     VARCHAR DEFAULT ''
        )
    """)


def start_run(con: duckdb.DuckDBPyConnection, reference_date: str, source_file_hash: str = "") -> int:
    """Register a new pipeline run. Returns run_id."""
    ensure_pipeline_runs_table(con)

    # Check for existing runs on same date
    existing = con.execute(f"""
        SELECT run_id, status, started_at
        FROM control.pipeline_runs
        WHERE reference_date = '{reference_date}'
        ORDER BY run_id DESC
        LIMIT 1
    """).fetchall()

    if existing:
        run_id, status, started_at = existing[0]
        if status == "IN_PROGRESS":
            # Check if stale (>1h)
            from datetime import datetime, timezone
            start_dt = datetime.fromisoformat(str(started_at))
            elapsed_hours = (datetime.now(timezone.utc) - start_dt).total_seconds() / 3600
            if elapsed_hours < 1:
                logger.warning(f"Run {run_id} for {reference_date} is IN_PROGRESS (started {elapsed_hours:.1f}h ago). Use --force to re-run.")
                return run_id

    # Get next run_id
    max_id = con.execute("SELECT COALESCE(MAX(run_id), 0) FROM control.pipeline_runs").fetchone()[0]
    run_id = max_id + 1

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    con.execute(f"""
        INSERT INTO control.pipeline_runs (run_id, reference_date, status, started_at, source_file_hash)
        VALUES ({run_id}, '{reference_date}', 'IN_PROGRESS', '{now}'::TIMESTAMPTZ, '{source_file_hash}')
    """)

    logger.info(f"Started pipeline run {run_id} for {reference_date}")
    return run_id


def complete_run(con: duckdb.DuckDBPyConnection, run_id: int, metrics: dict) -> None:
    """Mark a run as COMPLETED with metrics."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    con.execute(f"""
        UPDATE control.pipeline_runs
        SET status = 'COMPLETED',
            completed_at = '{now}'::TIMESTAMPTZ,
            total_processed = {metrics.get('total_processed', 0)},
            matched = {metrics.get('matched', 0)},
            mismatched = {metrics.get('mismatched', 0)},
            unreconciled_processor = {metrics.get('unreconciled_processor', 0)},
            unreconciled_internal = {metrics.get('unreconciled_internal', 0)},
            hard_checks_failed = '{",".join(metrics.get("hard_checks_failed", []))}',
            soft_checks_failed = '{",".join(metrics.get("soft_checks_failed", []))}'
        WHERE run_id = {run_id}
    """)

    logger.info(f"Completed pipeline run {run_id}")


def fail_run(con: duckdb.DuckDBPyConnection, run_id: int, error: str = "") -> None:
    """Mark a run as FAILED."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    con.execute(f"""
        UPDATE control.pipeline_runs
        SET status = 'FAILED',
            completed_at = '{now}'::TIMESTAMPTZ
        WHERE run_id = {run_id}
    """)

    logger.error(f"Failed pipeline run {run_id}: {error}")
