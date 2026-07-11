"""Observability tables: pipeline_run and dq_check_result.

These two tables are what an on-call engineer queries first when the dashboards
go dark. `pipeline_run` answers "did it run, when, and did it finish?";
`dq_check_result` answers "did the data pass the gates, and by how much?".
"""
from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .logging import get_logger

log = get_logger("pipeline.registry")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_run_id() -> str:
    return uuid.uuid4().hex


@dataclass
class StageRecord:
    run_id: str
    reference_date: str
    stage: str
    status: str  # IN_PROGRESS | COMPLETED | FAILED
    rows_in: int | None = None
    rows_out: int | None = None
    started_at: str = ""
    finished_at: str | None = None
    error: str | None = None


DDL = """
CREATE TABLE IF NOT EXISTS meta.pipeline_run (
    run_id          VARCHAR,
    reference_date  DATE,
    stage           VARCHAR,
    status          VARCHAR,
    rows_in         BIGINT,
    rows_out        BIGINT,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    error           VARCHAR
);
CREATE TABLE IF NOT EXISTS meta.dq_check_result (
    run_id          VARCHAR,
    reference_date  DATE,
    check_name      VARCHAR,
    severity        VARCHAR,   -- error | warn
    passed          BOOLEAN,
    observed_value  DOUBLE,
    threshold       DOUBLE,
    checked_at      TIMESTAMPTZ
);
"""


def init(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL)


def record_stage(con: duckdb.DuckDBPyConnection, rec: StageRecord) -> None:
    con.execute(
        """INSERT INTO meta.pipeline_run
           (run_id, reference_date, stage, status, rows_in, rows_out, started_at, finished_at, error)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            rec.run_id,
            rec.reference_date,
            rec.stage,
            rec.status,
            rec.rows_in,
            rec.rows_out,
            rec.started_at or _now(),
            rec.finished_at,
            rec.error,
        ],
    )
    log.info("stage.recorded", extra={"json": asdict(rec)})


def load_dq_results(
    duckdb_path: Path, run_id: str, reference_date: str, run_results_path: Path
) -> int:
    """Parse dbt's run_results.json and load test outcomes into dq_check_result.

    This is what makes the DQ observability surface real: after `dbt test`/`build`,
    every test's pass/warn/error + failing-row count lands in a queryable table,
    not just dbt's logs. Returns the number of checks recorded.
    """
    path = Path(run_results_path)
    if not path.exists():
        log.info("dq.no_run_results", extra={"json": {"path": str(path)}})
        return 0

    results = json.loads(path.read_text()).get("results", [])
    rows = []
    for r in results:
        uid = r.get("unique_id", "")
        if not uid.startswith("test."):
            continue
        status = r.get("status", "")
        failures = r.get("failures")
        severity = "warn" if status == "warn" else "error" if status in ("fail", "error") else "info"
        rows.append(
            (
                run_id,
                reference_date,
                uid.split(".")[-1],
                severity,
                status == "pass",
                float(failures) if failures is not None else None,
            )
        )
    if not rows:
        return 0

    from .db import connect  # local import to avoid cycle

    with connect(duckdb_path) as con:
        init(con)
        con.executemany(
            """INSERT INTO meta.dq_check_result
               (run_id, reference_date, check_name, severity, passed, observed_value, threshold, checked_at)
               VALUES (?, ?, ?, ?, ?, ?, NULL, now())""",
            rows,
        )
    log.info("dq.loaded", extra={"json": {"run_id": run_id, "checks": len(rows)}})
    return len(rows)


class record_stage_standalone:  # noqa: N801 - context manager verb
    """Record a stage using short-lived connections that are NOT held open.

    Required around subprocess stages (e.g. invoking dbt): DuckDB is
    single-writer, so the parent must not hold the warehouse file while the
    child process opens it. We write IN_PROGRESS, release the lock, run the
    work, then reopen to write COMPLETED/FAILED.
    """

    def __init__(self, duckdb_path, run_id: str, reference_date: str, stage: str):
        from .db import connect  # local import to avoid cycle

        self._connect = connect
        self._path = duckdb_path
        self.rec = StageRecord(
            run_id=run_id,
            reference_date=reference_date,
            stage=stage,
            status="IN_PROGRESS",
            started_at=_now(),
        )

    def __enter__(self) -> StageRecord:
        with self._connect(self._path) as con:
            init(con)
            record_stage(con, self.rec)
        return self.rec

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.rec.finished_at = _now()
        if exc_type is not None:
            self.rec.status = "FAILED"
            self.rec.error = f"{exc_type.__name__}: {exc}"
        else:
            self.rec.status = "COMPLETED"
        with self._connect(self._path) as con:
            record_stage(con, self.rec)
        return False


class stage_span:  # noqa: N801 - used as a context manager verb
    """Context manager that records a stage as IN_PROGRESS then COMPLETED/FAILED.

    Usage:
        with stage_span(con, run_id, ref_date, "ingest") as span:
            ...
            span.rows_out = n
    """

    def __init__(self, con, run_id: str, reference_date: str, stage: str):
        self.con = con
        self.rec = StageRecord(
            run_id=run_id,
            reference_date=reference_date,
            stage=stage,
            status="IN_PROGRESS",
            started_at=_now(),
        )

    def __enter__(self) -> StageRecord:
        record_stage(self.con, self.rec)
        return self.rec

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.rec.finished_at = _now()
        if exc_type is not None:
            self.rec.status = "FAILED"
            self.rec.error = f"{exc_type.__name__}: {exc}"
        else:
            self.rec.status = "COMPLETED"
        record_stage(self.con, self.rec)
        return False  # never suppress the exception
