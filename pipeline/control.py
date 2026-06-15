"""Control / watermark table — the operational source of truth.

Every pipeline run writes a row here. Status is updated in the SAME transaction as
the data write (see runner), so the control table can never disagree with the facts.
This is the first artifact to inspect when troubleshooting (Part 3.2).
"""
from __future__ import annotations

from datetime import datetime, timezone


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_control_table(con) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS control_pipeline_run (
            run_id BIGINT, reference_date VARCHAR, source_name VARCHAR,
            status VARCHAR, rows_read BIGINT, rows_written BIGINT, rows_rejected BIGINT,
            started_at VARCHAR, finished_at VARCHAR, error_message VARCHAR
        )
    """)
    con.execute("CREATE SEQUENCE IF NOT EXISTS control_run_seq START 1")


def start_run(con, reference_date: str, source: str) -> int:
    rid = con.execute("SELECT nextval('control_run_seq')").fetchone()[0]
    con.execute(
        "INSERT INTO control_pipeline_run VALUES (?,?,?, 'RUNNING', NULL, NULL, NULL, ?, NULL, NULL)",
        [rid, reference_date, source, _now()],
    )
    return rid


def complete_run(con, run_id: int, read: int, written: int, rejected: int) -> None:
    con.execute(
        """UPDATE control_pipeline_run SET status='SUCCESS',
           rows_read=?, rows_written=?, rows_rejected=?, finished_at=? WHERE run_id=?""",
        [read, written, rejected, _now(), run_id],
    )


def fail_run(con, run_id: int, error: str) -> None:
    con.execute(
        "UPDATE control_pipeline_run SET status='FAILED', error_message=?, finished_at=? WHERE run_id=?",
        [error, _now(), run_id],
    )


def get_status(con) -> list[tuple]:
    return con.execute(
        """SELECT reference_date, status, rows_written, rows_rejected, finished_at
           FROM control_pipeline_run ORDER BY run_id DESC"""
    ).fetchall()
