"""Orchestrator: run the full bronze -> silver -> gold pipeline for a reference_date.

Reprocesses the sliding 7-day window, building each date's facts idempotently.
Every run is bracketed by a control-table row; a circuit-breaker failure is recorded
as FAILED (not crashed) so the failure is observable.
"""
from __future__ import annotations

from datetime import date, timedelta

from pipeline import bronze, control, gold, quality, silver
from pipeline.adapters.paysettler import PaySettlerAdapter
from pipeline.config import RECONCILIATION_WINDOW_DAYS


def _window(reference_date: str) -> list[str]:
    end = date.fromisoformat(reference_date)
    return [(end - timedelta(days=i)).isoformat() for i in range(RECONCILIATION_WINDOW_DAYS + 1)]


def run(con, reference_date: str, data_dir: str, full: bool = False) -> None:
    control.ensure_control_table(con)
    run_id = control.start_run(con, reference_date, "paysettler")
    try:
        bronze.load_bronze(con, data_dir)
        silver.build_silver(con, PaySettlerAdapter())
        quality.assert_circuit_breakers(con, reference_date)   # STOP tier
        rejected = quality.quarantine_settlements(con)         # QUARANTINE tier
        gold.build_dimensions(con)
        gold.init_facts(con)

        if full:
            dates = [
                r[0] for r in con.execute(
                    "SELECT DISTINCT reference_date FROM silver_runs WHERE status = 'COMPLETED'"
                ).fetchall()
            ]
        else:
            dates = _window(reference_date)

        for d in dates:
            completed = con.execute(
                "SELECT count(*) FROM silver_runs WHERE reference_date = ? AND status = 'COMPLETED'",
                [d],
            ).fetchone()[0]
            if completed:
                gold.build_facts(con, d)

        gold.build_marts(con)
        written = con.sql("SELECT count(*) FROM fact_reconciliation_result").fetchone()[0]
        control.complete_run(con, run_id, read=written, written=written, rejected=rejected)
    except Exception as exc:  # noqa: BLE001 - record the failure for observability, do not crash
        control.fail_run(con, run_id, str(exc))
