"""Orchestrator: run the full bronze -> silver -> gold pipeline for a reference_date.

Order matters: quarantine runs BEFORE silver_settlement_current is derived, so the
analytics tables never see quarantined rows. The requested date's run must be COMPLETED
(STOP otherwise); FAILED runs that merely fall inside the 7-day lookback are skipped and
reported (not silently ignored).
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
        quality.init_quarantine(con)
        quality.assert_circuit_breakers(con, reference_date)        # STOP on the requested date
        rejected = quality.quarantine_settlements(con) + quality.quarantine_results(con)
        silver.build_settlement_current(con)                        # AFTER quarantine
        gold.build_dimensions(con)
        gold.init_facts(con)

        if full:
            for tbl in ("fact_reconciliation_result", "fact_reconciliation_daily", "fact_settlement_daily"):
                con.execute(f"DELETE FROM {tbl}")
            dates = [
                r[0] for r in con.execute(
                    "SELECT DISTINCT reference_date FROM silver_runs WHERE status = 'COMPLETED'"
                ).fetchall()
            ]
        else:
            dates = _window(reference_date)

        skipped: list[str] = []
        for d in dates:
            status = con.execute(
                "SELECT status FROM silver_runs WHERE reference_date = ?", [d]
            ).fetchone()
            if status and status[0] == "COMPLETED":
                gold.build_reconciliation_facts(con, d)
            elif status:  # a run exists for this date but is not COMPLETED
                skipped.append(f"{d}({status[0]})")

        gold.build_settlement_facts(con)
        gold.build_marts(con)

        written = con.sql("SELECT count(*) FROM fact_reconciliation_result").fetchone()[0]
        control.complete_run(con, run_id, read=written, written=written, rejected=rejected)
        if skipped:
            print(f"[pipeline] WARNING skipped non-COMPLETED runs in window: {', '.join(skipped)}")
    except Exception as exc:  # noqa: BLE001 - record the failure for observability, do not crash
        control.fail_run(con, run_id, str(exc))
