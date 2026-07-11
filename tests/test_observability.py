"""Observability integration: a blocked (error-severity) gate must still be
recorded in meta.dq_check_result, and the failing stage marked FAILED — the exact
evidence the troubleshooting runbook tells an operator to query."""
from __future__ import annotations

import duckdb

from conftest import REFERENCE_DATE, run_cli, run_pipeline


def test_failed_error_gate_is_recorded(controlled_warehouse):
    warehouse, data_dir = controlled_warehouse
    # 1. a normal run populates the warehouse
    run_pipeline(warehouse, data_dir, REFERENCE_DATE, command="run")

    # 2. break the fact grain so the blocking test assert_fct_grain_unique fails
    con = duckdb.connect(str(warehouse))
    con.execute(
        "insert into main.fct_reconciliation_result "
        "select * from main.fct_reconciliation_result limit 1"
    )
    con.close()

    # 3. dbt-test must now exit non-zero
    result = run_cli(
        warehouse, data_dir, ["dbt-test", "--reference-date", REFERENCE_DATE, "--run-id", "fail-1"],
        check=False,
    )
    assert result.returncode != 0, "blocking dbt test should fail the stage"

    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        # 4. the dbt stage is recorded FAILED
        statuses = [
            r[0] for r in con.execute(
                "select status from meta.pipeline_run "
                "where run_id = 'fail-1' and stage = 'dbt_test'"
            ).fetchall()
        ]
        assert "FAILED" in statuses

        # 5. the failed error-gate IS persisted (the whole point of N1)
        gate = con.execute(
            "select severity, passed, failure_count from meta.dq_check_result "
            "where run_id = 'fail-1' and check_name = 'assert_fct_grain_unique'"
        ).fetchone()
        assert gate is not None, "failed error-gate must be recorded even on non-zero exit"
        assert gate[0] == "error"
        assert gate[1] is False
        assert gate[2] >= 1
    finally:
        con.close()
