from pipeline import cli, control, runner


def test_run_e2e_idempotent(con, data_dir):
    runner.run(con, "2025-03-16", data_dir)
    a = con.sql("select count(*) from fact_reconciliation_result").fetchone()[0]
    runner.run(con, "2025-03-16", data_dir)  # second run, same date
    b = con.sql("select count(*) from fact_reconciliation_result").fetchone()[0]
    assert a == b and a > 0                  # idempotent: no duplication
    assert control.get_status(con)[0][1] == "SUCCESS"


def test_run_records_failure_on_failed_upstream(con, data_dir):
    runner.run(con, "2025-03-22", data_dir)  # upstream run is FAILED for this date -> STOP
    assert control.get_status(con)[0][1] == "FAILED"


def test_full_audit_trail_reconciles_to_kept_results(con, data_dir):
    runner.run(con, "2025-03-31", data_dir, full=True)
    audit = con.sql("select count(*) from fact_reconciliation_result").fetchone()[0]
    distinct_ids = con.sql("select count(distinct result_id) from fact_reconciliation_result").fetchone()[0]
    kept_results = con.sql("select count(*) from silver_results").fetchone()[0]
    # No silent loss: every kept result is in the audit trail, exactly once.
    assert audit == distinct_ids == kept_results > 0


def test_full_cfo_volume_single_settlement_date(con, data_dir):
    runner.run(con, "2025-03-31", data_dir, full=True)
    # The fix: settled volume is bucketed by settlement date, not fanned across run dates.
    assert con.sql("select count(distinct settlement_date) from fact_settlement_daily").fetchone()[0] == 1


def test_run_quarantines_orphans_and_nulls(con, data_dir):
    runner.run(con, "2025-03-16", data_dir)
    assert con.sql("select count(*) from quarantine_results").fetchone()[0] == 21   # 5 orphan-run + 16 null-merchant
    assert con.sql("select count(*) from quarantine_settlement").fetchone()[0] == 1  # orphan REVERSED


def test_cli_run_smoke(data_dir):
    assert cli.main(["run", "--reference-date", "2025-03-16", "--data", data_dir, "--db", ":memory:"]) == 0
