from pipeline import cli, control, runner


def test_run_e2e_idempotent(con, data_dir):
    runner.run(con, "2025-03-16", data_dir)
    a = con.sql("select count(*) from fact_reconciliation_result").fetchone()[0]
    runner.run(con, "2025-03-16", data_dir)  # second run, same date
    b = con.sql("select count(*) from fact_reconciliation_result").fetchone()[0]
    assert a == b and a > 0                  # idempotent: no duplication
    assert control.get_status(con)[0][1] == "SUCCESS"


def test_run_records_failure_on_failed_upstream(con, data_dir):
    runner.run(con, "2025-03-22", data_dir)  # upstream run is FAILED for this date
    assert control.get_status(con)[0][1] == "FAILED"


def test_cli_run_smoke(data_dir):
    assert cli.main(["run", "--reference-date", "2025-03-16", "--data", data_dir, "--db", ":memory:"]) == 0
