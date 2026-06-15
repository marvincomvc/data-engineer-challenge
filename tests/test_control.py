import duckdb

from pipeline import control


def test_run_lifecycle():
    con = duckdb.connect()
    control.ensure_control_table(con)
    rid = control.start_run(con, "2025-03-16", "paysettler")
    control.complete_run(con, rid, read=510, written=480, rejected=30)
    row = con.execute(
        "select status, rows_written, rows_rejected from control_pipeline_run where run_id=?",
        [rid],
    ).fetchone()
    assert row == ("SUCCESS", 480, 30)


def test_fail_run_records_error():
    con = duckdb.connect()
    control.ensure_control_table(con)
    rid = control.start_run(con, "2025-03-22", "paysettler")
    control.fail_run(con, rid, "run upstream FAILED")
    status = con.execute(
        "select status, error_message from control_pipeline_run where run_id=?", [rid]
    ).fetchone()
    assert status == ("FAILED", "run upstream FAILED")
