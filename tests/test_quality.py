import duckdb
import pytest

from pipeline import quality


def _seed(con):
    con.execute("create table silver_runs(id bigint, reference_date varchar, status varchar, total_transactions bigint)")
    con.execute("create table silver_settlement(transaction_id varchar, merchant_id varchar, settled_amount decimal(18,2), status varchar, has_internal boolean)")
    con.execute("create table quarantine_settlement(transaction_id varchar, reason varchar)")


def test_stop_on_failed_upstream_run():
    con = duckdb.connect()
    _seed(con)
    con.execute("insert into silver_runs values (1,'2025-03-22','FAILED',100)")
    with pytest.raises(quality.DataQualityError):
        quality.assert_circuit_breakers(con, "2025-03-22")


def test_completed_run_passes():
    con = duckdb.connect()
    _seed(con)
    con.execute("insert into silver_runs values (1,'2025-03-16','COMPLETED',100)")
    quality.assert_circuit_breakers(con, "2025-03-16")  # no raise


def test_quarantine_orphan_reversed():
    con = duckdb.connect()
    _seed(con)
    con.execute("""insert into silver_settlement values
        ('t1','MERCH_001',10.0,'SETTLED',true),
        ('t2','MERCH_002',20.0,'REVERSED',false)""")
    moved = quality.quarantine_settlements(con)
    assert moved == 1
    assert con.sql("select reason from quarantine_settlement").fetchone()[0] == "ORPHAN_REVERSED"
    assert con.sql("select count(*) from silver_settlement").fetchone()[0] == 1  # orphan removed
