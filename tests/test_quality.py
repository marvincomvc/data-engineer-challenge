import duckdb
import pytest

from pipeline import quality


def _seed(con):
    con.execute("create table silver_runs(id bigint, reference_date varchar, status varchar)")
    con.execute("create table silver_results(id bigint, run_id bigint, transaction_id varchar, merchant_id varchar, category varchar)")
    con.execute("create table silver_settlement(transaction_id varchar, status varchar, has_internal boolean)")
    quality.init_quarantine(con)


def test_stop_on_failed_upstream_run():
    con = duckdb.connect()
    _seed(con)
    con.execute("insert into silver_runs values (1,'2025-03-22','FAILED')")
    with pytest.raises(quality.DataQualityError):
        quality.assert_circuit_breakers(con, "2025-03-22")


def test_completed_run_passes():
    con = duckdb.connect()
    _seed(con)
    con.execute("insert into silver_runs values (1,'2025-03-16','COMPLETED')")
    quality.assert_circuit_breakers(con, "2025-03-16")  # nao deve estourar


def test_quarantine_orphan_reversed_settlement():
    con = duckdb.connect()
    _seed(con)
    con.execute("insert into silver_settlement values ('t1','SETTLED',true),('t2','REVERSED',false)")
    assert quality.quarantine_settlements(con) == 1
    assert con.sql("select reason from quarantine_settlement").fetchone()[0] == "ORPHAN_REVERSED"
    assert con.sql("select count(*) from silver_settlement").fetchone()[0] == 1


def test_quarantine_results_orphan_run_and_null_merchant():
    con = duckdb.connect()
    _seed(con)
    con.execute("insert into silver_runs values (1,'2025-03-16','COMPLETED')")
    con.execute("""insert into silver_results values
        (10, 1,  't1', 'MERCH_1', 'MATCHED'),
        (11, 99, 't2', 'MERCH_2', 'MATCHED'),
        (12, 1,  't3', NULL,      'MATCHED')""")
    moved = quality.quarantine_results(con)
    assert moved == 2                                       # run orfa + merchant nulo
    assert con.sql("select count(*) from silver_results").fetchone()[0] == 1
    reasons = {r[0] for r in con.sql("select reason from quarantine_results").fetchall()}
    assert reasons == {"ORPHAN_RUN", "NULL_MERCHANT"}
