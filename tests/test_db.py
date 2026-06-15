from pipeline import db


def test_connect_runs_query():
    con = db.connect()
    assert con.sql("select 42").fetchone()[0] == 42


def test_transaction_rolls_back_on_error():
    con = db.connect()
    con.execute("create table t(x int)")
    try:
        with db.transaction(con):
            con.execute("insert into t values (1)")
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert con.sql("select count(*) from t").fetchone()[0] == 0


def test_transaction_commits_on_success():
    con = db.connect()
    con.execute("create table t(x int)")
    with db.transaction(con):
        con.execute("insert into t values (1)")
    assert con.sql("select count(*) from t").fetchone()[0] == 1
