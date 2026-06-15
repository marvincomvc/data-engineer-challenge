import duckdb

from pipeline import bronze
from pipeline.silver import dedup_latest


def test_dedup_picks_latest_and_drops_delete():
    con = duckdb.connect()
    con.execute("""
        create table src(id bigint, transaction_id varchar, amount double, Op varchar, _timestamp varchar);
        insert into src values
          (1,'a',10,'I','2025-03-01T00:00:00+00:00'),
          (2,'a',20,'U','2025-03-02T00:00:00+00:00'),
          (3,'b',30,'I','2025-03-01T00:00:00+00:00'),
          (4,'b',30,'D','2025-03-03T00:00:00+00:00');
    """)
    out = {
        r[0]: r[1]
        for r in con.sql(f"select transaction_id, amount from ({dedup_latest('src','transaction_id')}) d").fetchall()
    }
    assert out == {"a": 20.0}   # 'a' -> latest U(20); 'b' -> latest D -> dropped


def test_dedup_real_active_count(con, data_dir):
    bronze.load_bronze(con, data_dir)
    n = con.sql(
        f"select count(*) from ({dedup_latest('bronze_transactions','transaction_id')}) d"
    ).fetchone()[0]
    assert n == 995   # 947 latest-I + 48 latest-U; 5 latest-D excluded
