from pipeline import bronze, silver
from pipeline.adapters.paysettler import PaySettlerAdapter


def test_build_silver_entities(con, data_dir):
    bronze.load_bronze(con, data_dir)
    silver.build_silver(con, PaySettlerAdapter())
    n = lambda t: con.sql(f"select count(*) from {t}").fetchone()[0]  # noqa: E731
    assert n("silver_transactions") == 995          # CDC dedup, deletes excluded
    assert n("silver_settlement") == 510            # canonical feed (single day, every event)
    assert n("silver_runs") == 10                   # latest state per run id
    assert con.sql("select count(*) from silver_runs where status='FAILED'").fetchone()[0] == 1
    cols = {r[0] for r in con.sql("describe silver_settlement").fetchall()}
    assert {"settled_amount", "settled_at", "source_name", "has_internal"} <= cols


def test_settlement_current_dedupes_per_transaction(con, data_dir):
    bronze.load_bronze(con, data_dir)
    silver.build_silver(con, PaySettlerAdapter())
    silver.build_settlement_current(con)
    assert con.sql("select count(*) from silver_settlement_current").fetchone()[0] == 500
    # one row per transaction_id
    dup = con.sql("""select count(*) from (
        select transaction_id, count(*) c from silver_settlement_current group by 1 having c>1)""").fetchone()[0]
    assert dup == 0
