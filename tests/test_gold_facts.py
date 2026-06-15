from pipeline import bronze, gold, silver
from pipeline.adapters.paysettler import PaySettlerAdapter


def _prep(con, data_dir):
    bronze.load_bronze(con, data_dir)
    silver.build_silver(con, PaySettlerAdapter())
    gold.build_dimensions(con)
    gold.init_facts(con)


def test_fact_result_grain(con, data_dir):
    _prep(con, data_dir)
    gold.build_facts(con, "2025-03-16")
    # Grain = one row per reconciliation_result (result_id). The settlement join must
    # NOT fan out (settlement is deduped to one row per transaction_id upstream).
    total = con.sql("select count(*) from fact_reconciliation_result").fetchone()[0]
    distinct_ids = con.sql("select count(distinct result_id) from fact_reconciliation_result").fetchone()[0]
    expected = con.sql("""
        select count(*) from silver_results r
        join silver_runs ru on ru.id = r.run_id
        where ru.reference_date = '2025-03-16'
    """).fetchone()[0]
    assert total == distinct_ids == expected and total > 0


def test_facts_idempotent_rerun(con, data_dir):
    _prep(con, data_dir)
    gold.build_facts(con, "2025-03-16")
    a = con.sql("select count(*) from fact_reconciliation_result").fetchone()[0]
    b = con.sql("select count(*) from fact_settlement_daily").fetchone()[0]
    gold.build_facts(con, "2025-03-16")  # second run, same date
    a2 = con.sql("select count(*) from fact_reconciliation_result").fetchone()[0]
    b2 = con.sql("select count(*) from fact_settlement_daily").fetchone()[0]
    assert (a, b) == (a2, b2) and a > 0  # no duplication on rerun
