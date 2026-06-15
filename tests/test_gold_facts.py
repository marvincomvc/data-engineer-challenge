from pipeline import gold


def test_audit_fact_grain_reconciles_to_results(prepared):
    con = prepared
    gold.build_reconciliation_facts(con, "2025-03-16")
    total = con.sql("select count(*) from fact_reconciliation_result").fetchone()[0]
    distinct_ids = con.sql("select count(distinct result_id) from fact_reconciliation_result").fetchone()[0]
    # Grain = one row per kept reconciliation_result for that run (no silent join loss, no fan-out).
    expected = con.sql("""
        select count(*) from silver_results r
        join silver_runs ru on ru.id = r.run_id
        where ru.reference_date = '2025-03-16'
    """).fetchone()[0]
    assert total == distinct_ids == expected and total > 0
    # null-merchant results were quarantined upstream, so no NULL merchant_key leaks in.
    assert con.sql("select count(*) from fact_reconciliation_result where merchant_key is null").fetchone()[0] == 0


def test_ops_daily_is_per_transaction(prepared):
    con = prepared
    gold.build_reconciliation_facts(con, "2025-03-16")
    # total_transactions must not exceed distinct (transaction_id, run) for the date.
    fact_total = con.sql("select coalesce(sum(total_transactions),0) from fact_reconciliation_daily").fetchone()[0]
    true_txns = con.sql("""
        select count(*) from (
            select distinct r.transaction_id, r.run_id
            from silver_results r join silver_runs ru on ru.id = r.run_id
            where ru.reference_date = '2025-03-16')
    """).fetchone()[0]
    assert fact_total == true_txns and fact_total > 0


def test_settlement_fact_no_cross_date_fanout(prepared):
    con = prepared
    gold.build_settlement_facts(con)
    # CSV covers a single settlement day -> exactly one settlement_date, no fan-out.
    assert con.sql("select count(distinct settlement_date) from fact_settlement_daily").fetchone()[0] == 1
    # Settled volume in the fact equals the settlement feed total (counted once, not per run).
    feed = con.sql("select sum(settled_amount) from silver_settlement_current where status='SETTLED'").fetchone()[0]
    fact = con.sql("select sum(total_settled_amount) from fact_settlement_daily").fetchone()[0]
    assert abs(float(feed) - float(fact)) < 0.01


def test_facts_idempotent_rerun(prepared):
    con = prepared
    gold.build_reconciliation_facts(con, "2025-03-16")
    gold.build_settlement_facts(con)
    a = (
        con.sql("select count(*) from fact_reconciliation_result").fetchone()[0],
        con.sql("select count(*) from fact_reconciliation_daily").fetchone()[0],
        con.sql("select count(*) from fact_settlement_daily").fetchone()[0],
    )
    gold.build_reconciliation_facts(con, "2025-03-16")
    gold.build_settlement_facts(con)
    b = (
        con.sql("select count(*) from fact_reconciliation_result").fetchone()[0],
        con.sql("select count(*) from fact_reconciliation_daily").fetchone()[0],
        con.sql("select count(*) from fact_settlement_daily").fetchone()[0],
    )
    assert a == b and a[0] > 0
