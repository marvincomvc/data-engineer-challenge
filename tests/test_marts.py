from pipeline import bronze, gold, silver
from pipeline.adapters.paysettler import PaySettlerAdapter


def test_marts_answer_consumer_questions(con, data_dir):
    bronze.load_bronze(con, data_dir)
    silver.build_silver(con, PaySettlerAdapter())
    gold.build_dimensions(con)
    gold.init_facts(con)
    gold.build_facts(con, "2025-03-16")
    gold.build_marts(con)

    # Operations: a discrepancy rate exists and is a valid proportion.
    rate = con.sql("select discrepancy_rate from mart_ops_reconciliation_health limit 1").fetchone()
    assert rate is not None and 0.0 <= rate[0] <= 1.0

    # CFO: settled volume is positive.
    assert con.sql("select sum(total_settled_amount) from mart_cfo_settled_volume").fetchone()[0] > 0

    # Compliance: per-transaction trail carries merchant identity (CNPJ).
    cols = {c[0] for c in con.sql("describe mart_compliance_audit_trail").fetchall()}
    assert {"document", "transaction_id", "legal_name"} <= cols
