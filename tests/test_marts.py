from pipeline import gold


def test_marts_answer_consumer_questions(prepared):
    con = prepared
    gold.build_reconciliation_facts(con, "2025-03-16")
    gold.build_settlement_facts(con)
    gold.build_marts(con)

    # Operations: a discrepancy rate exists and is a valid proportion.
    rate = con.sql("select discrepancy_rate from mart_ops_reconciliation_health limit 1").fetchone()
    assert rate is not None and 0.0 <= rate[0] <= 1.0

    # CFO: settled volume is positive and avg_ticket is exposed.
    row = con.sql("select sum(total_settled_amount), sum(net_settled_amount) from mart_cfo_settled_volume").fetchone()
    assert row[0] > 0
    cfo_cols = {c[0] for c in con.sql("describe mart_cfo_settled_volume").fetchall()}
    assert {"settlement_date", "net_settled_amount", "avg_ticket"} <= cfo_cols

    # Compliance: per-transaction trail carries merchant identity (CNPJ) and the result key.
    comp_cols = {c[0] for c in con.sql("describe mart_compliance_audit_trail").fetchall()}
    assert {"result_id", "document", "transaction_id", "legal_name"} <= comp_cols
