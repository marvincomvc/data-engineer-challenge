from pipeline import gold


def test_marts_answer_consumer_questions(prepared):
    con = prepared
    gold.build_reconciliation_facts(con, "2025-03-16")
    gold.build_settlement_facts(con)
    gold.build_marts(con)

    # Operacoes: existe uma taxa de divergencia e ela e uma proporcao valida.
    rate = con.sql("select discrepancy_rate from mart_ops_reconciliation_health limit 1").fetchone()
    assert rate is not None and 0.0 <= rate[0] <= 1.0

    # CFO: o volume liquidado e positivo e o avg_ticket esta exposto.
    row = con.sql("select sum(total_settled_amount), sum(net_settled_amount) from mart_cfo_settled_volume").fetchone()
    assert row[0] > 0
    cfo_cols = {c[0] for c in con.sql("describe mart_cfo_settled_volume").fetchall()}
    assert {"settlement_date", "net_settled_amount", "avg_ticket"} <= cfo_cols

    # Compliance: a trilha por transacao carrega a identidade do lojista (CNPJ) e a chave do resultado.
    comp_cols = {c[0] for c in con.sql("describe mart_compliance_audit_trail").fetchall()}
    assert {"result_id", "document", "transaction_id", "legal_name"} <= comp_cols
