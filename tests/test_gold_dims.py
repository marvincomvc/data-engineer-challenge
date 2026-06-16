from pipeline import gold


def test_dim_merchant_scd2(prepared):
    con = prepared
    cols = {r[0] for r in con.sql("describe dim_merchant").fetchall()}
    assert {"merchant_key", "merchant_id", "valid_from", "valid_to", "is_current"} <= cols
    # As 70 linhas brutas de merchant (CDC: I=50, U=20) se reduzem a 50 merchants atuais distintos.
    assert con.sql("select count(*) from dim_merchant where is_current").fetchone()[0] == 50


def test_merchant_keys_are_deterministic(prepared):
    con = prepared
    before = dict(con.sql("select merchant_id, merchant_key from dim_merchant").fetchall())
    gold.build_dimensions(con)  # reconstrói
    after = dict(con.sql("select merchant_id, merchant_key from dim_merchant").fetchall())
    assert before == after  # mesma chave substituta por merchant entre reconstruções (sem drift)


def test_dim_date_covers_settlement_day(prepared):
    con = prepared
    assert con.sql("select count(*) from dim_date where date_key = 20250316").fetchone()[0] == 1
    assert con.sql("select count(*) from dim_date").fetchone()[0] >= 28


def test_junk_only_existing_combos(prepared):
    con = prepared
    n = con.sql("select count(*) from dim_transaction_profile").fetchone()[0]
    assert 1 <= n <= 24  # no máximo o produto cartesiano; só as combinações que de fato aparecem
