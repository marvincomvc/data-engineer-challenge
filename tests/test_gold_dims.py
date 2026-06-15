from pipeline import gold


def test_dim_merchant_scd2(prepared):
    con = prepared
    cols = {r[0] for r in con.sql("describe dim_merchant").fetchall()}
    assert {"merchant_key", "merchant_id", "valid_from", "valid_to", "is_current"} <= cols
    # 70 raw merchant rows (CDC: I=50, U=20) collapse to 50 distinct current merchants.
    assert con.sql("select count(*) from dim_merchant where is_current").fetchone()[0] == 50


def test_merchant_keys_are_deterministic(prepared):
    con = prepared
    before = dict(con.sql("select merchant_id, merchant_key from dim_merchant").fetchall())
    gold.build_dimensions(con)  # rebuild
    after = dict(con.sql("select merchant_id, merchant_key from dim_merchant").fetchall())
    assert before == after  # same surrogate per merchant across rebuilds (no drift)


def test_dim_date_covers_settlement_day(prepared):
    con = prepared
    assert con.sql("select count(*) from dim_date where date_key = 20250316").fetchone()[0] == 1
    assert con.sql("select count(*) from dim_date").fetchone()[0] >= 28


def test_junk_only_existing_combos(prepared):
    con = prepared
    n = con.sql("select count(*) from dim_transaction_profile").fetchone()[0]
    assert 1 <= n <= 24  # at most the cartesian product; only combinations that occur
