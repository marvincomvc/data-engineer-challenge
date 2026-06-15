from pipeline import bronze, gold, silver
from pipeline.adapters.paysettler import PaySettlerAdapter


def _silver(con, data_dir):
    bronze.load_bronze(con, data_dir)
    silver.build_silver(con, PaySettlerAdapter())


def test_dim_merchant_scd2(con, data_dir):
    _silver(con, data_dir)
    gold.build_dimensions(con)
    cols = {r[0] for r in con.sql("describe dim_merchant").fetchall()}
    assert {"merchant_key", "merchant_id", "valid_from", "valid_to", "is_current"} <= cols
    # 70 raw merchant rows (CDC: I=50, U=20) collapse to 50 distinct current merchants.
    assert con.sql("select count(*) from dim_merchant where is_current").fetchone()[0] == 50


def test_dim_date_covers_settlement_day(con, data_dir):
    _silver(con, data_dir)
    gold.build_dimensions(con)
    assert con.sql("select count(*) from dim_date").fetchone()[0] >= 28
    assert con.sql("select count(*) from dim_date where date_key = 20250316").fetchone()[0] == 1


def test_junk_only_existing_combos(con, data_dir):
    _silver(con, data_dir)
    gold.build_dimensions(con)
    n = con.sql("select count(*) from dim_transaction_profile").fetchone()[0]
    assert 1 <= n <= 24  # at most the cartesian product; only combinations that occur
