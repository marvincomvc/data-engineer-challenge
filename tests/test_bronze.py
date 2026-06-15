from pipeline import bronze


def test_bronze_loads_real_counts(con, data_dir):
    bronze.load_bronze(con, data_dir)
    n = lambda t: con.sql(f"select count(*) from {t}").fetchone()[0]  # noqa: E731
    assert n("bronze_transactions") == 1060        # 742 + 318 via union_by_name
    assert n("bronze_results") == 837
    assert n("bronze_runs") == 19
    assert n("bronze_merchants") == 70
    assert n("bronze_settlement_raw") == 510


def test_bronze_preserves_cdc_and_drift_columns(con, data_dir):
    bronze.load_bronze(con, data_dir)
    cols = {r[0] for r in con.sql("describe bronze_transactions").fetchall()}
    assert "payment_method" in cols   # schema drift column (batch_2 only)
    assert "Op" in cols and "_timestamp" in cols
