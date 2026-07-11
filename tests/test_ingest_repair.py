"""Bronze ingest + staging repair: schema drift, CDC, dirty amounts, lineage."""
from __future__ import annotations

import duckdb

from conftest import REFERENCE_DATE, run_pipeline


def _con(warehouse):
    return duckdb.connect(str(warehouse), read_only=True)


def test_schema_drift_absorbed(controlled_warehouse):
    # batch_1 lacks payment_method, batch_2 has it: both must load, column nullable.
    warehouse, data_dir = controlled_warehouse
    run_pipeline(warehouse, data_dir, REFERENCE_DATE, command="run")
    con = _con(warehouse)
    try:
        cols = [c[0] for c in con.execute("describe main.stg_transactions").fetchall()]
        assert "payment_method" in cols
        # the drift row has a value, batch_1 rows are null
        n_null = con.execute(
            "select count(*) from main.stg_transactions where payment_method is null"
        ).fetchone()[0]
        assert n_null > 0
    finally:
        con.close()


def test_ptbr_and_comma_amounts_repaired(controlled_warehouse):
    # "R$ 75,00" -> 75.00 and "100,02" -> 100.02, zero unparseable -> gate green.
    warehouse, data_dir = controlled_warehouse
    run_pipeline(warehouse, data_dir, REFERENCE_DATE, command="run")
    con = _con(warehouse)
    try:
        nulls = con.execute(
            "select count(*) from main.stg_paysettler where amount is null"
        ).fetchone()[0]
        assert nulls == 0
        # pt-BR "R$ 75,00" -> 75.00
        val = con.execute(
            "select amount from main.stg_paysettler where transaction_id = 't5-processor'"
        ).fetchone()[0]
        assert float(val) == 75.00
        # comma-decimal "100,02" -> 100.02 (assert the value, not just non-null)
        val3 = con.execute(
            "select amount from main.stg_paysettler where transaction_id = 't3-mismatch'"
        ).fetchone()[0]
        assert float(val3) == 100.02
    finally:
        con.close()


def test_lineage_columns_present(controlled_warehouse):
    warehouse, data_dir = controlled_warehouse
    run_pipeline(warehouse, data_dir, REFERENCE_DATE, command="run")
    con = _con(warehouse)
    try:
        row = con.execute(
            "select _source_file, _run_id from bronze.raw_transactions limit 1"
        ).fetchone()
        assert row[0] and row[1]
    finally:
        con.close()
