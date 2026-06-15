"""Bronze layer: raw ingestion of every source, as-is.

CDC columns (Op, _timestamp) are preserved untouched — nothing is cleaned here.
transactions_batch_* are stacked with union_by_name so the schema drift
(payment_method only in batch_2) is tolerated; missing columns become NULL.
The PaySettler CSV is read all-VARCHAR so the adapter can fix dirty values later.
"""
from __future__ import annotations


def load_bronze(con, data_dir: str) -> None:
    d = data_dir.replace("\\", "/")
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze_transactions AS
        SELECT * FROM read_parquet(
            ['{d}/transactions_batch_1.parquet', '{d}/transactions_batch_2.parquet'],
            union_by_name=true)
    """)
    con.execute(
        f"CREATE OR REPLACE TABLE bronze_results AS "
        f"SELECT * FROM read_parquet('{d}/reconciliation_results.parquet')"
    )
    con.execute(
        f"CREATE OR REPLACE TABLE bronze_runs AS "
        f"SELECT * FROM read_parquet('{d}/reconciliation_runs.parquet')"
    )
    con.execute(
        f"CREATE OR REPLACE TABLE bronze_merchants AS "
        f"SELECT * FROM read_parquet('{d}/enterprise_company.parquet')"
    )
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze_settlement_raw AS
        SELECT * FROM read_csv('{d}/settlement_paysettler.csv', all_varchar=true, header=true)
    """)
