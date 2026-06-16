"""Camada bronze: ingestao crua de todas as fontes, do jeito que vieram.

As colunas de CDC (Op, _timestamp) ficam intactas — aqui nao limpamos nada.
Os transactions_batch_* sao empilhados com union_by_name pra tolerar a
diferenca de schema (payment_method so existe no batch_2); as colunas que
faltam viram NULL.
O CSV do PaySettler e lido tudo como VARCHAR pra que o adapter consiga
corrigir os valores sujos mais pra frente.
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
