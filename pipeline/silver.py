"""Camada silver: dedup de CDC + padronização para entidades limpas e canônicas."""
from __future__ import annotations

from pipeline.adapters.base import SettlementAdapter


def dedup_latest(relation: str, key: str, ts: str = "_timestamp", idcol: str = "id") -> str:
    """SQL que mantém a versão mais recente do CDC por `key` e descarta as linhas deletadas.

    Pega a linha com o `_timestamp` mais recente por chave natural e depois exclui
    as linhas cuja última operação foi um delete. O `id` NÃO é único na união (um
    artefato do replay do CDC), então adicionamos o `rowid` físico como critério final
    de desempate pra garantir um resultado estável e reproduzível. A checagem de delete
    é NULL-safe, então uma linha com Op faltando é mantida (e fica visível) em vez de
    sumir silenciosamente.
    """
    return f"""
        SELECT * EXCLUDE (_rn) FROM (
            SELECT *, row_number() OVER (
                PARTITION BY {key} ORDER BY {ts} DESC, {idcol} DESC, rowid DESC
            ) AS _rn
            FROM {relation}
        ) AS _ranked
        WHERE _rn = 1 AND COALESCE(Op, '') <> 'D'
    """


def build_silver(con, adapter: SettlementAdapter) -> None:
    """Monta as entidades silver limpas a partir da bronze (canônicas, ainda sem quarentena)."""
    con.execute(f"CREATE OR REPLACE TABLE silver_transactions AS {dedup_latest('bronze_transactions', 'transaction_id')}")
    con.execute(f"CREATE OR REPLACE TABLE silver_results AS {dedup_latest('bronze_results', 'id')}")
    con.execute(f"CREATE OR REPLACE TABLE silver_merchants AS {dedup_latest('bronze_merchants', 'merchant_id')}")
    con.execute(f"CREATE OR REPLACE TABLE silver_runs AS {dedup_latest('bronze_runs', 'id')}")

    # Feed de settlement -> schema canônico (via adapter) + flag has_internal.
    con.execute("CREATE OR REPLACE VIEW _raw_ps AS SELECT * FROM bronze_settlement_raw")
    con.execute(f"""
        CREATE OR REPLACE TABLE silver_settlement AS
        WITH canon AS ({adapter.canonical_sql('_raw_ps')})
        SELECT c.*, (t.transaction_id IS NOT NULL) AS has_internal
        FROM canon c
        LEFT JOIN silver_transactions t USING (transaction_id)
    """)


def build_settlement_current(con) -> None:
    """Uma linha por transaction_id (o settled_at mais recente) — montada DEPOIS da quarentena
    pra refletir os órfãos removidos. O rowid é o desempate final determinístico."""
    con.execute("""
        CREATE OR REPLACE TABLE silver_settlement_current AS
        SELECT * EXCLUDE (_rn) FROM (
            SELECT *, row_number() OVER (
                PARTITION BY transaction_id
                ORDER BY settled_at DESC, processor_reference DESC, rowid DESC
            ) AS _rn
            FROM silver_settlement
        ) AS _r
        WHERE _rn = 1
    """)
