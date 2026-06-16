"""Checagens de qualidade de dados — politica em camadas: FIX (no adapter/silver) / STOP / QUARANTINE.

STOP       = circuit breakers que abortam a execucao (ameacas de integridade na data solicitada).
QUARANTINE = isola as linhas ruins mas nao corruptoras em tabelas separadas e segue em frente (contadas + registradas).

As tabelas de quarentena sao recriadas a cada execucao (CREATE OR REPLACE), entao rodar a mesma data
de novo e idempotente e as tabelas sempre refletem o estado atual em vez de acumular duplicatas.
"""
from __future__ import annotations


class DataQualityError(Exception):
    """Disparada por um circuit breaker para abortar a execucao (a transacao sofre rollback)."""


def init_quarantine(con) -> None:
    con.execute("CREATE OR REPLACE TABLE quarantine_settlement (transaction_id VARCHAR, reason VARCHAR)")
    con.execute("CREATE OR REPLACE TABLE quarantine_results (result_id BIGINT, transaction_id VARCHAR, reason VARCHAR)")


def assert_circuit_breakers(con, reference_date: str) -> None:
    """Checagens de nivel STOP para a reference_date solicitada."""
    # STOP 1: a execucao de reconciliacao upstream da data pedida precisa estar COMPLETED.
    runs = con.execute(
        "SELECT status FROM silver_runs WHERE reference_date = ?", [reference_date]
    ).fetchall()
    if any(status != "COMPLETED" for (status,) in runs):
        raise DataQualityError(
            f"upstream run for {reference_date} is not COMPLETED: {[r[0] for r in runs]}"
        )
    # STOP 2: nenhum settlement pode ter a chave de join NULL.
    nulls = con.sql("SELECT count(*) FROM silver_settlement WHERE transaction_id IS NULL").fetchone()[0]
    if nulls:
        raise DataQualityError(f"{nulls} settlement rows with NULL transaction_id")


def quarantine_settlements(con) -> int:
    """QUARENTENA settlements REVERSED orfaos (sem par interno). Retorna a quantidade movida."""
    moved = con.execute("""
        INSERT INTO quarantine_settlement
        SELECT transaction_id, 'ORPHAN_REVERSED'
        FROM silver_settlement
        WHERE status = 'REVERSED' AND has_internal = false
        RETURNING transaction_id
    """).fetchall()
    con.execute("DELETE FROM silver_settlement WHERE status = 'REVERSED' AND has_internal = false")
    return len(moved)


def quarantine_results(con) -> int:
    """QUARENTENA results que nao da pra modelar com seguranca: um run_id sem run (FK orfa),
    ou um merchant_id NULL. Sao registrados com um motivo e removidos do silver_results, assim a
    contagem de linhas da trilha de auditoria fecha com (results mantidos), em vez de encolher silenciosamente num join."""
    moved = con.execute("""
        INSERT INTO quarantine_results
        SELECT id, transaction_id,
               CASE WHEN run_id NOT IN (SELECT id FROM silver_runs) THEN 'ORPHAN_RUN'
                    ELSE 'NULL_MERCHANT' END AS reason
        FROM silver_results
        WHERE run_id NOT IN (SELECT id FROM silver_runs) OR merchant_id IS NULL
        RETURNING result_id
    """).fetchall()
    con.execute("""
        DELETE FROM silver_results
        WHERE run_id NOT IN (SELECT id FROM silver_runs) OR merchant_id IS NULL
    """)
    return len(moved)
