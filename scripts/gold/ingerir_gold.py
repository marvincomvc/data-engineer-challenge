
from pathlib import Path

import duckdb


RAIZ = Path("/app")
CAMINHO_BANCO = RAIZ / "data" / "analytics.duckdb"


SQL_MOEDAS = """
    SELECT
        c.resultado_id,
        CASE
            WHEN (
                SELECT COUNT(DISTINCT t.currency)
                FROM silver_transactions t
                WHERE t.transaction_id = c.transaction_id
            ) = 1
            THEN (
                SELECT MIN(t.currency)
                FROM silver_transactions t
                WHERE t.transaction_id = c.transaction_id
            )
            WHEN (
                SELECT COUNT(DISTINCT t.currency)
                FROM silver_transactions t
                WHERE t.transaction_id = c.transaction_id
            ) > 1
            THEN 'MULTIPLE'
            ELSE NULL
        END AS moeda_interna,
        CASE
            WHEN (
                SELECT COUNT(DISTINCT p.currency)
                FROM silver_settlement_paysettler p
                WHERE p.transaction_id = c.transaction_id
                  AND CAST(p.settled_at AS DATE)
                      = TRY_CAST(c.reference_date AS DATE)
            ) = 1
            THEN (
                SELECT MIN(p.currency)
                FROM silver_settlement_paysettler p
                WHERE p.transaction_id = c.transaction_id
                  AND CAST(p.settled_at AS DATE)
                      = TRY_CAST(c.reference_date AS DATE)
            )
            WHEN (
                SELECT COUNT(DISTINCT p.currency)
                FROM silver_settlement_paysettler p
                WHERE p.transaction_id = c.transaction_id
                  AND CAST(p.settled_at AS DATE)
                      = TRY_CAST(c.reference_date AS DATE)
            ) > 1
            THEN 'MULTIPLE'
            ELSE NULL
        END AS moeda_externa
    FROM silver_transacoes_conciliacao c
"""


def criar_gold_operacoes(con):
    con.execute("""
        CREATE OR REPLACE TABLE gold_operacoes_conciliacao AS
        WITH moedas AS (
            SELECT * FROM (
                SELECT
                    c.resultado_id,
                    CASE
                        WHEN COUNT(DISTINCT t.currency) = 1
                            THEN MIN(t.currency)
                        WHEN COUNT(DISTINCT t.currency) > 1
                            THEN 'MULTIPLE'
                        ELSE NULL
                    END AS moeda_interna
                FROM silver_transacoes_conciliacao c
                LEFT JOIN silver_transactions t
                    ON t.transaction_id = c.transaction_id
                GROUP BY c.resultado_id
            ) mi
            JOIN (
                SELECT
                    c.resultado_id,
                    CASE
                        WHEN COUNT(DISTINCT p.currency) = 1
                            THEN MIN(p.currency)
                        WHEN COUNT(DISTINCT p.currency) > 1
                            THEN 'MULTIPLE'
                        ELSE NULL
                    END AS moeda_externa
                FROM silver_transacoes_conciliacao c
                LEFT JOIN silver_settlement_paysettler p
                    ON p.transaction_id = c.transaction_id
                   AND CAST(p.settled_at AS DATE)
                       = TRY_CAST(c.reference_date AS DATE)
                GROUP BY c.resultado_id
            ) me USING (resultado_id)
        )
        SELECT
            c.reference_date,
            c.run_id,
            c.file_name,
            c.status_execucao,
            c.category,
            m.moeda_interna,
            m.moeda_externa,
            COUNT(*) AS quantidade_resultados,
            COUNT(*) FILTER (
                WHERE ABS(c.diferenca) = 0
            ) AS quantidade_sem_diferenca,
            COUNT(*) FILTER (
                WHERE ABS(c.diferenca) > 0
                  AND ABS(c.diferenca) <= 0.01
            ) AS quantidade_diferenca_ate_001,
            COUNT(*) FILTER (
                WHERE ABS(c.diferenca) > 0.01
            ) AS quantidade_diferenca_acima_001,
            SUM(c.valor_interno) AS valor_interno_total,
            SUM(c.valor_externo) AS valor_externo_total,
            SUM(ABS(c.diferenca)) AS diferenca_absoluta_total,
            MIN(c.started_at) AS inicio_execucao,
            MAX(c.completed_at) AS fim_execucao
        FROM silver_transacoes_conciliacao c
        LEFT JOIN moedas m USING (resultado_id)
        GROUP BY
            c.reference_date,
            c.run_id,
            c.file_name,
            c.status_execucao,
            c.category,
            m.moeda_interna,
            m.moeda_externa
    """)


def criar_gold_cfo(con):
    con.execute("""

        CREATE OR REPLACE TABLE gold_cfo_volume_financeiro AS

        WITH moedas_internas AS (
                SELECT
                    transaction_id,
                    MAX(currency) AS currency
                FROM silver_transactions
                GROUP BY transaction_id
            ),

            moedas_processador AS (
                SELECT
                    transaction_id,
                    MAX(currency) AS currency
                FROM silver_settlement_paysettler
                GROUP BY transaction_id
            )

        SELECT
            s.reference_date,
            mi.currency AS moeda_interna,
            mp.currency AS moeda_externa,
            s.category,
            COUNT(*) AS quantidade_resultados,
            COUNT(DISTINCT s.transaction_id) AS transacoes_distintas,
            SUM(s.valor_interno) AS volume_interno,
            SUM(s.valor_externo) AS volume_processador,
            SUM(ABS(s.diferenca)) AS diferenca_absoluta_total
        FROM silver_transacoes_conciliacao s
            LEFT JOIN moedas_internas mi
                ON s.transaction_id = mi.transaction_id
            LEFT JOIN moedas_processador mp
                ON s.transaction_id = mp.transaction_id
        GROUP BY
            s.reference_date,
            mi.currency,
            mp.currency,
            s.category;
        """)


def criar_gold_compliance(con):
    con.execute("""
        CREATE OR REPLACE TABLE gold_compliance_auditoria AS
        SELECT
            r.id AS resultado_id,
            r.run_id,
            r.transaction_id,
            r.merchant_id,
            r.category,
            r.internal_amount,
            r.processor_amount,
            r.difference,
            r.created_at AS resultado_criado_em,
            r.Op AS operacao_cdc_resultado,
            r._timestamp AS timestamp_cdc_resultado,
            r.row_hash AS hash_resultado,
            r.ingested_at AS resultado_ingestado_em,
            run.reference_date,
            run.file_name,
            run.status AS status_execucao,
            run.total_transactions,
            run.started_at,
            run.completed_at,
            run.created_at AS execucao_criada_em,
            run.Op AS operacao_cdc_execucao,
            run._timestamp AS timestamp_cdc_execucao,
            run.row_hash AS hash_execucao,
            run.ingested_at AS execucao_ingestada_em
        FROM silver_reconciliation_results r
        LEFT JOIN silver_reconciliation_runs run
            ON run.id = r.run_id
    """)


def main():
    if not CAMINHO_BANCO.exists():
        raise FileNotFoundError(
            f"Banco DuckDB não encontrado: {CAMINHO_BANCO}"
        )

    con = duckdb.connect(str(CAMINHO_BANCO))

    try:
        con.execute("BEGIN TRANSACTION")

        criar_gold_operacoes(con)
        print("[OK] gold_operacoes_conciliacao criada.")

        criar_gold_cfo(con)
        print("[OK] gold_cfo_volume_financeiro criada.")

        criar_gold_compliance(con)
        print("[OK] gold_compliance_auditoria criada.")

        con.execute("COMMIT")
        print("\n[OK] As três tabelas Gold foram atualizadas.")

    except Exception:
        con.execute("ROLLBACK")
        raise

    finally:
        con.close()


if __name__ == "__main__":
    main()
