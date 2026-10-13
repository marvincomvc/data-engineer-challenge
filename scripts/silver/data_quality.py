
from datetime import datetime, timezone
import json
import uuid

import duckdb


CAMINHO_BANCO = "/app/data/analytics.duckdb"

CATEGORIAS_VALIDAS = [
    "MATCHED",
    "MISMATCHED",
    "UNRECONCILED_PROCESSOR",
    "UNRECONCILED_INTERNAL",
]


def executar_verificacao(conexao, regra, consulta, descricao):
    quantidade = conexao.execute(consulta).fetchone()[0]

    return {
        "execucao_id": EXECUCAO_ID,
        "data_execucao": DATA_EXECUCAO,
        "regra": regra,
        "severidade": "ERROR",
        "status": "PASSOU" if quantidade == 0 else "FALHOU",
        "descricao": descricao,
        "tabela_origem": "Silver",
        "registro_id": None,
        "detalhes": json.dumps(
            {"quantidade_problemas": quantidade}
        ),
    }


def main():
    global EXECUCAO_ID, DATA_EXECUCAO

    EXECUCAO_ID = str(uuid.uuid4())
    DATA_EXECUCAO = datetime.now(timezone.utc)

    verificacoes = [
        (
            "TRANSACAO_SEM_ID",
            """
            SELECT COUNT(*)
            FROM silver_transactions
            WHERE transaction_id IS NULL
               OR TRIM(transaction_id) = ''
            """,
            "Verifica transações sem identificador.",
        ),
        (
            "RESULTADO_SEM_EXECUCAO",
            """
            SELECT COUNT(*)
            FROM silver_reconciliation_results r
            LEFT JOIN silver_reconciliation_runs e
                ON r.run_id = e.id
            WHERE e.id IS NULL
            """,
            "Verifica resultados sem execução de conciliação correspondente.",
        ),
        (
            "CATEGORIA_INVALIDA",
            """
            SELECT COUNT(*)
            FROM silver_reconciliation_results
            WHERE category IS NULL
               OR category NOT IN (
                   'MATCHED',
                   'MISMATCHED',
                   'UNRECONCILED_PROCESSOR',
                   'UNRECONCILED_INTERNAL'
               )
            """,
            "Verifica se as categorias pertencem ao domínio permitido.",
        ),
        (
            "VALOR_NEGATIVO",
            """
            SELECT COUNT(*)
            FROM silver_reconciliation_results
            WHERE internal_amount < 0
               OR processor_amount < 0
            """,
            "Verifica se existem valores financeiros negativos nos resultados.",
        ),
        (
            "DIFERENCA_INCORRETA",
            """
            SELECT COUNT(*)
            FROM silver_reconciliation_results
            WHERE internal_amount IS NOT NULL
              AND processor_amount IS NOT NULL
              AND (
                  difference IS NULL
                  OR ABS(
                      difference
                      - ABS(internal_amount - processor_amount)
                  ) > 0.000001
              )
            """,
            "Verifica se a diferença corresponde ao valor absoluto entre os dois lados.",
        ),
    ]

    conexao = duckdb.connect(CAMINHO_BANCO)

    try:
        conexao.execute("""
            CREATE TABLE IF NOT EXISTS silver_data_quality_results (
                execucao_id VARCHAR,
                data_execucao TIMESTAMPTZ,
                regra VARCHAR,
                severidade VARCHAR,
                status VARCHAR,
                descricao VARCHAR,
                tabela_origem VARCHAR,
                registro_id VARCHAR,
                detalhes JSON
            )
        """)

        resultados = []

        for regra, consulta, descricao in verificacoes:
            resultado = executar_verificacao(
                conexao,
                regra,
                consulta,
                descricao,
            )
            resultados.append(resultado)

            conexao.execute(
                """
                INSERT INTO silver_data_quality_results
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    resultado["execucao_id"],
                    resultado["data_execucao"],
                    resultado["regra"],
                    resultado["severidade"],
                    resultado["status"],
                    resultado["descricao"],
                    resultado["tabela_origem"],
                    resultado["registro_id"],
                    resultado["detalhes"],
                ],
            )

        print("\nRESULTADO DAS VERIFICACOES DE QUALIDADE\n")

        for resultado in resultados:
            print(
                f'{resultado["status"]}: '
                f'{resultado["regra"]} - '
                f'{resultado["descricao"]} '
                f'Problemas: '
                f'{json.loads(resultado["detalhes"])["quantidade_problemas"]}'
            )

        falhas = sum(
            resultado["status"] == "FALHOU"
            for resultado in resultados
        )

        print(f"\nVerificações executadas: {len(resultados)}")
        print(f"Verificações com problemas: {falhas}")
        print(f"ID da execução: {EXECUCAO_ID}")

    finally:
        conexao.close()


if __name__ == "__main__":
    main()
