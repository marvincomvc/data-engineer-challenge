from pathlib import Path

import duckdb


#RAIZ = Path("/app")
#DIRETORIO_DADOS = RAIZ / "docs" / "sample-data"
#CAMINHO_BANCO = RAIZ / "data" / "analytics.duckdb"


RAIZ = Path("/app")
DIRETORIO_DADOS = RAIZ / "data" / "dataset_teste"
CAMINHO_BANCO = RAIZ / "data" / "analytics.duckdb"


COLUNAS_CDC = {"op", "_timestamp"}

FONTES = {
    "bronze_transactions_batch_1": (
        "transactions_batch_1.parquet",
        "parquet",
    ),
    "bronze_transactions_batch_2": (
        "transactions_batch_2.parquet",
        "parquet",
    ),
    "bronze_reconciliation_runs": (
        "reconciliation_runs.parquet",
        "parquet",
    ),
    "bronze_reconciliation_results": (
        "reconciliation_results.parquet",
        "parquet",
    ),
    "bronze_enterprise_company": (
        "enterprise_company.parquet",
        "parquet",
    ),
    "bronze_settlement_paysettler": (
        "settlement_paysettler.csv",
        "csv",
    ),
}


def expressao_origem(formato):
    if formato == "parquet":
        return "read_parquet(?)"

    if formato == "csv":
        return "read_csv_auto(?, header = true)"

    raise ValueError(f"Formato não suportado: {formato}")


def obter_colunas(conexao, caminho, formato):
    consulta = (
        f"DESCRIBE SELECT * FROM {expressao_origem(formato)}"
    )

    resultado = conexao.execute(
        consulta, [str(caminho)]
    ).fetchall()

    return [coluna[0] for coluna in resultado]


def criar_tabela_auditoria(conexao):
    conexao.execute("""
        CREATE TABLE IF NOT EXISTS bronze_linhas_nao_inseridas (
            tabela_origem VARCHAR NOT NULL,
            arquivo_origem VARCHAR NOT NULL,
            row_hash VARCHAR NOT NULL,
            dados_origem JSON NOT NULL,
            motivo VARCHAR NOT NULL,
            data_ingestao TIMESTAMPTZ NOT NULL,
            audit_hash VARCHAR PRIMARY KEY
        )
    """)


def criar_staging(
    conexao,
    tabela,
    arquivo,
    formato,
    colunas,
):
    nome_staging = f"stg_{tabela.removeprefix('bronze_')}"

    colunas_hash = [
        coluna
        for coluna in colunas
        if coluna.lower() not in COLUNAS_CDC
    ]

    if not colunas_hash:
        raise ValueError(
            f"Nenhuma coluna disponível para o hash de {tabela}"
        )

    campos_hash = ", ".join(
        f'"{coluna}" := t."{coluna}"'
        for coluna in colunas_hash
    )

    campos_origem = ", ".join(
        f't."{coluna}"'
        for coluna in colunas
    )

    campos_json = ", ".join(
        f'"{coluna}" := t."{coluna}"'
        for coluna in colunas
    )

    consulta = f"""
        CREATE TEMP TABLE {nome_staging} AS
        WITH preparados AS (
            SELECT
                {campos_origem},
                sha256(
                    to_json(struct_pack({campos_hash}))
                ) AS row_hash,
                current_timestamp AS ingested_at,
                to_json(
                    struct_pack({campos_json})
                ) AS dados_origem
            FROM {expressao_origem(formato)} AS t
        ),
        numerados AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY row_hash
                    ORDER BY row_hash
                ) AS numero_ocorrencia
            FROM preparados
        )
        SELECT *
        FROM numerados
    """

    caminho = DIRETORIO_DADOS / arquivo

    conexao.execute(
        consulta,
        [str(caminho)],
    )

    return nome_staging


def inserir_novos_registros(
    conexao,
    tabela,
    staging,
    colunas,
):
    campos_destino = ", ".join(
        [f'"{coluna}"' for coluna in colunas]
        + ['"row_hash"', '"ingested_at"']
    )

    campos_staging = ", ".join(
        [f'n."{coluna}"' for coluna in colunas]
        + ['n."row_hash"', 'n."ingested_at"']
    )

    consulta = f"""
        INSERT INTO {tabela} ({campos_destino})
        SELECT {campos_staging}
        FROM {staging} AS n
        WHERE n.numero_ocorrencia = 1
          AND NOT EXISTS (
              SELECT 1
              FROM {tabela} AS b
              WHERE b.row_hash = n.row_hash
          )
    """

    conexao.execute(consulta)


def registrar_nao_inseridos(
    conexao,
    tabela,
    arquivo,
    staging,
):
    consulta = f"""
        INSERT INTO bronze_linhas_nao_inseridas (
            tabela_origem,
            arquivo_origem,
            row_hash,
            dados_origem,
            motivo,
            data_ingestao,
            audit_hash
        )
        SELECT
            '{tabela}',
            '{arquivo}',
            n.row_hash,
            n.dados_origem,
            CASE
                WHEN n.numero_ocorrencia > 1
                    THEN 'DUPLICADO_NO_ARQUIVO'
                ELSE 'JA_EXISTE_NA_BRONZE'
            END,
            n.ingested_at,
            sha256(
                '{tabela}' || '|' ||
                '{arquivo}' || '|' ||
                n.row_hash || '|' ||
                CAST(n.numero_ocorrencia AS VARCHAR) || '|' ||
                CASE
                    WHEN n.numero_ocorrencia > 1
                        THEN 'DUPLICADO_NO_ARQUIVO'
                    ELSE 'JA_EXISTE_NA_BRONZE'
                END
            )
        FROM {staging} AS n
        WHERE
            n.numero_ocorrencia > 1
            OR EXISTS (
                SELECT 1
                FROM {tabela} AS b
                WHERE b.row_hash = n.row_hash
            )
        ON CONFLICT (audit_hash) DO NOTHING
    """

    conexao.execute(consulta)


def main():
    if not CAMINHO_BANCO.is_file():
        raise FileNotFoundError(
            f"Banco DuckDB não encontrado: {CAMINHO_BANCO}"
        )

    for arquivo, _ in FONTES.values():
        caminho = DIRETORIO_DADOS / arquivo

        if not caminho.is_file():
            raise FileNotFoundError(
                f"Arquivo de origem não encontrado: {caminho}"
            )

    conexao = duckdb.connect(
        str(CAMINHO_BANCO),
        read_only=False,
    )

    transacao_iniciada = False

    try:
        esquemas = {}

        for tabela, (arquivo, formato) in FONTES.items():
            caminho = DIRETORIO_DADOS / arquivo
            colunas = obter_colunas(
                conexao,
                caminho,
                formato,
            )
            esquemas[tabela] = colunas

            colunas_tabela = {
                coluna[0]
                for coluna in conexao.execute(
                    f"DESCRIBE SELECT * FROM {tabela}"
                ).fetchall()
            }

            esperadas = set(colunas) | {
                "row_hash",
                "ingested_at",
            }

            if colunas_tabela != esperadas:
                raise RuntimeError(
                    f"Esquema incompatível na tabela {tabela}. "
                    "Verifique as colunas da origem e da Bronze."
                )

        conexao.execute("BEGIN TRANSACTION")
        transacao_iniciada = True

        criar_tabela_auditoria(conexao)

        total_novos = 0
        total_nao_inseridos = 0

        for tabela, (arquivo, formato) in FONTES.items():
            staging = criar_staging(
                conexao,
                tabela,
                arquivo,
                formato,
                esquemas[tabela],
            )

            quantidade_anterior = conexao.execute(
                f"SELECT COUNT(*) FROM {tabela}"
            ).fetchone()[0]

            quantidade_candidatos = conexao.execute(
                f"SELECT COUNT(*) FROM {staging}"
            ).fetchone()[0]

            inserir_novos_registros(
                conexao,
                tabela,
                staging,
                esquemas[tabela],
            )

            registrar_nao_inseridos(
                conexao,
                tabela,
                arquivo,
                staging,
            )

            quantidade_atual = conexao.execute(
                f"SELECT COUNT(*) FROM {tabela}"
            ).fetchone()[0]

            inseridos = quantidade_atual - quantidade_anterior

            quantidade_auditoria = conexao.execute(
                f"""
                SELECT COUNT(*)
                FROM {staging} AS n
                WHERE n.numero_ocorrencia > 1
                   OR EXISTS (
                       SELECT 1
                       FROM {tabela} AS b
                       WHERE b.row_hash = n.row_hash
                   )
                """
            ).fetchone()[0]

            total_novos += inseridos
            total_nao_inseridos += quantidade_auditoria

            print(
                f"{tabela}: "
                f"{quantidade_candidatos:,} candidatos, "
                f"{inseridos:,} inseridos, "
                f"{quantidade_auditoria:,} não inseridos"
            )

            conexao.execute(
                f"DROP TABLE {staging}"
            )

        conexao.execute("COMMIT")
        transacao_iniciada = False

        print("\nCarga incremental concluída.")
        print(f"Novos registros inseridos: {total_novos:,}")
        print(
            "Ocorrências não inseridas identificadas: "
            f"{total_nao_inseridos:,}"
        )

    except Exception:
        if transacao_iniciada:
            conexao.execute("ROLLBACK")

        print(
            "\nFalha na carga incremental. "
            "As alterações foram revertidas."
        )
        raise

    finally:
        conexao.close()


if __name__ == "__main__":
    main()