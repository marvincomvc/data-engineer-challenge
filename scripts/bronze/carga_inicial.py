from pathlib import Path

import duckdb


RAIZ = Path("/app")
DIRETORIO_DADOS = RAIZ / "docs" / "sample-data"
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


def criar_consulta(tabela, formato, colunas):
    origem = expressao_origem(formato)

    # Mantém todas as colunas na Bronze, mas exclui as colunas
    # técnicas de CDC do cálculo do hash.
    colunas_hash = [
        coluna
        for coluna in colunas
        if coluna.lower() not in COLUNAS_CDC
    ]

    if not colunas_hash:
        raise ValueError(
            f"Nenhuma coluna disponível para calcular o hash de {tabela}"
        )

    campos_hash = ", ".join(
        f'"{coluna}" := t."{coluna}"'
        for coluna in colunas_hash
    )

    return f"""
        CREATE OR REPLACE TABLE {tabela} AS
        SELECT
            t.*,
            sha256(
                to_json(struct_pack({campos_hash}))
            ) AS row_hash,
            current_timestamp AS ingested_at
        FROM {origem} AS t
    """


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
        # Descobre e valida os esquemas antes de substituir as tabelas.
        esquemas = {}

        for tabela, (arquivo, formato) in FONTES.items():
            caminho = DIRETORIO_DADOS / arquivo
            colunas = obter_colunas(conexao, caminho, formato)

            if any(
                coluna.lower() in {"row_hash", "ingested_at"}
                for coluna in colunas
            ):
                raise ValueError(
                    f"A origem {arquivo} já contém row_hash ou ingested_at."
                )

            esquemas[tabela] = colunas

        conexao.execute("BEGIN TRANSACTION")
        transacao_iniciada = True

        for tabela, (arquivo, formato) in FONTES.items():
            caminho = DIRETORIO_DADOS / arquivo

            consulta = criar_consulta(
                tabela,
                formato,
                esquemas[tabela],
            )

            print(f"Iniciando carga: {tabela}", flush=True)
            print(f"Arquivo de origem: {caminho}", flush=True)

            conexao.execute(consulta, [str(caminho)])

            print(f"Carga concluída: {tabela}", flush=True)

            quantidade = conexao.execute(
                f"SELECT COUNT(*) FROM {tabela}"
            ).fetchone()[0]

            hashes_distintos = conexao.execute(
                f"SELECT COUNT(DISTINCT row_hash) FROM {tabela}"
            ).fetchone()[0]

            print(
                f"{tabela}: {quantidade:,} registros; "
                f"{hashes_distintos:,} hashes distintos"
            )

        conexao.execute("COMMIT")
        transacao_iniciada = False

        print("\nCarga inicial Bronze concluída com sucesso.")

    except Exception:
        if transacao_iniciada:
            conexao.execute("ROLLBACK")

        print(
            "\nFalha na carga inicial. "
            "As alterações foram revertidas."
        )
        raise

    finally:
        conexao.close()


if __name__ == "__main__":
    main()