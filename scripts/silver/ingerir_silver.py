
from pathlib import Path

import duckdb


RAIZ = Path("/app")
CAMINHO_BANCO = RAIZ / "data" / "analytics.duckdb"

# Todas as entidades CDC conhecidas no projeto.
TABELAS_CDC = (
    {
        "destino": "silver_transactions",
        "origens": (
            "bronze_transactions_batch_1",
            "bronze_transactions_batch_2",
        ),
        "chave": "transaction_id",
    },
    {
        "destino": "silver_reconciliation_runs",
        "origens": ("bronze_reconciliation_runs",),
        "chave": "id",
    },
    {
        "destino": "silver_reconciliation_results",
        "origens": ("bronze_reconciliation_results",),
        "chave": "id",
    },
    {
        "destino": "silver_enterprise_company",
        "origens": ("bronze_enterprise_company",),
        "chave": "id",
    },
)

COLUNAS_TEMPORAIS = (
    "created_at",
    "updated_at",
    "_timestamp",
)


def ident(nome: str) -> str:
    """Escapa identificadores SQL."""
    return '"' + nome.replace('"', '""') + '"'


def tabelas_existentes(con) -> set[str]:
    return {
        linha[0]
        for linha in con.execute("SHOW TABLES").fetchall()
    }


def obter_colunas(con, tabela: str) -> list[str]:
    return [
        linha[0]
        for linha in con.execute(
            f"DESCRIBE {ident(tabela)}"
        ).fetchall()
    ]


def verificar_origens(con) -> None:
    obrigatorias = {
        origem
        for config in TABELAS_CDC
        for origem in config["origens"]
    }
    obrigatorias.add("bronze_paysettler_arquivo")

    faltantes = obrigatorias - tabelas_existentes(con)

    if faltantes:
        raise RuntimeError(
            "Tabelas Bronze ausentes: "
            + ", ".join(sorted(faltantes))
        )

    for config in TABELAS_CDC:
        for origem in config["origens"]:
            colunas = set(obter_colunas(con, origem))
            necessarias = {config["chave"], "Op", "_timestamp"}

            if not necessarias.issubset(colunas):
                raise RuntimeError(
                    f"{origem}: faltam colunas CDC: "
                    + ", ".join(sorted(necessarias - colunas))
                )

            if "row_hash" not in colunas:
                raise RuntimeError(
                    f"{origem}: row_hash não encontrado."
                )


def construir_cdc(con, config: dict) -> None:
    """
    Reconstrói o estado corrente por chave.

    Ordem temporal:
      1. created_at DESC, se disponível;
      2. updated_at DESC, se disponível;
      3. _timestamp DESC.

    row_hash não substitui a recência. É usado como desempate
    determinístico final, junto à operação quando necessário.
    """
    destino = config["destino"]
    origens = config["origens"]
    chave = config["chave"]

    colunas_origens = {
        origem: set(obter_colunas(con, origem))
        for origem in origens
    }

    # UNION ALL BY NAME preserva as diferenças de schema entre lotes.
    consulta_origem = " UNION ALL BY NAME ".join(
        f"SELECT * FROM {ident(origem)}"
        for origem in origens
    )

    # Só ordena por colunas temporais realmente presentes em todas
    # as origens da entidade. Evita assumir schemas idênticos.
    colunas_comuns = set.intersection(*colunas_origens.values())
    temporais = [
        coluna
        for coluna in COLUNAS_TEMPORAIS
        if coluna in colunas_comuns
    ]

    if not temporais:
        raise RuntimeError(
            f"{destino}: nenhuma coluna temporal disponível."
        )

    ordenacao_temporal = ",\n".join(
        f"TRY_CAST({ident(coluna)} AS TIMESTAMPTZ) DESC NULLS LAST"
        for coluna in temporais
    )

    chave_sql = ident(chave)
    destino_sql = ident(destino)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _eventos_cdc AS
        {consulta_origem}
    """)

    # Não processa registros sem chave.
    nulos = con.execute(f"""
        SELECT COUNT(*)
        FROM _eventos_cdc
        WHERE {chave_sql} IS NULL
    """).fetchone()[0]

    if nulos:
        print(
            f"[AVISO] {destino}: {nulos} eventos sem chave "
            "serão excluídos da visão corrente."
        )

    # Verifica datas inválidas nas colunas temporais disponíveis.
    for coluna in temporais:
        invalidos = con.execute(f"""
            SELECT COUNT(*)
            FROM _eventos_cdc
            WHERE {chave_sql} IS NOT NULL
              AND {ident(coluna)} IS NOT NULL
              AND TRY_CAST({ident(coluna)} AS TIMESTAMPTZ) IS NULL
        """).fetchone()[0]

        if invalidos:
            raise RuntimeError(
                f"{destino}: {invalidos} valores inválidos em "
                f"{coluna}. Corrija a origem antes de publicar a Silver."
            )

    con.execute(f"""
        CREATE OR REPLACE TABLE {destino_sql} AS
        WITH classificados AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY {chave_sql}
                    ORDER BY
                        {ordenacao_temporal},
                        row_hash DESC NULLS LAST,
                        CASE UPPER(TRIM(CAST("Op" AS VARCHAR)))
                            WHEN 'D' THEN 3
                            WHEN 'U' THEN 2
                            WHEN 'I' THEN 1
                            ELSE 0
                        END DESC
                ) AS _rn_cdc
            FROM _eventos_cdc
            WHERE {chave_sql} IS NOT NULL
        )
        SELECT * EXCLUDE (_rn_cdc)
        FROM classificados
        WHERE _rn_cdc = 1
          AND UPPER(TRIM(CAST("Op" AS VARCHAR))) <> 'D'
    """)

    total, distintos = con.execute(f"""
        SELECT
            COUNT(*),
            COUNT(DISTINCT {chave_sql})
        FROM {destino_sql}
    """).fetchone()

    if total != distintos:
        raise RuntimeError(
            f"{destino}: chave {chave} não ficou única."
        )

    print(
        f"[OK] {destino}: {total} registros correntes; "
        f"chave = {chave}; ordenação = {', '.join(temporais)}"
    )


def construir_paysettler(con) -> None:
    """
    Normaliza a fonte CSV, que não possui CDC.
    Remove apenas duplicatas exatas identificadas por row_hash.
    """
    origem = "bronze_paysettler_arquivo"
    colunas = set(obter_colunas(con, origem))

    obrigatorias = {
        "transaction_id",
        "merchant_id",
        "amount",
        "currency",
        "settled_at",
        "processor_reference",
        "status",
        "row_hash",
    }

    faltantes = obrigatorias - colunas
    if faltantes:
        raise RuntimeError(
            f"{origem}: faltam colunas: "
            + ", ".join(sorted(faltantes))
        )

    tecnicas = [
        coluna
        for coluna in ("ingested_at", "arquivo_origem", "hash_arquivo")
        if coluna in colunas
    ]

    selecao_tecnica = "".join(
        f", {ident(coluna)}" for coluna in tecnicas
    )

    ordenacao = (
        "ingested_at DESC NULLS LAST"
        if "ingested_at" in colunas
        else "row_hash"
    )

    con.execute(f"""
        CREATE OR REPLACE TABLE silver_settlement_paysettler AS
        WITH normalizado AS (
            SELECT
                NULLIF(TRIM(CAST(transaction_id AS VARCHAR)), '')
                    AS transaction_id,
                NULLIF(UPPER(TRIM(CAST(merchant_id AS VARCHAR))), '')
                    AS merchant_id,
                TRY_CAST(
                    REPLACE(TRIM(CAST(amount AS VARCHAR)), ',', '.')
                    AS DECIMAL(18, 2)
                ) AS amount,
                UPPER(TRIM(CAST(currency AS VARCHAR))) AS currency,
                TRY_CAST(settled_at AS TIMESTAMPTZ) AS settled_at,
                NULLIF(
                    TRIM(CAST(processor_reference AS VARCHAR)), ''
                ) AS processor_reference,
                UPPER(TRIM(CAST(status AS VARCHAR))) AS status,
                row_hash
                {selecao_tecnica}
            FROM {ident(origem)}
        ),
        classificados AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY row_hash
                    ORDER BY {ordenacao}
                ) AS _rn
            FROM normalizado
            WHERE row_hash IS NOT NULL
        )
        SELECT * EXCLUDE (_rn)
        FROM classificados
        WHERE _rn = 1
    """)

    quantidade = con.execute("""
        SELECT COUNT(*)
        FROM silver_settlement_paysettler
    """).fetchone()[0]

    print(
        f"[OK] silver_settlement_paysettler: "
        f"{quantidade} registros após deduplicação exata"
    )


def construir_conciliacao_historica(con) -> None:
    """
    Integra o resultado já produzido pelo sistema com sua execução.
    Não recalcula a conciliação e não mistura períodos do CSV.
    """
    resultados = set(
        obter_colunas(con, "silver_reconciliation_results")
    )
    runs = set(
        obter_colunas(con, "silver_reconciliation_runs")
    )

    campos_resultado = {
        "id": "r.id AS resultado_id",
        "run_id": "r.run_id",
        "transaction_id": "r.transaction_id",
        "category": "r.category",
        "internal_amount": "r.internal_amount AS valor_interno",
        "processor_amount": "r.processor_amount AS valor_externo",
        "difference": "r.difference AS diferenca",
    }

    campos_run = {
        "reference_date": "run.reference_date",
        "file_name": "run.file_name",
        "status": "run.status AS status_execucao",
        "started_at": "run.started_at",
        "completed_at": "run.completed_at",
    }

    selecao = [
        expressao
        for coluna, expressao in campos_resultado.items()
        if coluna in resultados
    ]
    selecao.extend(
        expressao
        for coluna, expressao in campos_run.items()
        if coluna in runs
    )

    if "run_id" not in resultados or "id" not in runs:
        raise RuntimeError(
            "Não foi possível vincular resultados e execuções."
        )

    con.execute(f"""
        CREATE OR REPLACE TABLE silver_transacoes_conciliacao AS
        SELECT {', '.join(selecao)}
        FROM silver_reconciliation_results r
        INNER JOIN silver_reconciliation_runs run
            ON r.run_id = run.id
    """)

    quantidade = con.execute("""
        SELECT COUNT(*)
        FROM silver_transacoes_conciliacao
    """).fetchone()[0]

    print(
        f"[OK] silver_transacoes_conciliacao: "
        f"{quantidade} resultados históricos integrados"
    )


def construir_qualidade(con) -> None:
    con.execute("""
        CREATE OR REPLACE TABLE silver_qualidade_dados AS
        SELECT
            'silver_transactions' AS tabela,
            'valor_interno_nulo' AS verificacao,
            COUNT(*) AS quantidade
        FROM silver_transactions
        WHERE amount IS NULL

        UNION ALL

        SELECT
            'silver_settlement_paysettler',
            'valor_externo_nulo_ou_invalido',
            COUNT(*)
        FROM silver_settlement_paysettler
        WHERE amount IS NULL

        UNION ALL

        SELECT
            'silver_settlement_paysettler',
            'data_liquidacao_nula_ou_invalida',
            COUNT(*)
        FROM silver_settlement_paysettler
        WHERE settled_at IS NULL

        UNION ALL

        SELECT
            'silver_settlement_paysettler',
            'status_desconhecido',
            COUNT(*)
        FROM silver_settlement_paysettler
        WHERE status NOT IN ('SETTLED', 'REVERSED')
           OR status IS NULL

        UNION ALL

        SELECT
            'silver_reconciliation_runs',
            'execucao_nao_concluida',
            COUNT(*)
        FROM silver_reconciliation_runs
        WHERE status <> 'COMPLETED'
           OR status IS NULL
    """)

    print("[OK] silver_qualidade_dados criada")



def validar_silver(con) -> None:
    for tabela, chave in (
        ("silver_transactions", "transaction_id"),
        ("silver_reconciliation_runs", "id"),
        ("silver_reconciliation_results", "id"),
        ("silver_enterprise_company", "id"),
    ):
        total, distintos = con.execute(f"""
            SELECT COUNT(*), COUNT(DISTINCT {ident(chave)})
            FROM {ident(tabela)}
        """).fetchone()

        if total != distintos:
            raise RuntimeError(
                f"Validação falhou: {tabela} não possui chave única."
            )

    sem_execucao = con.execute("""
        SELECT COUNT(*)
        FROM silver_reconciliation_results r
        LEFT JOIN silver_reconciliation_runs run
            ON r.run_id = run.id
        WHERE run.id IS NULL
    """).fetchone()[0]

    if sem_execucao:
        print(
            f"[AVISO] {sem_execucao} resultados sem execução correspondente. "
            "Registros preservados para investigação."
        )
    else:
        print("[OK] Todos os resultados possuem execução correspondente.")

    print("[OK] Validações estruturais concluídas")



def main() -> None:
    if not CAMINHO_BANCO.exists():
        raise FileNotFoundError(
            f"Banco DuckDB não encontrado: {CAMINHO_BANCO}"
        )

    con = duckdb.connect(str(CAMINHO_BANCO))

    try:
        con.execute("SET TimeZone = 'UTC'")
        verificar_origens(con)
        con.execute("BEGIN TRANSACTION")

        try:
            for config in TABELAS_CDC:
                construir_cdc(con, config)

            construir_paysettler(con)
            construir_conciliacao_historica(con)
            construir_qualidade(con)
            validar_silver(con)

            con.execute("COMMIT")

        except Exception:
            con.execute("ROLLBACK")
            raise

        print("\nCamada Silver reconstruída com sucesso.")

    finally:
        con.close()


if __name__ == "__main__":
    main()
