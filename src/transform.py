"""
Transformações do pipeline de reconciliação.

Funções puras e operações DuckDB para:
- Deduplicação CDC (manter última versão por id via Op/_timestamp)
- União de batches de transações
- Recálculo de diferenças entre valores internos e do processador
- Aplicação da tolerância de R$ 0,01
- Aplicação da janela de 7 dias sobre created_at
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from src.config import (
    CATEGORIA_MATCHED,
    CATEGORIA_MISMATCHED,
    JANELA_DIAS,
    TOLERANCIA_REAIS,
)
from src.quality import gravar_bronze_generico

logger = logging.getLogger(__name__)


def deduplicar_cdc(
    con: duckdb.DuckDBPyConnection,
    nome_tabela: str,
    bronze_path: Path,
) -> str:
    """
    Remove duplicatas CDC (Change Data Capture) de uma tabela.

    O gerador produz pares I/U (Insert/Update) por id. Esta função
    mantém apenas a versão mais recente (_timestamp) de cada id.
    Registros descartados vão para a camada bronze.

    Args:
        con: Conexão DuckDB.
        nome_tabela: Nome da tabela fonte no DuckDB.
        bronze_path: Caminho da pasta bronze/.

    Returns:
        Nome da tabela temporária com dados deduplicados.
    """
    logger.info("Deduplicando CDC da tabela %s...", nome_tabela)

    # Verifica se as colunas CDC existem
    colunas = [r[0] for r in con.execute(f"DESCRIBE {nome_tabela}").fetchall()]

    if "Op" not in colunas or "_timestamp" not in colunas:
        logger.info(
            "Tabela %s não possui colunas CDC (Op/_timestamp). "
            "Pulando dedup CDC.",
            nome_tabela,
        )
        tabela_limpa = f"{nome_tabela}_cdc_clean"
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {tabela_limpa} AS
            SELECT * FROM {nome_tabela}
        """)
        return tabela_limpa

    tabela_limpa = f"{nome_tabela}_cdc_clean"

    # Conta duplicatas CDC
    dup_cdc = con.execute(f"""
        SELECT count(*) FROM (
            SELECT id, count(*) AS cnt
            FROM {nome_tabela}
            GROUP BY id
            HAVING count(*) > 1
        )
    """).fetchone()[0]

    if dup_cdc > 0:
        logger.warning(
            "Encontrados %s ids com múltiplas versões CDC em %s. "
            "Mantendo versão mais recente.",
            dup_cdc,
            nome_tabela,
        )

        # Cria tabela com apenas a versão mais recente de cada id
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {tabela_limpa} AS
            SELECT * EXCLUDE (_rn) FROM (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY id
                        ORDER BY _timestamp DESC, Op DESC
                    ) AS _rn
                FROM {nome_tabela}
            ) sub
            WHERE _rn = 1
        """)

        # Grava versões antigas no bronze
        gravar_bronze_generico(
            con,
            f"""
                SELECT t.*
                FROM {nome_tabela} t
                WHERE t.id NOT IN (SELECT id FROM {tabela_limpa})
            """,
            bronze_path,
            f"{nome_tabela}_cdc_versoes_antigas.parquet",
            "DEDUP_CDC_DESCARTA_ANTERIOR",
            is_query=True,
        )
    else:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {tabela_limpa} AS
            SELECT * FROM {nome_tabela}
        """)

    total = con.execute(f"SELECT count(*) FROM {tabela_limpa}").fetchone()[0]
    logger.info("CDC dedup concluído: %s registros limpos.", total)
    return tabela_limpa


def unir_transacoes_internas(
    con: duckdb.DuckDBPyConnection,
    tabela_batch1: str,
    tabela_batch2: str,
) -> str:
    """
    Une os dois batches de transações internas (UNION ALL),
    resolve colunas com union_by_name, e desduplica transações
    que aparecem nos dois batches (mantendo a mais recente por created_at).

    Args:
        con: Conexão DuckDB.
        tabela_batch1: Nome da tabela temporária do batch 1 (já dedup CDC).
        tabela_batch2: Nome da tabela temporária do batch 2 (já dedup CDC).

    Returns:
        Nome da tabela temporária com transações internas unificadas.
    """
    logger.info("Unindo batches de transações internas...")
    tabela_unificada = "transactions_unified"

    # UNION ALL dos dois batches. Como batch2 pode ter coluna extra (payment_method),
    # usamos EXCLUDE para manter compatibilidade.
    colunas_b1 = [
        r[0] for r in con.execute(f"DESCRIBE {tabela_batch1}").fetchall()
    ]
    colunas_b2 = [
        r[0] for r in con.execute(f"DESCRIBE {tabela_batch2}").fetchall()
    ]

    # Colunas comuns (interseção) para garantir compatibilidade
    colunas_comuns = [c for c in colunas_b1 if c in colunas_b2]

    colunas_select = ", ".join(f'"{c}"' if c.upper() == c and not c.islower() else c for c in colunas_comuns)
    # Simplificando: DuckDB aceita nomes de coluna sem aspas na maioria dos casos
    colunas_str = ", ".join(colunas_comuns)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {tabela_unificada} AS
        SELECT {colunas_str} FROM {tabela_batch1}
        UNION ALL
        SELECT {colunas_str} FROM {tabela_batch2}
    """)

    total = con.execute(f"SELECT count(*) FROM {tabela_unificada}").fetchone()[0]
    logger.info("Transações internas unificadas: %s registros.", total)
    return tabela_unificada


def dentro_da_janela(
    created_at_str: str,
    reference_date_str: str,
) -> bool:
    """
    Verifica se uma data created_at está dentro da janela de 7 dias
    em relação à reference_date.

    Janela: [reference_date - 7 dias, reference_date] (inclusivo).

    Esta função é usada tanto no pipeline quanto nos testes unitários.

    Args:
        created_at_str: Data/hora da transação em formato ISO 8601.
        reference_date_str: Data de referência em formato YYYY-MM-DD.

    Returns:
        True se created_at está dentro da janela.
    """
    from datetime import timedelta

    created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
    reference_date = datetime.strptime(reference_date_str, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )

    # Início da janela: reference_date - 7 dias, início do dia (00:00)
    inicio_janela = reference_date - timedelta(days=JANELA_DIAS)
    inicio_janela = inicio_janela.replace(hour=0, minute=0, second=0, microsecond=0)

    # Fim da janela: reference_date, fim do dia (23:59:59.999999)
    fim_janela = reference_date.replace(
        hour=23, minute=59, second=59, microsecond=999999
    )

    return inicio_janela <= created_at <= fim_janela


def aplicar_tolerancia(
    internal_amount: float | None,
    processor_amount: float | None,
) -> bool:
    """
    Verifica se a diferença entre valores está dentro da tolerância de R$ 0,01.

    Se ambos os valores forem informados, calcula a diferença absoluta
    arredondada para 2 casas decimais (evitando imprecisão de float).
    Se a diferença <= TOLERANCIA_REAIS (R$ 0,01), retorna True.

    Esta função é usada tanto no pipeline quanto nos testes unitários.

    Args:
        internal_amount: Valor no sistema interno.
        processor_amount: Valor no processador.

    Returns:
        True se a diferença absoluta for <= tolerância.
    """
    if internal_amount is None or processor_amount is None:
        return False
    # Arredonda para 2 casas decimais antes de comparar, evitando imprecisão float
    diff = round(abs(internal_amount - processor_amount), 2)
    return diff <= TOLERANCIA_REAIS


def recalcular_diferencas(
    internal_amount: float | None,
    processor_amount: float | None,
) -> tuple[float | None, float | None]:
    """
    Recalcula as diferenças absoluta e sinalizada entre valores.

    Args:
        internal_amount: Valor no sistema interno.
        processor_amount: Valor no processador.

    Returns:
        Tupla (difference_abs, difference_signed) ou (None, None) se
        algum valor for nulo.
    """
    if internal_amount is None or processor_amount is None:
        return None, None

    diff_abs = round(abs(internal_amount - processor_amount), 2)
    diff_signed = round(internal_amount - processor_amount, 2)
    return diff_abs, diff_signed


def aplicar_tolerancia_e_recalcular(
    con: duckdb.DuckDBPyConnection,
    nome_tabela_fato: str,
    bronze_path: Path,
) -> str:
    """
    Aplica a regra de tolerância na tabela fato temporária:
    - Recalcula difference_abs e difference_signed para todos os registros.
    - Se difference_abs <= R$ 0,01 e categoria atual for MISMATCHED,
      altera a categoria para MATCHED.
    - Registra no bronze os registros que tiveram tolerância aplicada.

    Args:
        con: Conexão DuckDB.
        nome_tabela_fato: Nome da tabela fato temporária.
        bronze_path: Caminho da pasta bronze/.

    Returns:
        Nome da tabela temporária com tolerância aplicada.
    """
    logger.info("Aplicando tolerância de R$ %.2f...", TOLERANCIA_REAIS)

    tabela_tolerancia = f"{nome_tabela_fato}_tolerancia"

    agora = datetime.now(timezone.utc).isoformat()

    # Cria nova tabela com diferenças recalculadas e tolerância aplicada
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {tabela_tolerancia} AS
        SELECT
            reconciliation_result_id,
            run_id,
            merchant_id,
            transaction_id,
            CASE
                WHEN internal_amount IS NOT NULL
                 AND processor_amount IS NOT NULL
                 AND abs(internal_amount - processor_amount) <= {TOLERANCIA_REAIS}
                 AND category = '{CATEGORIA_MISMATCHED}'
                THEN '{CATEGORIA_MATCHED}'
                ELSE category
            END AS category,
            internal_amount,
            processor_amount,
            CASE
                WHEN internal_amount IS NOT NULL
                 AND processor_amount IS NOT NULL
                THEN round(abs(internal_amount - processor_amount), 2)
                ELSE NULL
            END AS difference_abs,
            CASE
                WHEN internal_amount IS NOT NULL
                 AND processor_amount IS NOT NULL
                THEN round(internal_amount - processor_amount, 2)
                ELSE NULL
            END AS difference_signed,
            reference_date,
            created_at
        FROM {nome_tabela_fato}
    """)

    # Identifica registros que tiveram tolerância aplicada
    total_tolerancia = con.execute(f"""
        SELECT count(*) FROM {tabela_tolerancia} t
        INNER JOIN {nome_tabela_fato} o
            ON t.reconciliation_result_id = o.reconciliation_result_id
        WHERE o.category = '{CATEGORIA_MISMATCHED}'
          AND t.category = '{CATEGORIA_MATCHED}'
    """).fetchone()[0]

    if total_tolerancia > 0:
        logger.warning(
            "%s registros alterados de MISMATCHED para MATCHED "
            "devido à tolerância de R$ %.2f.",
            total_tolerancia,
            TOLERANCIA_REAIS,
        )
        # Grava no bronze os registros corrigidos
        gravar_bronze_generico(
            con,
            f"""
                SELECT t.*
                FROM {tabela_tolerancia} t
                INNER JOIN {nome_tabela_fato} o
                    ON t.reconciliation_result_id = o.reconciliation_result_id
                WHERE o.category = '{CATEGORIA_MISMATCHED}'
                  AND t.category = '{CATEGORIA_MATCHED}'
            """,
            bronze_path,
            "tolerancia_aplicada.parquet",
            "TOLERANCIA_APLICADA",
            is_query=True,
        )

    # Grava registros com diferença recalculada (todos que têm ambos os valores)
    gravar_bronze_generico(
        con,
        f"""
            SELECT *
            FROM {tabela_tolerancia}
            WHERE difference_abs IS NOT NULL
              AND difference_abs != 0
        """,
        bronze_path,
        "diferenca_recalculada.parquet",
        "DIFERENCA_RECALCULADA",
        is_query=True,
    )

    logger.info("Tolerância aplicada. Registros corrigidos: %s.", total_tolerancia)
    return tabela_tolerancia