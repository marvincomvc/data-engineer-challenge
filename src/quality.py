"""
Validação de qualidade de dados e gravação na camada bronze.

Cada registro com problema é salvo em parquet na pasta bronze/
com a coluna 'qualidade_problema' descrevendo o tipo de problema
e 'qualidade_timestamp' com o momento da detecção/ação.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from src.config import (
    QUALIDADE_DUPLICATA_DESCARTADA,
    QUALIDADE_NULL_AMOUNT,
    QUALIDADE_NULL_MERCHANT_ID,
    QUALIDADE_NULL_TRANSACTION_ID,
)

logger = logging.getLogger(__name__)


def validar_schema(
    con: duckdb.DuckDBPyConnection,
    nome_fonte: str,
    colunas_esperadas: set,
) -> None:
    """
    Valida se as colunas esperadas existem na tabela/fonte.

    Se alguma coluna obrigatória estiver faltando, encerra com FAIL
    (crítico — schema inválido é impeditivo).

    Args:
        con: Conexão DuckDB.
        nome_fonte: Nome da tabela ou arquivo (ex: 'transactions_batch_1').
        colunas_esperadas: Conjunto de nomes de colunas obrigatórios.
    """
    logger.info("Validando schema da fonte: %s", nome_fonte)

    colunas_reais = set(
        row[0] for row in con.execute(f"DESCRIBE {nome_fonte}").fetchall()
    )

    # Colunas CDC (Op, _timestamp, payment_method) são opcionais — não exigimos
    faltantes = colunas_esperadas - colunas_reais

    if faltantes:
        logger.error(
            "Schema inválido em %s. Colunas faltantes: %s",
            nome_fonte,
            sorted(faltantes),
        )
        raise SystemExit(
            f"FALHA CRÍTICA: Schema inválido na fonte '{nome_fonte}'. "
            f"Colunas faltantes: {sorted(faltantes)}"
        )

    logger.info("Schema de %s validado com sucesso.", nome_fonte)


def verificar_arquivo_vazio(
    con: duckdb.DuckDBPyConnection,
    nome_tabela: str,
    nome_fonte: str,
) -> None:
    """
    Verifica se a fonte está vazia. Se estiver, falha criticamente.

    Args:
        con: Conexão DuckDB.
        nome_tabela: Nome da tabela já registrada no DuckDB.
        nome_fonte: Nome descritivo da fonte para o log.
    """
    total = con.execute(f"SELECT count(*) FROM {nome_tabela}").fetchone()[0]
    if total == 0:
        logger.error("Fonte %s está vazia.", nome_fonte)
        raise SystemExit(f"FALHA CRÍTICA: Fonte '{nome_fonte}' está vazia.")
    logger.info("Fonte %s possui %s registros.", nome_fonte, total)


def detectar_nulos_e_rejeitar(
    con: duckdb.DuckDBPyConnection,
    nome_tabela: str,
    bronze_path: Path,
) -> tuple[str, int]:
    """
    Detecta registros com NULL em transaction_id, merchant_id ou amount.
    Registra WARN, grava os registros problemáticos no bronze, e retorna
    uma cláusula SQL para filtrá-los nas queries seguintes.

    Registros com transaction_id NULL são rejeitados (não dá para casar).
    merchant_id e amount NULL geram WARN, mas não rejeitam o registro
    (podem ser úteis para auditoria).

    Args:
        con: Conexão DuckDB.
        nome_tabela: Nome da tabela no DuckDB.
        bronze_path: Caminho da pasta bronze/.

    Returns:
        Tupla com (cláusula WHERE para excluir nulos em transaction_id,
        quantidade de registros rejeitados).
    """
    logger.info("Verificando nulos em colunas críticas de %s...", nome_tabela)

    agora = datetime.now(timezone.utc).isoformat()

    # ---- Nulos em transaction_id → REJEITADOS ----
    nulos_txn = con.execute(f"""
        SELECT count(*) FROM {nome_tabela}
        WHERE transaction_id IS NULL
    """).fetchone()[0]

    if nulos_txn > 0:
        logger.warning(
            "%s registros com transaction_id NULL em %s. Serão rejeitados.",
            nulos_txn,
            nome_tabela,
        )
        # Grava bronze dos rejeitados
        con.execute(f"""
            COPY (
                SELECT
                    *,
                    '{QUALIDADE_NULL_TRANSACTION_ID}' AS qualidade_problema,
                    '{agora}' AS qualidade_timestamp
                FROM {nome_tabela}
                WHERE transaction_id IS NULL
            ) TO '{bronze_path / f'{nome_tabela}_null_transaction_id.parquet'}'
            (FORMAT PARQUET, COMPRESSION 'snappy')
        """)

    # ---- Nulos em merchant_id → WARN apenas ----
    nulos_merch = con.execute(f"""
        SELECT count(*) FROM {nome_tabela}
        WHERE transaction_id IS NOT NULL AND merchant_id IS NULL
    """).fetchone()[0]
    if nulos_merch > 0:
        logger.warning(
            "%s registros com merchant_id NULL em %s. Mantidos, mas registrados no bronze.",
            nulos_merch,
            nome_tabela,
        )
        con.execute(f"""
            COPY (
                SELECT
                    *,
                    '{QUALIDADE_NULL_MERCHANT_ID}' AS qualidade_problema,
                    '{agora}' AS qualidade_timestamp
                FROM {nome_tabela}
                WHERE transaction_id IS NOT NULL AND merchant_id IS NULL
            ) TO '{bronze_path / f'{nome_tabela}_null_merchant_id.parquet'}'
            (FORMAT PARQUET, COMPRESSION 'snappy')
        """)

    # ---- Nulos em amount → WARN apenas ----
    nulos_amt = con.execute(f"""
        SELECT count(*) FROM {nome_tabela}
        WHERE transaction_id IS NOT NULL AND amount IS NULL
    """).fetchone()[0]
    if nulos_amt > 0:
        logger.warning(
            "%s registros com amount NULL em %s. Mantidos, mas registrados no bronze.",
            nulos_amt,
            nome_tabela,
        )
        con.execute(f"""
            COPY (
                SELECT
                    *,
                    '{QUALIDADE_NULL_AMOUNT}' AS qualidade_problema,
                    '{agora}' AS qualidade_timestamp
                FROM {nome_tabela}
                WHERE transaction_id IS NOT NULL AND amount IS NULL
            ) TO '{bronze_path / f'{nome_tabela}_null_amount.parquet'}'
            (FORMAT PARQUET, COMPRESSION 'snappy')
        """)

    # Filtro: remove apenas transaction_id NULL
    filtro = f"{nome_tabela}.transaction_id IS NOT NULL"
    return filtro, nulos_txn


def detectar_duplicatas_e_desduplicar(
    con: duckdb.DuckDBPyConnection,
    nome_tabela: str,
    coluna_id: str,
    coluna_timestamp: str,
    bronze_path: Path,
) -> tuple[str, int]:
    """
    Detecta duplicatas de transaction_id em uma fonte.
    Mantém o registro mais recente (por coluna_timestamp) e descarta os demais.
    Registra WARN e grava os descartados no bronze.

    Args:
        con: Conexão DuckDB.
        nome_tabela: Nome da tabela no DuckDB.
        coluna_id: Nome da coluna de identificador (ex: 'transaction_id').
        coluna_timestamp: Coluna de timestamp para decidir qual manter.
        bronze_path: Caminho da pasta bronze/.

    Returns:
        Tupla com (nome de uma tabela temporária deduplicada,
        quantidade de duplicatas descartadas).
    """
    logger.info("Verificando duplicatas de %s em %s...", coluna_id, nome_tabela)

    agora = datetime.now(timezone.utc).isoformat()

    # Conta duplicatas
    dup_count = con.execute(f"""
        SELECT count(*) FROM (
            SELECT {coluna_id}, count(*) AS cnt
            FROM {nome_tabela}
            WHERE {coluna_id} IS NOT NULL
            GROUP BY {coluna_id}
            HAVING count(*) > 1
        )
    """).fetchone()[0]

    total_duplicatas_descartadas = 0

    if dup_count > 0:
        logger.warning(
            "Encontrados %s transaction_id com duplicatas em %s.",
            dup_count,
            nome_tabela,
        )

        # Cria tabela deduplicada mantendo a mais recente por coluna_timestamp
        tabela_dedup = f"{nome_tabela}_dedup"

        # Primeiro, cria ranked: para cada transaction_id, rank = 1 é o mais recente
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {tabela_dedup} AS
            SELECT * FROM (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY {coluna_id}
                        ORDER BY {coluna_timestamp} DESC NULLS LAST
                    ) AS _rn
                FROM {nome_tabela}
                WHERE {coluna_id} IS NOT NULL
            ) sub
            WHERE _rn = 1
        """)

        # Grava os descartados no bronze
        con.execute(f"""
            COPY (
                SELECT
                    t.*,
                    '{QUALIDADE_DUPLICATA_DESCARTADA}' AS qualidade_problema,
                    '{agora}' AS qualidade_timestamp
                FROM {nome_tabela} t
                WHERE t.{coluna_id} IS NOT NULL
                  AND t.{coluna_id} NOT IN (
                      SELECT {coluna_id} FROM {tabela_dedup}
                  )
            ) TO '{bronze_path / f'{nome_tabela}_duplicatas_descartadas.parquet'}'
            (FORMAT PARQUET, COMPRESSION 'snappy')
        """)

        total_duplicatas_descartadas = con.execute(f"""
            SELECT count(*) FROM {nome_tabela} t
            WHERE t.{coluna_id} IS NOT NULL
              AND t.{coluna_id} NOT IN (
                  SELECT {coluna_id} FROM {tabela_dedup}
              )
        """).fetchone()[0]

        logger.warning(
            "%s registros duplicados descartados em %s.",
            total_duplicatas_descartadas,
            nome_tabela,
        )

        return tabela_dedup, total_duplicatas_descartadas

    # Sem duplicatas: apenas copia a tabela original como temporária
    tabela_dedup = f"{nome_tabela}_dedup"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {tabela_dedup} AS
        SELECT * FROM {nome_tabela} WHERE {coluna_id} IS NOT NULL
    """)
    return tabela_dedup, 0


def gravar_bronze_generico(
    con: duckdb.DuckDBPyConnection,
    tabela_ou_query: str,
    bronze_path: Path,
    nome_arquivo: str,
    problema: str,
    is_query: bool = True,
) -> None:
    """
    Grava registros no bronze com o problema especificado.

    Args:
        con: Conexão DuckDB.
        tabela_ou_query: Nome da tabela ou query SQL completa.
        bronze_path: Caminho da pasta bronze/.
        nome_arquivo: Nome do arquivo parquet de saída.
        problema: Descrição do problema de qualidade.
        is_query: Se True, tabela_ou_query é uma query; se False, é nome de tabela.
    """
    agora = datetime.now(timezone.utc).isoformat()

    if is_query:
        source = f"({tabela_ou_query})"
    else:
        source = tabela_ou_query

    con.execute(f"""
        COPY (
            SELECT
                *,
                '{problema}' AS qualidade_problema,
                '{agora}' AS qualidade_timestamp
            FROM {source}
        ) TO '{bronze_path / nome_arquivo}'
        (FORMAT PARQUET, COMPRESSION 'snappy')
    """)
    logger.info("Bronze: %s registros gravados em %s.", problema, nome_arquivo)