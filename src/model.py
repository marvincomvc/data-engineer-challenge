"""
Modelagem analítica: criação e carga das tabelas fato, dimensões e agregações.

Implementa o modelo dimensional definido no enunciado:
- fct_reconciliation (fato)
- dim_reconciliation_run, dim_merchant, dim_date, dim_category (dimensões)
- agg_daily_reconciliation, agg_monthly_financial (agregações)

Todas as operações são feitas diretamente via DuckDB.
"""

import logging
from pathlib import Path

import duckdb

from src.config import (
    CATEGORIA_DESCRICOES,
    CATEGORIAS_FIXAS,
)

logger = logging.getLogger(__name__)


def criar_tabelas_analiticas(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """
    Cria as tabelas do modelo analítico (DDL) se não existirem.

    Cria no schema main (padrão do DuckDB):
    - dim_date, dim_category, dim_merchant, dim_reconciliation_run
    - fct_reconciliation
    - agg_daily_reconciliation, agg_monthly_financial

    Args:
        con: Conexão DuckDB no banco de destino.
    """
    logger.info("Criando tabelas analíticas (se não existirem)...")

    # ---- Dimensão: dim_date ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id      DATE PRIMARY KEY,
            year         INTEGER NOT NULL,
            month        INTEGER NOT NULL,
            day          INTEGER NOT NULL,
            quarter      INTEGER NOT NULL,
            day_of_week  INTEGER NOT NULL,
            is_weekend   BOOLEAN NOT NULL
        )
    """)

    # ---- Dimensão: dim_category ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_category (
            category_code  VARCHAR PRIMARY KEY,
            category_name  VARCHAR NOT NULL,
            description    VARCHAR
        )
    """)

    # ---- Dimensão: dim_merchant (SCD Tipo 1) ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_merchant (
            merchant_id   VARCHAR PRIMARY KEY,
            legal_name    VARCHAR,
            trade_name    VARCHAR,
            document      VARCHAR,
            primary_cnae  VARCHAR,
            created_at    TIMESTAMP,
            updated_at    TIMESTAMP
        )
    """)

    # ---- Dimensão: dim_reconciliation_run ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_reconciliation_run (
            run_id              BIGINT PRIMARY KEY,
            reference_date      DATE NOT NULL,
            file_name           VARCHAR,
            status              VARCHAR,
            started_at          TIMESTAMP,
            completed_at        TIMESTAMP,
            total_transactions  INTEGER,
            created_at          TIMESTAMP
        )
    """)

    # ---- Fato: fct_reconciliation ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS fct_reconciliation (
            reconciliation_result_id  BIGINT PRIMARY KEY,
            run_id                    BIGINT NOT NULL,
            merchant_id               VARCHAR,
            transaction_id            VARCHAR NOT NULL,
            category                  VARCHAR NOT NULL,
            internal_amount           DOUBLE,
            processor_amount          DOUBLE,
            difference_abs            DOUBLE,
            difference_signed         DOUBLE,
            reference_date            DATE NOT NULL,
            created_at                TIMESTAMP
        )
    """)

    # ---- Agregação diária ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS agg_daily_reconciliation (
            reference_date    DATE NOT NULL,
            merchant_id       VARCHAR NOT NULL,
            category          VARCHAR NOT NULL,
            total_transactions BIGINT NOT NULL,
            sum_internal      DOUBLE,
            sum_processor     DOUBLE,
            sum_diff_abs      DOUBLE,
            PRIMARY KEY (reference_date, merchant_id, category)
        )
    """)

    # ---- Agregação mensal financeira ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS agg_monthly_financial (
            year                 INTEGER NOT NULL,
            month                INTEGER NOT NULL,
            total_processor_volume  DOUBLE,
            total_internal_volume   DOUBLE,
            total_diff              DOUBLE,
            PRIMARY KEY (year, month)
        )
    """)

    logger.info("Tabelas analíticas verificadas/criadas com sucesso.")


def popular_dim_category(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """
    Popula a dimensão dim_category com as 4 categorias fixas de reconciliação.

    Usa INSERT OR REPLACE para idempotência.
    """
    logger.info("Populando dim_category...")

    for codigo in CATEGORIAS_FIXAS:
        descricao = CATEGORIA_DESCRICOES.get(codigo, "")
        con.execute(
            """
            INSERT OR REPLACE INTO dim_category (category_code, category_name, description)
            VALUES (?, ?, ?)
            """,
            [codigo, codigo, descricao],
        )

    logger.info("dim_category populada com %s categorias.", len(CATEGORIAS_FIXAS))


def popular_dim_date(
    con: duckdb.DuckDBPyConnection,
    data_min: str,
    data_max: str,
) -> None:
    """
    Gera dinamicamente a dimensão dim_date para todas as datas
    entre data_min e data_max (inclusive).

    Args:
        con: Conexão DuckDB.
        data_min: Data mínima no formato YYYY-MM-DD.
        data_max: Data máxima no formato YYYY-MM-DD.
    """
    logger.info("Gerando dim_date de %s a %s...", data_min, data_max)

    con.execute(f"""
        INSERT OR IGNORE INTO dim_date
        SELECT
            date_col AS date_id,
            EXTRACT(YEAR FROM date_col)::INTEGER AS year,
            EXTRACT(MONTH FROM date_col)::INTEGER AS month,
            EXTRACT(DAY FROM date_col)::INTEGER AS day,
            EXTRACT(QUARTER FROM date_col)::INTEGER AS quarter,
            EXTRACT(DAYOFWEEK FROM date_col)::INTEGER AS day_of_week,
            (EXTRACT(DAYOFWEEK FROM date_col) IN (0, 6)) AS is_weekend
        FROM generate_series(
            DATE '{data_min}',
            DATE '{data_max}',
            INTERVAL '1 day'
        ) AS t(date_col)
    """)

    total = con.execute("SELECT count(*) FROM dim_date").fetchone()[0]
    logger.info("dim_date: %s datas geradas.", total)


def popular_dim_merchant(
    con: duckdb.DuckDBPyConnection,
    fonte_merchants: str,
) -> None:
    """
    Popula dim_merchant com SCD Tipo 1 (sobrescreve se merchant_id já existe).

    Args:
        con: Conexão DuckDB.
        fonte_merchants: Nome da tabela DuckDB com os dados de enterprise_company.
    """
    logger.info("Populando dim_merchant...")

    con.execute(f"""
        INSERT OR REPLACE INTO dim_merchant
        SELECT
            merchant_id,
            legal_name,
            trade_name,
            document,
            primary_cnae,
            CAST(created_at AS TIMESTAMP) AS created_at,
            CAST(updated_at AS TIMESTAMP) AS updated_at
        FROM {fonte_merchants}
        WHERE merchant_id IS NOT NULL
    """)

    total = con.execute("SELECT count(*) FROM dim_merchant").fetchone()[0]
    logger.info("dim_merchant: %s merchants carregados.", total)


def popular_dim_reconciliation_run(
    con: duckdb.DuckDBPyConnection,
    fonte_runs: str,
) -> None:
    """
    Popula dim_reconciliation_run a partir dos dados de reconciliation_runs.

    Mantém apenas a versão COMPLETED (Op='U') para cada run_id,
    pois é o estado final.

    Args:
        con: Conexão DuckDB.
        fonte_runs: Nome da tabela com reconciliation_runs (já dedup CDC).
    """
    logger.info("Populando dim_reconciliation_run...")

    con.execute(f"""
        INSERT OR REPLACE INTO dim_reconciliation_run
        SELECT
            id AS run_id,
            CAST(reference_date AS DATE) AS reference_date,
            file_name,
            status,
            CAST(started_at AS TIMESTAMP) AS started_at,
            CAST(completed_at AS TIMESTAMP) AS completed_at,
            total_transactions,
            CAST(created_at AS TIMESTAMP) AS created_at
        FROM {fonte_runs}
        WHERE status = 'COMPLETED'
    """)

    total = con.execute(
        "SELECT count(*) FROM dim_reconciliation_run"
    ).fetchone()[0]
    logger.info("dim_reconciliation_run: %s runs carregados.", total)


def deletar_da_reference_date(
    con: duckdb.DuckDBPyConnection,
    reference_date: str,
) -> None:
    """
    Garante idempotência: deleta todos os registros da fato e das
    agregações vinculados a uma reference_date antes de reinserir.

    Args:
        con: Conexão DuckDB no banco de destino.
        reference_date: Data de referência no formato YYYY-MM-DD.
    """
    logger.info(
        "Garantindo idempotência para reference_date = %s...",
        reference_date,
    )

    # Deleta da fato (usa interpolação f-string pois DATE com ? pode falhar)
    con.execute(f"""
        DELETE FROM fct_reconciliation
        WHERE reference_date = DATE '{reference_date}'
    """)
    logger.info("Registros removidos da fato para a data %s.", reference_date)

    # Deleta da agregação diária
    con.execute(f"""
        DELETE FROM agg_daily_reconciliation
        WHERE reference_date = DATE '{reference_date}'
    """)
    logger.info("Registros removidos da agg_daily para a data %s.", reference_date)

    # Para agregação mensal, deletamos se não houver dados na fato
    # para aquele mês (recalcularemos depois)
    logger.info("Idempotência garantida para %s.", reference_date)


def popular_fct_reconciliation(
    con: duckdb.DuckDBPyConnection,
    tabela_fato_temporaria: str,
) -> int:
    """
    Insere os dados da tabela fato temporária na fct_reconciliation final.

    Args:
        con: Conexão DuckDB.
        tabela_fato_temporaria: Nome da tabela temporária com os dados prontos.

    Returns:
        Número de registros inseridos.
    """
    logger.info("Inserindo dados na fct_reconciliation...")

    # O banco de destino pode ser diferente da conexão atual se estivermos
    # usando dois bancos. Assumimos que a tabela temporária está na mesma
    # conexão que as tabelas analíticas.
    con.execute(f"""
        INSERT OR REPLACE INTO fct_reconciliation
        SELECT
            reconciliation_result_id,
            run_id,
            merchant_id,
            transaction_id,
            category,
            internal_amount,
            processor_amount,
            difference_abs,
            difference_signed,
            reference_date,
            created_at
        FROM {tabela_fato_temporaria}
        WHERE transaction_id IS NOT NULL
    """)

    total = con.execute(
        "SELECT count(*) FROM fct_reconciliation"
    ).fetchone()[0]
    logger.info("fct_reconciliation: %s registros no total.", total)
    return total


def popular_agg_daily_reconciliation(
    con: duckdb.DuckDBPyConnection,
    reference_date: str,
) -> int:
    """
    Calcula e insere a agregação diária para a reference_date informada.

    Agrupa por reference_date, merchant_id, category e calcula:
    - total_transactions (count)
    - sum_internal (soma de internal_amount)
    - sum_processor (soma de processor_amount)
    - sum_diff_abs (soma de difference_abs)

    Args:
        con: Conexão DuckDB.
        reference_date: Data de referência.

    Returns:
        Número de registros inseridos.
    """
    logger.info("Calculando agg_daily_reconciliation para %s...", reference_date)

    con.execute(
        """
        INSERT INTO agg_daily_reconciliation
        SELECT
            reference_date,
            merchant_id,
            category,
            count(*)::BIGINT AS total_transactions,
            SUM(internal_amount) AS sum_internal,
            SUM(processor_amount) AS sum_processor,
            SUM(difference_abs) AS sum_diff_abs
        FROM fct_reconciliation
        WHERE reference_date = ?
        GROUP BY reference_date, merchant_id, category
        """,
        [reference_date],
    )

    total = con.execute(
        """
        SELECT count(*) FROM agg_daily_reconciliation
        WHERE reference_date = ?
        """,
        [reference_date],
    ).fetchone()[0]
    logger.info("agg_daily_reconciliation: %s grupos inseridos.", total)
    return total


def popular_agg_monthly_financial(
    con: duckdb.DuckDBPyConnection,
    reference_date: str,
) -> None:
    """
    Recalcula a agregação mensal financeira para o mês da reference_date.

    Para garantir consistência, recalcula o mês inteiro (não apenas a data)
    usando DELETE + INSERT.

    Agrupa por year, month e calcula:
    - total_processor_volume (soma de processor_amount)
    - total_internal_volume (soma de internal_amount)
    - total_diff (soma de difference_abs)

    Args:
        con: Conexão DuckDB.
        reference_date: Data de referência para identificar o mês.
    """
    logger.info(
        "Recalculando agg_monthly_financial para o mês de %s...",
        reference_date,
    )

    # Extrai ano e mês da reference_date
    ano_mes = con.execute(f"""
        SELECT EXTRACT(YEAR FROM DATE '{reference_date}'),
               EXTRACT(MONTH FROM DATE '{reference_date}')
    """).fetchone()
    ano, mes = int(ano_mes[0]), int(ano_mes[1])

    # Deleta agregações do mês
    con.execute(
        "DELETE FROM agg_monthly_financial WHERE year = ? AND month = ?",
        [ano, mes],
    )

    # Reinsere
    con.execute(
        """
        INSERT INTO agg_monthly_financial
        SELECT
            EXTRACT(YEAR FROM reference_date)::INTEGER AS year,
            EXTRACT(MONTH FROM reference_date)::INTEGER AS month,
            SUM(processor_amount) AS total_processor_volume,
            SUM(internal_amount) AS total_internal_volume,
            SUM(difference_abs) AS total_diff
        FROM fct_reconciliation
        WHERE EXTRACT(YEAR FROM reference_date) = ?
          AND EXTRACT(MONTH FROM reference_date) = ?
        GROUP BY year, month
        """,
        [ano, mes],
    )

    total = con.execute(
        "SELECT count(*) FROM agg_monthly_financial WHERE year = ? AND month = ?",
        [ano, mes],
    ).fetchone()[0]
    logger.info("agg_monthly_financial: %s registros para %s-%s.", total, ano, mes)