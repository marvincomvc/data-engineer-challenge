"""
Orquestrador principal do pipeline de reconciliação.

Recebe uma reference_date e coordena todas as etapas:
1. Leitura das fontes
2. Qualidade de dados + bronze
3. Transformação (dedup CDC, união, janela, tolerância)
4. Carga no modelo analítico

Totalmente procedural — sem classes.
"""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from src.config import (
    DEFAULT_BRONZE_PATH,
    DEFAULT_DATA_PATH,
    DEFAULT_OUTPUT_PATH,
    EXPECTED_COLUMNS_ENTERPRISE_COMPANY,
    EXPECTED_COLUMNS_PAYSETTLER,
    EXPECTED_COLUMNS_RECONCILIATION_RESULTS,
    EXPECTED_COLUMNS_RECONCILIATION_RUNS,
    EXPECTED_COLUMNS_TRANSACTIONS,
    FILE_ENTERPRISE_COMPANY,
    FILE_PAYSETTLER_CSV,
    FILE_RECONCILIATION_RESULTS,
    FILE_RECONCILIATION_RUNS,
    FILE_TRANSACTIONS_BATCH_1,
    FILE_TRANSACTIONS_BATCH_2,
    JANELA_DIAS,
)
from src.model import (
    criar_tabelas_analiticas,
    deletar_da_reference_date,
    popular_agg_daily_reconciliation,
    popular_agg_monthly_financial,
    popular_dim_category,
    popular_dim_date,
    popular_dim_merchant,
    popular_dim_reconciliation_run,
    popular_fct_reconciliation,
)
from src.quality import (
    detectar_duplicatas_e_desduplicar,
    detectar_nulos_e_rejeitar,
    validar_schema,
    verificar_arquivo_vazio,
)
from src.transform import (
    aplicar_tolerancia_e_recalcular,
    deduplicar_cdc,
    unir_transacoes_internas,
)

logger = logging.getLogger(__name__)


def run_pipeline(
    reference_date: str,
    data_path: Path = DEFAULT_DATA_PATH,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    bronze_path: Path = DEFAULT_BRONZE_PATH,
) -> dict:
    """
    Executa o pipeline completo para uma data de referência.

    Args:
        reference_date: Data de referência no formato YYYY-MM-DD.
        data_path: Caminho da pasta com os arquivos de entrada.
        output_path: Caminho do arquivo DuckDB de saída.
        bronze_path: Caminho da pasta bronze/ para registros de qualidade.

    Returns:
        Dicionário com sumário da execução (contagens e tempos).
    """
    inicio_total = time.time()
    sumario = {
        "reference_date": reference_date,
        "inicio": datetime.now(timezone.utc).isoformat(),
        "registros_lidos": {},
        "registros_rejeitados": 0,
        "registros_corrigidos_tolerancia": 0,
        "registros_carregados_fato": 0,
        "tempo_total_segundos": 0.0,
    }

    # Garante que as pastas de saída existam
    output_path.parent.mkdir(parents=True, exist_ok=True)
    bronze_path.mkdir(parents=True, exist_ok=True)

    logger.info("===== INÍCIO DO PIPELINE =====")
    logger.info("Data de referência: %s", reference_date)
    logger.info("Pasta de dados: %s", data_path)
    logger.info("Banco de saída: %s", output_path)
    logger.info("Pasta bronze: %s", bronze_path)

    # ----------------------------------------------------------------
    # ETAPA 1: LEITURA DAS FONTES
    # ----------------------------------------------------------------
    logger.info("--- ETAPA 1: Leitura das fontes ---")
    t1 = time.time()

    # Conecta no banco de destino
    con = duckdb.connect(str(output_path))

    # Cria as tabelas analíticas no banco de destino
    criar_tabelas_analiticas(con)

    # 1a) Transactions batch 1
    path_b1 = data_path / FILE_TRANSACTIONS_BATCH_1
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE src_transactions_batch_1 AS "
        f"SELECT * FROM read_parquet('{path_b1}')"
    )
    n_b1 = con.execute(
        "SELECT count(*) FROM src_transactions_batch_1"
    ).fetchone()[0]
    sumario["registros_lidos"]["transactions_batch_1"] = n_b1
    logger.info("transactions_batch_1: %s registros lidos.", n_b1)

    # 1b) Transactions batch 2
    path_b2 = data_path / FILE_TRANSACTIONS_BATCH_2
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE src_transactions_batch_2 AS "
        f"SELECT * FROM read_parquet('{path_b2}')"
    )
    n_b2 = con.execute(
        "SELECT count(*) FROM src_transactions_batch_2"
    ).fetchone()[0]
    sumario["registros_lidos"]["transactions_batch_2"] = n_b2
    logger.info("transactions_batch_2: %s registros lidos.", n_b2)

    # 1c) Reconciliation runs
    path_runs = data_path / FILE_RECONCILIATION_RUNS
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE src_reconciliation_runs AS "
        f"SELECT * FROM read_parquet('{path_runs}')"
    )
    n_runs = con.execute(
        "SELECT count(*) FROM src_reconciliation_runs"
    ).fetchone()[0]
    sumario["registros_lidos"]["reconciliation_runs"] = n_runs
    logger.info("reconciliation_runs: %s registros lidos.", n_runs)

    # 1d) Reconciliation results
    path_results = data_path / FILE_RECONCILIATION_RESULTS
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE src_reconciliation_results AS "
        f"SELECT * FROM read_parquet('{path_results}')"
    )
    n_results = con.execute(
        "SELECT count(*) FROM src_reconciliation_results"
    ).fetchone()[0]
    sumario["registros_lidos"]["reconciliation_results"] = n_results
    logger.info("reconciliation_results: %s registros lidos.", n_results)

    # 1e) Enterprise company
    path_merch = data_path / FILE_ENTERPRISE_COMPANY
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE src_enterprise_company AS "
        f"SELECT * FROM read_parquet('{path_merch}')"
    )
    n_merch = con.execute(
        "SELECT count(*) FROM src_enterprise_company"
    ).fetchone()[0]
    sumario["registros_lidos"]["enterprise_company"] = n_merch
    logger.info("enterprise_company: %s registros lidos.", n_merch)

    # 1f) PaySettler CSV (com dados sujos: R$, vírgula decimal, etc.)
    path_csv = data_path / FILE_PAYSETTLER_CSV
    # Lê tudo como VARCHAR primeiro, depois limpa e converte no SQL
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE src_paysettler_raw AS "
        f"SELECT * FROM read_csv('{path_csv}', header=true, "
        f"all_varchar=true)"
    )
    # Limpeza dos dados sujos no CSV:
    # - amount: remove "R$ ", trata vírgula como decimal (padrão BR),
    #   mantém ponto decimal do padrão internacional.
    # - settled_at: adiciona 'Z' se não tiver timezone.
    # - merchant_id: uppercase para normalizar.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE src_paysettler AS
        SELECT
            transaction_id,
            UPPER(merchant_id) AS merchant_id,
            CAST(
                CASE
                    WHEN amount LIKE '%,%'
                    THEN REPLACE(REPLACE(REPLACE(amount, 'R$ ', ''), '.', ''), ',', '.')
                    ELSE REPLACE(amount, 'R$ ', '')
                END
                AS DOUBLE
            ) AS amount,
            currency,
            CASE
                WHEN settled_at NOT LIKE '%Z' AND settled_at NOT LIKE '%+%'
                THEN settled_at || 'Z'
                ELSE settled_at
            END AS settled_at,
            processor_reference,
            UPPER(status) AS status
        FROM src_paysettler_raw
        WHERE transaction_id IS NOT NULL
    """)
    con.execute("DROP TABLE IF EXISTS src_paysettler_raw")
    n_csv = con.execute("SELECT count(*) FROM src_paysettler").fetchone()[0]
    sumario["registros_lidos"]["settlement_paysettler"] = n_csv
    logger.info("settlement_paysettler: %s registros lidos (após limpeza).", n_csv)

    logger.info("Etapa 1 concluída em %.2fs.", time.time() - t1)

    # ----------------------------------------------------------------
    # ETAPA 2: QUALIDADE DE DADOS
    # ----------------------------------------------------------------
    logger.info("--- ETAPA 2: Qualidade de dados ---")
    t2 = time.time()

    # 2a) Validação de schemas (crítico: para se falhar)
    logger.info("2a) Validando schemas...")
    validar_schema(con, "src_transactions_batch_1", EXPECTED_COLUMNS_TRANSACTIONS)
    validar_schema(con, "src_transactions_batch_2", EXPECTED_COLUMNS_TRANSACTIONS)
    validar_schema(
        con, "src_reconciliation_runs", EXPECTED_COLUMNS_RECONCILIATION_RUNS
    )
    validar_schema(
        con, "src_reconciliation_results", EXPECTED_COLUMNS_RECONCILIATION_RESULTS
    )
    validar_schema(
        con, "src_enterprise_company", EXPECTED_COLUMNS_ENTERPRISE_COMPANY
    )
    validar_schema(con, "src_paysettler", EXPECTED_COLUMNS_PAYSETTLER)

    # 2b) Verificar arquivos vazios (crítico)
    logger.info("2b) Verificando arquivos vazios...")
    verificar_arquivo_vazio(
        con, "src_transactions_batch_1", "transactions_batch_1"
    )
    verificar_arquivo_vazio(
        con, "src_transactions_batch_2", "transactions_batch_2"
    )
    verificar_arquivo_vazio(con, "src_reconciliation_results", "reconciliation_results")

    # 2c) Detectar nulos e duplicatas nas transações internas
    logger.info("2c) Verificando qualidade nas transações...")
    
    total_rejeitados = 0

    for nome_tabela in ["src_transactions_batch_1", "src_transactions_batch_2"]:
        # Nulos
        _, nulos = detectar_nulos_e_rejeitar(con, nome_tabela, bronze_path)
        total_rejeitados += nulos

        # Duplicatas
        tabela_dedup_name, dups = detectar_duplicatas_e_desduplicar(
            con,
            nome_tabela,
            coluna_id="transaction_id",
            coluna_timestamp="created_at",
            bronze_path=bronze_path,
        )
        # Renomeia a tabela deduplicada para substituir a original
        con.execute(f"DROP TABLE IF EXISTS {nome_tabela}")
        con.execute(f"ALTER TABLE {tabela_dedup_name} RENAME TO {nome_tabela}")

        total_rejeitados += dups

    # 2d) Qualidade no CSV do PaySettler
    _, nulos_csv = detectar_nulos_e_rejeitar(con, "src_paysettler", bronze_path)
    total_rejeitados += nulos_csv

    csv_dedup, dups_csv = detectar_duplicatas_e_desduplicar(
        con,
        "src_paysettler",
        coluna_id="transaction_id",
        coluna_timestamp="settled_at",
        bronze_path=bronze_path,
    )
    con.execute("DROP TABLE IF EXISTS src_paysettler")
    con.execute(f"ALTER TABLE {csv_dedup} RENAME TO src_paysettler")
    total_rejeitados += dups_csv

    sumario["registros_rejeitados"] = total_rejeitados
    logger.info("Total de registros rejeitados: %s", total_rejeitados)
    logger.info("Etapa 2 concluída em %.2fs.", time.time() - t2)

    # ----------------------------------------------------------------
    # ETAPA 3: TRANSFORMAÇÃO
    # ----------------------------------------------------------------
    logger.info("--- ETAPA 3: Transformação ---")
    t3 = time.time()

    # 3a) Deduplicação CDC nos batches de transações
    logger.info("3a) Deduplicação CDC...")
    batch1_clean = deduplicar_cdc(con, "src_transactions_batch_1", bronze_path)
    batch2_clean = deduplicar_cdc(con, "src_transactions_batch_2", bronze_path)

    # 3b) Deduplicação CDC nos reconciliation_results e reconciliation_runs
    results_clean = deduplicar_cdc(con, "src_reconciliation_results", bronze_path)
    runs_clean = deduplicar_cdc(con, "src_reconciliation_runs", bronze_path)
    merchants_clean = deduplicar_cdc(con, "src_enterprise_company", bronze_path)

    # 3c) União das transações internas
    logger.info("3b) Unindo transações internas...")
    transacoes_unificadas = unir_transacoes_internas(con, batch1_clean, batch2_clean)

    # 3d) Construção da tabela fato temporária (join entre results + paysettler)
    logger.info("3c) Construindo tabela fato temporária...")

    # A partir dos reconciliation_results, já temos internal_amount e processor_amount
    # Precisamos apenas recalcular as diferenças e aplicar a janela

    # Cria a fato temporária base com dados dos reconciliation_results
    # + enriquecimento com reference_date vinda dos runs
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE tmp_fct_base AS
        SELECT
            r.id AS reconciliation_result_id,
            r.run_id,
            r.merchant_id,
            r.transaction_id,
            r.category,
            r.internal_amount,
            r.processor_amount,
            r.difference AS difference_original,
            r.created_at AS result_created_at,
            run.reference_date AS run_reference_date
        FROM {results_clean} r
        LEFT JOIN {runs_clean} run ON r.run_id = run.id
        WHERE r.transaction_id IS NOT NULL
    """)

    total_fato_base = con.execute(
        "SELECT count(*) FROM tmp_fct_base"
    ).fetchone()[0]
    logger.info("Fato base: %s registros.", total_fato_base)

    # 3e) Aplicar janela de 7 dias para filtrar transações internas
    logger.info(
        "3d) Aplicando janela de %s dias (reference_date = %s)...",
        JANELA_DIAS,
        reference_date,
    )

    # Filtra a fato base para manter apenas registros cuja reference_date (do run)
    # está dentro da janela em relação à reference_date informada
    # NOTA: a janela se aplica às transações internas. O reconciliation_results
    # já contém a associação com o run, e o run tem a reference_date.
    # Para este pipeline, a janela é aplicada filtrando runs com
    # reference_date entre [ref - 7, ref].

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE tmp_fct_janela AS
        SELECT * FROM tmp_fct_base
        WHERE run_reference_date IS NOT NULL
          AND CAST(run_reference_date AS DATE)
              BETWEEN DATE '{reference_date}' - INTERVAL '{JANELA_DIAS}' DAY
              AND DATE '{reference_date}'
    """)

    total_janela = con.execute(
        "SELECT count(*) FROM tmp_fct_janela"
    ).fetchone()[0]
    logger.info("Registros dentro da janela: %s.", total_janela)

    # 3f) Adiciona reference_date final e created_at do resultado
    con.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_fct_final AS
        SELECT
            reconciliation_result_id,
            run_id,
            merchant_id,
            transaction_id,
            category,
            internal_amount,
            processor_amount,
            CAST(run_reference_date AS DATE) AS reference_date,
            CAST(result_created_at AS TIMESTAMP) AS created_at
        FROM tmp_fct_janela
    """)

    # 3g) Aplicar tolerância e recalcular diferenças
    logger.info("3e) Aplicando tolerância e recalculando diferenças...")
    fato_pronta = aplicar_tolerancia_e_recalcular(
        con, "tmp_fct_final", bronze_path
    )

    logger.info("Etapa 3 concluída em %.2fs.", time.time() - t3)

    # ----------------------------------------------------------------
    # ETAPA 4: CARGA NO MODELO ANALÍTICO
    # ----------------------------------------------------------------
    logger.info("--- ETAPA 4: Carga no modelo analítico ---")
    t4 = time.time()

    # 4a) Idempotência: deleta dados existentes da reference_date
    deletar_da_reference_date(con, reference_date)

    # 4b) Popula dimensões
    popular_dim_category(con)
    popular_dim_merchant(con, merchants_clean)
    popular_dim_reconciliation_run(con, runs_clean)

    # Gera dim_date para o range relevante
    popular_dim_date(
        con,
        data_min=f"{reference_date}",
        data_max=f"{reference_date}",
    )

    # 4c) Popula a fato
    n_fato = popular_fct_reconciliation(con, fato_pronta)
    sumario["registros_carregados_fato"] = n_fato

    # 4d) Popula agregações
    popular_agg_daily_reconciliation(con, reference_date)
    popular_agg_monthly_financial(con, reference_date)

    logger.info("Etapa 4 concluída em %.2fs.", time.time() - t4)

    # ----------------------------------------------------------------
    # FINALIZAÇÃO
    # ----------------------------------------------------------------
    con.close()

    tempo_total = time.time() - inicio_total
    sumario["tempo_total_segundos"] = round(tempo_total, 2)

    logger.info("===== PIPELINE CONCLUÍDO =====")
    logger.info("Tempo total: %.2f segundos.", tempo_total)
    logger.info("Sumário: %s", sumario)

    return sumario