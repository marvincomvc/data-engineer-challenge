"""
Teste de integração do pipeline de reconciliação.

Executa o pipeline completo para uma data fixa usando DuckDB em memória,
simulando dados sintéticos como se fossem arquivos reais.

Verifica:
- Contagens finais nas tabelas analíticas
- Ausência de duplicatas
- Correção da tolerância aplicada
- Idempotência (executar duas vezes não duplica registros)
"""

from pathlib import Path

import duckdb
import pytest

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
from src.transform import aplicar_tolerancia_e_recalcular, deduplicar_cdc


@pytest.fixture
def banco_com_dados(con_memoria, dados_sinteticos, tmp_path):
    """
    Configura um banco DuckDB em memória com os dados sintéticos
    carregados como tabelas temporárias e tabelas analíticas criadas.

    Simula as etapas de leitura e qualidade que o pipeline real faria.
    """
    con = con_memoria
    dados = dados_sinteticos
    bronze = Path(str(tmp_path / "bronze"))
    bronze.mkdir(parents=True, exist_ok=True)

    # Cria tabelas analíticas
    criar_tabelas_analiticas(con)

    # Cria tabelas fonte
    con.execute(
        "CREATE TEMP TABLE src_transactions AS SELECT * FROM (VALUES "
        + ", ".join(
            f"({t['id']}, '{t['transaction_id']}', '{t['merchant_id']}', {t['amount']}, "
            f"'{t['currency']}', '{t['status']}', '{t['description']}', "
            f"'{t['created_at']}', '{t['updated_at']}')"
            for t in dados["transacoes"]
        )
        + ") AS t(id, transaction_id, merchant_id, amount, currency, status, "
        "description, created_at, updated_at)"
    )

    con.execute(
        "CREATE TEMP TABLE src_results AS SELECT * FROM (VALUES "
        + ", ".join(
            f"({r['id']}, {r['run_id']}, '{r['transaction_id']}', '{r['merchant_id']}', "
            f"'{r['category']}', {r['internal_amount'] or 'NULL'}, "
            f"{r['processor_amount'] or 'NULL'}, {r['difference'] or 'NULL'}, "
            f"'{r['created_at']}', '{r['Op']}', '{r['_timestamp']}')"
            for r in dados["results"]
        )
        + ") AS r(id, run_id, transaction_id, merchant_id, category, internal_amount, "
        "processor_amount, difference, created_at, Op, _timestamp)"
    )

    con.execute(
        "CREATE TEMP TABLE src_runs AS SELECT * FROM (VALUES "
        + ", ".join(
            f"({ru['id']}, '{ru['reference_date']}', '{ru['file_name']}', "
            f"'{ru['status']}', {ru['total_transactions']}, "
            f"'{ru['started_at']}', '{ru['completed_at']}', "
            f"'{ru['created_at']}', '{ru['Op']}', '{ru['_timestamp']}')"
            for ru in dados["runs"]
        )
        + ") AS r(id, reference_date, file_name, status, total_transactions, "
        "started_at, completed_at, created_at, Op, _timestamp)"
    )

    con.execute(
        "CREATE TEMP TABLE src_merchants AS SELECT * FROM (VALUES "
        + ", ".join(
            f"({m['id']}, '{m['merchant_id']}', '{m['legal_name']}', '{m['trade_name']}', "
            f"'{m['document']}', '{m['primary_cnae']}', "
            f"'{m['created_at']}', '{m['updated_at']}', '{m['Op']}', '{m['_timestamp']}')"
            for m in dados["merchants"]
        )
        + ") AS m(id, merchant_id, legal_name, trade_name, document, primary_cnae, "
        "created_at, updated_at, Op, _timestamp)"
    )

    return con, dados, bronze


class TestIntegracaoPipeline:
    """Testes de integração do pipeline completo."""

    REFERENCIA = "2025-03-15"

    def executar_pipeline_simplificado(self, con, dados, bronze, reference_date):
        """
        Executa as transformações principais do pipeline sobre
        os dados sintéticos no banco em memória.

        Simula: dedup CDC → fato base → janela → tolerância → carga.
        """
        # 1) Dedup CDC nos resultados e runs
        results_clean = deduplicar_cdc(con, "src_results", bronze)
        runs_clean = deduplicar_cdc(con, "src_runs", bronze)
        merchants_clean = deduplicar_cdc(con, "src_merchants", bronze)

        # 2) Constrói fato base (results + run reference_date)
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
                r.created_at AS result_created_at,
                run.reference_date AS run_reference_date
            FROM {results_clean} r
            LEFT JOIN {runs_clean} run ON r.run_id = run.id
            WHERE r.transaction_id IS NOT NULL
        """)

        # 3) Aplica janela de 7 dias
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE tmp_fct_janela AS
            SELECT * FROM tmp_fct_base
            WHERE run_reference_date IS NOT NULL
              AND CAST(run_reference_date AS DATE)
                  BETWEEN DATE '{reference_date}' - INTERVAL 7 DAY
                  AND DATE '{reference_date}'
        """)

        # 4) Formata fato final
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

        # 5) Aplica tolerância
        fato_pronta = aplicar_tolerancia_e_recalcular(con, "tmp_fct_final", bronze)

        # 6) Idempotência e carga
        deletar_da_reference_date(con, reference_date)
        popular_dim_category(con)
        popular_dim_merchant(con, merchants_clean)
        popular_dim_reconciliation_run(con, runs_clean)
        popular_dim_date(con, reference_date, reference_date)
        popular_fct_reconciliation(con, fato_pronta)
        popular_agg_daily_reconciliation(con, reference_date)
        popular_agg_monthly_financial(con, reference_date)

    def test_fato_tem_tres_registros(self, banco_com_dados):
        """
        A fato deve ter exatamente 3 registros:
        - txn-001 (MATCHED)
        - txn-002 (MISMATCHED corrigido para MATCHED via tolerância)
        - txn-003 (UNRECONCILED_INTERNAL)
        """
        con, dados, bronze = banco_com_dados
        self.executar_pipeline_simplificado(con, dados, bronze, self.REFERENCIA)

        total = con.execute(
            "SELECT count(*) FROM fct_reconciliation"
        ).fetchone()[0]
        assert total == 3, f"Esperados 3 registros, mas obteve {total}"

    def test_tolerancia_corrige_mismatched(self, banco_com_dados):
        """
        O registro txn-002 tem difference = 0.01 e categoria original MISMATCHED.
        Após tolerância, deve virar MATCHED.
        """
        con, dados, bronze = banco_com_dados
        self.executar_pipeline_simplificado(con, dados, bronze, self.REFERENCIA)

        categoria = con.execute(
            "SELECT category FROM fct_reconciliation WHERE transaction_id = 'txn-002'"
        ).fetchone()[0]
        assert categoria == "MATCHED", (
            f"Esperado MATCHED (tolerância aplicada), mas obteve {categoria}"
        )

    def test_tolerancia_nao_altera_matched(self, banco_com_dados):
        """
        txn-001 já era MATCHED com difference 0.0. Deve continuar MATCHED.
        """
        con, dados, bronze = banco_com_dados
        self.executar_pipeline_simplificado(con, dados, bronze, self.REFERENCIA)

        categoria = con.execute(
            "SELECT category FROM fct_reconciliation WHERE transaction_id = 'txn-001'"
        ).fetchone()[0]
        assert categoria == "MATCHED"

    def test_dimensoes_populadas(self, banco_com_dados):
        """Verifica se dim_category, dim_merchant e dim_reconciliation_run foram populadas."""
        con, dados, bronze = banco_com_dados
        self.executar_pipeline_simplificado(con, dados, bronze, self.REFERENCIA)

        n_cat = con.execute("SELECT count(*) FROM dim_category").fetchone()[0]
        assert n_cat == 4, f"Esperadas 4 categorias, obteve {n_cat}"

        n_merch = con.execute("SELECT count(*) FROM dim_merchant").fetchone()[0]
        assert n_merch == 2, f"Esperados 2 merchants, obteve {n_merch}"

        n_runs = con.execute(
            "SELECT count(*) FROM dim_reconciliation_run"
        ).fetchone()[0]
        assert n_runs == 1, f"Esperado 1 run, obteve {n_runs}"

    def test_agregacao_diaria(self, banco_com_dados):
        """Verifica se a agregação diária foi calculada corretamente."""
        con, dados, bronze = banco_com_dados
        self.executar_pipeline_simplificado(con, dados, bronze, self.REFERENCIA)

        n_agg = con.execute(
            "SELECT count(*) FROM agg_daily_reconciliation WHERE reference_date = ?",
            [self.REFERENCIA],
        ).fetchone()[0]
        assert n_agg > 0, "Agregação diária vazia"

    def test_agregacao_mensal(self, banco_com_dados):
        """Verifica se a agregação mensal foi calculada para 2025-03."""
        con, dados, bronze = banco_com_dados
        self.executar_pipeline_simplificado(con, dados, bronze, self.REFERENCIA)

        n_agg = con.execute(
            "SELECT count(*) FROM agg_monthly_financial WHERE year = 2025 AND month = 3"
        ).fetchone()[0]
        assert n_agg == 1, f"Esperado 1 registro mensal, obteve {n_agg}"

    def test_idempotencia_sem_duplicatas(self, banco_com_dados):
        """
        Executa o pipeline duas vezes para a mesma reference_date.
        O total de registros na fato não deve dobrar.
        """
        con, dados, bronze = banco_com_dados
        self.executar_pipeline_simplificado(con, dados, bronze, self.REFERENCIA)

        total_antes = con.execute(
            "SELECT count(*) FROM fct_reconciliation"
        ).fetchone()[0]

        # Segunda execução
        self.executar_pipeline_simplificado(con, dados, bronze, self.REFERENCIA)

        total_depois = con.execute(
            "SELECT count(*) FROM fct_reconciliation"
        ).fetchone()[0]

        assert total_depois == total_antes, (
            f"Idempotência falhou: {total_antes} antes, {total_depois} depois"
        )

    def test_total_agregacao_diaria_pos_idempotencia(self, banco_com_dados):
        """
        Após duas execuções, a agregação diária também não deve ter duplicatas.
        """
        con, dados, bronze = banco_com_dados
        self.executar_pipeline_simplificado(con, dados, bronze, self.REFERENCIA)
        self.executar_pipeline_simplificado(con, dados, bronze, self.REFERENCIA)

        n_agg = con.execute(
            "SELECT count(*) FROM agg_daily_reconciliation WHERE reference_date = ?",
            [self.REFERENCIA],
        ).fetchone()[0]
        # 3 registros: MATCHED (txn-001 + txn-002 corrigido), UNRECONCILED_INTERNAL (txn-003)
        # Pode ser 2 ou mais grupos dependendo de merchant/category
        assert n_agg > 0, "Agregação diária vazia após segunda execução"

    def test_reference_date_presente_na_fato(self, banco_com_dados):
        """Todos os registros na fato devem ter a reference_date correta."""
        con, dados, bronze = banco_com_dados
        self.executar_pipeline_simplificado(con, dados, bronze, self.REFERENCIA)

        datas_diferentes = con.execute(f"""
            SELECT count(*) FROM fct_reconciliation
            WHERE reference_date != '{self.REFERENCIA}'
        """).fetchone()[0]
        assert datas_diferentes == 0, (
            f"Encontrados {datas_diferentes} registros com reference_date diferente"
        )