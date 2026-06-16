"""Camada gold: modelo dimensional (Kimball) — dimensões, fatos e marts.

Fatos (separados por processo de medição / granularidade de data):
  - fact_reconciliation_result : grão de transação (1 linha por reconciliation_result) — trilha de auditoria de Compliance.
  - fact_reconciliation_daily  : snapshot periódico, reference_date x merchant x currency — saúde operacional.
                                 Colapsado para uma linha por (transaction_id, run) pra contar por transação.
  - fact_settlement_daily      : snapshot periódico, settlement_date x merchant x currency — volume liquidado pro CFO.
                                 Montado a partir do feed de settlement (1 linha/transação) pra não inflar o volume
                                 espalhando entre as runs de reconciliação.

As surrogate keys são DETERMINÍSTICAS (dense_rank sobre as chaves naturais) pra que os fatos escritos por runs
anteriores continuem apontando pras linhas certas da dimensão depois que uma run posterior reconstrói as dimensões.
Os fatos são escritos com delete+insert transacional por partição (idempotente).
"""
from __future__ import annotations

from pipeline.db import transaction

# Expressões compartilhadas — mantidas em sincronia entre a junk dimension e o join do fato.
_WITHIN_TOL = "(r.difference IS NOT NULL AND abs(r.difference) <= 0.01)"   # |diff| <= R$0,01 (simétrico)
_SETTLE_STATUS = "COALESCE(s.status, 'N/A')"
_HAS_INTERNAL = "(r.internal_amount IS NOT NULL)"
_HAS_PROCESSOR = "(r.processor_amount IS NOT NULL)"
# Quando um (transaction, run) tem várias linhas de resultado, vence a de pior severidade.
_SEVERITY = ("CASE r.category WHEN 'MISMATCHED' THEN 3 "
             "WHEN 'UNRECONCILED_PROCESSOR' THEN 2 WHEN 'UNRECONCILED_INTERNAL' THEN 2 ELSE 1 END")
# SCD2 "como era na época": a versão do merchant válida na reference_date da run.
_ASWAS = """LEFT JOIN dim_merchant m
        ON m.merchant_id = r.merchant_id
       AND CAST(ru.reference_date AS DATE) >= CAST(m.valid_from AS DATE)
       AND (m.valid_to IS NULL OR CAST(ru.reference_date AS DATE) < CAST(m.valid_to AS DATE))"""


def build_dimensions(con) -> None:
    # dim_date — estendida pra cobrir os meses inteiros em volta das datas dos eventos observados, garantindo
    # que toda reference_date / settlement_date dos fatos tenha um date_key (dimensão role-playing conformada).
    con.execute("""
        CREATE OR REPLACE TABLE dim_date AS
        WITH all_dates AS (
            SELECT CAST(reference_date AS DATE) AS d FROM silver_runs
            UNION ALL SELECT CAST(settled_at AS DATE) FROM silver_settlement_current
            UNION ALL SELECT CAST(substr(created_at, 1, 10) AS DATE) FROM silver_transactions
        ),
        span AS (SELECT min(d) AS lo, max(d) AS hi FROM all_dates),
        days AS (
            SELECT CAST(unnest(generate_series(
                date_trunc('month', (SELECT lo FROM span))::TIMESTAMP,
                (last_day((SELECT hi FROM span)))::TIMESTAMP,
                INTERVAL 1 DAY)) AS DATE) AS date
        )
        SELECT CAST(strftime(date, '%Y%m%d') AS INTEGER) AS date_key, date,
               year(date) AS year, quarter(date) AS quarter, month(date) AS month,
               monthname(date) AS month_name, day(date) AS day,
               dayofweek(date) AS day_of_week, (dayofweek(date) IN (0, 6)) AS is_weekend,
               week(date) AS iso_week
        FROM days
    """)

    # dim_merchant — SCD Tipo 2. merchant_key determinística (dense_rank sobre a chave natural)
    # pra que o mesmo merchant_id sempre caia na mesma surrogate entre as runs.
    con.execute("""
        CREATE OR REPLACE TABLE dim_merchant AS
        SELECT dense_rank() OVER (ORDER BY merchant_id, created_at) AS merchant_key,
               merchant_id, legal_name, trade_name, document, primary_cnae,
               CAST(created_at AS TIMESTAMP) AS valid_from,
               CAST(NULL AS TIMESTAMP) AS valid_to, TRUE AS is_current
        FROM silver_merchants
    """)

    # dim_transaction_profile — junk dimension; chave determinística; montada a partir da MESMA
    # relação que o fato faz join (silver_settlement_current) pra toda combinação do fato ter match.
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_transaction_profile AS
        SELECT dense_rank() OVER (
                   ORDER BY category, within_tolerance, settlement_status,
                            has_internal_record, has_processor_record) AS profile_key,
               category, within_tolerance, settlement_status, has_internal_record, has_processor_record
        FROM (
            SELECT DISTINCT
                r.category AS category,
                {_WITHIN_TOL} AS within_tolerance,
                {_SETTLE_STATUS} AS settlement_status,
                {_HAS_INTERNAL} AS has_internal_record,
                {_HAS_PROCESSOR} AS has_processor_record
            FROM silver_results r
            LEFT JOIN silver_settlement_current s ON s.transaction_id = r.transaction_id
        )
    """)


def init_facts(con) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_reconciliation_result (
            result_id BIGINT, transaction_id VARCHAR, reconciliation_run_id BIGINT, processor_reference VARCHAR,
            merchant_key BIGINT, reference_date DATE, settlement_date DATE, profile_key BIGINT,
            category VARCHAR, within_tolerance BOOLEAN, settlement_status VARCHAR,
            has_internal_record BOOLEAN, has_processor_record BOOLEAN,
            internal_amount DECIMAL(18,2), processor_amount DECIMAL(18,2),
            settled_amount DECIMAL(18,2), difference DECIMAL(18,2),
            is_matched INTEGER, is_mismatched INTEGER, is_unreconciled INTEGER
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_reconciliation_daily (
            reference_date DATE, merchant_key BIGINT, currency VARCHAR,
            total_transactions BIGINT, matched_count BIGINT, mismatched_count BIGINT,
            unreconciled_processor_count BIGINT, unreconciled_internal_count BIGINT,
            discrepancy_rate DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_settlement_daily (
            settlement_date DATE, merchant_key BIGINT, currency VARCHAR,
            settled_count BIGINT, reversed_count BIGINT,
            total_settled_amount DECIMAL(18,2), total_reversed_amount DECIMAL(18,2),
            net_settled_amount DECIMAL(18,2), avg_ticket DECIMAL(18,2)
        )
    """)


def build_reconciliation_facts(con, reference_date: str) -> None:
    """Fato de auditoria (por resultado) + fato diário de Ops (por transação), pra uma reference_date."""
    with transaction(con):
        # --- trilha de auditoria: uma linha por reconciliation_result ---
        con.execute("DELETE FROM fact_reconciliation_result WHERE reference_date = CAST(? AS DATE)",
                    [reference_date])
        con.execute(f"""
            INSERT INTO fact_reconciliation_result
            SELECT
                r.id AS result_id, r.transaction_id, r.run_id, s.processor_reference,
                m.merchant_key, CAST(ru.reference_date AS DATE), CAST(s.settled_at AS DATE),
                p.profile_key, r.category,
                {_WITHIN_TOL}, {_SETTLE_STATUS}, {_HAS_INTERNAL}, {_HAS_PROCESSOR},
                r.internal_amount, r.processor_amount, s.settled_amount, r.difference,
                (r.category = 'MATCHED')::INTEGER,
                (r.category = 'MISMATCHED')::INTEGER,
                (r.category LIKE 'UNRECONCILED%')::INTEGER
            FROM silver_results r
            JOIN silver_runs ru ON ru.id = r.run_id
            {_ASWAS}
            LEFT JOIN silver_settlement_current s ON s.transaction_id = r.transaction_id
            LEFT JOIN dim_transaction_profile p
                   ON p.category = r.category
                  AND p.within_tolerance = {_WITHIN_TOL}
                  AND p.settlement_status = {_SETTLE_STATUS}
                  AND p.has_internal_record = {_HAS_INTERNAL}
                  AND p.has_processor_record = {_HAS_PROCESSOR}
            WHERE ru.reference_date = ?
        """, [reference_date])

        # --- saúde diária de Ops: colapsa pra uma linha por (transaction_id, run), vencendo a pior severidade ---
        con.execute("DELETE FROM fact_reconciliation_daily WHERE reference_date = CAST(? AS DATE)",
                    [reference_date])
        con.execute(f"""
            INSERT INTO fact_reconciliation_daily
            WITH per_txn AS (
                SELECT CAST(ru.reference_date AS DATE) AS reference_date,
                       m.merchant_key,
                       COALESCE(tx.currency, s.currency, 'UNKNOWN') AS currency,
                       r.category,
                       row_number() OVER (PARTITION BY r.transaction_id, r.run_id
                                          ORDER BY {_SEVERITY} DESC, r.id DESC) AS rn
                FROM silver_results r
                JOIN silver_runs ru ON ru.id = r.run_id
                {_ASWAS}
                LEFT JOIN silver_transactions tx ON tx.transaction_id = r.transaction_id
                LEFT JOIN silver_settlement_current s ON s.transaction_id = r.transaction_id
                WHERE ru.reference_date = ?
            )
            SELECT reference_date, merchant_key, currency,
                   count(*) AS total_transactions,
                   count(*) FILTER (WHERE category = 'MATCHED') AS matched_count,
                   count(*) FILTER (WHERE category = 'MISMATCHED') AS mismatched_count,
                   count(*) FILTER (WHERE category = 'UNRECONCILED_PROCESSOR') AS unreconciled_processor_count,
                   count(*) FILTER (WHERE category = 'UNRECONCILED_INTERNAL') AS unreconciled_internal_count,
                   1.0 - (count(*) FILTER (WHERE category = 'MATCHED'))::DOUBLE / count(*) AS discrepancy_rate
            FROM per_txn
            WHERE rn = 1
            GROUP BY reference_date, merchant_key, currency
        """, [reference_date])


def build_settlement_facts(con) -> None:
    """Volume liquidado pro CFO por data de SETTLEMENT — vindo do feed de settlement (sem inflar entre runs)."""
    with transaction(con):
        con.execute("""
            DELETE FROM fact_settlement_daily WHERE settlement_date IN (
                SELECT DISTINCT CAST(settled_at AS DATE) FROM silver_settlement_current
            )
        """)
        con.execute("""
            INSERT INTO fact_settlement_daily
            SELECT
                CAST(s.settled_at AS DATE) AS settlement_date,
                m.merchant_key,
                COALESCE(s.currency, 'UNKNOWN') AS currency,
                count(*) FILTER (WHERE s.status = 'SETTLED') AS settled_count,
                count(*) FILTER (WHERE s.status = 'REVERSED') AS reversed_count,
                sum(s.settled_amount) FILTER (WHERE s.status = 'SETTLED') AS total_settled_amount,
                sum(s.settled_amount) FILTER (WHERE s.status = 'REVERSED') AS total_reversed_amount,
                COALESCE(sum(s.settled_amount) FILTER (WHERE s.status = 'SETTLED'), 0)
                    - COALESCE(sum(s.settled_amount) FILTER (WHERE s.status = 'REVERSED'), 0) AS net_settled_amount,
                sum(s.settled_amount) FILTER (WHERE s.status = 'SETTLED')
                    / nullif(count(*) FILTER (WHERE s.status = 'SETTLED'), 0) AS avg_ticket
            FROM silver_settlement_current s
            LEFT JOIN dim_merchant m ON m.merchant_id = s.merchant_id AND m.is_current
            GROUP BY CAST(s.settled_at AS DATE), m.merchant_key, COALESCE(s.currency, 'UNKNOWN')
        """)


def build_marts(con) -> None:
    con.execute("""
        CREATE OR REPLACE TABLE mart_ops_reconciliation_health AS
        SELECT reference_date,
               sum(total_transactions) AS total_transactions,
               sum(matched_count) AS matched,
               sum(mismatched_count) AS mismatched,
               sum(unreconciled_processor_count) AS unreconciled_processor,
               sum(unreconciled_internal_count) AS unreconciled_internal,
               1.0 - sum(matched_count)::DOUBLE / nullif(sum(total_transactions), 0) AS discrepancy_rate
        FROM fact_reconciliation_daily
        GROUP BY reference_date ORDER BY reference_date
    """)
    con.execute("""
        CREATE OR REPLACE TABLE mart_cfo_settled_volume AS
        SELECT f.settlement_date, f.currency, m.primary_cnae,
               sum(f.total_settled_amount) AS total_settled_amount,
               sum(f.total_reversed_amount) AS total_reversed_amount,
               sum(f.net_settled_amount) AS net_settled_amount,
               sum(f.settled_count) AS settled_count,
               sum(f.total_settled_amount) / nullif(sum(f.settled_count), 0) AS avg_ticket
        FROM fact_settlement_daily f
        LEFT JOIN dim_merchant m ON m.merchant_key = f.merchant_key
        GROUP BY f.settlement_date, f.currency, m.primary_cnae
        ORDER BY f.settlement_date
    """)
    con.execute("""
        CREATE OR REPLACE TABLE mart_compliance_audit_trail AS
        SELECT f.result_id, f.transaction_id, f.reconciliation_run_id, f.reference_date, f.settlement_date,
               m.merchant_id, m.legal_name, m.document, f.category, f.within_tolerance,
               f.internal_amount, f.processor_amount, f.settled_amount, f.difference,
               f.settlement_status, f.processor_reference
        FROM fact_reconciliation_result f
        LEFT JOIN dim_merchant m ON m.merchant_key = f.merchant_key
    """)
