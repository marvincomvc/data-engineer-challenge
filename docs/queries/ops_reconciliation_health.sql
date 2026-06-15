-- ============================================================================
-- OPERAÇÕES — Saúde da reconciliação
-- Fonte: mart_ops_reconciliation_health (sobre fact_reconciliation_daily, grão = por transação)
--        + control_pipeline_run
-- ============================================================================

-- P1: A taxa de discrepância está subindo dia a dia?
SELECT reference_date,
       total_transactions,
       mismatched,
       unreconciled_processor,
       unreconciled_internal,
       round(discrepancy_rate, 4) AS discrepancy_rate
FROM mart_ops_reconciliation_health
ORDER BY reference_date;

-- P2: Alguma execução falhou, demorou, ou rejeitou linhas? (observabilidade)
SELECT reference_date, status, rows_written, rows_rejected, started_at, finished_at
FROM control_pipeline_run
ORDER BY run_id DESC;

-- P3: Quais merchants concentram as discrepâncias num dia? (drill-down do fato diário)
SELECT m.merchant_id, m.legal_name,
       sum(f.mismatched_count + f.unreconciled_processor_count + f.unreconciled_internal_count) AS problemas
FROM fact_reconciliation_daily f
JOIN dim_merchant m ON m.merchant_key = f.merchant_key
WHERE f.reference_date = DATE '2025-03-16'
GROUP BY m.merchant_id, m.legal_name
HAVING problemas > 0
ORDER BY problemas DESC
LIMIT 10;
