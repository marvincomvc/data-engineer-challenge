-- ============================================================================
-- CFO — Volume financeiro liquidado
-- Fonte: mart_cfo_settled_volume (sobre fact_settlement_daily)
-- Observação: temos volume LIQUIDADO, mas não fees/receita (fora do escopo dos dados).
-- ============================================================================

-- P1: Volume liquidado e revertido por dia e moeda
SELECT reference_date, currency,
       sum(total_settled_amount)   AS settled,
       sum(total_reversed_amount)  AS reversed,
       sum(transactions)           AS txns
FROM mart_cfo_settled_volume
GROUP BY reference_date, currency
ORDER BY reference_date, currency;

-- P2: Top setores (CNAE) por volume liquidado no período
SELECT primary_cnae,
       sum(total_settled_amount) AS settled,
       sum(transactions)         AS txns
FROM mart_cfo_settled_volume
GROUP BY primary_cnae
ORDER BY settled DESC NULLS LAST
LIMIT 10;

-- P3: Ticket médio por dia (volume liquidado / nº de transações liquidadas)
SELECT reference_date,
       round(sum(total_settled_amount) / nullif(sum(transactions), 0), 2) AS ticket_medio
FROM mart_cfo_settled_volume
GROUP BY reference_date
ORDER BY reference_date;
