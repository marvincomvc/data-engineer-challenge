-- ============================================================================
-- CFO — Volume financeiro liquidado (por DATA DE LIQUIDAÇÃO, settled_at)
-- Fonte: mart_cfo_settled_volume (sobre fact_settlement_daily, grão = settlement_date x merchant x moeda)
-- Observação: temos volume LIQUIDADO, mas não fees/receita (fora do escopo dos dados).
-- ============================================================================

-- P1: Volume liquidado / revertido / líquido por dia de liquidação e moeda
SELECT settlement_date, currency,
       sum(total_settled_amount)  AS settled,
       sum(total_reversed_amount) AS reversed,
       sum(net_settled_amount)    AS net
FROM mart_cfo_settled_volume
GROUP BY settlement_date, currency
ORDER BY settlement_date, currency;

-- P2: Top setores (CNAE) por volume liquidado no período
SELECT primary_cnae,
       sum(total_settled_amount) AS settled,
       sum(settled_count)        AS txns
FROM mart_cfo_settled_volume
GROUP BY primary_cnae
ORDER BY settled DESC NULLS LAST
LIMIT 10;

-- P3: Ticket médio por dia de liquidação (volume liquidado / nº de liquidações)
SELECT settlement_date,
       round(sum(total_settled_amount) / nullif(sum(settled_count), 0), 2) AS ticket_medio
FROM mart_cfo_settled_volume
GROUP BY settlement_date
ORDER BY settlement_date;
