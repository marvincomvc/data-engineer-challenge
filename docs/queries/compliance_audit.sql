-- ============================================================================
-- COMPLIANCE — Rastreabilidade e auditoria
-- Fonte: mart_compliance_audit_trail (sobre fact_reconciliation_result, grão = result_id)
-- ============================================================================

-- P1: Histórico completo de uma transação específica (troque o UUID)
SELECT *
FROM mart_compliance_audit_trail
WHERE transaction_id = '4d1969cf-2ef3-459d-af5d-293560a338ce';

-- P2: Quais transações ficaram sem conciliar, por categoria?
SELECT category, count(*) AS n
FROM mart_compliance_audit_trail
WHERE category LIKE 'UNRECONCILED%'
GROUP BY category
ORDER BY n DESC;

-- P3: Qual CNPJ (merchant) acumula mais divergências de valor (MISMATCHED)?
SELECT document, legal_name, count(*) AS mismatches,
       round(sum(difference), 2) AS soma_diferencas
FROM mart_compliance_audit_trail
WHERE category = 'MISMATCHED'
GROUP BY document, legal_name
ORDER BY mismatches DESC
LIMIT 10;
