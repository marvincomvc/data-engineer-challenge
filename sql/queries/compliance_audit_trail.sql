-- compliance_audit_trail.sql
-- Consumer: Compliance / Audit team
-- Questions: What is the full history of a specific transaction?
--            Which transactions were categorized differently across runs?
--            Which runs produced mismatches for a given merchant?
--
-- Usage (inside container):
--   python -c "import duckdb; c = duckdb.connect('/data/warehouse.duckdb'); c.sql(open('/app/sql/queries/compliance_audit_trail.sql').read()).show()"

-- Full audit trail for a specific transaction (replace the UUID below)
SELECT
    transaction_id,
    run_id,
    reference_date,
    file_name,
    merchant_id,
    category,
    internal_amount,
    processor_amount,
    difference,
    internal_status,
    transaction_created_at,
    result_created_at,
    run_started_at,
    run_completed_at
FROM gold.mart_compliance
WHERE transaction_id = '<paste-transaction-uuid-here>'
ORDER BY run_started_at DESC;


-- Category distribution for the latest run
SELECT
    category,
    COUNT(*)                                    AS total,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM gold.mart_compliance
WHERE reference_date = (SELECT MAX(reference_date) FROM gold.mart_compliance)
GROUP BY category
ORDER BY total DESC;


-- Mismatched transactions with largest discrepancy
SELECT
    transaction_id,
    merchant_id,
    internal_amount,
    processor_amount,
    difference,
    internal_status,
    file_name,
    run_started_at
FROM gold.mart_compliance
WHERE category = 'MISMATCHED'
ORDER BY ABS(difference) DESC
LIMIT 20;


-- Transactions that appear in processor but not internally (UNRECONCILED_PROCESSOR)
-- These represent payments the processor claims to have settled with no internal record
SELECT
    transaction_id,
    merchant_id,
    processor_amount,
    file_name,
    reference_date,
    run_started_at
FROM gold.mart_compliance
WHERE category = 'UNRECONCILED_PROCESSOR'
ORDER BY processor_amount DESC;
