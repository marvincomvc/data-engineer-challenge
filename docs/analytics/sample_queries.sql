-- =============================================================================
-- Sample Analytical Queries — Settlement Reconciliation Data Platform
-- =============================================================================
-- These queries target the DuckDB warehouse (data/warehouse.duckdb) and are
-- organized by consumer context: Operations, CFO, and Compliance.
--
-- Run with:  duckdb data/warehouse.duckdb < docs/analytics/sample_queries.sql
-- Or interactively in the DuckDB CLI:
--   OPEN 'data/warehouse.duckdb';
--   \i docs/analytics/sample_queries.sql
-- =============================================================================


-- =============================================================================
-- 1. OPERATIONS — Consumer of gold.daily_reconciliation_health
-- =============================================================================

-- 1.1 Daily reconciliation health — last 30 days
--     Answers: What is the discrepancy trend? Are unreconciled counts stable?
SELECT
    reference_date,
    total_results,
    matched_count,
    mismatched_count,
    unreconciled_processor_count,
    unreconciled_internal_count,
    ROUND(discrepancy_rate * 100, 2) AS discrepancy_pct,
    ROUND(processing_time_minutes, 1)  AS processing_min,
    run_status
FROM gold.daily_reconciliation_health
WHERE reference_date >= (CURRENT_DATE - INTERVAL '30 days')::VARCHAR
ORDER BY reference_date DESC;


-- 1.2 Days with discrepancy rate above 5%
--     Answers: Which dates require operational investigation?
SELECT
    reference_date,
    ROUND(discrepancy_rate * 100, 2) AS discrepancy_pct,
    mismatched_count,
    unreconciled_processor_count + unreconciled_internal_count AS total_unreconciled,
    total_results
FROM gold.daily_reconciliation_health
WHERE discrepancy_rate > 0.05
ORDER BY discrepancy_rate DESC;


-- 1.3 Processing time outliers (runs > 60 minutes)
--     Answers: Are any runs taking unusually long?
SELECT
    reference_date,
    ROUND(processing_time_minutes, 1) AS processing_min,
    total_results,
    run_status
FROM gold.daily_reconciliation_health
WHERE processing_time_minutes > 60
ORDER BY processing_time_minutes DESC;


-- 1.4 Weekly discrepancy rate trend
--     Answers: Is the discrepancy rate improving or worsening over weeks?
SELECT
    DATE_TRUNC('week', reference_date::DATE)::DATE AS week_start,
    SUM(mismatched_count)                          AS total_mismatched,
    SUM(total_results)                             AS total_results,
    ROUND(SUM(mismatched_count)::DOUBLE / NULLIF(SUM(total_results), 0) * 100, 2)
                                                   AS weekly_discrepancy_pct
FROM gold.daily_reconciliation_health
GROUP BY week_start
ORDER BY week_start;


-- 1.5 Unreconciled breakdown — processor vs internal imbalance
--     Answers: Which side contributes more to unreconciled volume?
SELECT
    reference_date,
    unreconciled_processor_count,
    unreconciled_internal_count,
    unreconciled_processor_count - unreconciled_internal_count AS imbalance
FROM gold.daily_reconciliation_health
WHERE unreconciled_processor_count + unreconciled_internal_count > 0
ORDER BY reference_date DESC
LIMIT 30;


-- =============================================================================
-- 2. CFO — Consumer of gold.daily_financial_volume & gold.merchant_volume_summary
-- =============================================================================

-- 2.1 Daily financial volume — last 30 days
--     Answers: What is the daily settlement volume and net difference?
SELECT
    reference_date,
    ROUND(internal_volume_brl, 2)     AS internal_brl,
    ROUND(processor_volume_brl, 2)    AS processor_brl,
    ROUND(daily_difference, 2)        AS net_diff_brl,
    ROUND(total_abs_difference, 2)    AS abs_diff_brl,
    transaction_count
FROM gold.daily_financial_volume
WHERE reference_date >= (CURRENT_DATE - INTERVAL '30 days')::VARCHAR
ORDER BY reference_date DESC;


-- 2.2 Cumulative financial difference over time
--     Answers: Is the cumulative net difference growing (systematic drift)?
SELECT
    reference_date,
    ROUND(daily_difference, 2)                                       AS daily_diff_brl,
    ROUND(SUM(daily_difference) OVER (ORDER BY reference_date), 2)   AS cumulative_diff_brl,
    ROUND(total_abs_difference, 2)                                   AS abs_diff_brl,
    ROUND(SUM(total_abs_difference) OVER (ORDER BY reference_date), 2)
                                                                      AS cumulative_abs_diff_brl
FROM gold.daily_financial_volume
ORDER BY reference_date;


-- 2.3 Days with absolute difference above R$ 10,000
--     Answers: Which dates had the largest financial impact from discrepancies?
SELECT
    reference_date,
    ROUND(total_abs_difference, 2) AS abs_diff_brl,
    ROUND(daily_difference, 2)     AS net_diff_brl,
    transaction_count,
    ROUND(total_abs_difference / NULLIF(transaction_count, 0), 2)
                                   AS avg_diff_per_txn
FROM gold.daily_financial_volume
WHERE total_abs_difference > 10000
ORDER BY total_abs_difference DESC;


-- 2.4 Top 20 merchants by total volume
--     Answers: Which merchants represent the largest settlement exposure?
SELECT
    merchant_id,
    legal_name,
    trade_name,
    total_transactions,
    ROUND(total_internal_volume, 2)  AS internal_brl,
    ROUND(total_processor_volume, 2) AS processor_brl,
    ROUND(total_difference, 2)       AS diff_brl
FROM gold.merchant_volume_summary
ORDER BY total_internal_volume DESC
LIMIT 20;


-- 2.5 Top 20 merchants by discrepancy (highest mismatched count)
--     Answers: Which merchants should the CFO prioritize for reconciliation review?
SELECT
    merchant_id,
    legal_name,
    trade_name,
    mismatched_count,
    total_transactions,
    ROUND(total_difference, 2) AS diff_brl,
    ROUND(mismatched_count::DOUBLE / NULLIF(total_transactions, 0) * 100, 2)
                                AS mismatched_pct
FROM gold.merchant_volume_summary
WHERE mismatched_count > 0
ORDER BY mismatched_count DESC
LIMIT 20;


-- 2.6 Merchants where processor volume exceeds internal volume
--     Answers: Are there merchants where we are settling more than we recorded?
SELECT
    merchant_id,
    legal_name,
    ROUND(total_internal_volume, 2)  AS internal_brl,
    ROUND(total_processor_volume, 2) AS processor_brl,
    ROUND(total_processor_volume - total_internal_volume, 2) AS overpayment_brl,
    total_transactions
FROM gold.merchant_volume_summary
WHERE total_processor_volume > total_internal_volume
ORDER BY overpayment_brl DESC
LIMIT 20;


-- 2.7 Monthly volume summary (derived from daily)
--     Answers: What is the monthly settlement volume and difference?
SELECT
    DATE_TRUNC('month', reference_date::DATE)::DATE AS month,
    ROUND(SUM(internal_volume_brl), 2)              AS internal_brl,
    ROUND(SUM(processor_volume_brl), 2)             AS processor_brl,
    ROUND(SUM(daily_difference), 2)                 AS net_diff_brl,
    ROUND(SUM(total_abs_difference), 2)             AS abs_diff_brl,
    SUM(transaction_count)                          AS total_transactions
FROM gold.daily_financial_volume
GROUP BY month
ORDER BY month;


-- =============================================================================
-- 3. COMPLIANCE — Consumer of gold.audit_discrepancies,
--                  gold.audit_unreconciled, gold.audit_reconciliation_runs_history
-- =============================================================================

-- 3.1 All discrepancies for a specific reference date
--     Answers: What are the mismatched transactions for a given settlement date?
SELECT
    transaction_id,
    merchant_id,
    legal_name,
    document,
    internal_amount,
    processor_amount,
    difference,
    created_at
FROM gold.audit_discrepancies
WHERE reference_date = '2025-03-15'
ORDER BY difference DESC;


-- 3.2 Discrepancies above R$ 100 (high-value investigation)
--     Answers: Which discrepancies are too large to ignore?
SELECT
    reference_date,
    transaction_id,
    merchant_id,
    legal_name,
    document,
    internal_amount,
    processor_amount,
    difference
FROM gold.audit_discrepancies
WHERE difference > 100
ORDER BY difference DESC;


-- 3.3 Discrepancy count and total by merchant
--     Answers: Which merchants have the most discrepancy cases?
SELECT
    merchant_id,
    legal_name,
    document,
    COUNT(*)              AS discrepancy_count,
    ROUND(SUM(difference), 2) AS total_discrepancy_brl,
    ROUND(AVG(difference), 2) AS avg_discrepancy_brl,
    MAX(difference)           AS max_discrepancy_brl
FROM gold.audit_discrepancies
GROUP BY merchant_id, legal_name, document
ORDER BY discrepancy_count DESC
LIMIT 20;


-- 3.4 Discrepancy direction analysis (internal > processor vs processor > internal)
--     Answers: Are we overcharging or undercharging systematically?
SELECT
    CASE
        WHEN internal_amount > processor_amount THEN 'INTERNAL_EXCEEDS_PROCESSOR'
        WHEN processor_amount > internal_amount THEN 'PROCESSOR_EXCEEDS_INTERNAL'
        ELSE 'EQUAL'
    END AS direction,
    COUNT(*)                                  AS count,
    ROUND(SUM(difference), 2)                  AS total_diff_brl,
    ROUND(AVG(difference), 2)                  AS avg_diff_brl
FROM gold.audit_discrepancies
GROUP BY direction
ORDER BY total_diff_brl DESC;


-- 3.5 Unreconciled transactions by category and date
--     Answers: What is the volume and type of unreconciled transactions per day?
SELECT
    reference_date,
    category,
    COUNT(*)                AS count,
    ROUND(SUM(amount), 2)   AS total_amount_brl,
    ROUND(AVG(amount), 2)   AS avg_amount_brl
FROM gold.audit_unreconciled
GROUP BY reference_date, category
ORDER BY reference_date DESC, category;


-- 3.6 Unreconciled processor transactions — potential unregistered sales
--     Answers: What did PaySettler settle that we have no internal record of?
SELECT
    reference_date,
    transaction_id,
    merchant_id,
    legal_name,
    document,
    amount AS processor_amount_brl,
    created_at
FROM gold.audit_unreconciled
WHERE category = 'UNRECONCILED_PROCESSOR'
ORDER BY amount DESC
LIMIT 50;


-- 3.7 Unreconciled internal transactions — potential missing settlements
--     Answers: What did we register internally that PaySettler never settled?
SELECT
    reference_date,
    transaction_id,
    merchant_id,
    legal_name,
    document,
    amount AS internal_amount_brl,
    created_at
FROM gold.audit_unreconciled
WHERE category = 'UNRECONCILED_INTERNAL'
ORDER BY amount DESC
LIMIT 50;


-- 3.8 Merchants with the most unreconciled transactions
--     Answers: Which merchants have the highest exposure from missing counterparts?
SELECT
    merchant_id,
    legal_name,
    document,
    SUM(CASE WHEN category = 'UNRECONCILED_PROCESSOR' THEN 1 ELSE 0 END) AS processor_missing,
    SUM(CASE WHEN category = 'UNRECONCILED_INTERNAL' THEN 1 ELSE 0 END)  AS internal_missing,
    COUNT(*)                                                                AS total_unreconciled,
    ROUND(SUM(amount), 2)                                                  AS total_at_risk_brl
FROM gold.audit_unreconciled
GROUP BY merchant_id, legal_name, document
ORDER BY total_unreconciled DESC
LIMIT 20;


-- 3.9 Reconciliation run history — failed or stuck runs
--     Answers: Were there execution failures that need compliance attention?
SELECT
    run_id,
    reference_date,
    file_name,
    status,
    total_transactions,
    started_at,
    completed_at,
    ROUND(processing_time_minutes, 1) AS processing_min
FROM gold.audit_reconciliation_runs_history
WHERE status != 'COMPLETED'
ORDER BY started_at DESC;


-- 3.10 Reconciliation runs with processing time above 60 minutes
--      Answers: Which runs took unusually long (potential data quality or scale issues)?
SELECT
    run_id,
    reference_date,
    file_name,
    status,
    total_transactions,
    ROUND(processing_time_minutes, 1) AS processing_min,
    started_at,
    completed_at
FROM gold.audit_reconciliation_runs_history
WHERE processing_time_minutes > 60
ORDER BY processing_time_minutes DESC;


-- 3.11 Full audit trail for a specific merchant
--      Answers: What are all discrepancies and unreconciled items for a given merchant?
SELECT 'DISCREPANCY' AS source, reference_date, transaction_id,
       internal_amount, processor_amount, difference AS amount_or_diff
FROM gold.audit_discrepancies
WHERE merchant_id = 'MERCH_0001'

UNION ALL

SELECT 'UNRECONCILED' AS source, reference_date, transaction_id,
       NULL, NULL, amount AS amount_or_diff
FROM gold.audit_unreconciled
WHERE merchant_id = 'MERCH_0001'

ORDER BY reference_date DESC, source;


-- 3.12 Complete daily compliance summary
--      Answers: For a given date, how many issues of each type exist and what is the total at risk?
SELECT
    h.reference_date,
    h.mismatched_count,
    h.unreconciled_processor_count,
    h.unreconciled_internal_count,
    ROUND(h.discrepancy_rate * 100, 2)       AS discrepancy_pct,
    ROUND(v.total_abs_difference, 2)         AS abs_diff_brl,
    ROUND(v.daily_difference, 2)             AS net_diff_brl
FROM gold.daily_reconciliation_health h
JOIN gold.daily_financial_volume v
  ON h.reference_date = v.reference_date
ORDER BY h.reference_date DESC
LIMIT 30;


-- =============================================================================
-- 4. CROSS-CUTTING — Queries that span multiple Gold tables
-- =============================================================================

-- 4.1 Daily reconciliation scorecard (combines health + financial volume)
--     Answers: One-stop daily view for dashboards
SELECT
    h.reference_date,
    h.total_results,
    h.matched_count,
    h.mismatched_count,
    h.unreconciled_processor_count,
    h.unreconciled_internal_count,
    ROUND(h.discrepancy_rate * 100, 2)       AS discrepancy_pct,
    ROUND(h.processing_time_minutes, 1)      AS processing_min,
    v.transaction_count,
    ROUND(v.internal_volume_brl, 2)          AS internal_brl,
    ROUND(v.processor_volume_brl, 2)         AS processor_brl,
    ROUND(v.daily_difference, 2)             AS net_diff_brl,
    ROUND(v.total_abs_difference, 2)         AS abs_diff_brl
FROM gold.daily_reconciliation_health  h
JOIN gold.daily_financial_volume       v ON h.reference_date = v.reference_date
ORDER BY h.reference_date DESC;


-- 4.2 Total financial exposure from all outstanding issues
--     Answers: What is the total amount at risk from mismatches + unreconciled?
SELECT
    'MISMATCHED'               AS issue_type,
    COUNT(*)                    AS count,
    ROUND(SUM(difference), 2)   AS total_brl
FROM gold.audit_discrepancies

UNION ALL

SELECT
    'UNRECONCILED_PROCESSOR'   AS issue_type,
    COUNT(*)                    AS count,
    ROUND(SUM(amount), 2)       AS total_brl
FROM gold.audit_unreconciled
WHERE category = 'UNRECONCILED_PROCESSOR'

UNION ALL

SELECT
    'UNRECONCILED_INTERNAL'    AS issue_type,
    COUNT(*)                    AS count,
    ROUND(SUM(amount), 2)       AS total_brl
FROM gold.audit_unreconciled
WHERE category = 'UNRECONCILED_INTERNAL'

ORDER BY total_brl DESC;


-- 4.3 Pipeline run audit — join gold run history with control pipeline runs
--     Answers: Cross-reference reconciliation runs with pipeline execution metadata
SELECT
    rh.run_id,
    rh.reference_date,
    rh.file_name,
    rh.status                                    AS run_status,
    rh.total_transactions,
    ROUND(rh.processing_time_minutes, 1)         AS rec_processing_min,
    pr.status                                    AS pipeline_status,
    pr.matched,
    pr.mismatched,
    pr.unreconciled_processor,
    pr.unreconciled_internal,
    pr.hard_checks_failed,
    pr.soft_checks_failed
FROM gold.audit_reconciliation_runs_history rh
LEFT JOIN control.pipeline_runs pr
  ON rh.reference_date = pr.reference_date
ORDER BY rh.reference_date DESC
LIMIT 30;
