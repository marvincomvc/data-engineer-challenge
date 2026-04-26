-- ops_daily_health.sql
-- Consumer: Operations team
-- Questions: Did today's reconciliation complete? What is the match rate?
--            Which runs have the highest discrepancy volume?
--
-- Usage (inside container):
--   python -c "import duckdb; c = duckdb.connect('/data/warehouse.duckdb'); c.sql(open('/app/sql/queries/ops_daily_health.sql').read()).show()"

-- Daily reconciliation summary
SELECT
    reference_date,
    run_id,
    run_status,
    run_duration_seconds,
    total_transactions,
    matched_count,
    mismatched_count,
    unreconciled_processor_count,
    unreconciled_internal_count,
    match_rate_pct,
    total_discrepancy_amount
FROM gold.mart_operations
ORDER BY reference_date DESC, run_id DESC;


-- Runs with match rate below 95% (attention threshold)
SELECT
    reference_date,
    run_id,
    match_rate_pct,
    mismatched_count,
    total_discrepancy_amount
FROM gold.mart_operations
WHERE match_rate_pct < 95
ORDER BY match_rate_pct ASC;


-- Top discrepancy days (financial exposure ranking)
SELECT
    reference_date,
    SUM(total_discrepancy_amount) AS total_discrepancy,
    SUM(mismatched_count)         AS total_mismatches
FROM gold.mart_operations
GROUP BY reference_date
ORDER BY total_discrepancy DESC
LIMIT 10;
