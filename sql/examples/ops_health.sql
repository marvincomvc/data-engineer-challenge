-- ============================================================================
-- OPERATIONS — reconciliation health
-- Run: duckdb warehouse/settlement.duckdb < sql/examples/ops_health.sql
-- ============================================================================

-- Q1. Is the discrepancy rate rising? Daily mismatch + unreconciled rates, with
--     a day-over-day delta so ops can spot a trend before it becomes an incident.
select
    reference_date,
    total_transactions,
    mismatch_rate,
    unreconciled_rate,
    round(mismatch_rate - lag(mismatch_rate) over (order by reference_date), 4) as mismatch_rate_dod
from mart_ops_health
order by reference_date;

-- Q2. What is my investigation backlog today, and how much money is at stake?
select
    reference_date,
    needs_investigation,
    mismatched,
    unreconciled_processor,
    unreconciled_internal,
    round(mismatch_exposure, 2) as mismatch_exposure_brl
from mart_ops_health
order by reference_date desc
limit 1;

-- Q3. Which reference dates had a failed SOURCE reconciliation run (upstream
--     service — data may be missing/stale)? For THIS pipeline's own run health,
--     see sql/examples/pipeline_health.sql.
select reference_date, source_run_status, total_transactions
from mart_ops_health
where source_run_status is distinct from 'COMPLETED'
order by reference_date;
