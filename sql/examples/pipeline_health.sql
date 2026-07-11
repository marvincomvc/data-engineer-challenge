-- ============================================================================
-- OPERATIONS — THIS pipeline's run health (from meta.pipeline_run / dq_check_result)
-- Answers "which of OUR runs failed, and which data-quality gates fired?" — the
-- operational counterpart to mart_ops_health (which reflects the source service).
-- Run: duckdb warehouse/settlement.duckdb < sql/examples/pipeline_health.sql
-- ============================================================================

-- Q1. Recent pipeline runs per stage: did each stage complete, and how long?
select
    reference_date,
    run_id,
    stage,
    status,
    rows_out,
    date_diff('second', started_at, finished_at) as duration_s
from meta.pipeline_run
order by started_at desc
limit 20;

-- Q2. Any FAILED stage in the last few runs? (the "which run failed?" answer)
select reference_date, run_id, stage, error, started_at
from meta.pipeline_run
where status = 'FAILED'
order by started_at desc;

-- Q3. Data-quality gate outcomes for the latest run: what blocked vs. warned?
select check_name, severity, passed, observed_value
from meta.dq_check_result
where run_id = (select run_id from meta.pipeline_run order by started_at desc limit 1)
order by passed, severity;
