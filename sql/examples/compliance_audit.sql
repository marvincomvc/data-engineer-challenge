-- ============================================================================
-- COMPLIANCE — audit trail & traceability
-- Run (against the serving catalog, per docs/consumption.md; `make run` first):
--   duckdb gold/serving.duckdb < sql/examples/compliance_audit.sql
-- ============================================================================

-- Q1. Full audit record for a single transaction: what was decided, for which
--     merchant (legal identity), by which run, from which source file.
select
    transaction_id,
    category,
    internal_amount,
    processor_amount,
    difference,
    merchant_legal_name,
    merchant_cnpj,
    run_id,
    source_file,
    audited_at
from mart_compliance_audit
order by audited_at desc
limit 5;

-- Q2. Per-merchant discrepancy summary for a reference date (who to audit).
select
    merchant_legal_name,
    merchant_cnpj,
    count(*)                                          as transactions,
    count(*) filter (where needs_investigation)       as flagged,
    round(sum(difference), 2)                          as total_difference_brl
from mart_compliance_audit
where reference_date = date '2025-03-16'
group by 1, 2
having count(*) filter (where needs_investigation) > 0
order by total_difference_brl desc
limit 10;

-- Q3. Immutability / completeness check: every result ties to exactly one run
--     and every transaction has a category (no audit gaps).
select
    count(*)                                        as audit_rows,
    count(*) filter (where category is null)         as missing_category,
    count(distinct run_id)                           as runs_represented
from mart_compliance_audit;
