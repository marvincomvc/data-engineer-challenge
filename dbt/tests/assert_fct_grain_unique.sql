-- INTEGRITY (error): the fact grain must be exactly one row per
-- (reference_date, transaction_id). Any duplicate is a correctness bug in the
-- reconciliation engine or the incremental strategy — block the pipeline.
select
    reference_date,
    transaction_id,
    count(*) as n
from {{ ref('fct_reconciliation_result') }}
group by 1, 2
having count(*) > 1
