-- ANOMALY (warn): a mismatch rate above threshold does not mean the data is
-- structurally wrong — it means operations should look. Alert, don't block.
{{ config(severity='warn') }}
with rate as (
    select
        reference_date,
        count(*) filter (where category = 'MISMATCHED')::double
            / nullif(count(*), 0) as mismatch_rate
    from {{ ref('fct_reconciliation_result') }}
    where reference_date = date '{{ var("reference_date") }}'
    group by reference_date
)
select *
from rate
where mismatch_rate > {{ var('mismatch_rate_warn', 0.10) }}
