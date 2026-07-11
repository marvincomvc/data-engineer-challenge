-- ANOMALY (warn): a spike in unreconciled (orphan) transactions — present on one
-- side but not the other — is an operational signal, not a structural break.
-- Alert, don't block. Threshold comes from ORPHAN_RATE_WARN via dbt vars.
{{ config(severity='warn') }}
with rate as (
    select
        reference_date,
        count(*) filter (
            where category in ('UNRECONCILED_PROCESSOR', 'UNRECONCILED_INTERNAL')
        )::double / nullif(count(*), 0) as orphan_rate
    from {{ ref('fct_reconciliation_result') }}
    where reference_date = date '{{ var("reference_date") }}'
    group by reference_date
)
select *
from rate
where orphan_rate > {{ var('orphan_rate_warn', 0.05) }}
