-- OPERATIONS: daily reconciliation health.
-- Answers: "is the discrepancy rate rising?", "how many items need investigation
-- today?", "which run failed?". One row per reference_date.
with results as (
    select * from {{ ref('fct_reconciliation_result') }}
),
by_date as (
    select
        reference_date,
        count(*)                                                          as total_transactions,
        count(*) filter (where category = 'MATCHED')                      as matched,
        count(*) filter (where category = 'MISMATCHED')                   as mismatched,
        count(*) filter (where category = 'UNRECONCILED_PROCESSOR')       as unreconciled_processor,
        count(*) filter (where category = 'UNRECONCILED_INTERNAL')        as unreconciled_internal,
        count(*) filter (where category <> 'MATCHED')                     as needs_investigation,
        sum(difference) filter (where category = 'MISMATCHED')            as mismatch_exposure
    from results
    group by reference_date
),
latest_run as (
    -- one header per reference_date (a retry/backfill can produce several) so the
    -- join below cannot fan out the daily grain
    select reference_date, status
    from {{ ref('stg_reconciliation_runs') }}
    qualify row_number() over (partition by reference_date order by started_at desc nulls last) = 1
)
select
    b.reference_date,
    d.is_weekend,
    r.status                                     as run_status,
    b.total_transactions,
    b.matched,
    b.mismatched,
    b.unreconciled_processor,
    b.unreconciled_internal,
    b.needs_investigation,
    b.mismatch_exposure,
    round(b.matched::double     / nullif(b.total_transactions, 0), 4) as matched_rate,
    round(b.mismatched::double  / nullif(b.total_transactions, 0), 4) as mismatch_rate,
    round((b.unreconciled_processor + b.unreconciled_internal)::double
          / nullif(b.total_transactions, 0), 4)                       as unreconciled_rate
from by_date b
join {{ ref('dim_date') }} d on d.date_day = b.reference_date
left join latest_run r on r.reference_date = b.reference_date
