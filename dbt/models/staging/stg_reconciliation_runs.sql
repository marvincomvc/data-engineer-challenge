-- Historical reconciliation run headers, current state (CDC collapse).
with src as (
    select * from {{ source('bronze', 'raw_reconciliation_runs') }}
),
ranked as (
    select
        *,
        -- created_at as a secondary tie-breaker on equal _timestamp (the CDC
        -- extract lacks a true source sequence/offset — see docs/review-decisions.md)
        row_number() over (
            partition by id order by _timestamp desc nulls last, created_at desc nulls last
        ) as _rn
    from src
)
select
    cast(id as bigint)                  as run_id,
    cast(reference_date as date)        as reference_date,
    file_name,
    upper(trim(status))                 as status,
    cast(total_transactions as bigint)  as total_transactions,
    cast(started_at as timestamp)       as started_at,
    cast(completed_at as timestamp)     as completed_at
from ranked
where _rn = 1
  and coalesce(Op, 'c') <> 'd'
