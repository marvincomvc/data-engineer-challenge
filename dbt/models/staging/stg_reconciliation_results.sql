-- Historical categorized results (current state). Used only as a cross-check
-- reference against our own recomputation — never as pipeline input.
with src as (
    select * from {{ source('bronze', 'raw_reconciliation_results') }}
),
ranked as (
    select
        *,
        row_number() over (partition by id order by _timestamp desc) as _rn
    from src
)
select
    cast(id as bigint)                    as id,
    cast(run_id as bigint)                as run_id,
    transaction_id,
    lower(trim(merchant_id))              as merchant_id,
    upper(trim(category))                 as category,
    cast(internal_amount as decimal(18, 2))  as internal_amount,
    cast(processor_amount as decimal(18, 2)) as processor_amount,
    cast(difference as decimal(18, 2))    as difference
from ranked
where _rn = 1
  and coalesce(Op, 'c') <> 'd'
