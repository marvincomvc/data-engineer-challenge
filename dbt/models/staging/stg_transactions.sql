-- Current state of internal transactions.
-- CDC collapse: keep the latest row per transaction_id (by _timestamp), drop
-- deletes. Schema drift (payment_method) already absorbed at Bronze via
-- union_by_name, so it is simply nullable here.
with src as (
    select * from {{ source('bronze', 'raw_transactions') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by transaction_id
            order by _timestamp desc nulls last, id desc
        ) as _rn
    from src
    where transaction_id is not null
)
select
    cast(id as bigint)                       as id,
    transaction_id,
    lower(trim(merchant_id))                 as merchant_id,
    cast(amount as decimal(18, 2))           as amount,
    upper(trim(currency))                    as currency,
    upper(trim(status))                      as status,
    description,
    payment_method,
    cast(created_at as timestamp)            as created_at,
    cast(updated_at as timestamp)            as updated_at,
    _source_file,
    _run_id
from ranked
where _rn = 1
  and coalesce(Op, 'c') <> 'd'
