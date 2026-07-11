-- Merchant master data, current state (CDC collapse).
with src as (
    select
        *,
        lower(trim(merchant_id)) as _merchant_key   -- normalize BEFORE ranking so
    from {{ source('bronze', 'raw_enterprise_company') }}   -- case/space variants dedup as one
),
ranked as (
    select
        *,
        row_number() over (partition by _merchant_key order by _timestamp desc nulls last, id desc) as _rn
    from src
    where _merchant_key is not null
)
select
    cast(id as bigint)              as id,
    _merchant_key                   as merchant_id,
    legal_name,
    trade_name,
    document                        as cnpj,
    primary_cnae,
    cast(created_at as timestamp)   as created_at,
    cast(updated_at as timestamp)   as updated_at
from ranked
where _rn = 1
  and coalesce(Op, 'c') <> 'd'
  and merchant_id is not null
