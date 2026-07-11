-- PaySettler adapter: maps the PaySettler settlement file onto the canonical
-- settlement contract consumed via stg_settlements. A second processor gets its
-- own analogous adapter (stg_<name>.sql) with the same output columns.
--
-- PaySettler settlement file, cleaned.
-- Dirty-data repairs applied here (all deterministic, all unit tested):
--   * comma decimals   "152,30" -> 152.30
--   * missing TZ suffix "...T14:30:00" -> treated as UTC (naive timestamp)
--   * lowercased / padded merchant ids normalized
-- try_cast on amount leaves unparseable values NULL so the not_null integrity
-- gate can block the run rather than silently coercing bad money.
with src as (
    select * from {{ source('bronze', 'raw_paysettler') }}
)
select
    transaction_id,
    lower(trim(merchant_id))                                   as merchant_id,
    {{ clean_amount('amount') }}                               as amount,
    upper(trim(currency))                                      as currency,
    try_cast(replace(settled_at, 'Z', '') as timestamp)       as settled_at,
    processor_reference,
    upper(trim(status))                                        as status,
    _source_file,
    _run_id
from src
where transaction_id is not null
