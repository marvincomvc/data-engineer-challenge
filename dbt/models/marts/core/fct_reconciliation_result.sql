-- Grain: one row per (reference_date, transaction_id).
-- The immutable audit spine of the platform.
--
-- Idempotency: incremental + delete+insert on the natural key. Re-running a
-- reference_date deletes exactly that date's slice and re-inserts it, so a
-- re-run (or a retry after a mid-run crash) can never duplicate or corrupt rows.
{{
    config(
        materialized='incremental',
        unique_key=['reference_date', 'transaction_id'],
        incremental_strategy='delete+insert'
    )
}}
select
    reference_date,
    transaction_id,
    merchant_id,
    category,
    internal_amount,
    processor_amount,
    processor_status,
    processor_source,   -- which processor settled it (paysettler, …)
    difference,
    run_id,          -- provenance of the run that last wrote this row
    now() as _loaded_at
from {{ ref('int_reconciliation') }}
{% if is_incremental() %}
where reference_date = date '{{ var("reference_date") }}'
{% endif %}
