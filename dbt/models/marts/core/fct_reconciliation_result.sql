-- Grain: one row per (reference_date, transaction_id).
-- The CURRENT-STATE reconciliation fact (what the latest run decided per txn).
--
-- NOTE: this is current-state, NOT append-only audit history. A re-run
-- overwrites the prior category/amounts/run_id for a key (delete+insert). If
-- Compliance ever needs to reconstruct what an EARLIER run decided after a
-- corrected re-run, add an append-only fact keyed by (run_id, transaction_id)
-- alongside this one — see docs/data-products.md. The run_id + _loaded_at
-- columns record which run last wrote each row.
--
-- Idempotency: incremental + delete+insert on the natural key. Re-running a
-- reference_date replaces that date's rows, so a re-run (or a retry after a
-- mid-run crash) can never duplicate.
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
    currency,           -- resolved currency; downstream never sums across currencies
    internal_currency,
    processor_currency,
    processor_status,
    processor_source,   -- which processor settled it (paysettler, …)
    difference,
    run_id,          -- provenance of the run that last wrote this row
    now() as _loaded_at
from {{ ref('int_reconciliation') }}
{% if is_incremental() %}
where reference_date = date '{{ var("reference_date") }}'
{% endif %}
