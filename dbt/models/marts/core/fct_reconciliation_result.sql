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
-- Idempotency: incremental + delete+insert keyed by reference_date, so a re-run
-- deletes the WHOLE date slice and re-inserts it. This replaces the full partition
-- (not just the keys in the new batch), so a corrected re-run that DROPS a
-- transaction no longer leaves a stale row. Grain (reference_date, transaction_id)
-- stays enforced by the assert_fct_grain_unique test.
{{
    config(
        materialized='incremental',
        unique_key='reference_date',
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
