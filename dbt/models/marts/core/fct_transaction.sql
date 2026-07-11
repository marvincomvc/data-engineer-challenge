-- Grain: one row per internal transaction (current state).
-- Feeds CFO volume views that need the full internal book, not just the
-- reconciled slice.
--
-- Idempotency: delete+insert on transaction_id refreshes current state without
-- duplicating on re-run.
{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='delete+insert'
    )
}}
select
    transaction_id,
    merchant_id,
    amount,
    currency,
    status,
    payment_method,
    created_at,
    cast(created_at as date) as created_date,
    updated_at
from {{ ref('stg_transactions') }}
{% if is_incremental() %}
-- refresh rows created OR updated within the window, so a late status change
-- (e.g. PENDING -> COMPLETED) on an older transaction is still pulled in.
-- (Full CDC-watermark incrementality remains a documented extension —
-- see docs/review-decisions.md.)
where cast(created_at as date)
        between (date '{{ var("reference_date") }}' - {{ var("reconciliation_window_days") }})
            and  date '{{ var("reference_date") }}'
   or cast(updated_at as date)
        between (date '{{ var("reference_date") }}' - {{ var("reconciliation_window_days") }})
            and  date '{{ var("reference_date") }}'
{% endif %}
