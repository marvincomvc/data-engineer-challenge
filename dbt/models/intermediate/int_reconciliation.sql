-- ==========================================================================
-- RECONCILIATION ENGINE
-- Recomputes categorization from raw sources for a single reference_date.
--
--   Scope   : internal transactions with created_at in
--             [reference_date - window_days, reference_date]  (inclusive)
--   Match   : on transaction_id (same UUID in both systems)
--   Tolerance: |processor_amount - internal_amount| <= tolerance  -> MATCHED
--
--   MATCHED                 both sides present, within tolerance
--   MISMATCHED              both sides present, difference beyond tolerance
--   UNRECONCILED_PROCESSOR  in processor file, no internal pair in window
--   UNRECONCILED_INTERNAL   internal in window, not in processor file
--
-- Both sides are de-duplicated to transaction_id so the output grain is exactly
-- one row per (reference_date, transaction_id).
-- ==========================================================================
with processor as (
    -- canonical stream across ALL processors; if the same transaction_id arrives
    -- from more than one processor, the latest settlement wins.
    -- Scope to the settlement file's business day so the recompute is deterministic
    -- and each settlement belongs to exactly one reference_date, regardless of
    -- whether the feed table accumulates multiple days.
    select
        transaction_id,
        merchant_id,
        amount,
        status,
        source_system,
        row_number() over (
            partition by transaction_id order by settled_at desc nulls last
        ) as _rn
    from {{ ref('stg_settlements') }}
    where cast(settled_at as date) = date '{{ var("reference_date") }}'
),
processor_dedup as (
    select transaction_id, merchant_id, amount, status, source_system
    from processor
    where _rn = 1
),
internal as (
    select transaction_id, merchant_id, amount
    from {{ ref('stg_transactions') }}
    where cast(created_at as date)
          between (date '{{ var("reference_date") }}' - {{ var("reconciliation_window_days") }})
              and  date '{{ var("reference_date") }}'
),
joined as (
    select
        coalesce(p.transaction_id, i.transaction_id) as transaction_id,
        coalesce(p.merchant_id, i.merchant_id)       as merchant_id,
        i.amount                                     as internal_amount,
        p.amount                                     as processor_amount,
        p.status                                     as processor_status,
        p.source_system                              as processor_source,
        abs(coalesce(p.amount, 0) - coalesce(i.amount, 0)) as difference
    from processor_dedup p
    full outer join internal i using (transaction_id)
)
select
    date '{{ var("reference_date") }}'  as reference_date,
    '{{ var("run_id") }}'               as run_id,
    transaction_id,
    merchant_id,
    internal_amount,
    processor_amount,
    processor_status,
    processor_source,
    difference,
    case
        when internal_amount is null  then 'UNRECONCILED_PROCESSOR'
        when processor_amount is null then 'UNRECONCILED_INTERNAL'
        when difference <= {{ var("reconciliation_tolerance") }} then 'MATCHED'
        else 'MISMATCHED'
    end as category
from joined
