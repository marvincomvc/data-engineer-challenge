-- ==========================================================================
-- RECONCILIATION ENGINE
-- Recomputes categorization from raw sources for a single reference_date.
--
--   Scope   : internal transactions with created_at in
--             [reference_date - window_days, reference_date]  (inclusive)
--   Match   : on transaction_id (same UUID in both systems)
--   Tolerance: |processor_amount - internal_amount| <= tolerance  -> MATCHED
--   Currency: a cross-currency pair can NEVER match (amounts aren't comparable);
--             it is categorized MISMATCHED regardless of numeric difference.
--
--   MATCHED                 both sides present, same currency, within tolerance
--   MISMATCHED              both sides present; different currency OR beyond tolerance
--   UNRECONCILED_PROCESSOR  in processor file, no internal pair in window
--   UNRECONCILED_INTERNAL   internal in window, not in processor file
--
-- Both sides are de-duplicated to transaction_id so the output grain is exactly
-- one row per (reference_date, transaction_id). Currency is carried through so
-- downstream marts never sum across currencies.
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
        currency,
        status,
        source_system,
        -- deterministic tie-break so equal settled_at collisions don't flip
        -- category between identical re-runs
        row_number() over (
            partition by transaction_id
            order by settled_at desc nulls last, processor_reference desc nulls last
        ) as _rn
    from {{ ref('stg_settlements') }}
    where cast(settled_at as date) = date '{{ var("reference_date") }}'
),
processor_dedup as (
    select transaction_id, merchant_id, amount, currency, status, source_system
    from processor
    where _rn = 1
),
internal as (
    select transaction_id, merchant_id, amount, currency
    from {{ ref('stg_transactions') }}
    where cast(created_at as date)
          between (date '{{ var("reference_date") }}' - {{ var("reconciliation_window_days") }})
              and  date '{{ var("reference_date") }}'
),
joined as (
    select
        coalesce(p.transaction_id, i.transaction_id) as transaction_id,
        coalesce(p.merchant_id, i.merchant_id)       as merchant_id,
        (p.transaction_id is not null)               as _has_processor,
        (i.transaction_id is not null)               as _has_internal,
        i.amount                                     as internal_amount,
        p.amount                                     as processor_amount,
        i.currency                                   as internal_currency,
        p.currency                                   as processor_currency,
        -- resolved currency: internal side is authoritative, else the processor's
        coalesce(i.currency, p.currency)             as currency,
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
    currency,
    internal_currency,
    processor_currency,
    processor_status,
    processor_source,
    difference,
    case
        -- categorize by which SIDE is present (join result), not amount nullability
        when not _has_internal  then 'UNRECONCILED_PROCESSOR'
        when not _has_processor then 'UNRECONCILED_INTERNAL'
        -- both sides present: currencies must agree before amounts are comparable
        when internal_currency is distinct from processor_currency then 'MISMATCHED'
        when difference <= {{ var("reconciliation_tolerance") }} then 'MATCHED'
        else 'MISMATCHED'
    end as category
from joined
