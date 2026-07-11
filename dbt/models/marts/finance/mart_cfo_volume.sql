-- CFO: consolidated financial volume per reference_date AND currency.
-- Answers: "what did we transact/settle per day?", "how much is settled and
-- confirmed (matched) vs. still in dispute (mismatched/unreconciled)?",
-- "what is our reversal (chargeback) volume?".
--
-- Grain is (reference_date, currency): amounts of different currencies are NEVER
-- summed into one figure — a mixed-currency day yields one row per currency.
with enriched as (
    select
        r.*,
        coalesce(t.currency, 'BRL') as currency  -- processor-only rows default to BRL
    from {{ ref('fct_reconciliation_result') }} r
    left join {{ ref('fct_transaction') }} t using (transaction_id)
)
select
    reference_date,
    currency,
    count(*)                                                                    as transactions,
    -- processor-confirmed settled volume (what PaySettler says it moved)
    sum(coalesce(processor_amount, 0))                                          as processor_volume,
    -- internal booked volume
    sum(coalesce(internal_amount, 0))                                           as internal_volume,
    -- reconciled (matched) volume: the number finance can trust
    coalesce(sum(processor_amount) filter (where category = 'MATCHED'), 0)      as matched_volume,
    -- financial exposure still under investigation
    coalesce(sum(difference) filter (where category = 'MISMATCHED'), 0)         as mismatch_exposure,
    coalesce(sum(processor_amount) filter (where processor_status = 'REVERSED'), 0) as reversed_volume
from enriched
group by reference_date, currency
