-- CFO: consolidated financial volume per reference_date AND currency.
-- Answers: "what did we transact/settle per day?", "how much is settled and
-- confirmed (matched) vs. still in dispute (mismatched/unreconciled)?",
-- "what is our reversal (chargeback) volume?".
--
-- Grain is (reference_date, currency): amounts of different currencies are NEVER
-- summed into one figure. Currency comes straight from the fact (resolved from
-- the internal side, else the processor side) — no BRL assumption.
-- Only amounts actually denominated in the group's currency are summed. For a
-- cross-currency row (internal ≠ processor currency, resolved currency = internal),
-- the processor_amount is in the OTHER currency, so it is excluded from this
-- currency's processor/reversed volume and from mismatch exposure — never conflated.
select
    reference_date,
    currency,
    count(*)                                                                    as transactions,
    -- processor-confirmed settled volume (only rows whose processor amount IS in this currency)
    coalesce(sum(processor_amount) filter (
        where processor_currency is not distinct from currency), 0)             as processor_volume,
    -- internal booked volume (internal amount is always in the resolved currency)
    sum(coalesce(internal_amount, 0))                                           as internal_volume,
    -- reconciled (matched) volume: the number finance can trust (matched ⇒ same currency)
    coalesce(sum(processor_amount) filter (where category = 'MATCHED'), 0)      as matched_volume,
    -- exposure from SAME-currency mismatches (cross-currency diffs aren't comparable)
    coalesce(sum(difference) filter (
        where category = 'MISMATCHED'
          and internal_currency is not distinct from processor_currency), 0)    as mismatch_exposure,
    coalesce(sum(processor_amount) filter (
        where processor_status = 'REVERSED'
          and processor_currency is not distinct from currency), 0)            as reversed_volume
from {{ ref('fct_reconciliation_result') }}
group by reference_date, currency
