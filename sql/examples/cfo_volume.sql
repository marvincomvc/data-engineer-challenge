-- ============================================================================
-- CFO — consolidated financial volume
-- Run (against the serving catalog, per docs/consumption.md; `make run` first):
--   duckdb gold/serving.duckdb < sql/examples/cfo_volume.sql
-- Grain is (reference_date, currency) — every query carries currency so amounts
-- of different currencies are never read as one figure.
-- ============================================================================

-- Q1. What did we settle per day, and how much of it is trustworthy (matched)?
select
    reference_date,
    currency,
    transactions,
    round(processor_volume, 2) as settled_volume,
    round(matched_volume, 2)   as confirmed_volume,
    round(matched_volume / nullif(processor_volume, 0), 4) as confirmed_pct
from mart_cfo_volume
order by reference_date, currency;

-- Q2. What is our financial exposure still under investigation (mismatches)?
select
    reference_date,
    currency,
    round(mismatch_exposure, 2) as exposure
from mart_cfo_volume
order by exposure desc
limit 5;

-- Q3. Reversal (chargeback/estorno) volume — a fraud / risk signal.
select
    reference_date,
    currency,
    round(reversed_volume, 2) as reversed_amount,
    round(reversed_volume / nullif(processor_volume, 0), 4) as reversed_pct
from mart_cfo_volume
where reversed_volume > 0
order by reference_date, currency;
