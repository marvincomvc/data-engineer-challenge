-- INTEGRITY (error): the CFO volume mart must be one row per
-- (reference_date, currency). A duplicate would mean currencies were collapsed
-- or the grain drifted — block it.
select
    reference_date,
    currency,
    count(*) as n
from {{ ref('mart_cfo_volume') }}
group by 1, 2
having count(*) > 1
