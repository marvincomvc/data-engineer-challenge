-- ANOMALY (warn): cross-check our recomputed category mix for a reference_date
-- against the historical `reconciliation_results` for the run on that date.
--
-- NOTE: on the *sample* fixture this test is EXPECTED to warn — the provided
-- settlement CSV is a synthetic standalone file, not the actual file that
-- produced the historical results (see docs/data-products.md). We keep the test
-- to demonstrate the warn-gate firing on a real distribution drift; in
-- production, feeding the true daily file, it should stay green within tolerance.
{{ config(severity='warn') }}
with ours as (
    select category, count(*) as n_ours
    from {{ ref('fct_reconciliation_result') }}
    where reference_date = date '{{ var("reference_date") }}'
    group by category
),
theirs as (
    select res.category, count(*) as n_theirs
    from {{ ref('stg_reconciliation_results') }} res
    join {{ ref('stg_reconciliation_runs') }} run on run.run_id = res.run_id
    where run.reference_date = date '{{ var("reference_date") }}'
    group by res.category
),
compared as (
    select
        coalesce(o.category, t.category) as category,
        coalesce(n_ours, 0)   as n_ours,
        coalesce(n_theirs, 0) as n_theirs,
        abs(coalesce(n_ours, 0) - coalesce(n_theirs, 0)) as delta
    from ours o
    full outer join theirs t using (category)
)
select * from compared where delta > 0
