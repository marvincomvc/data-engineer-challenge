-- COMPLIANCE: fully-attributed, immutable audit trail.
-- Answers: "for any transaction, what was decided, when, by which run, for which
-- merchant (with legal identity)?". One row per (reference_date, transaction_id),
-- enriched with merchant identity and run provenance for regulator-ready lookups.
with latest_run as (
    -- one header per reference_date so the join can't duplicate the audit grain
    select reference_date, status, file_name
    from {{ ref('stg_reconciliation_runs') }}
    qualify row_number() over (partition by reference_date order by started_at desc nulls last) = 1
)
select
    f.reference_date,
    f.transaction_id,
    f.category,
    c.needs_investigation,
    f.internal_amount,
    f.processor_amount,
    f.currency,
    f.difference,
    f.processor_status,
    f.processor_source,
    f.merchant_id,
    m.legal_name         as merchant_legal_name,
    m.cnpj               as merchant_cnpj,
    m.primary_cnae       as merchant_cnae,
    f.run_id,
    run.status           as run_status,
    run.file_name        as source_file,
    f._loaded_at         as audited_at
from {{ ref('fct_reconciliation_result') }} f
left join {{ ref('dim_merchant') }} m  on m.merchant_id = f.merchant_id
left join {{ ref('dim_category') }} c   on c.category = f.category
left join latest_run run on run.reference_date = f.reference_date
