-- Merchant dimension (Type 1 / current state).
--
-- Design note: this is a current-state dimension. Full SCD2 history (tracking
-- legal_name / cnae changes over time) is a documented extension via a dbt
-- snapshot keyed on merchant_id + _timestamp — see docs/data-products.md. The
-- audit need for *point-in-time reconciliation* is already met by the immutable
-- fct_reconciliation_result (keyed by run_id + reference_date), so SCD2 here is
-- deferred rather than built.
select
    merchant_id,
    legal_name,
    trade_name,
    cnpj,
    primary_cnae,
    created_at,
    updated_at
from {{ ref('stg_enterprise_company') }}
