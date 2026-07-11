-- Canonical settlement stream consumed by the reconciliation engine.
--
-- Each processor gets its own staging *adapter* (stg_paysettler, stg_acmepay, …)
-- that maps its own "similar but not identical" schema onto this one contract:
--   transaction_id, merchant_id, amount, currency, settled_at,
--   processor_reference, status, source_system
--
-- Adding a processor = add its adapter model + one UNION ALL line below. The
-- reconciliation engine (int_reconciliation) and its tests never change.
select
    transaction_id,
    merchant_id,
    amount,
    currency,
    settled_at,
    processor_reference,
    status,
    'paysettler' as source_system
from {{ ref('stg_paysettler') }}

{#
  To onboard a second processor, add its adapter model and union it here
  (Jinja comment so the example ref() is not parsed as a real dependency):

  union all
  select
      transaction_id, merchant_id, amount, currency, settled_at,
      processor_reference, status, 'acmepay' as source_system
  from {{ ref('stg_acmepay') }}
#}
