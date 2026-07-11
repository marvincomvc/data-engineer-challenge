-- INTEGRITY (error): the compliance audit trail must be exactly one row per
-- (reference_date, transaction_id). A duplicate (e.g. a run-header join fan-out)
-- would silently double the audit record — block it.
select
    reference_date,
    transaction_id,
    count(*) as n
from {{ ref('mart_compliance_audit') }}
group by 1, 2
having count(*) > 1
