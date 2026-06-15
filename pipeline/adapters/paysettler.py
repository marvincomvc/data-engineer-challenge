"""PaySettler CSV adapter — the FIX layer for this source's dirty data.

Normalizes the known dirty patterns into the canonical schema:
  - Brazilian currency amounts ("R$ 47.401,20" -> 47401.20: strip R$, dots are
    thousands separators, comma is the decimal) and plain dot decimals ("873.16")
  - timezone drift ("...Z", "...+00:00", or no suffix) -> naive UTC TIMESTAMP
  - merchant_id case ("merch_001" -> "MERCH_001")
  - tags every row with source_name so downstream is source-agnostic
"""
from __future__ import annotations

from pipeline.adapters.base import SettlementAdapter


class PaySettlerAdapter(SettlementAdapter):
    name = "paysettler"

    def canonical_sql(self, raw_relation: str) -> str:
        # raw_relation is read as all-VARCHAR; we fix and type here.
        return f"""
        SELECT
            transaction_id,
            upper(merchant_id)                                  AS merchant_id,
            CAST(
                CASE
                    WHEN regexp_replace(amount, '[^0-9.,-]', '', 'g') LIKE '%,%'
                    THEN replace(replace(regexp_replace(amount, '[^0-9.,-]', '', 'g'), '.', ''), ',', '.')
                    ELSE regexp_replace(amount, '[^0-9.,-]', '', 'g')
                END AS DECIMAL(18,2)
            )                                                   AS settled_amount,
            currency,
            CAST(
                replace(
                    regexp_replace(settled_at, '(Z|[+-][0-9]{{2}}:?[0-9]{{2}})$', ''),
                    'T', ' '
                ) AS TIMESTAMP
            )                                                   AS settled_at,
            processor_reference,
            status,
            '{self.name}'                                       AS source_name
        FROM {raw_relation}
        """
