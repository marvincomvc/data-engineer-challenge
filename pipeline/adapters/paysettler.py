"""Adapter de CSV do PaySettler — a camada que corrige os dados sujos dessa fonte.

Normaliza os padroes de sujeira ja conhecidos para o schema canonico:
  - valores em moeda brasileira ("R$ 47.401,20" -> 47401.20: tira o R$, ponto eh
    separador de milhar e virgula eh o decimal) e decimais com ponto ("873.16")
  - drift de timezone ("...Z", "...+00:00" ou sem sufixo) -> TIMESTAMP UTC naive
  - caixa do merchant_id ("merch_001" -> "MERCH_001")
  - marca cada linha com source_name pra deixar o downstream agnostico de fonte
"""
from __future__ import annotations

from pipeline.adapters.base import SettlementAdapter


class PaySettlerAdapter(SettlementAdapter):
    name = "paysettler"

    def canonical_sql(self, raw_relation: str) -> str:
        # raw_relation vem como tudo VARCHAR; aqui a gente corrige e tipa.
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
