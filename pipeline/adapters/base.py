"""Interface dos adapters de fontes de settlement.

Cada processador externo (PaySettler e os futuros) implementa essa interface para
mapear o formato bruto do seu arquivo no schema canonico de settlement (config.CANONICAL_SETTLEMENT_COLUMNS).
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class SettlementAdapter(ABC):
    name: str

    @abstractmethod
    def canonical_sql(self, raw_relation: str) -> str:
        """Retorna um SELECT que mapeia `raw_relation` para as colunas canonicas de settlement."""
        ...
