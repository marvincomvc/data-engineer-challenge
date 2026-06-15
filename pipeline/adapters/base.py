"""Settlement source adapter interface.

Each external processor (PaySettler, future ones) implements this to map its
raw file shape into the canonical settlement schema (config.CANONICAL_SETTLEMENT_COLUMNS).
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class SettlementAdapter(ABC):
    name: str

    @abstractmethod
    def canonical_sql(self, raw_relation: str) -> str:
        """Return a SELECT that maps `raw_relation` to the canonical settlement columns."""
        ...
