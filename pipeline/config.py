"""Pipeline configuration: canonical settlement schema and source registry.

Adding a second processor = one new adapter module + one entry in SOURCES.
Silver/Gold operate only on the canonical schema, so they do not change.
"""
from __future__ import annotations

from dataclasses import dataclass

# Canonical settlement schema — every adapter must output exactly these columns.
CANONICAL_SETTLEMENT_COLUMNS = [
    "transaction_id",
    "merchant_id",
    "settled_amount",
    "currency",
    "settled_at",
    "processor_reference",
    "status",
    "source_name",
]

AMOUNT_TOLERANCE = 0.01          # R$ 0,01 — difference <= tolerance counts as matched
RECONCILIATION_WINDOW_DAYS = 7   # sliding window [reference_date - 7d, reference_date]


@dataclass(frozen=True)
class SourceConfig:
    name: str
    file_glob: str
    decimal_separator: str
    assume_utc: bool
    column_map: dict[str, str]   # raw column -> canonical column


SOURCES: dict[str, SourceConfig] = {
    "paysettler": SourceConfig(
        name="paysettler",
        file_glob="settlement_paysettler.csv",
        decimal_separator=",",
        assume_utc=True,
        column_map={
            "transaction_id": "transaction_id",
            "merchant_id": "merchant_id",
            "amount": "settled_amount",
            "currency": "currency",
            "settled_at": "settled_at",
            "processor_reference": "processor_reference",
            "status": "status",
        },
    ),
}
