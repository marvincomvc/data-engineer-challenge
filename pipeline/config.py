"""Configuração do pipeline: schema canônico de liquidação e registro de fontes.

Adicionar um segundo processador = um novo módulo de adapter + uma entrada em SOURCES.
Silver/Gold trabalham só com o schema canônico, então não precisam mudar.
"""
from __future__ import annotations

from dataclasses import dataclass

# Schema canônico de liquidação — todo adapter precisa devolver exatamente estas colunas.
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

AMOUNT_TOLERANCE = 0.01          # R$ 0,01 — diferença <= tolerância já conta como casada
RECONCILIATION_WINDOW_DAYS = 7   # janela deslizante [data_referencia - 7d, data_referencia]


@dataclass(frozen=True)
class SourceConfig:
    name: str
    file_glob: str
    decimal_separator: str
    assume_utc: bool
    column_map: dict[str, str]   # coluna bruta -> coluna canônica


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
