"""Central configuration for the settlement reconciliation pipeline.

All tunables are read from the environment so the same code runs identically
in local Docker, CI, and (hypothetically) production. Defaults match the
challenge fixtures so `python -m pipeline run` works with zero setup.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


@dataclass(frozen=True)
class Config:
    # --- paths -------------------------------------------------------------
    data_path: Path = field(default_factory=lambda: Path(_env("DATA_PATH", "docs/sample-data")))
    duckdb_path: Path = field(
        default_factory=lambda: Path(_env("DUCKDB_PATH", "warehouse/settlement.duckdb"))
    )

    # --- reconciliation business rules ------------------------------------
    tolerance: Decimal = field(
        default_factory=lambda: Decimal(_env("RECONCILIATION_TOLERANCE", "0.01"))
    )
    window_days: int = field(default_factory=lambda: int(_env("RECONCILIATION_WINDOW_DAYS", "7")))

    # --- data-quality anomaly thresholds (warn, not block) ----------------
    mismatch_rate_warn: float = field(
        default_factory=lambda: float(_env("MISMATCH_RATE_WARN", "0.10"))
    )
    orphan_rate_warn: float = field(default_factory=lambda: float(_env("ORPHAN_RATE_WARN", "0.05")))

    # --- observability ----------------------------------------------------
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))
    environment: str = field(default_factory=lambda: _env("ENVIRONMENT", "local"))

    def gold_path(self) -> Path:
        """Published data-products zone. Defaults to a `gold/` sibling of the
        warehouse dir; override with GOLD_PATH."""
        default = self.duckdb_path.parent.parent / "gold"
        return Path(_env("GOLD_PATH", str(default)))

    def csv_path(self) -> Path:
        """PaySettler settlement file location."""
        return self.data_path / "settlement_paysettler.csv"

    def parquet(self, name: str) -> Path:
        return self.data_path / f"{name}.parquet"


CONFIG = Config()
