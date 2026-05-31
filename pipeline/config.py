"""Pipeline configuration — single source of truth for paths and defaults."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_DATA_PATH = Path(os.getenv("DATA_PATH", "docs/sample-data"))
DEFAULT_WAREHOUSE_PATH = Path(os.getenv("WAREHOUSE_PATH", "data/warehouse.duckdb"))
DEFAULT_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DEFAULT_ENVIRONMENT = os.getenv("ENVIRONMENT", "local")

RECONCILIATION_WINDOW_DAYS = 7
MISMATCH_TOLERANCE = 0.01  # R$ 0.01
STALE_RUN_HOURS = 1


@dataclass(frozen=True)
class PipelineConfig:
    data_path: Path = DEFAULT_DATA_PATH
    warehouse_path: Path = DEFAULT_WAREHOUSE_PATH
    log_level: str = DEFAULT_LOG_LEVEL
    environment: str = DEFAULT_ENVIRONMENT
    reconciliation_window_days: int = RECONCILIATION_WINDOW_DAYS
    mismatch_tolerance: float = MISMATCH_TOLERANCE
    stale_run_hours: int = STALE_RUN_HOURS

    @property
    def reconciliation_runs(self) -> Path:
        return self.data_path / "reconciliation_runs.parquet"

    @property
    def reconciliation_results(self) -> Path:
        return self.data_path / "reconciliation_results.parquet"

    @property
    def enterprise_company(self) -> Path:
        return self.data_path / "enterprise_company.parquet"

    @property
    def paysettler_csv(self) -> Path:
        return self.data_path / "settlement_paysettler.csv"

    @property
    def paysettler_dir(self) -> Path:
        d = self.data_path / "paysettler"
        if d.is_dir():
            return d
        return self.data_path

    def __post_init__(self):
        self.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
