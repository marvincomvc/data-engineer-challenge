from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

_DEFAULT_SQL_DIR = str(Path(__file__).resolve().parents[1] / "sql")


@dataclass(frozen=True)
class Config:
    data_path: Path
    db_path: Path
    sql_dir: Path
    log_level: str
    environment: str

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            data_path=Path(os.environ.get("DATA_PATH", "/app/docs/sample-data")),
            db_path=Path(os.environ.get("DB_PATH", "/data/warehouse.duckdb")),
            sql_dir=Path(os.environ.get("SQL_DIR", _DEFAULT_SQL_DIR)),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
            environment=os.environ.get("ENVIRONMENT", "production"),
        )
