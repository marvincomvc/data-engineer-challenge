# app/config.py

import os
from dotenv import load_dotenv
import logging
import sys

load_dotenv()


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

    return logger


class Settings:
    # ==========================
    # Application
    # ==========================

    ENV = os.getenv("ENVIRONMENT", "local")

    PIPELINE_NAME = os.getenv("PIPELINE_NAME", "settlement-reconciliation")

    # ==========================
    # DuckDB / Storage
    # ==========================

    DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/duckdb-data/warehouse.duckdb")

    RAW_SOURCE_PATH = os.getenv("RAW_SOURCE_PATH", "/app/raw")

    DATA_PATH = os.getenv("DATA_PATH", "/app/data")

    BRONZE_PATH = os.getenv("BRONZE_PATH", "/app/data/bronze")

    SILVER_PATH = os.getenv("SILVER_PATH", "/app/data/silver")

    GOLD_PATH = os.getenv("GOLD_PATH", "/app/data/gold")

    DIAMOND_PATH = os.getenv("DIAMOND_PATH", "/app/data/diamond")

    # ==========================
    # Processing
    # ==========================

    REFERENCE_DATE = os.getenv("REFERENCE_DATE")

    # ==========================
    # Data Quality
    # ==========================

    FAIL_ON_DATA_QUALITY_ERROR = os.getenv("FAIL_ON_DATA_QUALITY_ERROR", "true").lower() == "true"


settings = Settings()
