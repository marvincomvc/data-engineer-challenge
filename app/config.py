# app/config.py

import os

from dotenv import load_dotenv

load_dotenv()


class Settings:
    # ==========================
    # Application
    # ==========================

    ENV = os.getenv("ENV", "local")

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
