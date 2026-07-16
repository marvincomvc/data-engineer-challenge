from pathlib import Path

import duckdb

from app.config import settings, get_logger

logger = get_logger("bronze.load")

DOMAINS = {
    "transactions": "transactions_batch_*.parquet",
    "reconciliation_runs": "reconciliation_runs.parquet",
    "reconciliation_results": "reconciliation_results.parquet",
    "enterprise_company": "enterprise_company.parquet",
}


def load_bronze():

    conn = duckdb.connect(settings.DUCKDB_PATH)

    conn.execute(
        """
        CREATE SCHEMA IF NOT EXISTS bronze;
        """
    )

    raw_path = Path(settings.DATA_PATH) / "raw"

    bronze_path = Path(settings.BRONZE_PATH)

    bronze_path.mkdir(parents=True, exist_ok=True)

    for domain, filename in DOMAINS.items():
        source_file = raw_path / filename

        target_path = bronze_path / domain

        target_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Processing bronze: {domain}")

        query = f"""

        CREATE OR REPLACE TABLE
            bronze.{domain}

        AS

        SELECT

            *,

            current_timestamp
                AS ingestion_timestamp,

            current_date
                AS ingestion_date,

            CAST(created_at AS DATE)
                AS date_partition
        FROM read_parquet(
            '{source_file}'
        );

        """

        # Alguns arquivos não possuem created_at
        if domain == "reconciliation_results":
            query = f"""

            CREATE OR REPLACE TABLE
                bronze.{domain}

            AS

            SELECT

                *,

                current_timestamp
                    AS ingestion_timestamp,

                current_date
                    AS ingestion_date,

                CAST(created_at AS DATE)
                    AS date_partition
            FROM read_parquet(
                '{source_file}'
            );

            """

        elif domain == "reconciliation_runs":
            query = f"""

            CREATE OR REPLACE TABLE
                bronze.{domain}

            AS

            SELECT

                *,

                current_timestamp
                    AS ingestion_timestamp,

                current_date
                    AS ingestion_date,

                reference_date
                    AS date_partition
            FROM read_parquet(
                '{source_file}'
            );

            """

        elif domain == "enterprise_company":
            query = f"""

            CREATE OR REPLACE TABLE
                bronze.{domain}

            AS

            SELECT

                *,

                current_timestamp
                    AS ingestion_timestamp,

                current_date
                    AS ingestion_date,

                CAST(created_at AS DATE)
                    AS date_partition
            FROM read_parquet(
                '{source_file}'
            );

            """

        conn.execute(query)

        # Exporta Bronze em parquet

        conn.execute(
            f"""
            COPY bronze.{domain}

            TO '{target_path}'

            (
                FORMAT PARQUET,
                PARTITION_BY (
                    date_partition
                ),
                OVERWRITE_OR_IGNORE TRUE
            );
            """
        )

        logger.info(f"Bronze created: {domain}")

    conn.close()
