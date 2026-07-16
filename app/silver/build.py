import duckdb

from app.config import settings, get_logger

logger = get_logger("silver.build")

TABLES = {
    "transactions": "transaction_id",
    "reconciliation_runs": "run_id",
    "reconciliation_results": "result_id",
    "enterprise_company": "merchant_id",
}
TIMESTAMP_COLUMNS = {
    "created_at",
    "updated_at",
    "processed_at",
    "settled_at",
    "started_at",
    "finished_at",
}

DATE_COLUMNS = {
    "reference_date",
    "settlement_date",
    "date_partition",
    "ingestion_date",
}

DOUBLE_COLUMNS = {
    "amount",
    "processor_amount",
    "fee_amount",
    "net_amount",
}

INTEGER_COLUMNS = {
    "retry_number",
    "attempt",
}


def build_silver():

    conn = duckdb.connect(settings.DUCKDB_PATH)

    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS silver;
    """)

    for table, pk in TABLES.items():
        logger.info(f"Building silver.{table}")

        columns = conn.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='bronze'
              AND table_name='{table}'
            ORDER BY ordinal_position
        """).fetchall()

        columns = [c[0] for c in columns]

        select_columns = []

        for column in columns:
            if column in TIMESTAMP_COLUMNS:
                select_columns.append(f"TRY_CAST({column} AS TIMESTAMP) AS {column}")

            elif column in DATE_COLUMNS:
                select_columns.append(f"TRY_CAST({column} AS DATE) AS {column}")

            elif column in DOUBLE_COLUMNS:
                select_columns.append(
                    f"TRY_CAST(REPLACE(CAST({column} AS VARCHAR), ',', '.') AS DOUBLE) AS {column}"
                )

            elif column in INTEGER_COLUMNS:
                select_columns.append(f"TRY_CAST({column} AS INTEGER) AS {column}")

            else:
                select_columns.append(column)

        if "updated_at" not in columns:
            conn.execute(f"""
                CREATE OR REPLACE TABLE silver.{table} AS

                SELECT
                    {", ".join(select_columns)}

                FROM bronze.{table}
            """)

            continue

        conn.execute(f"""
            CREATE OR REPLACE TABLE silver.{table} AS

            SELECT
                {", ".join(select_columns)}

            FROM (

                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {pk}
                        ORDER BY CAST(updated_at AS TIMESTAMP) DESC
                    ) AS rn

                FROM bronze.{table}

            )

            WHERE rn = 1
        """)

    conn.close()
