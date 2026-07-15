import duckdb
import pyarrow as pa

from app.config import settings


def save_quality_results(results):

    conn = duckdb.connect(settings.DUCKDB_PATH)

    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS gold;
    """)

    table = pa.Table.from_pylist(results)

    conn.register(
        "quality_tmp",
        table,
    )

    conn.execute("""
        CREATE OR REPLACE TABLE gold.quality_report AS

        SELECT
            check_name,
            status,
            message,
            rows,
            checked_at
        FROM quality_tmp
    """)

    conn.close()
