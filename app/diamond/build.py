from pathlib import Path

import duckdb

from app.config import settings


def build_diamond():

    conn = duckdb.connect(settings.DUCKDB_PATH)

    conn.execute(
        """
        CREATE SCHEMA IF NOT EXISTS diamond;
        """
    )

    diamond_path = Path(settings.DIAMOND_PATH)

    diamond_path.mkdir(parents=True, exist_ok=True)

    #
    # Operações
    #

    conn.execute(
        """

        CREATE OR REPLACE TABLE
        diamond.operations_reconciliation_health AS


        SELECT


            date_partition AS reference_date,


            COUNT(*) AS total_transactions,


            SUM(
                CASE
                    WHEN is_reconciled
                    THEN 1
                    ELSE 0
                END
            ) AS matched_transactions,


            SUM(
                CASE
                    WHEN NOT is_reconciled
                    THEN 1
                    ELSE 0
                END
            ) AS discrepancy_transactions,


            ROUND(

                SUM(
                    CASE
                        WHEN NOT is_reconciled
                        THEN 1
                        ELSE 0
                    END
                )
                /
                COUNT(*)

                *
                100,

                2

            ) AS discrepancy_rate



        FROM gold.fact_reconciliation


        GROUP BY 1;


        """
    )

    #
    # CFO
    #

    conn.execute(
        """

        CREATE OR REPLACE TABLE
        diamond.cfo_daily_metrics AS


        SELECT


            date_partition AS reference_date,


            COUNT(*) AS transaction_volume,


            SUM(
                COALESCE(
                    internal_amount,
                    0
                )
            ) AS total_internal_amount,


            SUM(
                COALESCE(
                    processor_amount,
                    0
                )
            ) AS total_processor_amount,


            SUM(
                COALESCE(
                    difference,
                    0
                )
            ) AS total_difference



        FROM gold.fact_reconciliation


        GROUP BY 1;


        """
    )

    #
    # Compliance
    #

    conn.execute(
        """

        CREATE OR REPLACE TABLE
        diamond.compliance_audit_history AS


        SELECT


            rr.run_id,


            rr.transaction_id,


            rr.merchant_id,


            rr.category,


            rr.internal_amount,


            rr.processor_amount,


            rr.difference,


            rr.created_at,


            r.reference_date,


            r.file_name,


            r.status AS reconciliation_status



        FROM gold.fact_reconciliation rr


        LEFT JOIN bronze.reconciliation_runs r

        ON rr.run_id = r.id;



        """
    )

    #
    # Export parquet
    #

    tables = ["operations_reconciliation_health", "cfo_daily_metrics", "compliance_audit_history"]

    for table in tables:
        conn.execute(
            f"""

            COPY diamond.{table}

            TO '{diamond_path}/{table}'

            (
                FORMAT PARQUET,
                OVERWRITE_OR_IGNORE TRUE
            );


            """
        )

    conn.close()

    print("Diamond layer created")
