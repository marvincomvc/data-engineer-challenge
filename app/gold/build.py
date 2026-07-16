from pathlib import Path

import duckdb

from app.config import settings, get_logger

logger = get_logger("gold.build")


def build_gold():

    conn = duckdb.connect(settings.DUCKDB_PATH)

    conn.execute(
        """
        CREATE SCHEMA IF NOT EXISTS gold;
        """
    )

    gold_path = Path(settings.GOLD_PATH)

    gold_path.mkdir(parents=True, exist_ok=True)

    #
    # Dimensão Merchant
    #
    conn.execute(
        """

        CREATE OR REPLACE TABLE gold.dim_merchant AS

        SELECT

            id AS merchant_key,

            merchant_id,

            legal_name,

            trade_name,

            document,

            primary_cnae,

            created_at,

            updated_at,

            ingestion_date
        FROM silver.enterprise_company;
        """
    )

    #
    # Fato de reconciliação
    #
    conn.execute(
        """

        CREATE OR REPLACE TABLE gold.fact_reconciliation AS

        SELECT

            rr.id AS reconciliation_result_id,

            rr.run_id,

            rr.transaction_id,

            rr.merchant_id,
            rr.category,
            rr.internal_amount,

            rr.processor_amount,

            rr.difference,
            rr.created_at,
            rr.ingestion_date,

            rr.date_partition,
            CASE

                WHEN rr.category = 'MATCHED'
                    THEN TRUE

                ELSE FALSE

            END AS is_reconciled,
           CASE WHEN rr.difference IS NOT NULL AND rr.difference > 0 THEN TRUE
            WHEN rr.difference IS NULL THEN NULL  -- ou trate como caso à parte
            ELSE FALSE END AS has_difference

        FROM silver.reconciliation_results rr;
        """
    )

    #
    # Exporta parquet gold
    #

    conn.execute(
        f"""

        COPY gold.fact_reconciliation

        TO '{gold_path}/fact_reconciliation'

        (
            FORMAT PARQUET,
            PARTITION_BY(date_partition),
            OVERWRITE_OR_IGNORE TRUE
        );
        """
    )

    conn.execute(
        f"""

        COPY gold.dim_merchant

        TO '{gold_path}/dim_merchant'

        (
            FORMAT PARQUET,
            OVERWRITE_OR_IGNORE TRUE
        );
        """
    )

    conn.close()

    logger.info("Gold layer created")
