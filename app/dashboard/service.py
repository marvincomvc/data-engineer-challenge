import duckdb

from app.config import settings


def get_dashboard_data():

    conn = duckdb.connect(settings.DUCKDB_PATH, read_only=True)

    #
    # KPIs CFO
    #

    kpis = conn.execute(
        """
        SELECT

            SUM(transaction_volume) AS transactions,

            SUM(total_internal_amount)
                AS volume,

            ROUND(
                (
                    SUM(total_difference)
                    /
                    NULLIF(
                        SUM(total_internal_amount),
                        0
                    )
                ) * 100,
                2
            ) AS difference_rate,


            SUM(total_difference)
                AS difference


        FROM diamond.cfo_daily_metrics
        """
    ).fetchone()

    #
    # Taxa reconciliação
    #

    reconciliation = conn.execute(
        """
        SELECT

            ROUND(
                (
                    SUM(matched_transactions)
                    /
                    NULLIF(
                        SUM(total_transactions),
                        0
                    )
                ) * 100,
                2
            )

        FROM diamond.operations_reconciliation_health
        """
    ).fetchone()

    #
    # Volume por dia
    #

    daily = conn.execute(
        """
        SELECT

            CAST(reference_date AS VARCHAR),

            total_internal_amount


        FROM diamond.cfo_daily_metrics

        ORDER BY reference_date

        """
    ).fetchall()

    #
    # Status reconciliação
    #

    status_rows = conn.execute(
        """
        SELECT

            CASE
                WHEN difference = 0
                THEN 'MATCHED'

                ELSE 'DISCREPANCY'

            END AS status,


            COUNT(*)


        FROM diamond.compliance_audit_history


        GROUP BY 1

        """
    ).fetchall()

    #
    # Top merchants
    #

    merchants = conn.execute(
        """
        SELECT

            merchant_id,

            SUM(internal_amount)


        FROM diamond.compliance_audit_history


        GROUP BY merchant_id


        ORDER BY 2 DESC


        LIMIT 10

        """
    ).fetchall()

    conn.close()

    return {
        "kpis": {
            "transactions": kpis[0] or 0,
            "volume": kpis[1] or 0,
            "match_rate": reconciliation[0] or 0,
            "difference": kpis[3] or 0,
        },
        "daily": {
            "date": [row[0] for row in daily],
            "volume": [row[1] for row in daily],
            # Mantém compatibilidade com o gráfico
            "match_rate": [reconciliation[0] or 0 for _ in daily],
        },
        "status": {row[0]: row[1] for row in status_rows},
        "merchants": [{"name": row[0], "volume": row[1]} for row in merchants],
    }
