"""CDC deduplication SQL helpers.

Generates the SQL window-function pattern that picks the latest state
per entity when multiple CDC rows exist for the same key.
"""
from __future__ import annotations


def dedup_latest(source: str, partition_by: str, order_by: str = "_timestamp") -> str:
    """Return a SQL subquery that keeps the single latest row per partition key.

    Uses ROW_NUMBER() DESC so the most recent _timestamp wins.
    The internal _rn column is excluded from the result via EXCLUDE.

    Example:
        dedup_latest("bronze.reconciliation_runs", "id")
        → SELECT * EXCLUDE (_rn) FROM (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _timestamp DESC) AS _rn
              FROM bronze.reconciliation_runs
          ) WHERE _rn = 1
    """
    return f"""
        SELECT * EXCLUDE (_rn)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_by}
                    ORDER BY {order_by} DESC
                ) AS _rn
            FROM {source}
        )
        WHERE _rn = 1
    """
