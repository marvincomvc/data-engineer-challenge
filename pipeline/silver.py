"""Silver layer: CDC dedup + conformance to clean, canonical entities."""
from __future__ import annotations

from pipeline.adapters.base import SettlementAdapter


def dedup_latest(relation: str, key: str, ts: str = "_timestamp", idcol: str = "id") -> str:
    """SQL that keeps the latest CDC version per `key` and drops deleted rows.

    Picks the row with the most recent `_timestamp` per natural key, then excludes
    rows whose latest operation is a delete (Op = 'D'). This is the heart of the
    CDC handling: the real fixture contains U (update) and D (delete) operations.

    `id` is NOT unique in the union (a CDC-replay artifact), so `_timestamp DESC,
    id DESC` alone leaves ties that make row_number non-deterministic. We add the
    physical `rowid` as a final tiebreak to guarantee a stable, reproducible result.
    """
    return f"""
        SELECT * EXCLUDE (_rn) FROM (
            SELECT *, row_number() OVER (
                PARTITION BY {key} ORDER BY {ts} DESC, {idcol} DESC, rowid DESC
            ) AS _rn
            FROM {relation}
        ) AS _ranked
        WHERE _rn = 1 AND Op <> 'D'
    """


def build_silver(con, adapter: SettlementAdapter) -> None:
    """Build the clean silver entities from bronze."""
    # Internal transactions: dedup CDC, drop deletes.
    con.execute(f"CREATE OR REPLACE TABLE silver_transactions AS {dedup_latest('bronze_transactions', 'transaction_id')}")
    # Reconciliation results / merchants / runs: latest state per key.
    con.execute(f"CREATE OR REPLACE TABLE silver_results AS {dedup_latest('bronze_results', 'id')}")
    con.execute(f"CREATE OR REPLACE TABLE silver_merchants AS {dedup_latest('bronze_merchants', 'merchant_id')}")
    con.execute(f"CREATE OR REPLACE TABLE silver_runs AS {dedup_latest('bronze_runs', 'id')}")

    # Settlement feed -> canonical schema (via adapter) + has_internal flag.
    con.execute("CREATE OR REPLACE VIEW _raw_ps AS SELECT * FROM bronze_settlement_raw")
    con.execute(f"""
        CREATE OR REPLACE TABLE silver_settlement AS
        WITH canon AS ({adapter.canonical_sql('_raw_ps')})
        SELECT c.*, (t.transaction_id IS NOT NULL) AS has_internal
        FROM canon c
        LEFT JOIN silver_transactions t USING (transaction_id)
    """)
    # Current settlement state: one row per transaction_id (latest settled_at).
    # The raw feed (silver_settlement) keeps every event; gold joins use this 1:1 view
    # so a transaction settled twice in a day does not fan out the transaction-grain fact.
    con.execute("""
        CREATE OR REPLACE TABLE silver_settlement_current AS
        SELECT * EXCLUDE (_rn) FROM (
            SELECT *, row_number() OVER (
                PARTITION BY transaction_id ORDER BY settled_at DESC, processor_reference DESC
            ) AS _rn
            FROM silver_settlement
        ) AS _r
        WHERE _rn = 1
    """)
    con.execute("CREATE TABLE IF NOT EXISTS quarantine_settlement(transaction_id VARCHAR, reason VARCHAR)")
