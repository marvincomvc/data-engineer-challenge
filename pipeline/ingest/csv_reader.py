"""Read PaySettler CSV with normalization of dates and amounts.

Handles:
- Missing 'Z' suffix on settled_at timestamps
- Comma-as-decimal in amount field
- Currency symbols or spaces in amount (e.g. "R$ 873.16")
- Thousand-separator dots (e.g. "32.245.91")
- Case normalization on merchant_id
"""
from __future__ import annotations

import re

import duckdb
from pipeline.config import PipelineConfig
from pipeline.observability.logging import get_logger

logger = get_logger(__name__)

# Pre-compiled regex for amount cleaning
_CURRENCY_RE = re.compile(r"[R$\s]")
_MULTI_DOT_RE = re.compile(r"\.(?=.*\.)")


def _normalize_amount(raw: str) -> float:
    """Parse a potentially dirty amount string to float.

    Handles: "152.30", "152,30", "R$ 873.16", "32.245,91"
    """
    if not raw or not raw.strip():
        return 0.0
    s = raw.strip()
    # Remove currency symbols and whitespace
    s = _CURRENCY_RE.sub("", s)
    # If contains comma: treat as decimal separator (Brazilian format)
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        # Multiple dots: all but last are thousand separators
        parts = s.split(".")
        if len(parts) > 2:
            s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(s)
    except ValueError:
        return 0.0


def _normalize_settled_at(raw: str) -> str:
    """Ensure settled_at has a Z suffix if no other timezone indicator."""
    if not raw:
        return raw
    s = raw.strip()
    if s and not s.endswith("Z") and "+" not in s:
        # Check if it already has a numeric timezone offset pattern
        if not re.search(r"[+-]\d{2}:\d{2}$", s):
            s = s + "Z"
    return s


def read_paysettler_csv(con: duckdb.DuckDBPyConnection, cfg: PipelineConfig) -> int:
    """Read PaySettler CSV, normalize, and create as table 'raw_paysettler'.

    Returns row count.
    """
    csv_path = str(cfg.paysettler_csv)

    # Create the target table first
    con.execute("""
        CREATE OR REPLACE TABLE raw_paysettler (
            transaction_id VARCHAR,
            merchant_id VARCHAR,
            amount DOUBLE,
            currency VARCHAR,
            settled_at VARCHAR,
            processor_reference VARCHAR,
            status VARCHAR
        )
    """)

    # Read raw CSV as all-varchar using DuckDB
    raw_rows = con.execute(f"""
        SELECT transaction_id, merchant_id, amount, currency,
               settled_at, processor_reference, status
        FROM read_csv('{csv_path}', header=true, all_varchar=true)
    """).fetchall()

    row_count = len(raw_rows)
    if row_count == 0:
        logger.warning("PaySettler CSV is empty (0 data rows)")
        return 0

    # Normalize each row
    rows_to_insert = []
    for txn_id, merch, amt, cur, settled, ref, st in raw_rows:
        amount_val = _normalize_amount(amt or "0")
        merch_clean = (merch or "").upper()
        settled_clean = _normalize_settled_at(settled or "")
        rows_to_insert.append((txn_id, merch_clean, amount_val, cur, settled_clean, ref, st))

    # Insert using parameterized queries in batches
    batch_size = 1000
    for i in range(0, len(rows_to_insert), batch_size):
        batch = rows_to_insert[i:i + batch_size]
        placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?)"] * len(batch))
        flat_params = []
        for row in batch:
            flat_params.extend(row)
        con.execute(
            f"INSERT INTO raw_paysettler VALUES {placeholders}",
            flat_params,
        )

    logger.info(f"PaySettler CSV loaded: {row_count} rows")
    return row_count
