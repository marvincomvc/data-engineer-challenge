"""Pure Python normalization functions.

These are the authoritative parsing rules — used by the silver layer SQL
equivalents and independently testable without any I/O.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation


def parse_amount(raw: str | None) -> Decimal | None:
    """Parse a settlement amount string to Decimal.

    Handles all formats found in source data:
      - Standard:           "13266.42"
      - Comma decimal:      "13266,42"
      - BRL thousands:      "23.696,77"   (dot=thousands, comma=decimal)
      - BRL prefix:         "R$ 23.696,77" or "R$ 873,16"
    """
    if not raw or not raw.strip():
        return None
    s = raw.strip()
    if s.startswith("R$"):
        s = s[2:].strip()
    if "," in s:
        # comma is the decimal separator; any dot before it is a thousands sep
        s = s.replace(".", "").replace(",", ".")
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def normalize_timestamp(raw: str | None) -> datetime | None:
    """Parse an ISO 8601 timestamp string to a timezone-aware datetime (UTC).

    Handles formats found in the source data:
      - "2025-03-04T02:00:00+00:00"  — standard ISO with UTC offset
      - "2025-03-16T14:30:00Z"        — Z suffix
      - "2025-03-16T14:30:00"         — bare datetime (assumed UTC)
    """
    if not raw or not raw.strip():
        return None
    s = raw.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def normalize_merchant_id(raw: str | None) -> str | None:
    """Normalize merchant_id to upper-case stripped form.

    Dirty data can have lowercase merchant ids (e.g. "merch_0001" → "MERCH_0001").
    Returns None when the input is None or empty — preserves NULL semantics.
    """
    if not raw or not raw.strip():
        return None
    return raw.strip().upper()
