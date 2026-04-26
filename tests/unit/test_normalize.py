"""Unit tests for pure normalization functions — no I/O required."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from pipeline.transform.normalize import normalize_merchant_id, normalize_timestamp, parse_amount


class TestParseAmount:
    def test_standard_float(self):
        assert parse_amount("13266.42") == Decimal("13266.42")

    def test_comma_decimal(self):
        assert parse_amount("13266,42") == Decimal("13266.42")

    def test_brl_thousands(self):
        assert parse_amount("23.696,77") == Decimal("23696.77")

    def test_brl_prefix_with_thousands(self):
        assert parse_amount("R$ 23.696,77") == Decimal("23696.77")

    def test_brl_prefix_comma_only(self):
        assert parse_amount("R$ 873,16") == Decimal("873.16")

    def test_none_returns_none(self):
        assert parse_amount(None) is None

    def test_empty_string_returns_none(self):
        assert parse_amount("") is None

    def test_whitespace_only_returns_none(self):
        assert parse_amount("   ") is None

    def test_junk_string_returns_none(self):
        assert parse_amount("N/A") is None

    def test_zero_is_valid(self):
        assert parse_amount("0.00") == Decimal("0.00")

    def test_integer_amount(self):
        assert parse_amount("100") == Decimal("100")

    def test_leading_trailing_whitespace_stripped(self):
        assert parse_amount("  500.00  ") == Decimal("500.00")

    def test_brl_prefix_no_space(self):
        """R$100,00 without space after prefix should still parse."""
        assert parse_amount("R$100,00") == Decimal("100.00")


class TestNormalizeTimestamp:
    def test_z_suffix_converted_to_utc(self):
        result = normalize_timestamp("2025-03-16T14:30:00Z")
        assert result == datetime(2025, 3, 16, 14, 30, 0, tzinfo=timezone.utc)

    def test_explicit_utc_offset(self):
        result = normalize_timestamp("2025-03-04T02:00:00+00:00")
        assert result == datetime(2025, 3, 4, 2, 0, 0, tzinfo=timezone.utc)

    def test_bare_datetime_assumed_utc(self):
        result = normalize_timestamp("2025-03-16T14:30:00")
        assert result is not None
        assert result.tzinfo is not None
        assert result.year == 2025

    def test_none_returns_none(self):
        assert normalize_timestamp(None) is None

    def test_empty_returns_none(self):
        assert normalize_timestamp("") is None

    def test_whitespace_returns_none(self):
        assert normalize_timestamp("   ") is None

    def test_junk_returns_none(self):
        assert normalize_timestamp("not-a-date") is None

    def test_date_only_parsed_as_midnight_utc(self):
        """Python 3.11+ fromisoformat accepts date-only strings as midnight UTC."""
        result = normalize_timestamp("2025-03-04")
        assert result is not None
        assert result.year == 2025 and result.month == 3 and result.day == 4
        assert result.tzinfo is not None

    def test_z_becomes_aware(self):
        result = normalize_timestamp("2025-01-01T00:00:00Z")
        assert result is not None
        assert result.tzinfo is not None


class TestNormalizeMerchantId:
    def test_lowercase_uppercased(self):
        assert normalize_merchant_id("merch_0001") == "MERCH_0001"

    def test_mixed_case_uppercased(self):
        assert normalize_merchant_id("Merch_0001") == "MERCH_0001"

    def test_strips_leading_trailing_whitespace(self):
        assert normalize_merchant_id("  MERCH_0001  ") == "MERCH_0001"

    def test_already_upper_unchanged(self):
        assert normalize_merchant_id("MERCH_0001") == "MERCH_0001"

    def test_none_returns_none(self):
        assert normalize_merchant_id(None) is None

    def test_empty_returns_none(self):
        assert normalize_merchant_id("") is None

    def test_whitespace_only_returns_none(self):
        assert normalize_merchant_id("   ") is None
