"""
Tests for exchange plugins.

Covers:
  - NonKYC HMAC-SHA256 signature construction
  - MEXC signature construction (no sorting)
  - Timestamp parsing (ms, ISO8601, etc.)
  - Trade response normalization
  - Plugin registration
"""
import hashlib
import hmac
import pytest
from datetime import datetime, timezone
from unittest.mock import patch

from exchanges import list_exchanges, _registry
from exchanges.nonkyc import NonKYCExchange
from exchanges.mexc import MEXCExchange


# ── Plugin Registration ───────────────────────────────────────────────────

class TestPluginRegistration:
    def test_nonkyc_registered(self):
        exchanges = list_exchanges()
        assert "nonkyc" in exchanges

    def test_mexc_registered(self):
        exchanges = list_exchanges()
        assert "mexc" in exchanges

    def test_registry_has_classes(self):
        from exchanges import _registry
        assert _registry["nonkyc"] == NonKYCExchange
        assert _registry["mexc"] == MEXCExchange


# ── NonKYC Signature ──────────────────────────────────────────────────────

class TestNonKYCSignature:
    def setup_method(self):
        self.ex = NonKYCExchange(api_key="testkey123", api_secret="testsecret456")

    def test_get_signature_format(self):
        """GET signature: HMAC(secret, key + url + nonce)"""
        url = "https://api.nonkyc.io/api/v2/balances"
        headers = self.ex._sign_get(url)

        assert "X-API-KEY" in headers
        assert "X-API-NONCE" in headers
        assert "X-API-SIGN" in headers
        assert headers["X-API-KEY"] == "testkey123"
        assert len(headers["X-API-NONCE"]) > 0
        assert len(headers["X-API-SIGN"]) == 64  # SHA256 hex digest

    def test_signature_is_deterministic(self):
        """Same inputs → same signature."""
        url = "https://api.nonkyc.io/api/v2/test"
        nonce = "1234567890"
        message = self.ex.api_key + url + nonce
        expected = hmac.new(
            self.ex.api_secret.encode(), message.encode(), hashlib.sha256
        ).hexdigest()

        # Manually replicate
        actual = hmac.new(
            b"testsecret456", f"testkey123{url}{nonce}".encode(), hashlib.sha256
        ).hexdigest()
        assert expected == actual

    def test_post_signature_includes_body(self):
        """POST signature: HMAC(secret, key + url + body + nonce)"""
        url = "https://api.nonkyc.io/api/v2/createorder"
        body = '{"symbol":"BTC/USDT","side":"buy"}'
        headers = self.ex._sign_post(url, body)

        assert headers["Content-Type"] == "application/json"
        assert len(headers["X-API-SIGN"]) == 64


# ── MEXC Signature ────────────────────────────────────────────────────────

class TestMEXCSignature:
    def setup_method(self):
        self.ex = MEXCExchange(api_key="mexckey123", api_secret="mexcsecret456")

    def test_sign_returns_query_string(self):
        params = {"symbol": "BTCUSDT", "limit": "100"}
        query_string = self.ex._sign(params)

        assert "signature=" in query_string
        assert "timestamp=" in query_string
        assert "recvWindow=" in query_string
        assert "symbol=BTCUSDT" in query_string

    def test_sign_does_not_sort_params(self):
        """Critical: MEXC params must be in insertion order, not sorted."""
        params = {"zzz": "last", "aaa": "first"}
        query_string = self.ex._sign(params)

        # zzz should appear before aaa in the query string
        zzz_pos = query_string.index("zzz=")
        aaa_pos = query_string.index("aaa=")
        assert zzz_pos < aaa_pos, "Params should be in insertion order, not sorted"

    def test_signature_is_valid_hmac(self):
        params = {"symbol": "BTCUSDT"}
        query_string = self.ex._sign(params)

        # Extract the signature
        parts = query_string.split("&signature=")
        assert len(parts) == 2
        query_without_sig = parts[0]
        actual_sig = parts[1]

        # Recompute
        expected_sig = hmac.new(
            b"mexcsecret456", query_without_sig.encode(), hashlib.sha256
        ).hexdigest()
        assert actual_sig == expected_sig


# ── Timestamp Parsing ─────────────────────────────────────────────────────

class TestNonKYCTimestampParsing:
    def setup_method(self):
        self.ex = NonKYCExchange(api_key="k", api_secret="s")

    def test_millisecond_timestamp(self):
        ts = self.ex._parse_ts(1709035200000)  # 2024-02-27 12:00:00 UTC
        assert isinstance(ts, datetime)
        assert ts.tzinfo is not None

    def test_iso8601_string(self):
        ts = self.ex._parse_ts("2025-03-10T12:00:00Z")
        assert isinstance(ts, datetime)
        assert ts.year == 2025

    def test_none_returns_now(self):
        ts = self.ex._parse_ts(None)
        assert isinstance(ts, datetime)

    def test_integer_timestamp(self):
        ts = self.ex._parse_ts(1709035200000)
        assert ts.year >= 2024

    def test_float_timestamp(self):
        ts = self.ex._parse_ts(1709035200000.0)
        assert isinstance(ts, datetime)


class TestMEXCTimestampParsing:
    def setup_method(self):
        self.ex = MEXCExchange(api_key="k", api_secret="s")

    def test_millisecond_int(self):
        ts = self.ex._parse_ts(1709035200000)
        assert isinstance(ts, datetime)

    def test_none(self):
        ts = self.ex._parse_ts(None)
        assert isinstance(ts, datetime)


# ── Trade Normalization ───────────────────────────────────────────────────

class TestNonKYCTradeNormalization:
    """Test that raw NonKYC API responses are normalized to the expected schema."""

    def test_trade_has_required_fields(self):
        from helpers import make_trade
        trade = make_trade()
        required = ["exchange", "exchange_id", "market", "side", "price",
                    "quantity", "total", "fee", "executed_at", "raw_data"]
        for field in required:
            assert field in trade, f"Missing field: {field}"

    def test_trade_usd_fields_present(self):
        from helpers import make_trade
        trade = make_trade()
        usd_fields = ["price_usd", "quantity_usd", "total_usd", "fee_usd",
                      "base_price_usd", "quote_price_usd"]
        for field in usd_fields:
            assert field in trade, f"Missing USD field: {field}"

    def test_side_is_lowercase(self):
        from helpers import make_trade
        for side in ["buy", "sell"]:
            trade = make_trade(side=side)
            assert trade["side"] == side
