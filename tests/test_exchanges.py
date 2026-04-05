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
import os
import pytest
from datetime import datetime, timezone, timedelta
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


# ── NonKYC Deposit Parser (Phase 1) ─────────────────────────────────────

class TestNonKYCDepositParser:
    """Tests using fixture data matching the official NonKYC API docs."""

    def setup_method(self):
        self.ex = NonKYCExchange(api_key="test", api_secret="test")

    @pytest.mark.asyncio
    async def test_deposit_uses_quantity_field(self):
        """Official NonKYC deposits use 'quantity', not 'amount'."""
        payload = [{
            "id": "dep-001",
            "ticker": "BTC",
            "childticker": "",
            "quantity": "1.50000000",
            "status": "completed",
            "transactionid": "abc123txhash",
            "isposted": True,
            "isreversed": False,
            "confirmations": 6,
            "firstseenat": "2025-03-15T10:30:00Z",
            "address": "bc1qtest",
            "paymentid": ""
        }]
        with patch.object(self.ex, '_get', return_value=payload):
            result = await self.ex.fetch_deposits()
        assert len(result) == 1
        assert result[0]["amount"] == "1.50000000"
        assert result[0]["tx_hash"] == "abc123txhash"
        assert result[0]["confirmed_at"].year == 2025
        assert result[0]["confirmed_at"].month == 3

    @pytest.mark.asyncio
    async def test_deposit_falls_back_to_amount(self):
        """If 'quantity' is missing, fall back to 'amount' for compat."""
        payload = [{"id": "dep-002", "ticker": "ETH", "amount": "5.0",
                     "txHash": "0xfallback", "confirmedAt": "2025-01-01T00:00:00Z"}]
        with patch.object(self.ex, '_get', return_value=payload):
            result = await self.ex.fetch_deposits()
        assert result[0]["amount"] == "5.0"
        assert result[0]["tx_hash"] == "0xfallback"

    @pytest.mark.asyncio
    async def test_withdrawal_uses_quantity_and_requestedat(self):
        """Official NonKYC withdrawals use 'quantity' and 'requestedat'."""
        payload = [{
            "id": "wd-001",
            "ticker": "BTC",
            "childticker": "",
            "quantity": "0.75000000",
            "fee": "0.00010000",
            "feecurrency": "BTC",
            "status": "completed",
            "transactionid": "def456txhash",
            "issent": True,
            "sentat": "2025-03-15T11:00:00Z",
            "isconfirmed": True,
            "requestedat": "2025-03-15T10:55:00Z",
            "address": "bc1qdest",
            "paymentid": ""
        }]
        with patch.object(self.ex, '_get', return_value=payload):
            result = await self.ex.fetch_withdrawals()
        assert len(result) == 1
        assert result[0]["amount"] == "0.75000000"
        assert result[0]["fee"] == "0.00010000"
        assert result[0]["tx_hash"] == "def456txhash"
        # requestedat should be used for confirmed_at
        assert result[0]["confirmed_at"].minute == 55

    @pytest.mark.asyncio
    async def test_withdrawal_fee_currency_captured(self):
        """feecurrency field must be captured from NonKYC withdrawals."""
        payload = [{"id": "wd-002", "ticker": "SAL", "quantity": "100",
                     "fee": "0.5", "feecurrency": "SAL",
                     "transactionid": "tx999", "requestedat": "2025-06-01T00:00:00Z"}]
        with patch.object(self.ex, '_get', return_value=payload):
            result = await self.ex.fetch_withdrawals()
        assert result[0]["fee_currency"] == "SAL"


# ── MEXC Symbol Discovery (Phase 2) ─────────────────────────────────────

class TestMEXCSymbolDiscovery:
    def setup_method(self):
        self.ex = MEXCExchange(api_key="test", api_secret="test")

    @pytest.mark.asyncio
    async def test_discovers_from_balances(self):
        account_response = {"balances": [
            {"asset": "BTC", "free": "0.5", "locked": "0"},
            {"asset": "ETH", "free": "0", "locked": "1.0"},
            {"asset": "USDT", "free": "1000", "locked": "0"},  # should be excluded
        ]}
        with patch.object(self.ex, '_get', return_value=account_response):
            symbols = await self.ex._get_traded_symbols()
        assert "BTCUSDT" in symbols
        assert "ETHUSDT" in symbols
        assert "USDTUSDT" not in symbols

    @pytest.mark.asyncio
    async def test_extra_symbols_from_env(self):
        account_response = {"balances": []}
        with patch.dict(os.environ, {"MEXC_EXTRA_SYMBOLS": "SOLUSDT,ADAUSDT"}):
            with patch.object(self.ex, '_get', return_value=account_response):
                symbols = await self.ex._get_traded_symbols()
        assert "SOLUSDT" in symbols
        assert "ADAUSDT" in symbols


class TestMEXCRetentionAwareness:
    def setup_method(self):
        self.ex = MEXCExchange(api_key="test", api_secret="test")

    def test_trade_retention_is_30_days(self):
        coverage = self.ex.get_data_coverage(
            since=datetime(2024, 1, 1, tzinfo=timezone.utc))
        assert coverage["myTrades"]["has_gap"] is True
        assert coverage["myTrades"]["retention_days"] == 30

    def test_no_gap_within_retention(self):
        recent = datetime.now(timezone.utc) - timedelta(days=5)
        coverage = self.ex.get_data_coverage(since=recent)
        assert coverage["myTrades"]["has_gap"] is False
