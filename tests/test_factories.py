"""
Tests for test data factories themselves.

Ensures our test helpers produce valid, consistent data structures
that match what the real exchange plugins and database expect.
"""
import json
import pytest
from datetime import datetime, timezone
from decimal import Decimal

from helpers import (
    make_trade, make_deposit, make_withdrawal,
    T_2024_01, T_2025_03, MockSession, MockResult, MockSettings,
)


class TestMakeTrade:
    def test_default_trade(self):
        t = make_trade()
        assert t["exchange"] == "nonkyc"
        assert t["side"] == "buy"
        assert t["market"] == "BTC/USDT"
        assert isinstance(t["executed_at"], datetime)

    def test_custom_trade(self):
        t = make_trade(exchange="mexc", side="sell", price="100", quantity="10")
        assert t["exchange"] == "mexc"
        assert t["side"] == "sell"
        assert t["price"] == "100"

    def test_trade_has_raw_data(self):
        t = make_trade()
        data = json.loads(t["raw_data"])
        assert isinstance(data, dict)

    def test_trade_has_usd_fields(self):
        t = make_trade()
        assert "total_usd" in t
        assert "fee_usd" in t
        assert "price_usd" in t

    def test_trade_base_quote_parsed(self):
        t = make_trade(market="ETH/USDC")
        assert t["base_asset"] == "ETH"
        assert t["quote_asset"] == "USDC"


class TestMakeDeposit:
    def test_default_deposit(self):
        d = make_deposit()
        assert d["asset"] == "BTC"
        assert d["exchange"] == "nonkyc"

    def test_custom_deposit(self):
        d = make_deposit(asset="ETH", amount="10", amount_usd="35000")
        assert d["asset"] == "ETH"
        assert d["amount"] == "10"
        assert d["amount_usd"] == "35000"


class TestMakeWithdrawal:
    def test_default_withdrawal(self):
        w = make_withdrawal()
        assert w["asset"] == "BTC"
        assert w["fee"] == "0.0001"

    def test_fee_usd_computed(self):
        w = make_withdrawal(amount="1.0", fee="0.001", amount_usd="50000")
        fee_usd = Decimal(w["fee_usd"])
        assert fee_usd > 0


class TestMockSession:
    def test_tracks_sql(self):
        session = MockSession()
        import asyncio
        from sqlalchemy import text
        asyncio.get_event_loop().run_until_complete(
            session.execute(text("SELECT 1"), {})
        )
        assert len(session.executed_sql) == 1

    def test_staged_results(self):
        session = MockSession()
        session.stage_rows([(1, "test")], columns=["id", "name"])
        import asyncio
        from sqlalchemy import text
        result = asyncio.get_event_loop().run_until_complete(
            session.execute(text("SELECT * FROM test"), {})
        )
        assert result.fetchall() == [(1, "test")]

    def test_commit_tracking(self):
        session = MockSession()
        import asyncio
        asyncio.get_event_loop().run_until_complete(session.commit())
        assert session.committed is True


class TestMockSettings:
    def test_has_exchanges(self):
        s = MockSettings()
        assert "nonkyc" in s.enabled_exchanges

    def test_has_credentials(self):
        s = MockSettings()
        assert s.nonkyc_api_key == "test_nonkyc_key"
        assert s.mexc_api_secret == "test_mexc_secret"
