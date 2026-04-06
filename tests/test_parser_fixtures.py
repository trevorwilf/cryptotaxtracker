"""
Parser fixture tests for NonKYC and MEXC exchanges.

Tests both parsers against realistic fixture data that matches
official API documentation.
"""
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import patch

from exchanges.nonkyc import NonKYCExchange
from exchanges.mexc import MEXCExchange


class TestNonKYCDepositFixture:
    """NonKYC deposit with all documented fields."""

    def setup_method(self):
        self.ex = NonKYCExchange(api_key="test", api_secret="test")

    @pytest.mark.asyncio
    async def test_full_deposit_fixture(self):
        payload = [{
            "id": "dep-100",
            "address": "bc1q0example",
            "paymentid": "pay123",
            "ticker": "BTC",
            "childticker": "WBTC",
            "quantity": "0.50000000",
            "status": "completed",
            "transactionid": "abc123def456",
            "isposted": True,
            "isreversed": False,
            "confirmations": 6,
            "firstseenat": "2025-04-10T14:30:00Z"
        }]
        with patch.object(self.ex, '_get', return_value=payload):
            result = await self.ex.fetch_deposits()

        d = result[0]
        assert d["exchange"] == "nonkyc"
        assert d["exchange_id"] == "dep-100"
        assert d["asset"] == "BTC"
        assert d["child_asset"] == "WBTC"
        assert d["amount"] == "0.50000000"
        assert d["tx_hash"] == "abc123def456"
        assert d["address"] == "bc1q0example"
        assert d["payment_id"] == "pay123"
        assert d["status"] == "completed"
        assert d["confirmations"] == 6
        assert d["is_posted"] is True
        assert d["is_reversed"] is False
        assert d["confirmed_at"].year == 2025
        assert d["confirmed_at"].month == 4
        assert d["confirmed_at"].day == 10
        assert d["asset_price_usd"] is None
        assert d["amount_usd"] is None
        assert "raw_data" in d
        raw = json.loads(d["raw_data"])
        assert raw["quantity"] == "0.50000000"


class TestNonKYCWithdrawalFixture:
    """NonKYC withdrawal with all documented fields."""

    def setup_method(self):
        self.ex = NonKYCExchange(api_key="test", api_secret="test")

    @pytest.mark.asyncio
    async def test_full_withdrawal_fixture(self):
        payload = [{
            "id": "wd-200",
            "address": "addr_dest_001",
            "paymentid": "pay456",
            "ticker": "SAL",
            "childticker": "",
            "quantity": "500.00000000",
            "fee": "1.00000000",
            "feecurrency": "SAL",
            "status": "completed",
            "transactionid": "txhash_wd_200",
            "issent": True,
            "sentat": "2025-04-11T10:00:00Z",
            "isconfirmed": True,
            "requestedat": "2025-04-11T09:55:00Z"
        }]
        with patch.object(self.ex, '_get', return_value=payload):
            result = await self.ex.fetch_withdrawals()

        w = result[0]
        assert w["exchange"] == "nonkyc"
        assert w["exchange_id"] == "wd-200"
        assert w["asset"] == "SAL"
        assert w["child_asset"] == ""
        assert w["amount"] == "500.00000000"
        assert w["fee"] == "1.00000000"
        assert w["fee_currency"] == "SAL"
        assert w["tx_hash"] == "txhash_wd_200"
        assert w["address"] == "addr_dest_001"
        assert w["payment_id"] == "pay456"
        assert w["is_sent"] is True
        assert w["is_confirmed"] is True
        assert w["confirmed_at"].minute == 55  # requestedat used


class TestNonKYCTradeFixture:
    """NonKYC trade with documented fields."""

    def setup_method(self):
        self.ex = NonKYCExchange(api_key="test", api_secret="test")

    @pytest.mark.asyncio
    async def test_full_trade_fixture(self):
        payload = [{
            "id": "t-300",
            "symbol": "BTC/USDT",
            "side": "buy",
            "price": "50000.00",
            "quantity": "0.25",
            "total": "12500.00",
            "fee": "12.50",
            "feeAsset": "USDT",
            "timestamp": "2025-04-12T16:00:00Z"
        }]
        with patch.object(self.ex, '_get', return_value=payload):
            result = await self.ex.fetch_trades()

        t = result[0]
        assert t["exchange"] == "nonkyc"
        assert t["exchange_id"] == "t-300"
        assert t["market"] == "BTC/USDT"
        assert t["side"] == "buy"
        assert t["price"] == "50000.00"
        assert t["quantity"] == "0.25"
        assert t["total"] == "12500.00"
        assert t["fee"] == "12.50"
        assert t["fee_asset"] == "USDT"


class TestMEXCTradeFixture:
    """MEXC trade response fixture."""

    def setup_method(self):
        self.ex = MEXCExchange(api_key="test", api_secret="test")

    @pytest.mark.asyncio
    async def test_full_trade_fixture(self):
        account_response = {"balances": [{"asset": "BTC", "free": "0.5", "locked": "0"}]}
        trade_response = [{
            "id": "mt-400",
            "symbol": "BTCUSDT",
            "price": "55000",
            "qty": "0.10",
            "quoteQty": "5500",
            "commission": "5.50",
            "commissionAsset": "USDT",
            "time": 1712937600000,  # 2024-04-12 16:00:00 UTC
            "isBuyer": True
        }]

        call_count = 0
        async def mock_get(path, params=None, signed=True):
            nonlocal call_count
            call_count += 1
            if "/api/v3/account" in path:
                return account_response
            return trade_response

        with patch.object(self.ex, '_get', side_effect=mock_get):
            result = await self.ex.fetch_trades()

        assert len(result) >= 1
        t = result[0]
        assert t["exchange"] == "mexc"
        assert t["side"] == "buy"
        assert t["price"] == "55000"
        assert t["quantity"] == "0.10"
        assert t["fee"] == "5.50"
        assert t["fee_asset"] == "USDT"


class TestMEXCDepositFixture:
    """MEXC deposit response fixture."""

    def setup_method(self):
        self.ex = MEXCExchange(api_key="test", api_secret="test")

    @pytest.mark.asyncio
    async def test_full_deposit_fixture(self):
        payload = [{
            "id": "md-500",
            "coin": "ETH",
            "amount": "2.5",
            "network": "ETH",
            "txId": "0xdeposit500",
            "address": "0xmyaddr",
            "status": "6",
            "insertTime": 1712937600000,
            "completeTime": 1712941200000,
        }]
        with patch.object(self.ex, '_get', return_value=payload):
            result = await self.ex.fetch_deposits()

        d = result[0]
        assert d["exchange"] == "mexc"
        assert d["asset"] == "ETH"
        assert d["amount"] == "2.5"
        assert d["tx_hash"] == "0xdeposit500"
        assert d["network"] == "ETH"


class TestMEXCWithdrawalFixture:
    """MEXC withdrawal response fixture."""

    def setup_method(self):
        self.ex = MEXCExchange(api_key="test", api_secret="test")

    @pytest.mark.asyncio
    async def test_full_withdrawal_fixture(self):
        payload = [{
            "id": "mw-600",
            "coin": "BTC",
            "amount": "0.3",
            "transactionFee": "0.0005",
            "network": "BTC",
            "txId": "0xwd600hash",
            "address": "bc1qdest",
            "status": "6",
            "applyTime": 1712937600000,
            "completeTime": 1712941200000,
        }]
        with patch.object(self.ex, '_get', return_value=payload):
            result = await self.ex.fetch_withdrawals()

        w = result[0]
        assert w["exchange"] == "mexc"
        assert w["asset"] == "BTC"
        assert w["amount"] == "0.3"
        assert w["fee"] == "0.0005"
        assert w["tx_hash"] == "0xwd600hash"


class TestMEXCUniversalTransferFixture:
    """MEXC universal transfer response fixture."""

    def setup_method(self):
        self.ex = MEXCExchange(api_key="test", api_secret="test")

    @pytest.mark.asyncio
    async def test_full_transfer_fixture(self):
        payload = {"rows": [{
            "tranId": "ut-700",
            "asset": "USDT",
            "amount": "1000",
            "fromAccountType": "SPOT",
            "toAccountType": "FUTURES",
            "status": "CONFIRMED",
            "timestamp": 1712937600000,
        }]}
        call_count = [0]
        async def mock_get(path, params=None, signed=True):
            call_count[0] += 1
            return payload if call_count[0] == 1 else {"rows": []}
        with patch.object(self.ex, '_get', side_effect=mock_get):
            result = await self.ex.fetch_transfers()

        assert len(result) == 1
        t = result[0]
        assert t["exchange"] == "mexc"
        assert t["exchange_id"] == "ut-700"
        assert t["asset"] == "USDT"
        assert t["amount"] == "1000"
        assert t["from_account"] == "SPOT"
        assert t["to_account"] == "FUTURES"
        assert t["status"] == "CONFIRMED"
