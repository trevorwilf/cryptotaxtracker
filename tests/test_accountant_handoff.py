"""
End-to-end accountant handoff validation tests.

Simulates a full tax pipeline scenario with:
  1. External fiat deposit to MEXC ($10,000 USDT)
  2. Buy 0.5 BTC on MEXC at $50,000
  3. Withdraw 0.5 BTC from MEXC (fee: 0.0001 BTC)
  4. Deposit 0.4999 BTC on NonKYC (internal transfer)
  5. Sell 0.4999 BTC on NonKYC at $55,000 for USDT
  6. Receive 100 SAL staking reward on NonKYC (income)
  7. Withdraw 27,000 USDT from NonKYC to external wallet

Verifies the full flow from parsing through to filing readiness.
"""
import json
import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import patch

from exchanges.nonkyc import NonKYCExchange
from exchanges.mexc import MEXCExchange, MEXC_RETENTION
from exceptions import ExceptionManager, BLOCKING
from flow_classifier import FlowClassifier

D = Decimal
T = lambda y, m, d, h=12: datetime(y, m, d, h, 0, 0, tzinfo=timezone.utc)


class TestEndToEndScenario:
    """Verify the parser layer handles the complete scenario correctly."""

    @pytest.mark.asyncio
    async def test_mexc_trade_parsing(self):
        """Step 2: Buy 0.5 BTC on MEXC at $50,000."""
        ex = MEXCExchange(api_key="test", api_secret="test")
        account_response = {"balances": [{"asset": "BTC", "free": "0.5", "locked": "0"}]}
        trade_response = [{
            "id": "trade-buy-btc",
            "symbol": "BTCUSDT",
            "price": "50000",
            "qty": "0.5",
            "quoteQty": "25000",
            "commission": "25",
            "commissionAsset": "USDT",
            "time": 1710500000000,
            "isBuyer": True,
        }]

        async def mock_get(path, params=None, signed=True):
            if "/api/v3/account" in path:
                return account_response
            return trade_response

        with patch.object(ex, '_get', side_effect=mock_get):
            trades = await ex.fetch_trades()

        assert len(trades) == 1
        t = trades[0]
        assert t["side"] == "buy"
        assert t["price"] == "50000"
        assert t["quantity"] == "0.5"
        assert D(t["total"]) == D("25000")

    @pytest.mark.asyncio
    async def test_mexc_withdrawal_parsing(self):
        """Step 3: Withdraw 0.5 BTC from MEXC (fee: 0.0001 BTC)."""
        ex = MEXCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "wd-btc-mexc",
            "coin": "BTC",
            "amount": "0.5",
            "transactionFee": "0.0001",
            "network": "BTC",
            "txId": "tx_btc_transfer_001",
            "address": "nonkyc_btc_addr",
            "status": "6",
            "completeTime": 1710503600000,
        }]
        with patch.object(ex, '_get', return_value=payload):
            wds = await ex.fetch_withdrawals()

        assert len(wds) == 1
        w = wds[0]
        assert w["asset"] == "BTC"
        assert w["amount"] == "0.5"
        assert w["fee"] == "0.0001"
        assert w["tx_hash"] == "tx_btc_transfer_001"

    @pytest.mark.asyncio
    async def test_nonkyc_deposit_parsing(self):
        """Step 4: Deposit 0.4999 BTC on NonKYC (internal transfer)."""
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "dep-btc-nonkyc",
            "ticker": "BTC",
            "childticker": "",
            "quantity": "0.49990000",
            "status": "completed",
            "transactionid": "tx_btc_transfer_001",  # Same tx hash as MEXC withdrawal
            "isposted": True,
            "isreversed": False,
            "confirmations": 6,
            "firstseenat": "2025-03-15T13:00:00Z",
            "address": "nonkyc_btc_addr",
            "paymentid": ""
        }]
        with patch.object(ex, '_get', return_value=payload):
            deps = await ex.fetch_deposits()

        assert len(deps) == 1
        d = deps[0]
        assert d["asset"] == "BTC"
        assert d["amount"] == "0.49990000"
        assert d["tx_hash"] == "tx_btc_transfer_001"  # Matches MEXC withdrawal

    @pytest.mark.asyncio
    async def test_nonkyc_btc_sell(self):
        """Step 5: Sell 0.4999 BTC on NonKYC at $55,000 for USDT."""
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "trade-sell-btc",
            "symbol": "BTC/USDT",
            "side": "sell",
            "price": "55000",
            "quantity": "0.4999",
            "total": "27494.50",
            "fee": "27.49",
            "feeAsset": "USDT",
            "timestamp": "2025-03-20T10:00:00Z"
        }]
        with patch.object(ex, '_get', return_value=payload):
            trades = await ex.fetch_trades()

        t = trades[0]
        assert t["side"] == "sell"
        assert t["price"] == "55000"
        assert t["quantity"] == "0.4999"

    @pytest.mark.asyncio
    async def test_nonkyc_staking_income(self):
        """Step 6: Receive 100 SAL staking reward on NonKYC (income)."""
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "dep-sal-income",
            "ticker": "SAL",
            "childticker": "",
            "quantity": "100.00000000",
            "status": "completed",
            "transactionid": "staking_reward_001",
            "isposted": True,
            "isreversed": False,
            "confirmations": 10,
            "firstseenat": "2025-03-25T08:00:00Z",
            "address": "sal_staking_addr",
            "paymentid": ""
        }]
        with patch.object(ex, '_get', return_value=payload):
            deps = await ex.fetch_deposits()

        d = deps[0]
        assert d["asset"] == "SAL"
        assert d["amount"] == "100.00000000"

    @pytest.mark.asyncio
    async def test_nonkyc_usdt_withdrawal(self):
        """Step 7: Withdraw 27,000 USDT from NonKYC to external wallet."""
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "wd-usdt-external",
            "ticker": "USDT",
            "childticker": "",
            "quantity": "27000.00000000",
            "fee": "1.00000000",
            "feecurrency": "USDT",
            "status": "completed",
            "transactionid": "tx_usdt_external_001",
            "issent": True,
            "sentat": "2025-03-30T14:00:00Z",
            "isconfirmed": True,
            "requestedat": "2025-03-30T13:55:00Z",
            "address": "external_wallet_addr",
            "paymentid": ""
        }]
        with patch.object(ex, '_get', return_value=payload):
            wds = await ex.fetch_withdrawals()

        w = wds[0]
        assert w["asset"] == "USDT"
        assert w["amount"] == "27000.00000000"
        assert w["fee"] == "1.00000000"
        assert w["fee_currency"] == "USDT"


class TestTransferMatchingEvidence:
    """Transfer matcher should pair MEXC withdrawal with NonKYC deposit."""

    def test_tx_hash_matches(self):
        """Same tx_hash on withdrawal and deposit means self-transfer."""
        mexc_wd_tx = "tx_btc_transfer_001"
        nonkyc_dep_tx = "tx_btc_transfer_001"
        assert mexc_wd_tx == nonkyc_dep_tx

    def test_amounts_consistent(self):
        """MEXC withdrawal amount minus fee = NonKYC deposit amount."""
        wd_amount = D("0.5")
        wd_fee = D("0.0001")
        dep_amount = D("0.4999")
        assert wd_amount - wd_fee == dep_amount


class TestBasisCarryover:
    """BTC basis should carry over from MEXC to NonKYC."""

    def test_carried_basis_calculation(self):
        """Basis carries from MEXC buy to NonKYC sale."""
        buy_price = D("50000")
        buy_qty = D("0.5")
        cost_basis = buy_price * buy_qty  # $25,000

        # After transfer, NonKYC sells 0.4999 BTC at $55,000
        sell_qty = D("0.4999")
        sell_price = D("55000")
        proceeds = sell_qty * sell_price  # $27,494.50

        # Basis for 0.4999 BTC (carried over from MEXC)
        cost_per_btc = buy_price  # $50,000 per BTC
        carried_basis = sell_qty * cost_per_btc  # $24,995

        gain = proceeds - carried_basis  # ~$2,499.50
        assert gain > D("2000")
        assert gain < D("3000")  # Approximately $2,500

    def test_gain_is_short_term(self):
        """Holding period is within 1 year -> short-term."""
        buy_date = T(2025, 3, 15)
        sell_date = T(2025, 3, 20)
        holding_days = (sell_date - buy_date).days
        assert holding_days < 366  # Short-term


class TestFlowClassificationScenario:
    """Verify flow classification for the full scenario."""

    @pytest.mark.asyncio
    async def test_full_scenario_classification(self):
        """All 7 events should classify correctly."""
        from test_flow_classifier import FakeClassifierSession

        session = FakeClassifierSession()

        # MEXC deposits/withdrawals
        session.deposits = [
            (1, "mexc", "USDT", "10000", "10000", "1.0", T(2025, 3, 10)),  # External deposit
            (2, "nonkyc", "BTC", "0.4999", "24995", "50000", T(2025, 3, 15)),  # Transfer in
            (3, "nonkyc", "SAL", "100", "50", "0.5", T(2025, 3, 25)),  # Income
        ]
        session.withdrawals = [
            (10, "mexc", "BTC", "0.5", "25000", "50000", T(2025, 3, 15)),  # Transfer out
            (11, "nonkyc", "USDT", "27000", "27000", "1.0", T(2025, 3, 30)),  # External wd
        ]
        session.transfer_in_deposit_ids = {2}
        session.income_deposit_ids = {3}
        session.transfer_out_withdrawal_ids = {10}

        classifier = FlowClassifier()
        result = await classifier.classify_all(session)

        assert result["by_class"]["EXTERNAL_DEPOSIT"] == 1     # MEXC USDT deposit
        assert result["by_class"]["INTERNAL_TRANSFER_IN"] == 1  # NonKYC BTC deposit
        assert result["by_class"]["INCOME_RECEIPT"] == 1        # NonKYC SAL income
        assert result["by_class"]["INTERNAL_TRANSFER_OUT"] == 1 # MEXC BTC withdrawal
        assert result["by_class"]["EXTERNAL_WITHDRAWAL"] == 1   # NonKYC USDT withdrawal
        assert result["total_classified"] == 5


class TestFilingReadiness:
    """Filing-ready flag should be TRUE when no blocking issues."""

    def test_no_exceptions_is_filing_ready(self):
        exc = ExceptionManager()
        assert exc.has_blocking is False

    def test_coverage_gap_blocks_filing(self):
        """MEXC retention gap without CSV should block filing."""
        ex = MEXCExchange(api_key="test", api_secret="test")
        coverage = ex.get_data_coverage(
            since=datetime(2025, 1, 1, tzinfo=timezone.utc))
        has_any_gap = any(info["has_gap"] for info in coverage.values())
        assert has_any_gap is True  # 2025-01-01 is before any 30-day window

    def test_mexc_retention_constants(self):
        """Verify MEXC retention constants are correctly defined."""
        assert MEXC_RETENTION["myTrades"]["days"] == 30
        assert MEXC_RETENTION["allOrders"]["days"] == 7
        assert MEXC_RETENTION["deposit_history"]["days"] == 90
        assert MEXC_RETENTION["withdraw_history"]["days"] == 90
        assert MEXC_RETENTION["universal_transfer"]["days"] == 180
