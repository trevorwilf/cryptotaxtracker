"""
Stop-ship tests — filing-readiness gate tests.

Tests that MUST result in filing_ready == False when:
1. MEXC history exceeds API coverage window without CSV
2. Unresolved blocking exceptions exist
3. Missing historical prices unresolved
4. NonKYC deposit/withdrawal parser returns zero amounts
"""
import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from exchanges.mexc import MEXCExchange, MEXC_RETENTION
from exchanges.nonkyc import NonKYCExchange
from exceptions import ExceptionManager, BLOCKING, MISSING_PRICE
from unittest.mock import patch


class TestMEXCRetentionGap:
    """MEXC history exceeds API coverage window without CSV -> not filing ready."""

    def test_coverage_gap_for_old_tax_year(self):
        """Tax year 2024 is definitely outside MEXC 30-day trade retention."""
        ex = MEXCExchange(api_key="test", api_secret="test")
        coverage = ex.get_data_coverage(
            since=datetime(2024, 1, 1, tzinfo=timezone.utc))
        assert coverage["myTrades"]["has_gap"] is True
        assert coverage["myTrades"]["requires_csv_import"] is True
        assert coverage["myTrades"]["gap_days"] > 300

    def test_all_endpoints_have_gaps_for_old_year(self):
        """All MEXC endpoints should report gaps for 2024."""
        ex = MEXCExchange(api_key="test", api_secret="test")
        coverage = ex.get_data_coverage(
            since=datetime(2024, 1, 1, tzinfo=timezone.utc))
        for endpoint in ["myTrades", "allOrders", "deposit_history",
                         "withdraw_history", "universal_transfer"]:
            assert coverage[endpoint]["has_gap"] is True, f"{endpoint} should have gap"

    def test_no_gap_within_retention_window(self):
        """Recent dates within retention window should not have gaps."""
        ex = MEXCExchange(api_key="test", api_secret="test")
        recent = datetime.now(timezone.utc) - timedelta(days=5)
        coverage = ex.get_data_coverage(since=recent)
        for endpoint in MEXC_RETENTION:
            assert coverage[endpoint]["has_gap"] is False


class TestBlockingExceptions:
    """Unresolved blocking exceptions -> not filing ready."""

    def test_blocking_exception_prevents_filing(self):
        exc = ExceptionManager()
        exc.log(BLOCKING, "UNKNOWN_BASIS",
                "No cost basis for BTC disposal",
                tax_year=2025, blocks_filing=True)
        assert exc.has_blocking is True

    def test_no_blocking_allows_filing(self):
        exc = ExceptionManager()
        exc.log("WARNING", "VALUATION_FALLBACK",
                "Used daily price instead of hourly",
                tax_year=2025)
        assert exc.has_blocking is False

    def test_multiple_blocking_all_flagged(self):
        exc = ExceptionManager()
        exc.log(BLOCKING, "UNKNOWN_BASIS", "msg1", tax_year=2025)
        exc.log(BLOCKING, MISSING_PRICE, "msg2", tax_year=2025)
        counts = exc.get_counts()
        assert counts[BLOCKING] == 2


class TestMissingPrices:
    """Missing historical prices unresolved -> not filing ready."""

    def test_missing_price_is_blocking(self):
        exc = ExceptionManager()
        exc.log(BLOCKING, MISSING_PRICE,
                "No historical price for SAL on 2025-03-15",
                tax_year=2025, blocks_filing=True)
        assert exc.has_blocking is True
        counts = exc.get_counts()
        assert counts[BLOCKING] == 1


class TestNonKYCZeroAmounts:
    """NonKYC parser should not return zero amounts for valid deposits."""

    @pytest.mark.asyncio
    async def test_deposit_quantity_not_zero(self):
        """Official NonKYC deposit with quantity field should not return 0."""
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "dep-test",
            "ticker": "BTC",
            "quantity": "1.50000000",
            "transactionid": "tx123",
            "firstseenat": "2025-06-01T00:00:00Z"
        }]
        with patch.object(ex, '_get', return_value=payload):
            result = await ex.fetch_deposits()
        assert result[0]["amount"] != "0"
        assert result[0]["amount"] == "1.50000000"

    @pytest.mark.asyncio
    async def test_withdrawal_quantity_not_zero(self):
        """Official NonKYC withdrawal with quantity field should not return 0."""
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "wd-test",
            "ticker": "BTC",
            "quantity": "0.75000000",
            "fee": "0.0001",
            "transactionid": "tx456",
            "requestedat": "2025-06-01T00:00:00Z"
        }]
        with patch.object(ex, '_get', return_value=payload):
            result = await ex.fetch_withdrawals()
        assert result[0]["amount"] != "0"
        assert result[0]["amount"] == "0.75000000"

    @pytest.mark.asyncio
    async def test_deposit_with_only_amount_field(self):
        """Legacy deposits using 'amount' field should still work."""
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{"id": "dep-legacy", "ticker": "ETH", "amount": "5.0",
                     "txHash": "0xabc", "createdAt": "2025-01-01T00:00:00Z"}]
        with patch.object(ex, '_get', return_value=payload):
            result = await ex.fetch_deposits()
        assert result[0]["amount"] == "5.0"
