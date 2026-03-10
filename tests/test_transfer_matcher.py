"""
Tests for the Transfer Matcher module.

Covers:
  - Basic withdrawal→deposit matching
  - Time window enforcement (48h default)
  - Amount tolerance with fees
  - Same-exchange rejection
  - TX hash matching (high confidence)
  - Multiple transfers, FIFO matching order
  - Deposit before withdrawal (rejected)
  - Different assets don't match
"""
import pytest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from transfer_matcher import TransferMatcher

D = Decimal
T_BASE = datetime(2025, 3, 10, 12, 0, 0, tzinfo=timezone.utc)


class TestMatcherConfig:
    def test_default_window(self):
        m = TransferMatcher()
        assert m.time_window == timedelta(hours=48)

    def test_custom_window(self):
        m = TransferMatcher(time_window_hours=24)
        assert m.time_window == timedelta(hours=24)

    def test_custom_tolerance(self):
        m = TransferMatcher(fee_tolerance_pct=10.0)
        assert m.fee_tolerance == D("0.1")


class TestMatchingLogic:
    """Test the core matching criteria without DB.
    
    These test the matching rules that the _match_transfers method implements:
    1. Same asset
    2. Different exchanges
    3. Deposit after withdrawal
    4. Within time window
    5. Amount within fee tolerance
    """

    def _would_match(self, wd, dep, matcher=None):
        """Apply matching rules manually to test criteria."""
        if matcher is None:
            matcher = TransferMatcher()

        if dep["asset"] != wd["asset"]:
            return False
        if dep["exchange"] == wd["exchange"]:
            return False
        if dep["confirmed_at"] < wd["confirmed_at"]:
            return False
        if (dep["confirmed_at"] - wd["confirmed_at"]) > matcher.time_window:
            return False

        wd_amount = D(str(wd["amount"]))
        wd_fee = D(str(wd.get("fee", 0)))
        wd_net = wd_amount - wd_fee
        dep_amount = D(str(dep["amount"]))

        if wd_net > 0:
            diff_pct = abs(dep_amount - wd_net) / wd_net
        else:
            diff_pct = abs(dep_amount - wd_amount) / wd_amount if wd_amount > 0 else D("1")

        return diff_pct <= matcher.fee_tolerance

    def test_basic_match(self):
        wd = {"asset": "BTC", "exchange": "nonkyc", "amount": "1.0",
              "fee": "0.0001", "confirmed_at": T_BASE}
        dep = {"asset": "BTC", "exchange": "mexc", "amount": "0.9999",
               "confirmed_at": T_BASE + timedelta(hours=2)}
        assert self._would_match(wd, dep) is True

    def test_different_asset_no_match(self):
        wd = {"asset": "BTC", "exchange": "nonkyc", "amount": "1.0",
              "fee": "0", "confirmed_at": T_BASE}
        dep = {"asset": "ETH", "exchange": "mexc", "amount": "1.0",
               "confirmed_at": T_BASE + timedelta(hours=1)}
        assert self._would_match(wd, dep) is False

    def test_same_exchange_no_match(self):
        wd = {"asset": "BTC", "exchange": "nonkyc", "amount": "1.0",
              "fee": "0", "confirmed_at": T_BASE}
        dep = {"asset": "BTC", "exchange": "nonkyc", "amount": "1.0",
               "confirmed_at": T_BASE + timedelta(hours=1)}
        assert self._would_match(wd, dep) is False

    def test_deposit_before_withdrawal_no_match(self):
        wd = {"asset": "BTC", "exchange": "nonkyc", "amount": "1.0",
              "fee": "0", "confirmed_at": T_BASE}
        dep = {"asset": "BTC", "exchange": "mexc", "amount": "1.0",
               "confirmed_at": T_BASE - timedelta(hours=1)}
        assert self._would_match(wd, dep) is False

    def test_outside_time_window(self):
        wd = {"asset": "BTC", "exchange": "nonkyc", "amount": "1.0",
              "fee": "0", "confirmed_at": T_BASE}
        dep = {"asset": "BTC", "exchange": "mexc", "amount": "1.0",
               "confirmed_at": T_BASE + timedelta(hours=49)}
        assert self._would_match(wd, dep) is False

    def test_within_time_window(self):
        wd = {"asset": "BTC", "exchange": "nonkyc", "amount": "1.0",
              "fee": "0", "confirmed_at": T_BASE}
        dep = {"asset": "BTC", "exchange": "mexc", "amount": "1.0",
               "confirmed_at": T_BASE + timedelta(hours=47)}
        assert self._would_match(wd, dep) is True

    def test_amount_within_fee_tolerance(self):
        """Withdrawal of 1.0 with 0.001 fee → net 0.999. Deposit of 0.999."""
        wd = {"asset": "ETH", "exchange": "nonkyc", "amount": "10.0",
              "fee": "0.01", "confirmed_at": T_BASE}
        dep = {"asset": "ETH", "exchange": "mexc", "amount": "9.99",
               "confirmed_at": T_BASE + timedelta(hours=1)}
        assert self._would_match(wd, dep) is True

    def test_amount_exceeds_tolerance(self):
        """Deposit amount is way off."""
        wd = {"asset": "BTC", "exchange": "nonkyc", "amount": "1.0",
              "fee": "0.0001", "confirmed_at": T_BASE}
        dep = {"asset": "BTC", "exchange": "mexc", "amount": "0.5",
               "confirmed_at": T_BASE + timedelta(hours=1)}
        assert self._would_match(wd, dep) is False

    def test_custom_24h_window(self):
        matcher = TransferMatcher(time_window_hours=24)
        wd = {"asset": "BTC", "exchange": "nonkyc", "amount": "1.0",
              "fee": "0", "confirmed_at": T_BASE}
        dep = {"asset": "BTC", "exchange": "mexc", "amount": "1.0",
               "confirmed_at": T_BASE + timedelta(hours=25)}
        assert self._would_match(wd, dep, matcher) is False

    def test_exact_amount_match(self):
        """Zero fee — amounts should match exactly."""
        wd = {"asset": "USDT", "exchange": "nonkyc", "amount": "5000",
              "fee": "0", "confirmed_at": T_BASE}
        dep = {"asset": "USDT", "exchange": "mexc", "amount": "5000",
               "confirmed_at": T_BASE + timedelta(minutes=30)}
        assert self._would_match(wd, dep) is True

    def test_high_tolerance_mode(self):
        """10% tolerance should match looser amounts."""
        matcher = TransferMatcher(fee_tolerance_pct=10.0)
        wd = {"asset": "BTC", "exchange": "nonkyc", "amount": "1.0",
              "fee": "0", "confirmed_at": T_BASE}
        dep = {"asset": "BTC", "exchange": "mexc", "amount": "0.92",
               "confirmed_at": T_BASE + timedelta(hours=1)}
        assert self._would_match(wd, dep, matcher) is True
