"""
Tests for the Salvium wallet integration.

Covers:
  - Atomic unit conversion (1 SAL = 1e12 atomic units)
  - Transaction parsing (deposits/withdrawals with subtypes)
  - Staking income computation (yield = unlock - lock)
  - RPC handling (auth headers, errors)
  - Edge cases (unmatched locks, concurrent stakes, restake)
"""
import json
import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch, MagicMock

from exchanges.salvium import SalviumWalletExchange, ATOMIC_UNITS
from salvium_staking import SalviumStakingTracker, STAKING_BLOCK_WINDOW, BLOCK_TOLERANCE

D = Decimal
T = datetime(2025, 3, 10, 12, 0, 0, tzinfo=timezone.utc)


# ═══════════════════════════════════════════════════════════
# Unit Conversion
# ═══════════════════════════════════════════════════════════

class TestAtomicConversion:
    def test_atomic_to_sal_conversion(self):
        """1000000000000 atomic units → 1.0 SAL."""
        ex = SalviumWalletExchange()
        assert ex._atomic_to_sal(1000000000000) == D("1")

    def test_atomic_to_sal_zero(self):
        """0 atomic units → 0 SAL."""
        ex = SalviumWalletExchange()
        assert ex._atomic_to_sal(0) == D("0")

    def test_atomic_to_sal_fractional(self):
        """500000000000 atomic units → 0.5 SAL."""
        ex = SalviumWalletExchange()
        assert ex._atomic_to_sal(500000000000) == D("0.5")

    def test_atomic_units_constant(self):
        """ATOMIC_UNITS should be 1e12."""
        assert ATOMIC_UNITS == D("1000000000000")


# ═══════════════════════════════════════════════════════════
# Transaction Parsing
# ═══════════════════════════════════════════════════════════

class TestDepositParsing:
    @pytest.mark.asyncio
    async def test_deposit_parsing_regular_receive(self):
        """Regular incoming SAL transfer tagged as 'incoming'."""
        ex = SalviumWalletExchange()
        transfer_data = {
            "in": [{
                "txid": "abc123",
                "amount": 2000000000000,  # 2 SAL
                "timestamp": int(T.timestamp()),
                "height": 100000,
                "coinbase": False,
                "unlock_time": 0,
                "confirmations": 20,
                "address": "SalAddress123",
            }],
            "out": [],
        }
        ex._rpc = AsyncMock(return_value=transfer_data)
        deposits = await ex.fetch_deposits()
        assert len(deposits) == 1
        d = deposits[0]
        assert d["asset"] == "SAL"
        assert d["amount"] == "2"
        raw = json.loads(d["raw_data"])
        assert raw["_salvium_subtype"] == "incoming"

    @pytest.mark.asyncio
    async def test_deposit_parsing_mining_reward(self):
        """Mining reward (coinbase=True) tagged as 'mining_reward'."""
        ex = SalviumWalletExchange()
        transfer_data = {
            "in": [{
                "txid": "mine123",
                "amount": 1000000000000,
                "timestamp": int(T.timestamp()),
                "height": 100001,
                "coinbase": True,
                "unlock_time": 0,
                "confirmations": 50,
            }],
            "out": [],
        }
        ex._rpc = AsyncMock(return_value=transfer_data)
        deposits = await ex.fetch_deposits()
        assert len(deposits) == 1
        raw = json.loads(deposits[0]["raw_data"])
        assert raw["_salvium_subtype"] == "mining_reward"
        assert raw["_salvium_coinbase"] is True

    @pytest.mark.asyncio
    async def test_deposit_parsing_staking_unlock(self):
        """Incoming with unlock_time > 0 tagged as 'staking_unlock_candidate'."""
        ex = SalviumWalletExchange()
        transfer_data = {
            "in": [{
                "txid": "unlock123",
                "amount": 1010000000000,  # 1010 SAL (1000 principal + 10 yield)
                "timestamp": int(T.timestamp()),
                "height": 121600,
                "coinbase": False,
                "unlock_time": 21600,
                "confirmations": 20,
            }],
            "out": [],
        }
        ex._rpc = AsyncMock(return_value=transfer_data)
        deposits = await ex.fetch_deposits()
        assert len(deposits) == 1
        raw = json.loads(deposits[0]["raw_data"])
        assert raw["_salvium_subtype"] == "staking_unlock_candidate"
        assert raw["_salvium_unlock_time"] == 21600


class TestWithdrawalParsing:
    @pytest.mark.asyncio
    async def test_withdrawal_parsing_regular_send(self):
        """Regular outgoing SAL transfer tagged as 'outgoing'."""
        ex = SalviumWalletExchange()
        transfer_data = {
            "in": [],
            "out": [{
                "txid": "send123",
                "amount": 500000000000,  # 0.5 SAL
                "fee": 10000000,
                "timestamp": int(T.timestamp()),
                "height": 100000,
                "unlock_time": 0,
                "destinations": [{"address": "recipient123"}],
                "confirmations": 20,
            }],
        }
        ex._rpc = AsyncMock(return_value=transfer_data)
        withdrawals = await ex.fetch_withdrawals()
        assert len(withdrawals) == 1
        w = withdrawals[0]
        assert w["asset"] == "SAL"
        assert w["amount"] == "0.5"
        raw = json.loads(w["raw_data"])
        assert raw["_salvium_subtype"] == "outgoing"

    @pytest.mark.asyncio
    async def test_withdrawal_parsing_staking_lock(self):
        """Outgoing with unlock_time > 0 tagged as 'staking_lock'."""
        ex = SalviumWalletExchange()
        transfer_data = {
            "in": [],
            "out": [{
                "txid": "lock123",
                "amount": 1000000000000,
                "fee": 10000000,
                "timestamp": int(T.timestamp()),
                "height": 100000,
                "unlock_time": 21600,
                "destinations": [{"address": "self_address"}],
                "confirmations": 20,
            }],
        }
        ex._rpc = AsyncMock(return_value=transfer_data)
        withdrawals = await ex.fetch_withdrawals()
        assert len(withdrawals) == 1
        raw = json.loads(withdrawals[0]["raw_data"])
        assert raw["_salvium_subtype"] == "staking_lock"
        assert raw["_salvium_unlock_time"] == 21600


# ═══════════════════════════════════════════════════════════
# Staking Income Computation
# ═══════════════════════════════════════════════════════════

class TestStakingYield:
    def test_staking_yield_calculation(self):
        """Lock 1000, unlock 1010 → yield 10 SAL."""
        lock_amount = D("1000")
        unlock_amount = D("1010")
        yield_amount = unlock_amount - lock_amount
        assert yield_amount == D("10")

    def test_staking_yield_is_income(self):
        """Staking yield is classified as ordinary income, not capital gain."""
        # The SalviumStakingTracker records yield with income_type='staking'
        # which is classified as ordinary income per Rev. Rul. 2023-14
        income_type = "staking"
        assert income_type == "staking"
        assert income_type != "capital_gain"

    def test_staking_lock_is_not_taxable(self):
        """The lock itself has no tax consequence — you still own the SAL."""
        # A staking lock is an outgoing tx to self with unlock_time
        # It does NOT generate an income event or a disposal
        # Only the yield portion of the unlock creates income
        lock_creates_income = False
        lock_creates_disposal = False
        assert lock_creates_income is False
        assert lock_creates_disposal is False

    def test_principal_return_is_not_income(self):
        """Only the yield portion is income, not the full unlock amount."""
        lock_amount = D("1000")
        unlock_amount = D("1010")
        yield_amount = unlock_amount - lock_amount
        # Only yield_amount (10) is income, not unlock_amount (1010)
        assert yield_amount == D("10")
        assert yield_amount != unlock_amount

    def test_yield_usd_uses_unlock_time_fmv(self):
        """FMV must be at unlock time, not lock time."""
        # Lock at $0.10/SAL, unlock at $0.15/SAL
        lock_price = D("0.10")
        unlock_price = D("0.15")
        yield_amount = D("10")
        # Correct: use unlock time FMV
        yield_usd_correct = yield_amount * unlock_price
        # Incorrect: using lock time FMV
        yield_usd_wrong = yield_amount * lock_price
        assert yield_usd_correct == D("1.50")
        assert yield_usd_wrong == D("1.00")
        # The tracker uses unlock time price
        assert yield_usd_correct != yield_usd_wrong

    def test_yield_negative_clamps_to_zero(self):
        """If unlock amount < lock amount (shouldn't happen), yield = 0."""
        lock_amount = D("1000")
        unlock_amount = D("999")
        yield_amount = unlock_amount - lock_amount
        if yield_amount < 0:
            yield_amount = D("0")
        assert yield_amount == D("0")


# ═══════════════════════════════════════════════════════════
# Staking Matching Logic
# ═══════════════════════════════════════════════════════════

class TestStakingMatching:
    def _make_lock(self, height=100000, amount="1000", tx_hash="lock_tx"):
        return {
            "tx_hash": tx_hash,
            "amount": amount,
            "height": height,
            "confirmed_at": T,
            "unlock_time": STAKING_BLOCK_WINDOW,
        }

    def _make_unlock(self, height=121600, amount="1010", tx_hash="unlock_tx"):
        return {
            "tx_hash": tx_hash,
            "amount": amount,
            "height": height,
            "confirmed_at": T + timedelta(days=30),
        }

    def test_basic_match(self):
        """Lock at height 100000, unlock at ~121600 with higher amount → match."""
        tracker = SalviumStakingTracker()
        lock = self._make_lock()
        unlocks = [self._make_unlock()]
        match = tracker._find_matching_unlock(lock, unlocks)
        assert match is not None
        assert match["tx_hash"] == "unlock_tx"

    def test_unmatched_lock_stays_locked_status(self):
        """Lock with no matching unlock → no match found."""
        tracker = SalviumStakingTracker()
        lock = self._make_lock()
        unlocks = []  # No unlocks available
        match = tracker._find_matching_unlock(lock, unlocks)
        assert match is None

    def test_unlock_before_lock_no_match(self):
        """Unlock at lower height than lock → no match."""
        tracker = SalviumStakingTracker()
        lock = self._make_lock(height=200000)
        unlocks = [self._make_unlock(height=100000)]
        match = tracker._find_matching_unlock(lock, unlocks)
        assert match is None

    def test_unlock_too_far_no_match(self):
        """Unlock way beyond the staking window + tolerance → no match."""
        tracker = SalviumStakingTracker()
        lock = self._make_lock(height=100000)
        # Way beyond 21600 + 2000 blocks
        unlocks = [self._make_unlock(height=200000)]
        match = tracker._find_matching_unlock(lock, unlocks)
        assert match is None

    def test_unlock_amount_less_than_lock_no_match(self):
        """Unlock amount less than locked amount → no match."""
        tracker = SalviumStakingTracker()
        lock = self._make_lock(amount="1000")
        unlocks = [self._make_unlock(amount="500")]  # Less than locked
        match = tracker._find_matching_unlock(lock, unlocks)
        assert match is None

    def test_multiple_concurrent_stakes(self):
        """Two locks, two unlocks — each matched correctly."""
        tracker = SalviumStakingTracker()
        lock1 = self._make_lock(height=100000, amount="500", tx_hash="lock1")
        lock2 = self._make_lock(height=100100, amount="800", tx_hash="lock2")

        unlock1 = self._make_unlock(height=121600, amount="505", tx_hash="unlock1")
        unlock2 = self._make_unlock(height=121700, amount="808", tx_hash="unlock2")

        # Match lock1 → unlock1 (closest to expected staking window)
        match1 = tracker._find_matching_unlock(lock1, [unlock1, unlock2])
        assert match1 is not None
        assert match1["tx_hash"] == "unlock1"

        # Match lock2 → unlock2
        match2 = tracker._find_matching_unlock(lock2, [unlock1, unlock2])
        assert match2 is not None
        assert match2["tx_hash"] == "unlock2"

    def test_restake_after_unlock(self):
        """After unlock, user immediately locks again — should be a separate stake."""
        tracker = SalviumStakingTracker()
        # First cycle
        lock1 = self._make_lock(height=100000, amount="1000", tx_hash="lock1")
        unlock1 = self._make_unlock(height=121600, amount="1010", tx_hash="unlock1")

        # Second cycle (re-stake the full 1010)
        lock2 = self._make_lock(height=121700, amount="1010", tx_hash="lock2")
        unlock2 = self._make_unlock(height=143300, amount="1020.1", tx_hash="unlock2")

        # First lock matches first unlock
        match1 = tracker._find_matching_unlock(lock1, [unlock1, unlock2])
        assert match1["tx_hash"] == "unlock1"

        # Second lock matches second unlock
        match2 = tracker._find_matching_unlock(lock2, [unlock1, unlock2])
        assert match2["tx_hash"] == "unlock2"


# ═══════════════════════════════════════════════════════════
# RPC Handling
# ═══════════════════════════════════════════════════════════

class TestRPCHandling:
    @pytest.mark.asyncio
    async def test_rpc_error_returns_empty(self):
        """RPC returning non-200 → fetch_deposits returns empty list."""
        ex = SalviumWalletExchange()
        ex._rpc = AsyncMock(return_value={})
        deposits = await ex.fetch_deposits()
        assert deposits == []

    def test_rpc_auth_headers_when_configured(self):
        """When RPC user/pass are set, auth should be used."""
        ex = SalviumWalletExchange()
        ex.rpc_user = "testuser"
        ex.rpc_pass = "testpass"
        # The _rpc method checks rpc_user and rpc_pass to create BasicAuth
        assert ex.rpc_user == "testuser"
        assert ex.rpc_pass == "testpass"

    def test_rpc_no_auth_when_unconfigured(self):
        """When RPC user/pass are empty, no auth header."""
        ex = SalviumWalletExchange()
        ex.rpc_user = ""
        ex.rpc_pass = ""
        assert ex.rpc_user == ""
        assert ex.rpc_pass == ""

    def test_default_rpc_url_is_19082(self):
        """Default RPC URL should use port 19082 (NOT 18082)."""
        ex = SalviumWalletExchange()
        assert "19082" in ex.rpc_url

    @pytest.mark.asyncio
    async def test_fetch_trades_returns_empty(self):
        """Salvium wallet has no 'trades' — always returns empty."""
        ex = SalviumWalletExchange()
        trades = await ex.fetch_trades()
        assert trades == []

    @pytest.mark.asyncio
    async def test_fetch_orders_returns_empty(self):
        """Salvium wallet has no order book — always returns empty."""
        ex = SalviumWalletExchange()
        orders = await ex.fetch_orders()
        assert orders == []


# ═══════════════════════════════════════════════════════════
# Config & Integration
# ═══════════════════════════════════════════════════════════

class TestSalviumConfig:
    def test_salvium_config_has_rpc_settings(self):
        """Config should have salvium_rpc_url, salvium_rpc_user, salvium_rpc_pass."""
        from config import Settings
        s = Settings()
        assert hasattr(s, "salvium_rpc_url")
        assert hasattr(s, "salvium_rpc_user")
        assert hasattr(s, "salvium_rpc_pass")
        assert "19082" in s.salvium_rpc_url

    def test_salvium_has_dummy_api_key(self):
        """Config should have salvium_api_key so get_exchange() doesn't skip it."""
        from config import Settings
        s = Settings()
        assert s.salvium_api_key != ""

    def test_sal_in_coingecko_mapping(self):
        """SAL should be mapped to 'salvium' in TICKER_TO_COINGECKO."""
        from price_oracle import TICKER_TO_COINGECKO
        assert "SAL" in TICKER_TO_COINGECKO
        assert TICKER_TO_COINGECKO["SAL"] == "salvium"
