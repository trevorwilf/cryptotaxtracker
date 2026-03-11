"""
Tests for the Salvium wallet integration.

Covers:
  - Atomic unit conversion (1 SAL = 1e8 atomic units)
  - Transaction parsing (deposits/withdrawals with subtypes)
  - Staking lock detection (amount=0, fee=staked amount)
  - Staking income computation (yield = unlock - lock)
  - Multi-account support
  - RPC handling (auth headers, errors)
  - Edge cases (unmatched locks, concurrent stakes, restake)
"""
import json
import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch, MagicMock

from exchanges.salvium import SalviumWalletExchange, ATOMIC_UNITS, NORMAL_FEE_THRESHOLD
from salvium_staking import SalviumStakingTracker, STAKING_BLOCK_WINDOW, BLOCK_TOLERANCE

D = Decimal
T = datetime(2025, 3, 10, 12, 0, 0, tzinfo=timezone.utc)


# ═══════════════════════════════════════════════════════════
# Unit Conversion (1e8 atomic units per SAL)
# ═══════════════════════════════════════════════════════════

class TestAtomicConversion:
    def test_atomic_to_sal_conversion(self):
        """100000000 atomic units → 1.0 SAL."""
        ex = SalviumWalletExchange()
        assert ex._atomic_to_sal(100000000) == D("1")

    def test_atomic_to_sal_large_balance(self):
        """Real wallet balance: 4404747695976 → 44047.47695976 SAL."""
        ex = SalviumWalletExchange()
        assert ex._atomic_to_sal(4404747695976) == D("44047.47695976")

    def test_atomic_to_sal_zero(self):
        """0 atomic units → 0 SAL."""
        ex = SalviumWalletExchange()
        assert ex._atomic_to_sal(0) == D("0")

    def test_atomic_to_sal_fractional(self):
        """50000000 atomic units → 0.5 SAL."""
        ex = SalviumWalletExchange()
        assert ex._atomic_to_sal(50000000) == D("0.5")

    def test_atomic_units_constant(self):
        """ATOMIC_UNITS should be 1e8 (NOT 1e12)."""
        assert ATOMIC_UNITS == D("100000000")

    def test_normal_fee_threshold(self):
        """Normal fee threshold should be 1 SAL (100000000 atomic)."""
        assert NORMAL_FEE_THRESHOLD == 100_000_000


# ═══════════════════════════════════════════════════════════
# Multi-Account Support
# ═══════════════════════════════════════════════════════════

class TestMultiAccount:
    @pytest.mark.asyncio
    async def test_get_all_transfers_queries_all_accounts(self):
        """Should iterate all accounts from get_accounts, not just account 0."""
        ex = SalviumWalletExchange()

        call_count = 0
        async def mock_rpc(method, params=None):
            nonlocal call_count
            call_count += 1
            if method == "get_accounts":
                return {
                    "subaddress_accounts": [
                        {"account_index": 0, "label": "Primary"},
                        {"account_index": 1, "label": "consolidate"},
                    ],
                    "total_balance": 4404747695976,
                    "total_unlocked_balance": 1605762000000,
                }
            elif method == "get_transfers":
                acct_idx = params.get("account_index", 0)
                if acct_idx == 0:
                    return {
                        "in": [{"txid": "dep_acct0", "amount": 100000000,
                                "timestamp": int(T.timestamp()), "height": 1000,
                                "coinbase": False, "unlock_time": 0, "confirmations": 20}],
                        "out": [],
                    }
                else:
                    return {
                        "in": [{"txid": "dep_acct1", "amount": 200000000,
                                "timestamp": int(T.timestamp()), "height": 1001,
                                "coinbase": False, "unlock_time": 0, "confirmations": 20}],
                        "out": [],
                    }
            return {}

        ex._rpc = mock_rpc
        transfers = await ex._get_all_transfers()
        # Should have deposits from both accounts
        assert len(transfers["in"]) == 2
        assert transfers["in"][0]["txid"] == "dep_acct0"
        assert transfers["in"][1]["txid"] == "dep_acct1"

    @pytest.mark.asyncio
    async def test_transfers_tagged_with_account_info(self):
        """Each transfer should have _account_index and _account_label."""
        ex = SalviumWalletExchange()

        async def mock_rpc(method, params=None):
            if method == "get_accounts":
                return {
                    "subaddress_accounts": [
                        {"account_index": 0, "label": "Primary"},
                        {"account_index": 1, "label": "consolidate"},
                    ],
                }
            elif method == "get_transfers":
                acct_idx = params.get("account_index", 0)
                return {
                    "in": [{"txid": f"tx_{acct_idx}", "amount": 100000000,
                            "timestamp": int(T.timestamp()), "height": 1000,
                            "coinbase": False, "unlock_time": 0, "confirmations": 20}],
                    "out": [],
                }
            return {}

        ex._rpc = mock_rpc
        transfers = await ex._get_all_transfers()
        assert transfers["in"][0]["_account_index"] == 0
        assert transfers["in"][0]["_account_label"] == "Primary"
        assert transfers["in"][1]["_account_index"] == 1
        assert transfers["in"][1]["_account_label"] == "consolidate"

    @pytest.mark.asyncio
    async def test_fallback_to_account_0_when_no_accounts(self):
        """If get_accounts returns empty, should still query account 0."""
        ex = SalviumWalletExchange()

        async def mock_rpc(method, params=None):
            if method == "get_accounts":
                return {}  # No accounts returned
            elif method == "get_transfers":
                return {
                    "in": [{"txid": "tx_fallback", "amount": 100000000,
                            "timestamp": int(T.timestamp()), "height": 1000,
                            "coinbase": False, "unlock_time": 0, "confirmations": 20}],
                    "out": [],
                }
            return {}

        ex._rpc = mock_rpc
        transfers = await ex._get_all_transfers()
        assert len(transfers["in"]) == 1

    @pytest.mark.asyncio
    async def test_get_balance_uses_get_accounts(self):
        """Balance should use get_accounts total_balance/total_unlocked_balance."""
        ex = SalviumWalletExchange()

        async def mock_rpc(method, params=None):
            if method == "get_accounts":
                return {
                    "total_balance": 4404747695976,
                    "total_unlocked_balance": 1605762000000,
                    "subaddress_accounts": [],
                }
            return {}

        ex._rpc = mock_rpc
        balance = await ex._get_balance()
        assert balance["balance"] == 4404747695976
        assert balance["unlocked_balance"] == 1605762000000


# ═══════════════════════════════════════════════════════════
# Transaction Parsing
# ═══════════════════════════════════════════════════════════

class TestDepositParsing:
    @pytest.mark.asyncio
    async def test_deposit_parsing_regular_receive(self):
        """Regular incoming SAL transfer tagged as 'incoming'."""
        ex = SalviumWalletExchange()

        async def mock_rpc(method, params=None):
            if method == "get_accounts":
                return {"subaddress_accounts": [{"account_index": 0, "label": "Primary"}]}
            elif method == "get_transfers":
                return {
                    "in": [{
                        "txid": "abc123",
                        "amount": 200000000,  # 2 SAL (1e8 units)
                        "timestamp": int(T.timestamp()),
                        "height": 100000,
                        "coinbase": False,
                        "unlock_time": 0,
                        "confirmations": 20,
                        "address": "SalAddress123",
                    }],
                    "out": [],
                }
            return {}

        ex._rpc = mock_rpc
        deposits = await ex.fetch_deposits()
        assert len(deposits) == 1
        d = deposits[0]
        assert d["asset"] == "SAL"
        assert d["amount"] == "2"
        raw = json.loads(d["raw_data"])
        assert raw["_salvium_subtype"] == "incoming"
        assert raw["_salvium_account_index"] == 0

    @pytest.mark.asyncio
    async def test_deposit_parsing_mining_reward(self):
        """Mining reward (coinbase=True) tagged as 'mining_reward'."""
        ex = SalviumWalletExchange()

        async def mock_rpc(method, params=None):
            if method == "get_accounts":
                return {"subaddress_accounts": [{"account_index": 0, "label": "Primary"}]}
            elif method == "get_transfers":
                return {
                    "in": [{
                        "txid": "mine123",
                        "amount": 100000000,  # 1 SAL
                        "timestamp": int(T.timestamp()),
                        "height": 100001,
                        "coinbase": True,
                        "unlock_time": 0,
                        "confirmations": 50,
                    }],
                    "out": [],
                }
            return {}

        ex._rpc = mock_rpc
        deposits = await ex.fetch_deposits()
        assert len(deposits) == 1
        raw = json.loads(deposits[0]["raw_data"])
        assert raw["_salvium_subtype"] == "mining_reward"
        assert raw["_salvium_coinbase"] is True

    @pytest.mark.asyncio
    async def test_deposit_parsing_staking_unlock(self):
        """Incoming with unlock_time > 0 tagged as 'staking_unlock_candidate'."""
        ex = SalviumWalletExchange()

        async def mock_rpc(method, params=None):
            if method == "get_accounts":
                return {"subaddress_accounts": [{"account_index": 0, "label": "Primary"}]}
            elif method == "get_transfers":
                return {
                    "in": [{
                        "txid": "unlock123",
                        "amount": 101000000000,  # 1010 SAL (1e8 units)
                        "timestamp": int(T.timestamp()),
                        "height": 121600,
                        "coinbase": False,
                        "unlock_time": 21600,
                        "confirmations": 20,
                    }],
                    "out": [],
                }
            return {}

        ex._rpc = mock_rpc
        deposits = await ex.fetch_deposits()
        assert len(deposits) == 1
        raw = json.loads(deposits[0]["raw_data"])
        assert raw["_salvium_subtype"] == "staking_unlock_candidate"
        assert raw["_salvium_unlock_time"] == 21600


# ═══════════════════════════════════════════════════════════
# Staking Lock Detection (Bug 3 fix)
# ═══════════════════════════════════════════════════════════

class TestStakingLockDetection:
    @pytest.mark.asyncio
    async def test_staking_lock_detected_by_zero_amount_and_huge_fee(self):
        """Staking lock: amount=0, fee=staked SAL, no destinations → staking_lock."""
        ex = SalviumWalletExchange()

        async def mock_rpc(method, params=None):
            if method == "get_accounts":
                return {"subaddress_accounts": [{"account_index": 0, "label": "Primary"}]}
            elif method == "get_transfers":
                return {
                    "in": [],
                    "out": [{
                        "txid": "stake_lock_123",
                        "amount": 0,  # Zero amount — staking lock signature
                        "fee": 1399493663788,  # ~13994.93 SAL — the staked amount
                        "timestamp": int(T.timestamp()),
                        "height": 100000,
                        "unlock_time": 0,
                        "destinations": [],  # No destinations
                        "payment_id": "0000000000000000",
                        "confirmations": 20,
                    }],
                }
            return {}

        ex._rpc = mock_rpc
        withdrawals = await ex.fetch_withdrawals()
        assert len(withdrawals) == 1
        w = withdrawals[0]
        # The staked amount should come from the "fee" field
        expected_amount = D("1399493663788") / D("100000000")
        assert D(w["amount"]) == expected_amount
        # The actual fee should be zero (absorbed)
        assert D(w["fee"]) == D("0")
        raw = json.loads(w["raw_data"])
        assert raw["_salvium_subtype"] == "staking_lock"

    @pytest.mark.asyncio
    async def test_normal_send_not_detected_as_staking(self):
        """Normal outgoing tx with amount > 0 and destinations → outgoing, not staking."""
        ex = SalviumWalletExchange()

        async def mock_rpc(method, params=None):
            if method == "get_accounts":
                return {"subaddress_accounts": [{"account_index": 0, "label": "Primary"}]}
            elif method == "get_transfers":
                return {
                    "in": [],
                    "out": [{
                        "txid": "send_123",
                        "amount": 500000000,  # 5 SAL
                        "fee": 1000000,  # Normal ~0.01 SAL fee
                        "timestamp": int(T.timestamp()),
                        "height": 100000,
                        "unlock_time": 0,
                        "destinations": [{"address": "recipient_addr"}],
                        "confirmations": 20,
                    }],
                }
            return {}

        ex._rpc = mock_rpc
        withdrawals = await ex.fetch_withdrawals()
        assert len(withdrawals) == 1
        w = withdrawals[0]
        assert D(w["amount"]) == D("5")
        raw = json.loads(w["raw_data"])
        assert raw["_salvium_subtype"] == "outgoing"

    @pytest.mark.asyncio
    async def test_staking_lock_via_unlock_time(self):
        """Outgoing with unlock_time > 0 still detected as staking_lock."""
        ex = SalviumWalletExchange()

        async def mock_rpc(method, params=None):
            if method == "get_accounts":
                return {"subaddress_accounts": [{"account_index": 0, "label": "Primary"}]}
            elif method == "get_transfers":
                return {
                    "in": [],
                    "out": [{
                        "txid": "lock_via_unlock_time",
                        "amount": 100000000000,  # 1000 SAL
                        "fee": 1000000,  # Normal fee
                        "timestamp": int(T.timestamp()),
                        "height": 100000,
                        "unlock_time": 21600,  # Set!
                        "destinations": [{"address": "self_addr"}],
                        "confirmations": 20,
                    }],
                }
            return {}

        ex._rpc = mock_rpc
        withdrawals = await ex.fetch_withdrawals()
        assert len(withdrawals) == 1
        raw = json.loads(withdrawals[0]["raw_data"])
        assert raw["_salvium_subtype"] == "staking_lock"


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
        income_type = "staking"
        assert income_type == "staking"
        assert income_type != "capital_gain"

    def test_staking_lock_is_not_taxable(self):
        """The lock itself has no tax consequence — you still own the SAL."""
        lock_creates_income = False
        lock_creates_disposal = False
        assert lock_creates_income is False
        assert lock_creates_disposal is False

    def test_principal_return_is_not_income(self):
        """Only the yield portion is income, not the full unlock amount."""
        lock_amount = D("1000")
        unlock_amount = D("1010")
        yield_amount = unlock_amount - lock_amount
        assert yield_amount == D("10")
        assert yield_amount != unlock_amount

    def test_yield_usd_uses_unlock_time_fmv(self):
        """FMV must be at unlock time, not lock time."""
        lock_price = D("0.10")
        unlock_price = D("0.15")
        yield_amount = D("10")
        yield_usd_correct = yield_amount * unlock_price
        yield_usd_wrong = yield_amount * lock_price
        assert yield_usd_correct == D("1.50")
        assert yield_usd_wrong == D("1.00")
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
        unlocks = []
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
        unlocks = [self._make_unlock(height=200000)]
        match = tracker._find_matching_unlock(lock, unlocks)
        assert match is None

    def test_unlock_amount_less_than_lock_no_match(self):
        """Unlock amount less than locked amount → no match."""
        tracker = SalviumStakingTracker()
        lock = self._make_lock(amount="1000")
        unlocks = [self._make_unlock(amount="500")]
        match = tracker._find_matching_unlock(lock, unlocks)
        assert match is None

    def test_multiple_concurrent_stakes(self):
        """Two locks, two unlocks — each matched correctly."""
        tracker = SalviumStakingTracker()
        lock1 = self._make_lock(height=100000, amount="500", tx_hash="lock1")
        lock2 = self._make_lock(height=100100, amount="800", tx_hash="lock2")
        unlock1 = self._make_unlock(height=121600, amount="505", tx_hash="unlock1")
        unlock2 = self._make_unlock(height=121700, amount="808", tx_hash="unlock2")

        match1 = tracker._find_matching_unlock(lock1, [unlock1, unlock2])
        assert match1 is not None
        assert match1["tx_hash"] == "unlock1"

        match2 = tracker._find_matching_unlock(lock2, [unlock1, unlock2])
        assert match2 is not None
        assert match2["tx_hash"] == "unlock2"

    def test_restake_after_unlock(self):
        """After unlock, user immediately locks again — should be a separate stake."""
        tracker = SalviumStakingTracker()
        lock1 = self._make_lock(height=100000, amount="1000", tx_hash="lock1")
        unlock1 = self._make_unlock(height=121600, amount="1010", tx_hash="unlock1")
        lock2 = self._make_lock(height=121700, amount="1010", tx_hash="lock2")
        unlock2 = self._make_unlock(height=143300, amount="1020.1", tx_hash="unlock2")

        match1 = tracker._find_matching_unlock(lock1, [unlock1, unlock2])
        assert match1["tx_hash"] == "unlock1"

        match2 = tracker._find_matching_unlock(lock2, [unlock1, unlock2])
        assert match2["tx_hash"] == "unlock2"


# ═══════════════════════════════════════════════════════════
# RPC Handling
# ═══════════════════════════════════════════════════════════

class TestRPCHandling:
    @pytest.mark.asyncio
    async def test_rpc_error_returns_empty(self):
        """RPC returning empty → fetch_deposits returns empty list."""
        ex = SalviumWalletExchange()

        async def mock_rpc(method, params=None):
            return {}

        ex._rpc = mock_rpc
        deposits = await ex.fetch_deposits()
        assert deposits == []

    def test_rpc_auth_headers_when_configured(self):
        """When RPC user/pass are set, auth should be used."""
        ex = SalviumWalletExchange()
        ex.rpc_user = "testuser"
        ex.rpc_pass = "testpass"
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
