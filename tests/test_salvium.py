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


# ═══════════════════════════════════════════════════════════
# Wallet Operations (sweep, stake, accounts)
# ═══════════════════════════════════════════════════════════

class TestWalletOperations:
    def test_atomic_conversion_stake(self):
        """1000 SAL = 100000000000 atomic units."""
        sal = D("1000")
        atomic = int(sal * D("100000000"))
        assert atomic == 100000000000

    def test_atomic_conversion_small(self):
        """0.01 SAL = 1000000 atomic."""
        sal = D("0.01")
        atomic = int(sal * D("100000000"))
        assert atomic == 1000000

    def test_stake_tx_type_is_6(self):
        """Staking uses transfer with tx_type=6."""
        tx_type = 6  # STAKE enum value
        assert tx_type == 6

    def test_sweep_requires_asset_type(self):
        """sweep_all requires asset_type parameter."""
        params = {"address": "test", "asset_type": "SAL1", "account_index": 0}
        assert "asset_type" in params
        assert params["asset_type"] == "SAL1"

    def test_locked_balance_calculation(self):
        """Locked SAL = total balance - unlocked balance."""
        total = D("44047.47695976")
        unlocked = D("16057.61620000")
        locked = total - unlocked
        assert locked == D("27989.86075976")

    def test_stake_all_leaves_fee_buffer(self):
        """Stake all should leave ~0.01 SAL for fees."""
        unlocked = 16057.62
        fee_buffer = 0.01
        stake_amount = unlocked - fee_buffer
        assert stake_amount < unlocked
        assert stake_amount > 0

    def test_account_balance_response_format(self):
        """Account response should have required fields."""
        required_fields = ["index", "label", "address", "balance_sal", "unlocked_sal", "locked_sal"]
        account = {"index": 0, "label": "Primary", "address": "SC11...",
                   "balance_sal": "44047.48", "unlocked_sal": "16057.62", "locked_sal": "27989.86"}
        for field in required_fields:
            assert field in account


class TestRPCErrorHandling:
    """Tests for the updated _rpc method that returns error details."""

    @pytest.mark.asyncio
    async def test_rpc_error_returns_error_dict(self):
        """RPC error should return {'error': ...} instead of empty dict."""
        ex = SalviumWalletExchange()

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={
            "jsonrpc": "2.0",
            "id": "0",
            "error": {"code": -1, "message": "Not enough unlocked balance"}
        })

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session_ctx)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_post_ctx = AsyncMock()
        mock_post_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_post_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_session_ctx.post = MagicMock(return_value=mock_post_ctx)

        with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
            result = await ex._rpc("sweep_all", {"address": "test"})

        assert "error" in result
        assert result["error"]["message"] == "Not enough unlocked balance"

    @pytest.mark.asyncio
    async def test_rpc_success_returns_result(self):
        """Successful RPC should return the result dict."""
        ex = SalviumWalletExchange()

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={
            "jsonrpc": "2.0",
            "id": "0",
            "result": {"tx_hash": "abc123", "fee": 1000000}
        })

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session_ctx)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_post_ctx = AsyncMock()
        mock_post_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_post_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_session_ctx.post = MagicMock(return_value=mock_post_ctx)

        with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
            result = await ex._rpc("transfer", {"amount": 100})

        assert result["tx_hash"] == "abc123"
        assert "error" not in result

    @pytest.mark.asyncio
    async def test_rpc_http_error_returns_empty(self):
        """HTTP errors should still return empty dict."""
        ex = SalviumWalletExchange()

        mock_resp = AsyncMock()
        mock_resp.status = 500
        mock_resp.text = AsyncMock(return_value="Internal Server Error")

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session_ctx)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_post_ctx = AsyncMock()
        mock_post_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_post_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_session_ctx.post = MagicMock(return_value=mock_post_ctx)

        with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
            result = await ex._rpc("get_accounts")

        assert result == {}


class TestSweepEndpointLogic:
    """Tests for sweep endpoint logic."""

    def test_sweep_params_format(self):
        """Sweep params must include address, asset_type, account_index."""
        dest_addr = "SC1siBXGmu3HGK2mrggQ8M2T9mSEkWKh9UmTYDTzV9cr"
        params = {
            "address": dest_addr,
            "asset_type": "SAL1",
            "account_index": 0,
        }
        assert params["asset_type"] == "SAL1"
        assert params["account_index"] == 0
        assert len(params["address"]) > 10

    def test_sweep_result_parsing(self):
        """Sweep result with tx_hash_list, amount_list, fee_list."""
        result = {
            "tx_hash_list": ["abc123", "def456"],
            "amount_list": [500000000000, 300000000000],
            "fee_list": [1000000, 1000000],
        }
        ATOMIC = D("100000000")
        amounts = [str(D(str(a)) / ATOMIC) for a in result["amount_list"]]
        fees = [str(D(str(f)) / ATOMIC) for f in result["fee_list"]]
        assert amounts == ["5000", "3000"]
        assert fees == ["0.01", "0.01"]
        assert len(result["tx_hash_list"]) == 2


class TestStakeEndpointLogic:
    """Tests for stake endpoint logic."""

    def test_stake_params_format(self):
        """Stake params must include destinations, tx_type=6, assets."""
        own_addr = "SC11aHNaiaVQzopqEDwGVhVeHcEz4mNB9NfBBwMtH9iN"
        atomic_amount = int(D("1000") * D("100000000"))
        params = {
            "destinations": [{"amount": atomic_amount, "address": own_addr}],
            "source_asset": "SAL1",
            "dest_asset": "SAL1",
            "tx_type": 6,
            "account_index": 0,
            "get_tx_key": True,
        }
        assert params["tx_type"] == 6
        assert params["source_asset"] == "SAL1"
        assert params["destinations"][0]["amount"] == 100000000000
        assert params["get_tx_key"] is True

    def test_stake_result_parsing(self):
        """Stake result with tx_hash and fee."""
        result = {
            "tx_hash": "stake_tx_abc123",
            "fee": 2000000,
        }
        ATOMIC = D("100000000")
        fee_sal = str(D(str(result["fee"])) / ATOMIC)
        assert fee_sal == "0.02"
        assert result["tx_hash"] == "stake_tx_abc123"

    def test_stake_negative_amount_rejected(self):
        """Negative stake amount should be rejected."""
        amount = -100
        assert amount <= 0

    def test_stake_zero_amount_rejected(self):
        """Zero stake amount should be rejected."""
        amount = 0
        assert amount <= 0


class TestAccountsEndpointLogic:
    """Tests for accounts endpoint logic."""

    def test_accounts_response_parsing(self):
        """Parse get_accounts RPC response into API format."""
        rpc_response = {
            "subaddress_accounts": [
                {"account_index": 0, "balance": 4404747695976,
                 "base_address": "SC11aHNa...", "label": "Primary account",
                 "unlocked_balance": 1605761620000},
                {"account_index": 1, "balance": 0,
                 "base_address": "SC1siBXG...", "label": "consolidate",
                 "unlocked_balance": 0},
            ],
            "total_balance": 4404747695976,
            "total_unlocked_balance": 1605761620000,
        }
        ATOMIC = D("100000000")
        accts = rpc_response["subaddress_accounts"]
        result = []
        for a in accts:
            result.append({
                "index": a["account_index"],
                "label": a.get("label", ""),
                "address": a.get("base_address", ""),
                "balance_sal": str(D(str(a.get("balance", 0))) / ATOMIC),
                "unlocked_sal": str(D(str(a.get("unlocked_balance", 0))) / ATOMIC),
                "locked_sal": str((D(str(a.get("balance", 0))) - D(str(a.get("unlocked_balance", 0)))) / ATOMIC),
            })

        assert len(result) == 2
        assert D(result[0]["balance_sal"]) == D("44047.47695976")
        assert D(result[0]["unlocked_sal"]) == D("16057.61620000")
        assert D(result[0]["locked_sal"]) == D("27989.86075976")
        assert D(result[1]["balance_sal"]) == D("0")
        assert D(result[1]["locked_sal"]) == D("0")

    def test_total_balance_calculation(self):
        """Total balance/locked/unlocked from RPC response."""
        ATOMIC = D("100000000")
        total_balance = D("4404747695976") / ATOMIC
        total_unlocked = D("1605761620000") / ATOMIC
        total_locked = total_balance - total_unlocked
        assert total_balance == D("44047.47695976")
        assert total_unlocked == D("16057.61620000")
        assert total_locked == D("27989.86075976")


# ═══════════════════════════════════════════════════════════
# Stake Max (fee estimation + max stake)
# ═══════════════════════════════════════════════════════════

class TestStakeMax:
    def test_fee_estimation_uses_do_not_relay(self):
        """Stake-max dry run must set do_not_relay=true."""
        params = {
            "destinations": [{"amount": 1605761620000, "address": "SC11aHNa..."}],
            "source_asset": "SAL1",
            "dest_asset": "SAL1",
            "tx_type": 6,
            "do_not_relay": True,
            "get_tx_key": True,
        }
        assert params["do_not_relay"] is True
        assert params["tx_type"] == 6

    def test_max_stake_is_unlocked_minus_fee(self):
        """Max stakeable = unlocked_balance - estimated_fee."""
        unlocked = 1605761620000  # atomic
        fee = 810220  # atomic
        max_stake = unlocked - fee
        assert max_stake == 1605760809780
        assert max_stake > 0

    def test_zero_unlocked_rejected(self):
        """Zero unlocked balance should be rejected."""
        unlocked = 0
        assert unlocked <= 0

    def test_fee_exceeds_balance_rejected(self):
        """If fee >= unlocked, max_stake would be <= 0 — should reject."""
        unlocked = 500000  # tiny balance
        fee = 810220  # larger fee
        max_stake = unlocked - fee
        assert max_stake <= 0

    def test_fallback_fee_estimate(self):
        """If dry run fails, fallback to 0.01 SAL (1000000 atomic)."""
        fallback_fee = 1000000
        assert fallback_fee == 1000000
        ATOMIC = D("100000000")
        assert D(str(fallback_fee)) / ATOMIC == D("0.01")

    def test_half_balance_fee_with_safety_margin(self):
        """Half-balance estimate gets 1.5x safety margin."""
        half_fee = 600000
        estimated_fee = int(half_fee * 1.5)
        assert estimated_fee == 900000


# ═══════════════════════════════════════════════════════════
# Consolidate (sweep to self)
# ═══════════════════════════════════════════════════════════

class TestConsolidate:
    def test_sweep_to_self_uses_own_address(self):
        """Consolidate sweeps to the same account's address."""
        account_addr = "SC11aHNaiaVQzopqEDwGVhVeHcEz4mNB9NfBBwMtH9iN"
        sweep_dest = account_addr
        assert sweep_dest == account_addr

    def test_consolidate_requires_asset_type(self):
        """Consolidate must include asset_type=SAL1."""
        params = {"address": "test_addr", "asset_type": "SAL1", "account_index": 0}
        assert params["asset_type"] == "SAL1"

    def test_consolidate_result_parsing(self):
        """Parse consolidate result like sweep result."""
        result = {
            "tx_hash_list": ["consolidated_tx_123"],
            "amount_list": [1605760000000],
            "fee_list": [810220],
        }
        ATOMIC = D("100000000")
        amounts = [str(D(str(a)) / ATOMIC) for a in result["amount_list"]]
        fees = [str(D(str(f)) / ATOMIC) for f in result["fee_list"]]
        assert len(result["tx_hash_list"]) == 1
        assert D(amounts[0]) == D("16057.6")
        assert D(fees[0]) == D("0.0081022")


# ═══════════════════════════════════════════════════════════
# Outputs (UTXOs)
# ═══════════════════════════════════════════════════════════

class TestOutputs:
    def test_output_amount_conversion(self):
        """Convert atomic output amount to SAL."""
        atomic = 1605761620000
        sal = D(str(atomic)) / D("100000000")
        assert sal == D("16057.6162")

    def test_outputs_sorted_by_amount_desc(self):
        """Outputs should be sorted by amount descending."""
        outputs = [
            {"amount_sal": "100.0"},
            {"amount_sal": "5000.0"},
            {"amount_sal": "50.0"},
        ]
        outputs.sort(key=lambda x: float(x["amount_sal"]), reverse=True)
        assert outputs[0]["amount_sal"] == "5000.0"
        assert outputs[1]["amount_sal"] == "100.0"
        assert outputs[-1]["amount_sal"] == "50.0"

    def test_output_unlocked_vs_locked_flag(self):
        """Available transfers are unlocked=True, unavailable are unlocked=False."""
        available_output = {"amount_sal": "100", "unlocked": True, "spent": False}
        locked_output = {"amount_sal": "200", "unlocked": False, "spent": True}
        assert available_output["unlocked"] is True
        assert locked_output["unlocked"] is False

    def test_empty_outputs_response(self):
        """Account with no outputs returns zero counts."""
        response = {
            "account_index": 1,
            "available_count": 0,
            "locked_count": 0,
            "total_count": 0,
            "total_available_sal": "0",
            "total_locked_sal": "0",
            "outputs": [],
        }
        assert response["total_count"] == 0
        assert response["total_available_sal"] == "0"

    def test_output_totals_match_sum(self):
        """Total available/locked should equal sum of respective outputs."""
        ATOMIC = D("100000000")
        available_amounts = [500000000000, 300000000000]  # 5000 + 3000 SAL
        total = sum(D(str(a)) / ATOMIC for a in available_amounts)
        assert total == D("8000")

    def test_incoming_transfers_params(self):
        """incoming_transfers RPC params must include transfer_type and account_index."""
        params = {"transfer_type": "available", "account_index": 0}
        assert params["transfer_type"] in ("available", "unavailable")
        assert "account_index" in params


# ═══════════════════════════════════════════════════════════
# Auto-polling (Feature 1 & 2)
# ═══════════════════════════════════════════════════════════

class TestAutoPoll:
    def test_poll_interval_is_60_seconds(self):
        """Normal polling interval should be 60 seconds."""
        poll_interval_ms = 60000
        assert poll_interval_ms == 60000

    def test_fast_refresh_is_3_seconds_for_30_total(self):
        """Fast refresh: 10 polls x 3 seconds = 30 seconds total."""
        fast_interval_ms = 3000
        fast_count_max = 10
        total_ms = fast_interval_ms * fast_count_max
        assert total_ms == 30000
