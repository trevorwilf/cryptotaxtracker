"""
Tests for the Transfer Match Preview system.

Covers:
  - TX hash matching (exact and with :N suffix normalization)
  - Amount/timing matching
  - Same-exchange exclusion
  - Different-asset exclusion
  - Time window enforcement
  - Address overlap detection
  - Score matching logic
"""
import pytest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from transfer_preview import TransferPreview

D = Decimal
T = lambda y, m, d, h=12, mi=0: datetime(y, m, d, h, mi, 0, tzinfo=timezone.utc)


# ── Helpers ──────────────────────────────────────────────────────────────

class FakeRow:
    """Simulates a DB row with dict-like access via zip(keys, values)."""
    def __init__(self, data, keys):
        self._data = data
        self._keys = keys

    def fetchone(self):
        if self._data:
            return self._data[0] if isinstance(self._data[0], (tuple, list)) else self._data
        return None

    def fetchall(self):
        return self._data

    def keys(self):
        return self._keys


class FakePreviewSession:
    """Mock DB session for transfer preview tests."""

    def __init__(self):
        self.deposits = {}   # id -> dict
        self.withdrawals = {}  # id -> dict
        self.dep_addresses = []   # list of (address, asset, exchange)
        self.wd_addresses = []
        self.claims = []

    async def execute(self, stmt, params=None):
        sql = str(stmt) if hasattr(stmt, 'text') else str(stmt)

        # Individual deposit lookup
        if "FROM tax.deposits WHERE id = :id" in sql:
            dep_id = params["id"]
            dep = self.deposits.get(dep_id)
            if dep:
                keys = ["id", "exchange", "asset", "amount", "tx_hash", "address",
                        "confirmed_at", "network"]
                values = tuple(dep.get(k) for k in keys)
                return FakeRow([values], keys)
            return FakeRow([], [])

        # Individual withdrawal lookup
        if "FROM tax.withdrawals WHERE id = :id" in sql:
            wd_id = params["id"]
            wd = self.withdrawals.get(wd_id)
            if wd:
                keys = ["id", "exchange", "asset", "amount", "fee",
                        "tx_hash", "address", "confirmed_at", "network"]
                values = tuple(wd.get(k) for k in keys)
                return FakeRow([values], keys)
            return FakeRow([], [])

        # Find withdrawal candidates (for deposit matching)
        if "FROM tax.withdrawals" in sql and "asset = :asset" in sql and "exchange != :ex" in sql:
            asset = params["asset"]
            ex = params["ex"]
            results = []
            keys = ["id", "exchange", "asset", "amount", "fee",
                    "tx_hash", "address", "confirmed_at", "network"]
            for wd in self.withdrawals.values():
                if wd["asset"] == asset and wd["exchange"] != ex:
                    if params.get("earliest") and wd.get("confirmed_at"):
                        if wd["confirmed_at"] < params["earliest"]:
                            continue
                        if wd["confirmed_at"] > params["dep_time"]:
                            continue
                    results.append(tuple(wd.get(k) for k in keys))
            return FakeRow(results, keys)

        # Find deposit candidates (for withdrawal matching)
        if "FROM tax.deposits" in sql and "asset = :asset" in sql and "exchange != :ex" in sql:
            asset = params["asset"]
            ex = params["ex"]
            results = []
            keys = ["id", "exchange", "asset", "amount",
                    "tx_hash", "address", "confirmed_at", "network"]
            for dep in self.deposits.values():
                if dep["asset"] == asset and dep["exchange"] != ex:
                    if params.get("wd_time") and dep.get("confirmed_at"):
                        if dep["confirmed_at"] < params["wd_time"]:
                            continue
                        if dep["confirmed_at"] > params["latest"]:
                            continue
                    results.append(tuple(dep.get(k) for k in keys))
            return FakeRow(results, keys)

        # Address overlap - get distinct addresses from imported records
        if "SELECT DISTINCT address, asset, exchange" in sql:
            keys = ["address", "asset", "exchange"]
            return FakeRow(
                [tuple(d.values()) for d in self.dep_addresses or self.wd_addresses],
                keys
            )

        # Address overlap - check deposits on other exchanges
        if "FROM tax.deposits" in sql and "address = :addr" in sql:
            return FakeRow([], ["exchange", "asset", "times_seen"])

        # Address overlap - check withdrawals on other exchanges
        if "FROM tax.withdrawals" in sql and "address = :addr" in sql:
            return FakeRow([], ["exchange", "asset", "times_seen"])

        # Claimed address check
        if "wallet_addresses" in sql and "wallet_address_claims" in sql:
            return FakeRow(self.claims, ["address", "claim_type", "confidence"])

        return FakeRow([], [])


# ── Score Matching Tests ─────────────────────────────────────────────────

class TestScoreMatching:

    def test_tx_hash_exact_match_is_high(self):
        preview = TransferPreview()
        dep = {"tx_hash": "abc123def456", "amount": "1.0", "address": None}
        wd = {"tx_hash": "abc123def456", "amount": "1.0", "fee": "0", "address": None}
        result = preview._score_match(dep, wd)
        assert result is not None
        assert result["level"] == "high"
        assert "tx_hash_exact_match" in result["evidence"]

    def test_amount_match_is_medium(self):
        preview = TransferPreview()
        dep = {"tx_hash": None, "amount": "0.999", "address": None}
        wd = {"tx_hash": None, "amount": "1.0", "fee": "0.001", "address": None}
        result = preview._score_match(dep, wd)
        assert result is not None
        assert result["level"] == "medium"

    def test_same_address_boosts_to_high(self):
        preview = TransferPreview()
        dep = {"tx_hash": None, "amount": "0.999", "address": "bc1q_shared_addr"}
        wd = {"tx_hash": None, "amount": "1.0", "fee": "0.001", "address": "bc1q_shared_addr"}
        result = preview._score_match(dep, wd)
        assert result is not None
        assert result["level"] == "high"

    def test_different_amounts_no_match(self):
        preview = TransferPreview()
        dep = {"tx_hash": None, "amount": "5.0", "address": None}
        wd = {"tx_hash": None, "amount": "1.0", "fee": "0", "address": None}
        result = preview._score_match(dep, wd)
        assert result is None

    def test_zero_wd_amount_no_match(self):
        preview = TransferPreview()
        dep = {"tx_hash": None, "amount": "1.0", "address": None}
        wd = {"tx_hash": None, "amount": "0", "fee": "0", "address": None}
        result = preview._score_match(dep, wd)
        assert result is None


# ── TX Hash Normalization ─────────────────────────────────────────────────

class TestTxHashNormalization:

    def test_normalize_strips_output_index(self):
        preview = TransferPreview()
        assert preview._normalize_tx_hash("abc123:0") == "abc123"
        assert preview._normalize_tx_hash("abc123:4") == "abc123"

    def test_normalize_preserves_non_numeric_suffix(self):
        preview = TransferPreview()
        # If suffix is not a number, don't strip
        assert preview._normalize_tx_hash("abc:def") == "abc:def"

    def test_normalize_lowercases(self):
        preview = TransferPreview()
        assert preview._normalize_tx_hash("ABC123DEF") == "abc123def"

    def test_mexc_deposit_matches_nonkyc_withdrawal(self):
        """MEXC tx_hash with :0 suffix should match plain hash."""
        preview = TransferPreview()
        mexc_hash = "4882e0cb2757960a2f98b245a08889d1954823ae2a29f4cb038ff9cd45b11df6:0"
        nonkyc_hash = "4882e0cb2757960a2f98b245a08889d1954823ae2a29f4cb038ff9cd45b11df6"
        dep = {"tx_hash": mexc_hash, "amount": "0.001377", "address": None}
        wd = {"tx_hash": nonkyc_hash, "amount": "0.001377", "fee": "0", "address": None}
        result = preview._score_match(dep, wd)
        assert result is not None
        assert result["level"] == "high"
        assert "tx_hash_exact_match" in result["evidence"]


# ── Scan Matching Tests ──────────────────────────────────────────────────

class TestScanImportedDeposits:

    @pytest.mark.asyncio
    async def test_finds_tx_hash_match(self):
        """Import a deposit, find a matching withdrawal on another exchange."""
        session = FakePreviewSession()
        session.deposits[1] = {
            "id": 1, "exchange": "nonkyc", "asset": "BTC",
            "amount": "0.4999", "tx_hash": "tx_btc_001",
            "address": "addr1", "confirmed_at": T(2026, 3, 15, 14),
            "network": "BTC",
        }
        session.withdrawals[10] = {
            "id": 10, "exchange": "mexc", "asset": "BTC",
            "amount": "0.5", "fee": "0.0001", "tx_hash": "tx_btc_001",
            "address": "addr1", "confirmed_at": T(2026, 3, 15, 12),
            "network": "BTC",
        }

        preview = TransferPreview()
        matches = await preview.scan_imported_deposits(session, [1], "nonkyc")
        assert len(matches) == 1
        assert matches[0]["confidence"] == "high"
        assert matches[0]["type"] == "deposit_matches_withdrawal"

    @pytest.mark.asyncio
    async def test_ignores_same_exchange(self):
        """Withdrawal on the same exchange should not match."""
        session = FakePreviewSession()
        session.deposits[1] = {
            "id": 1, "exchange": "nonkyc", "asset": "BTC",
            "amount": "0.5", "tx_hash": "tx_same",
            "address": "addr1", "confirmed_at": T(2026, 3, 15, 14),
            "network": "BTC",
        }
        # Withdrawal also on nonkyc — should be excluded
        session.withdrawals[10] = {
            "id": 10, "exchange": "nonkyc", "asset": "BTC",
            "amount": "0.5", "fee": "0", "tx_hash": "tx_same",
            "address": "addr1", "confirmed_at": T(2026, 3, 15, 12),
            "network": "BTC",
        }

        preview = TransferPreview()
        matches = await preview.scan_imported_deposits(session, [1], "nonkyc")
        assert len(matches) == 0

    @pytest.mark.asyncio
    async def test_ignores_different_asset(self):
        """BTC deposit should not match USDT withdrawal."""
        session = FakePreviewSession()
        session.deposits[1] = {
            "id": 1, "exchange": "nonkyc", "asset": "BTC",
            "amount": "0.5", "tx_hash": None,
            "address": None, "confirmed_at": T(2026, 3, 15, 14),
            "network": "BTC",
        }
        session.withdrawals[10] = {
            "id": 10, "exchange": "mexc", "asset": "USDT",
            "amount": "25000", "fee": "1", "tx_hash": None,
            "address": None, "confirmed_at": T(2026, 3, 15, 12),
            "network": "SOL",
        }

        preview = TransferPreview()
        matches = await preview.scan_imported_deposits(session, [1], "nonkyc")
        assert len(matches) == 0

    @pytest.mark.asyncio
    async def test_ignores_outside_time_window(self):
        """Withdrawal 5 days before deposit — outside 72h window."""
        session = FakePreviewSession()
        session.deposits[1] = {
            "id": 1, "exchange": "nonkyc", "asset": "BTC",
            "amount": "0.5", "tx_hash": None,
            "address": None, "confirmed_at": T(2026, 3, 20, 12),
            "network": "BTC",
        }
        session.withdrawals[10] = {
            "id": 10, "exchange": "mexc", "asset": "BTC",
            "amount": "0.5", "fee": "0", "tx_hash": None,
            "address": None, "confirmed_at": T(2026, 3, 15, 12),  # 5 days earlier
            "network": "BTC",
        }

        preview = TransferPreview()
        matches = await preview.scan_imported_deposits(session, [1], "nonkyc")
        assert len(matches) == 0


class TestScanImportedWithdrawals:

    @pytest.mark.asyncio
    async def test_finds_deposit_match(self):
        """Import a withdrawal, find a matching deposit on another exchange."""
        session = FakePreviewSession()
        session.withdrawals[1] = {
            "id": 1, "exchange": "mexc", "asset": "BTC",
            "amount": "0.5", "fee": "0.0001", "tx_hash": "tx_wd_001",
            "address": "addr_dest", "confirmed_at": T(2026, 3, 15, 10),
            "network": "BTC",
        }
        session.deposits[20] = {
            "id": 20, "exchange": "nonkyc", "asset": "BTC",
            "amount": "0.4999", "tx_hash": "tx_wd_001",
            "address": "addr_dest", "confirmed_at": T(2026, 3, 15, 14),
            "network": "BTC",
        }

        preview = TransferPreview()
        matches = await preview.scan_imported_withdrawals(session, [1], "mexc")
        assert len(matches) == 1
        assert matches[0]["confidence"] == "high"
        assert matches[0]["type"] == "withdrawal_matches_deposit"

    @pytest.mark.asyncio
    async def test_amount_timing_match(self):
        """Match by amount + timing without tx_hash."""
        session = FakePreviewSession()
        session.withdrawals[1] = {
            "id": 1, "exchange": "mexc", "asset": "SAL",
            "amount": "1000", "fee": "5", "tx_hash": None,
            "address": None, "confirmed_at": T(2026, 2, 10, 10),
            "network": "SAL",
        }
        session.deposits[20] = {
            "id": 20, "exchange": "nonkyc", "asset": "SAL",
            "amount": "995", "tx_hash": None,
            "address": None, "confirmed_at": T(2026, 2, 10, 14),
            "network": "SAL",
        }

        preview = TransferPreview()
        matches = await preview.scan_imported_withdrawals(session, [1], "mexc")
        assert len(matches) == 1
        assert matches[0]["confidence"] == "medium"

    @pytest.mark.asyncio
    async def test_empty_ids_returns_empty(self):
        session = FakePreviewSession()
        preview = TransferPreview()
        matches = await preview.scan_imported_withdrawals(session, [], "mexc")
        assert matches == []

    @pytest.mark.asyncio
    async def test_nonexistent_id_returns_empty(self):
        session = FakePreviewSession()
        preview = TransferPreview()
        matches = await preview.scan_imported_withdrawals(session, [999], "mexc")
        assert matches == []


# ── Module Import & Endpoint Registration ─────────────────────────────────

class TestEndpointRegistration:

    def test_transfer_preview_module_importable(self):
        from transfer_preview import TransferPreview
        preview = TransferPreview()
        assert hasattr(preview, "scan_imported_deposits")
        assert hasattr(preview, "scan_imported_withdrawals")
        assert hasattr(preview, "scan_address_overlaps")

    def test_import_file_accepts_preview_matches_param(self):
        """The import-file endpoint should accept preview_matches parameter."""
        import inspect
        import main as main_module
        sig = inspect.signature(main_module.import_file)
        assert "preview_matches" in sig.parameters

    def test_import_file_preview_endpoint_exists(self):
        import main as main_module
        routes = [r.path for r in main_module.app.routes]
        assert "/v4/import-file-preview" in routes

    def test_transfer_preview_scan_endpoint_exists(self):
        import main as main_module
        routes = [r.path for r in main_module.app.routes]
        assert "/v4/transfer-preview" in routes


# ── Constructor Configuration ─────────────────────────────────────────────

class TestConfiguration:

    def test_default_time_window(self):
        preview = TransferPreview()
        assert preview.time_window == timedelta(hours=72)

    def test_default_fee_tolerance(self):
        preview = TransferPreview()
        assert preview.fee_tolerance == D("0.1")

    def test_custom_config(self):
        preview = TransferPreview(time_window_hours=48, fee_tolerance_pct=5.0)
        assert preview.time_window == timedelta(hours=48)
        assert preview.fee_tolerance == D("0.05")
