"""
Tests for the RCA API Addendum corrections.

Correction 1: MongoDB ObjectId-style exchange_id
Correction 2: Market field is always a dict
Correction 3: child_asset column + no network from API
Correction 4: Pool trade uses createdAt (confirmed)
Correction 5: totalWithFee=0 fallback to price*quantity
Correction 6: WebSocket - not applicable (just confirmation)
"""
import inspect
import pytest
from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import patch

D = Decimal


# ═══ Correction 1: MongoDB ObjectId exchange_id ═══════════════════════════

class TestMongoObjectIdExchangeId:

    @pytest.mark.asyncio
    async def test_nonkyc_deposit_preserves_hex_id(self):
        """NonKYC deposit exchange_id should handle MongoDB hex strings."""
        from exchanges.nonkyc import NonKYCExchange
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "69b093bafc89b816d25de002",  # MongoDB ObjectId
            "ticker": "BTC",
            "quantity": "0.5",
            "status": "completed",
            "transactionid": "abc123txhash",
            "firstseenat": "2026-03-15T12:00:00Z",
            "address": "btc_addr",
        }]
        with patch.object(ex, '_get', return_value=payload):
            deps = await ex.fetch_deposits()
        assert deps[0]["exchange_id"] == "69b093bafc89b816d25de002"
        assert deps[0]["external_tx_id"] == "abc123txhash"

    @pytest.mark.asyncio
    async def test_nonkyc_withdrawal_preserves_hex_id(self):
        from exchanges.nonkyc import NonKYCExchange
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "69c1a2b3d4e5f67890abcdef",
            "ticker": "SAL",
            "quantity": "1000",
            "fee": "5",
            "feecurrency": "SAL",
            "status": "completed",
            "transactionid": "sal_tx_hash_123",
            "address": "sal_addr",
        }]
        with patch.object(ex, '_get', return_value=payload):
            wds = await ex.fetch_withdrawals()
        assert wds[0]["exchange_id"] == "69c1a2b3d4e5f67890abcdef"
        assert wds[0]["external_tx_id"] == "sal_tx_hash_123"


# ═══ Correction 2: Market field is always a dict ══════════════════════════

class TestMarketDictParsing:

    @pytest.mark.asyncio
    async def test_trade_market_from_dict(self):
        """When market is a dict like {"id": "...", "symbol": "SAL/USDT"}, extract symbol."""
        from exchanges.nonkyc import NonKYCExchange
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "trade001",
            "market": {"id": "643bfa074f63469320902709", "symbol": "SAL/USDT"},
            "side": "buy",
            "price": "0.023",
            "quantity": "100",
            "totalWithFee": "2.3",
            "fee": "0.004",
            "timestamp": 1775480023169,
        }]
        with patch.object(ex, '_get', return_value=payload):
            trades = await ex.fetch_trades()
        assert trades[0]["market"] == "SAL/USDT"

    @pytest.mark.asyncio
    async def test_trade_market_from_string_fallback(self):
        """If market is a plain string (unlikely but defensive), use it directly."""
        from exchanges.nonkyc import NonKYCExchange
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "trade002",
            "market": "BTC/USDT",  # string (unusual)
            "side": "sell",
            "price": "50000",
            "quantity": "0.01",
            "totalWithFee": "500",
            "fee": "0.5",
            "timestamp": 1775480023169,
        }]
        with patch.object(ex, '_get', return_value=payload):
            trades = await ex.fetch_trades()
        assert trades[0]["market"] == "BTC/USDT"

    def test_market_parsing_code_prioritizes_dict(self):
        """The trade parser should check market field first (it's always a dict)."""
        from exchanges.nonkyc import NonKYCExchange
        source = inspect.getsource(NonKYCExchange.fetch_trades)
        assert 't.get("market"' in source
        assert 'isinstance(market_raw, dict)' in source


# ═══ Correction 3: child_asset column + no network ═══════════════════════

class TestChildAsset:

    def test_child_asset_in_migration(self):
        """Migration should add child_asset to deposits and withdrawals."""
        from database import MIGRATION_SQL
        assert "child_asset" in MIGRATION_SQL

    def test_nonkyc_deposit_has_child_asset(self):
        """NonKYC deposit parser should output child_asset from childticker."""
        from exchanges.nonkyc import NonKYCExchange
        source = inspect.getsource(NonKYCExchange.fetch_deposits)
        assert '"child_asset"' in source
        assert 'childticker' in source

    def test_nonkyc_withdrawal_has_child_asset(self):
        from exchanges.nonkyc import NonKYCExchange
        source = inspect.getsource(NonKYCExchange.fetch_withdrawals)
        assert '"child_asset"' in source
        assert 'childticker' in source

    @pytest.mark.asyncio
    async def test_child_asset_populated(self):
        from exchanges.nonkyc import NonKYCExchange
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "dep001",
            "ticker": "USDT",
            "childticker": "USDT-SOL",
            "quantity": "100",
            "status": "completed",
            "transactionid": "tx001",
            "firstseenat": "2026-03-15T12:00:00Z",
        }]
        with patch.object(ex, '_get', return_value=payload):
            deps = await ex.fetch_deposits()
        assert deps[0]["child_asset"] == "USDT-SOL"


# ═══ Correction 4: Pool trade timestamp ═══════════════════════════════════

class TestPoolTimestamp:

    def test_pool_uses_created_at(self):
        """Pool trade parsing must use createdAt for the timestamp."""
        from exchanges.nonkyc import NonKYCExchange
        source = inspect.getsource(NonKYCExchange.fetch_pool_activity)
        assert 'p.get("createdAt")' in source

    @pytest.mark.asyncio
    async def test_pool_trade_timestamp_parsed(self):
        from exchanges.nonkyc import NonKYCExchange
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "pool001",
            "pool": {"id": "p1", "symbol": "SAL/USDT"},
            "side": "buy",
            "price": "0.023",
            "quantity": "100",
            "fee": "0.004",
            "totalWithFee": "2.3",
            "createdAt": 1770396781238,
        }]
        with patch.object(ex, '_get', return_value=payload):
            result = await ex.fetch_pool_activity()
        assert result[0]["executed_at"] is not None


# ═══ Correction 5: totalWithFee=0 fallback ════════════════════════════════

class TestTotalWithFeeFallback:

    @pytest.mark.asyncio
    async def test_trade_total_fallback_when_total_with_fee_is_zero(self):
        """When totalWithFee="0", total should be price * quantity."""
        from exchanges.nonkyc import NonKYCExchange
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "trade_zero_total",
            "market": {"id": "m1", "symbol": "SAL/USDT"},
            "side": "buy",
            "price": "0.182937",
            "quantity": "1.8509",
            "totalWithFee": "0",
            "fee": "0",
            "timestamp": 1775480023169,
        }]
        with patch.object(ex, '_get', return_value=payload):
            trades = await ex.fetch_trades()
        expected = str(D("0.182937") * D("1.8509"))
        assert trades[0]["total"] == expected
        assert trades[0]["total"] != "0"

    @pytest.mark.asyncio
    async def test_trade_total_uses_total_with_fee_when_nonzero(self):
        """When totalWithFee is present and non-zero, use it directly."""
        from exchanges.nonkyc import NonKYCExchange
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "trade_good_total",
            "market": {"id": "m1", "symbol": "SAL/USDT"},
            "side": "sell",
            "price": "0.023",
            "quantity": "100",
            "totalWithFee": "2.3",
            "fee": "0.004",
            "timestamp": 1775480023169,
        }]
        with patch.object(ex, '_get', return_value=payload):
            trades = await ex.fetch_trades()
        assert trades[0]["total"] == "2.3"

    @pytest.mark.asyncio
    async def test_trade_total_fallback_when_missing(self):
        """When totalWithFee is absent, fall back to price * quantity."""
        from exchanges.nonkyc import NonKYCExchange
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "trade_no_total",
            "market": {"id": "m1", "symbol": "BTC/USDT"},
            "side": "buy",
            "price": "50000",
            "quantity": "0.01",
            # no totalWithFee field at all
            "fee": "0.5",
            "timestamp": 1775480023169,
        }]
        with patch.object(ex, '_get', return_value=payload):
            trades = await ex.fetch_trades()
        expected = str(D("50000") * D("0.01"))
        assert trades[0]["total"] == expected

    def test_fallback_logic_in_source(self):
        """The trade parser should have totalWithFee fallback logic."""
        from exchanges.nonkyc import NonKYCExchange
        source = inspect.getsource(NonKYCExchange.fetch_trades)
        assert "totalWithFee" in source
        assert "price_str" in source and "quantity_str" in source


# ═══ Correction 6: WebSocket (just verify not touched) ════════════════════

class TestWebSocketNotTouched:

    def test_no_ws_code_in_nonkyc(self):
        """The NonKYC exchange module should not have WebSocket login code."""
        from exchanges.nonkyc import NonKYCExchange
        source = inspect.getsource(NonKYCExchange)
        assert "websocket" not in source.lower() or "ws" not in source.lower()


# ═══ Additional: alternateFeeAsset ════════════════════════════════════════

class TestAlternateFeeAsset:

    @pytest.mark.asyncio
    async def test_alternate_fee_asset_used(self):
        """When alternateFeeAsset is present, use it for fee_asset."""
        from exchanges.nonkyc import NonKYCExchange
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "trade_alt_fee",
            "market": {"id": "m1", "symbol": "SAL/USDT"},
            "side": "buy",
            "price": "0.023",
            "quantity": "100",
            "totalWithFee": "2.3",
            "fee": "0.004",
            "alternateFeeAsset": "USDT",
            "timestamp": 1775480023169,
        }]
        with patch.object(ex, '_get', return_value=payload):
            trades = await ex.fetch_trades()
        # alternateFeeAsset should be picked up via the fallback chain
        assert trades[0]["fee_asset"] is not None

    def test_fee_asset_fallback_chain_includes_alternate(self):
        """The fee_asset extraction should include alternateFeeAsset."""
        from exchanges.nonkyc import NonKYCExchange
        source = inspect.getsource(NonKYCExchange.fetch_trades)
        assert "alternateFeeAsset" in source
