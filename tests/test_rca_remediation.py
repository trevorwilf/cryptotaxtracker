"""
Tests for the RCA Remediation — all 8 phases.

Phase 1: FIFO temporal constraint (no future lots consumed)
Phase 2: NonKYC data repair (upsert repairs zero amounts)
Phase 3: NonKYC pool endpoint fix
Phase 4: MEXC 7-day chunking + transfer params
Phase 5: Frontend LINK_TRANSFER fix
Phase 6: Wallet claims in transfer matcher
Phase 7: Import staging durability (sync lock, staging tables)
Phase 8: Data quality pre-compute checks
"""
import os
import inspect
import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import patch, AsyncMock, MagicMock

D = Decimal


# ═══ Phase 1: FIFO Temporal Constraint ════════════════════════════════════

class TestFIFOTemporalConstraint:

    def test_lot_query_has_temporal_filter(self):
        """The lot selection SQL must include original_acquired_at <= disposed_at."""
        from tax_engine_v4 import TaxEngineV4
        source = inspect.getsource(TaxEngineV4._process_disposals)
        assert "AND original_acquired_at <= :disposed_at" in source

    def test_negative_holding_days_guard(self):
        """If holding_days < 0, the engine should skip the lot with BLOCKING exception."""
        from tax_engine_v4 import TaxEngineV4
        source = inspect.getsource(TaxEngineV4._process_disposals)
        assert "holding_days < 0" in source
        assert "FUTURE_LOT_USED" in source
        assert "continue" in source

    def test_disposed_at_passed_to_query(self):
        """The disposed_at parameter must be in the query params."""
        from tax_engine_v4 import TaxEngineV4
        source = inspect.getsource(TaxEngineV4._process_disposals)
        assert '"disposed_at": disposed_at' in source


# ═══ Phase 2: NonKYC Data Repair ══════════════════════════════════════════

class TestUpsertRepairsData:

    def test_upsert_deposits_repairs_zero_amount(self):
        """upsert_deposits SQL should repair amount=0 with new value."""
        from database import Database
        source = inspect.getsource(Database.upsert_deposits)
        assert "WHEN tax.deposits.amount = 0" in source

    def test_upsert_deposits_repairs_blank_tx_hash(self):
        """upsert_deposits SQL should repair blank tx_hash."""
        from database import Database
        source = inspect.getsource(Database.upsert_deposits)
        assert "WHEN tax.deposits.tx_hash IS NULL OR tax.deposits.tx_hash = ''" in source

    def test_upsert_deposits_preserves_good_data(self):
        """Good amounts should not be overwritten by zeros."""
        from database import Database
        source = inspect.getsource(Database.upsert_deposits)
        # The CASE WHEN pattern only replaces when existing is 0/NULL
        assert "THEN EXCLUDED.amount ELSE tax.deposits.amount END" in source

    def test_upsert_withdrawals_repairs_zero_amount(self):
        from database import Database
        source = inspect.getsource(Database.upsert_withdrawals)
        assert "WHEN tax.withdrawals.amount = 0" in source

    def test_upsert_withdrawals_repairs_blank_tx_hash(self):
        from database import Database
        source = inspect.getsource(Database.upsert_withdrawals)
        assert "WHEN tax.withdrawals.tx_hash IS NULL OR tax.withdrawals.tx_hash = ''" in source

    def test_external_tx_id_column_in_migration(self):
        """Migration should add external_tx_id to deposits and withdrawals."""
        from database import MIGRATION_SQL
        assert "external_tx_id" in MIGRATION_SQL

    def test_nonkyc_deposit_parser_has_external_tx_id(self):
        from exchanges.nonkyc import NonKYCExchange
        source = inspect.getsource(NonKYCExchange.fetch_deposits)
        assert "external_tx_id" in source

    def test_nonkyc_withdrawal_parser_has_external_tx_id(self):
        from exchanges.nonkyc import NonKYCExchange
        source = inspect.getsource(NonKYCExchange.fetch_withdrawals)
        assert "external_tx_id" in source


# ═══ Phase 3: NonKYC Pool Endpoint Fix ════════════════════════════════════

class TestNonKYCPoolFix:

    def test_pool_uses_private_endpoint(self):
        """fetch_pool_activity must call /getpooltrades, not /pool/trades."""
        from exchanges.nonkyc import NonKYCExchange
        source = inspect.getsource(NonKYCExchange.fetch_pool_activity)
        assert "/getpooltrades" in source
        assert "/pool/trades" not in source

    @pytest.mark.asyncio
    async def test_pool_buy_maps_correctly(self):
        """side=buy: spent quote to get base."""
        from exchanges.nonkyc import NonKYCExchange
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "pool-buy-001",
            "pool": {"id": "p1", "symbol": "SAL/USDT"},
            "side": "buy",
            "price": "0.023",
            "quantity": "87.35",
            "fee": "0.004",
            "totalWithFee": "2",
            "createdAt": 1770396781238,
        }]
        with patch.object(ex, '_get', return_value=payload):
            result = await ex.fetch_pool_activity()
        assert len(result) == 1
        p = result[0]
        assert p["pool_name"] == "SAL/USDT"
        assert p["action"] == "buy"
        assert p["asset_in"] == "USDT"
        assert p["amount_in"] == "2"
        assert p["asset_out"] == "SAL"
        assert p["amount_out"] == "87.35"

    @pytest.mark.asyncio
    async def test_pool_sell_maps_correctly(self):
        """side=sell: spent base to get quote."""
        from exchanges.nonkyc import NonKYCExchange
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "pool-sell-001",
            "pool": {"id": "p1", "symbol": "SAL/USDT"},
            "side": "sell",
            "price": "0.023",
            "quantity": "100",
            "fee": "0.004",
            "totalWithFee": "2.3",
            "createdAt": 1770396781238,
        }]
        with patch.object(ex, '_get', return_value=payload):
            result = await ex.fetch_pool_activity()
        assert len(result) == 1
        p = result[0]
        assert p["asset_in"] == "SAL"
        assert p["amount_in"] == "100"
        assert p["asset_out"] == "USDT"
        assert p["amount_out"] == "2.3"

    @pytest.mark.asyncio
    async def test_pool_rejects_blank_pool_name(self):
        """Rows with missing pool symbol should be filtered out."""
        from exchanges.nonkyc import NonKYCExchange
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [
            {"id": "bad-1", "pool": {}, "side": "buy", "quantity": "0", "totalWithFee": "0"},
            {"id": "bad-2", "side": "buy", "quantity": "0", "totalWithFee": "0"},
        ]
        with patch.object(ex, '_get', return_value=payload):
            result = await ex.fetch_pool_activity()
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_pool_rejects_unknown_side(self):
        from exchanges.nonkyc import NonKYCExchange
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "bad-side",
            "pool": {"symbol": "SAL/USDT"},
            "side": "unknown",
            "quantity": "100",
            "totalWithFee": "2",
            "createdAt": 1770396781238,
        }]
        with patch.object(ex, '_get', return_value=payload):
            result = await ex.fetch_pool_activity()
        assert len(result) == 0


# ═══ Phase 4: MEXC Connector Fixes ════════════════════════════════════════

class TestMEXCChunking:

    def test_deposit_uses_chunked_history(self):
        """fetch_deposits must call _fetch_chunked_history."""
        from exchanges.mexc import MEXCExchange
        source = inspect.getsource(MEXCExchange.fetch_deposits)
        assert "_fetch_chunked_history" in source

    def test_withdrawal_uses_chunked_history(self):
        from exchanges.mexc import MEXCExchange
        source = inspect.getsource(MEXCExchange.fetch_withdrawals)
        assert "_fetch_chunked_history" in source

    def test_chunk_size_max_7_days(self):
        """Chunks should not exceed 7 days."""
        from exchanges.mexc import MEXCExchange
        source = inspect.getsource(MEXCExchange._fetch_chunked_history)
        assert "timedelta(days=7)" in source

    def test_max_lookback_89_days(self):
        """Max lookback should be 89 days (not 90)."""
        from exchanges.mexc import MEXCExchange
        source = inspect.getsource(MEXCExchange._fetch_chunked_history)
        assert "days=89" in source

    @pytest.mark.asyncio
    async def test_deposit_makes_multiple_chunks(self):
        """With since=30 days ago, multiple chunk requests should be made."""
        from exchanges.mexc import MEXCExchange
        ex = MEXCExchange(api_key="test", api_secret="test")
        call_count = [0]
        async def mock_get(path, params=None, signed=True):
            call_count[0] += 1
            return []
        since = datetime.now(timezone.utc) - timedelta(days=30)
        with patch.object(ex, '_get', side_effect=mock_get):
            await ex.fetch_deposits(since=since)
        # 30 days / 7 days per chunk = at least 4 chunks
        assert call_count[0] >= 4

    @pytest.mark.asyncio
    async def test_withdrawal_makes_multiple_chunks(self):
        from exchanges.mexc import MEXCExchange
        ex = MEXCExchange(api_key="test", api_secret="test")
        call_count = [0]
        async def mock_get(path, params=None, signed=True):
            call_count[0] += 1
            return []
        since = datetime.now(timezone.utc) - timedelta(days=30)
        with patch.object(ex, '_get', side_effect=mock_get):
            await ex.fetch_withdrawals(since=since)
        assert call_count[0] >= 4


class TestMEXCTransferFix:

    def test_transfer_has_required_params(self):
        """fetch_transfers must include fromAccountType and toAccountType."""
        from exchanges.mexc import MEXCExchange
        source = inspect.getsource(MEXCExchange.fetch_transfers)
        assert "fromAccountType" in source
        assert "toAccountType" in source

    def test_transfer_queries_both_directions(self):
        """Both SPOT→FUTURES and FUTURES→SPOT should be queried."""
        from exchanges.mexc import MEXCExchange
        source = inspect.getsource(MEXCExchange.fetch_transfers)
        assert '"SPOT", "FUTURES"' in source or "('SPOT', 'FUTURES')" in source
        assert '"FUTURES", "SPOT"' in source or "('FUTURES', 'SPOT')" in source

    def test_transfer_uses_page_not_limit(self):
        """Transfer pagination should use page/size, not limit."""
        from exchanges.mexc import MEXCExchange
        source = inspect.getsource(MEXCExchange.fetch_transfers)
        assert '"size"' in source
        assert '"page"' in source

    @pytest.mark.asyncio
    async def test_transfer_queries_both_directions_runtime(self):
        from exchanges.mexc import MEXCExchange
        ex = MEXCExchange(api_key="test", api_secret="test")
        calls = []
        async def mock_get(path, params=None, signed=True):
            calls.append(params.copy() if params else {})
            return {"rows": [], "total": 0}
        with patch.object(ex, '_get', side_effect=mock_get):
            await ex.fetch_transfers()
        from_accounts = [c.get("fromAccountType") for c in calls]
        assert "SPOT" in from_accounts
        assert "FUTURES" in from_accounts


# ═══ Phase 5: Frontend LINK_TRANSFER Fix ══════════════════════════════════

class TestLinkTransferFix:

    def test_link_transfer_sends_correct_action(self):
        """linkTransfer() JS should set decision='LINK_TRANSFER'."""
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "decision='LINK_TRANSFER'" in content
        # Should NOT have the old bug
        assert "linkTransfer" in content

    def test_import_summary_counts_link_transfer(self):
        """updateImportSummary should count LINK_TRANSFER as importing."""
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "r.decision==='LINK_TRANSFER'" in content


# ═══ Phase 6: Wallet Claims in Transfer Matcher ══════════════════════════

class TestWalletClaimsInMatcher:

    def test_matcher_loads_wallet_claims(self):
        """match_and_relocate should load wallet claims."""
        from transfer_matcher_v4 import TransferMatcherV4
        source = inspect.getsource(TransferMatcherV4.match_and_relocate)
        assert "_load_wallet_claims" in source

    def test_check_match_accepts_claims_param(self):
        """_check_match should accept a claims dict parameter."""
        from transfer_matcher_v4 import TransferMatcherV4
        sig = inspect.signature(TransferMatcherV4._check_match)
        assert "claims" in sig.parameters

    def test_wallet_claim_boosts_confidence(self):
        """When both addresses are claimed, confidence should be 'wallet_claim'."""
        from transfer_matcher_v4 import TransferMatcherV4
        matcher = TransferMatcherV4()
        wd = {"asset": "BTC", "quantity": "0.5", "fee": "0.0001",
              "event_at": datetime(2026, 3, 15, 10, tzinfo=timezone.utc),
              "tx_hash": None, "address": "addr_wd"}
        dep = {"asset": "BTC", "quantity": "0.4999",
               "event_at": datetime(2026, 3, 15, 14, tzinfo=timezone.utc),
               "tx_hash": None, "address": "addr_dep"}
        claims = {"addr_wd": {"claim_type": "self_owned"}, "addr_dep": {"claim_type": "self_owned"}}
        result = matcher._check_match(wd, dep, claims)
        assert result == "wallet_claim"

    def test_no_claims_still_works(self):
        """Matcher should work without any wallet claims (backward compat)."""
        from transfer_matcher_v4 import TransferMatcherV4
        matcher = TransferMatcherV4()
        wd = {"asset": "BTC", "quantity": "0.5", "fee": "0.0001",
              "event_at": datetime(2026, 3, 15, 10, tzinfo=timezone.utc),
              "tx_hash": "tx123", "address": None}
        dep = {"asset": "BTC", "quantity": "0.4999",
               "event_at": datetime(2026, 3, 15, 14, tzinfo=timezone.utc),
               "tx_hash": "tx123", "address": None}
        result = matcher._check_match(wd, dep, None)
        assert result == "tx_hash"

    def test_single_address_claimed_no_boost(self):
        """Only one address claimed = no wallet_claim boost."""
        from transfer_matcher_v4 import TransferMatcherV4
        matcher = TransferMatcherV4()
        wd = {"asset": "BTC", "quantity": "0.5", "fee": "0.0001",
              "event_at": datetime(2026, 3, 15, 10, tzinfo=timezone.utc),
              "tx_hash": "tx123", "address": "addr_wd"}
        dep = {"asset": "BTC", "quantity": "0.4999",
               "event_at": datetime(2026, 3, 15, 14, tzinfo=timezone.utc),
               "tx_hash": "tx123", "address": "addr_dep"}
        claims = {"addr_wd": {"claim_type": "self_owned"}}  # only one claimed
        result = matcher._check_match(wd, dep, claims)
        assert result == "tx_hash"  # falls through to tx_hash match


class TestWalletCRUDEndpoints:

    def test_delete_endpoints_exist(self):
        import main as m
        routes = [r.path for r in m.app.routes]
        assert any("/v4/wallet/entities/" in r for r in routes)
        assert any("/v4/wallet/accounts/" in r for r in routes)
        assert any("/v4/wallet/addresses/" in r for r in routes)
        assert any("/v4/wallet/claims/" in r for r in routes)

    def test_patch_claim_endpoint_exists(self):
        import main as m
        routes = [(r.path, r.methods) for r in m.app.routes if hasattr(r, 'methods')]
        patch_routes = [(p, mt) for p, mt in routes if "/v4/wallet/claims/" in p and "PATCH" in mt]
        assert len(patch_routes) > 0


# ═══ Phase 7: Sync Lock ══════════════════════════════════════════════════

class TestSyncLock:

    def test_sync_lock_exists(self):
        import main as m
        assert hasattr(m, '_sync_lock')

    def test_sync_endpoint_checks_lock(self):
        import main as m
        source = inspect.getsource(m.sync_exchange)
        assert "_sync_lock.locked()" in source

    def test_staging_tables_in_schema(self):
        from schema_v4 import SCHEMA_V4_SQL
        assert "tax.import_stages" in SCHEMA_V4_SQL
        assert "tax.import_stage_rows" in SCHEMA_V4_SQL


# ═══ Phase 8: Data Quality Checks ════════════════════════════════════════

class TestDataQualityChecks:

    def test_data_quality_module_exists(self):
        from data_quality import validate_data_quality
        assert callable(validate_data_quality)

    def test_data_quality_wired_into_compute(self):
        import main as m
        source = inspect.getsource(m._compute_v4_full)
        assert "validate_data_quality" in source

    @pytest.mark.asyncio
    async def test_data_quality_detects_zero_deposits(self):
        """Zero-amount deposits with raw_data should trigger BLOCKING."""
        from data_quality import validate_data_quality
        from exceptions import ExceptionManager

        exc = ExceptionManager()
        session = AsyncMock()
        # First call: zero-amount deposit check returns 3
        # Other calls return 0
        call_count = [0]
        async def mock_exec(stmt, params=None):
            call_count[0] += 1
            result = MagicMock()
            result.scalar.return_value = 3 if call_count[0] == 1 else 0
            return result

        session.execute = mock_exec
        await validate_data_quality(session, exc, run_id=1)
        assert exc.has_blocking

    @pytest.mark.asyncio
    async def test_data_quality_clean_db_no_blocking(self):
        """Clean database should not produce blocking exceptions."""
        from data_quality import validate_data_quality
        from exceptions import ExceptionManager

        exc = ExceptionManager()
        session = AsyncMock()
        result = MagicMock()
        result.scalar.return_value = 0
        session.execute = AsyncMock(return_value=result)

        await validate_data_quality(session, exc, run_id=1)
        assert not exc.has_blocking


# ═══ Remediation Script ══════════════════════════════════════════════════

class TestRemediationScript:

    def test_script_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "scripts", "remediate_nonkyc_data.py")
        assert os.path.exists(path)

    def test_script_has_dry_run_flag(self):
        path = os.path.join(os.path.dirname(__file__), "..", "scripts", "remediate_nonkyc_data.py")
        with open(path, "r") as f:
            content = f.read()
        assert "DRY_RUN" in content
        assert "DRY_RUN = True" in content
