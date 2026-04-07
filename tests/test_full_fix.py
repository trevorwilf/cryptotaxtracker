"""
Tests for the RCA Full Fix prompt — Phases 1-9.
"""
import os
import inspect
import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch, AsyncMock, MagicMock

D = Decimal


# ═══ Phase 1: Run Scoping ═════════════════════════════════════════════════

class TestResolveRunId:

    def test_resolve_run_id_exists(self):
        import main as m
        assert hasattr(m, '_resolve_run_id')
        assert inspect.iscoroutinefunction(m._resolve_run_id)

    def test_resolve_run_id_returns_explicit_when_given(self):
        """When run_id is explicitly provided, return it as-is."""
        import asyncio, main as m
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(m._resolve_run_id(None, requested_run_id=42))
        loop.close()
        assert result == 42


class TestEndpointsHaveRunId:
    """All v4 read endpoints should accept run_id parameter."""

    def _check_param(self, func_name):
        import main as m
        func = getattr(m, func_name)
        sig = inspect.signature(func)
        assert "run_id" in sig.parameters, f"{func_name} missing run_id param"

    def test_v4_events_has_run_id(self):
        self._check_param("v4_events")

    def test_v4_lots_has_run_id(self):
        self._check_param("v4_lots")

    def test_v4_form_8949_has_run_id(self):
        self._check_param("v4_form_8949")

    def test_v4_schedule_d_has_run_id(self):
        self._check_param("v4_schedule_d")

    def test_v4_income_has_run_id(self):
        self._check_param("v4_income")

    def test_v4_transfers_has_run_id(self):
        self._check_param("v4_transfers")

    def test_v4_funding_by_exchange_has_run_id(self):
        self._check_param("v4_funding_by_exchange")

    def test_v4_pnl_by_exchange_has_run_id(self):
        self._check_param("v4_pnl_by_exchange")


class TestRunIdIndexes:
    def test_indexes_in_schema(self):
        from schema_v4 import SCHEMA_V4_SQL
        assert "idx_normalized_events_run_id" in SCHEMA_V4_SQL
        assert "idx_lots_v4_run_id" in SCHEMA_V4_SQL
        assert "idx_disposals_v4_run_id" in SCHEMA_V4_SQL
        assert "idx_transfer_carryover_run_id" in SCHEMA_V4_SQL
        assert "idx_classified_flows_run_id" in SCHEMA_V4_SQL
        assert "idx_income_events_v4_run_id" in SCHEMA_V4_SQL
        assert "idx_exceptions_run_id" in SCHEMA_V4_SQL


# ═══ Phase 2: Canonical Asset ═════════════════════════════════════════════

class TestAssetResolver:

    def test_resolve_sal_salvium1(self):
        from asset_resolver import resolve_canonical
        assert resolve_canonical("SAL-SALVIUM1", "mexc", "SALVIUM1") == "SAL"

    def test_resolve_plain_usdt(self):
        from asset_resolver import resolve_canonical
        assert resolve_canonical("USDT", "nonkyc") == "USDT"

    def test_resolve_plain_btc(self):
        from asset_resolver import resolve_canonical
        assert resolve_canonical("BTC", "mexc") == "BTC"

    def test_resolve_plain_sal(self):
        from asset_resolver import resolve_canonical
        assert resolve_canonical("SAL", "salvium") == "SAL"

    def test_resolve_mexc_hyphen_without_network(self):
        from asset_resolver import resolve_canonical
        assert resolve_canonical("SAL-SALVIUM1", "mexc") == "SAL"

    def test_resolve_preserves_non_hyphenated(self):
        from asset_resolver import resolve_canonical
        assert resolve_canonical("ARRR", "nonkyc") == "ARRR"

    def test_asset_aliases_table_in_schema(self):
        from schema_v4 import SCHEMA_V4_SQL
        assert "tax.asset_aliases" in SCHEMA_V4_SQL

    def test_canonical_asset_columns_in_migration(self):
        from database import MIGRATION_SQL
        assert "canonical_asset" in MIGRATION_SQL

    def test_transfer_matcher_uses_canonical(self):
        from transfer_matcher_v4 import TransferMatcherV4
        src = inspect.getsource(TransferMatcherV4._check_match)
        assert "canonical_asset" in src

    def test_matcher_queries_canonical(self):
        from transfer_matcher_v4 import TransferMatcherV4
        src = inspect.getsource(TransferMatcherV4.match_and_relocate)
        assert "canonical_asset" in src


# ═══ Phase 3: External Deposits → Acquisition Lots ════════════════════════

class TestExternalDepositReclassification:

    def test_compute_pipeline_has_reclassification_step(self):
        import main as m
        src = inspect.getsource(m._compute_v4_full)
        assert "ACQUISITION" in src
        assert "reclassified from UNRESOLVED" in src or "deposits_reclassified" in src

    def test_pipeline_creates_lots_after_reclassification(self):
        import main as m
        src = inspect.getsource(m._compute_v4_full)
        assert "deposit_acquisition_lots" in src


# ═══ Phase 4: Flow Classifier ═════════════════════════════════════════════

class TestFlowClassifierFixed:

    def test_classifier_queries_acquisition_deposits(self):
        from flow_classifier import FlowClassifier
        src = inspect.getsource(FlowClassifier.classify_all)
        assert "ACQUISITION" in src
        assert "acquisition_deposit_ids" in src

    def test_income_query_is_run_scoped(self):
        from flow_classifier import FlowClassifier
        src = inspect.getsource(FlowClassifier.classify_all)
        # The income query should use run filter
        assert "income_run_filter" in src or "run_id = :rid" in src

    @pytest.mark.asyncio
    async def test_external_deposit_classified(self):
        from flow_classifier import FlowClassifier
        from test_flow_classifier import FakeClassifierSession
        session = FakeClassifierSession()
        session.deposits = [(1, "mexc", "USDT", "100", "100", "1.0",
                             datetime(2026, 1, 1, tzinfo=timezone.utc))]
        c = FlowClassifier()
        result = await c.classify_all(session)
        assert result["by_class"]["EXTERNAL_DEPOSIT"] == 1

    @pytest.mark.asyncio
    async def test_transfer_in_classified(self):
        from flow_classifier import FlowClassifier
        from test_flow_classifier import FakeClassifierSession
        session = FakeClassifierSession()
        session.transfer_in_deposit_ids = {1}
        session.deposits = [(1, "nonkyc", "BTC", "0.5", "25000", "50000",
                             datetime(2026, 1, 1, tzinfo=timezone.utc))]
        c = FlowClassifier()
        result = await c.classify_all(session)
        assert result["by_class"]["INTERNAL_TRANSFER_IN"] == 1


# ═══ Phase 5: Sync Frontend ═══════════════════════════════════════════════

class TestSyncFrontend:

    def test_sync_status_reads_exchanges_key(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "d.exchanges||d" in content
        assert "last_recompute" in content


# ═══ Phase 9: Single-Exchange Sync Recompute ══════════════════════════════

class TestSingleExchangeRecompute:

    def test_run_sync_has_recompute_param(self):
        import main as m
        sig = inspect.signature(m.run_sync)
        assert "recompute" in sig.parameters

    def test_run_sync_all_disables_per_exchange_recompute(self):
        import main as m
        src = inspect.getsource(m.run_sync_all)
        assert "recompute=False" in src
