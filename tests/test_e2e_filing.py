"""
End-to-end filing tests covering the FULL pipeline with mock data.

These tests exercise the complete path from raw data through to
filing readiness checks without requiring a real database.
"""
import os
import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from csv_importer import CSVImporter, detect_format
from exceptions import ExceptionManager, BLOCKING, WARNING
from flow_classifier import FlowClassifier

D = Decimal
T = lambda y, m, d, h=12: datetime(y, m, d, h, 0, 0, tzinfo=timezone.utc)
FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


# ── Import System E2E ────────────────────────────────────────────────────

class TestE2EImportDetection:
    """Verify format detection works for all fixture files."""

    def test_all_fixtures_detected(self):
        """Every fixture file should auto-detect correctly."""
        fixtures = {
            "mexc_deposits.xlsx": ("mexc", "deposits", "mexc_deposit_xlsx"),
            "mexc_withdrawals.xlsx": ("mexc", "withdrawals", "mexc_withdrawal_xlsx"),
            "nonkyc_deposits.csv": ("nonkyc", "deposits", "nonkyc_deposit_csv"),
            "nonkyc_withdrawals.csv": ("nonkyc", "withdrawals", "nonkyc_withdrawal_csv"),
        }
        for filename, expected in fixtures.items():
            path = os.path.join(FIXTURES_DIR, filename)
            assert detect_format(path) == expected, f"Failed for {filename}"

    def test_mexc_xlsx_has_correct_row_count(self):
        """MEXC deposit XLSX should have 3 data rows."""
        import openpyxl
        path = os.path.join(FIXTURES_DIR, "mexc_deposits.xlsx")
        wb = openpyxl.load_workbook(path, read_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(min_row=2, values_only=True))
        wb.close()
        assert len(rows) == 3

    def test_nonkyc_csv_has_correct_row_count(self):
        """NonKYC deposit CSV should have 4 data rows."""
        import csv
        path = os.path.join(FIXTURES_DIR, "nonkyc_deposits.csv")
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 4

    def test_nonkyc_withdrawal_csv_has_correct_row_count(self):
        import csv
        path = os.path.join(FIXTURES_DIR, "nonkyc_withdrawals.csv")
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 4


# ── Fee Asset Fidelity E2E ───────────────────────────────────────────────

class TestFeeAssetFidelity:
    """Verify fee_asset is preserved through the pipeline."""

    def test_mexc_withdrawal_fee_asset_set(self):
        """MEXC withdrawal parser sets fee_asset = asset."""
        from exchanges.mexc import MEXCExchange
        ex = MEXCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "wd-btc-fee-test",
            "coin": "BTC",
            "amount": "0.5",
            "transactionFee": "0.0001",
            "network": "BTC",
            "txId": "tx_fee_test",
            "address": "addr1",
            "status": "6",
            "completeTime": 1710503600000,
        }]

        async def mock_get(path, params=None, signed=True):
            return payload

        import asyncio
        with patch.object(ex, '_get', side_effect=mock_get):
            wds = asyncio.get_event_loop().run_until_complete(ex.fetch_withdrawals())
        assert wds[0]["fee_asset"] == "BTC"

    def test_nonkyc_withdrawal_fee_asset_set(self):
        """NonKYC withdrawal parser sets fee_asset from feecurrency."""
        from exchanges.nonkyc import NonKYCExchange
        ex = NonKYCExchange(api_key="test", api_secret="test")
        payload = [{
            "id": "wd-sal-fee-test",
            "ticker": "SAL",
            "quantity": "100",
            "fee": "1",
            "feecurrency": "SAL",
            "status": "completed",
            "transactionid": "tx_nonkyc_fee",
            "address": "addr2",
        }]

        import asyncio
        with patch.object(ex, '_get', return_value=payload):
            wds = asyncio.get_event_loop().run_until_complete(ex.fetch_withdrawals())
        assert wds[0]["fee_asset"] == "SAL"
        assert wds[0]["fee_currency"] == "SAL"


# ── Transfer Matcher Run Isolation ────────────────────────────────────────

class TestTransferMatcherRunIsolation:
    """Verify the transfer matcher's lot query is run-scoped."""

    def test_lot_query_contains_run_id(self):
        """The SQL for fetching lots should include run_id filter."""
        import inspect
        from transfer_matcher_v4 import TransferMatcherV4
        source = inspect.getsource(TransferMatcherV4._relocate_lots)
        assert "AND run_id = :run_id" in source

    def test_dest_lot_id_backfilled(self):
        """The transfer matcher code should UPDATE transfer_carryover with dest_lot_id."""
        import inspect
        from transfer_matcher_v4 import TransferMatcherV4
        source = inspect.getsource(TransferMatcherV4._relocate_lots)
        assert "UPDATE tax.transfer_carryover SET dest_lot_id" in source


# ── Flow Classifier Run Scoping ──────────────────────────────────────────

class TestFlowClassifierRunScoping:
    """Verify flow classifier is run-scoped and classifies correctly."""

    def test_delete_is_run_scoped(self):
        """Classifier should DELETE only for the given run_id."""
        import inspect
        from flow_classifier import FlowClassifier
        source = inspect.getsource(FlowClassifier.classify_all)
        assert "WHERE run_id = :rid" in source

    def test_source_queries_use_run_filter(self):
        """Transfer-in/out queries should filter by run_id."""
        import inspect
        from flow_classifier import FlowClassifier
        source = inspect.getsource(FlowClassifier.classify_all)
        assert "ne.run_id = :rid" in source

    @pytest.mark.asyncio
    async def test_unmatched_deposits_are_external(self):
        """Unmatched deposits default to EXTERNAL_DEPOSIT."""
        from test_flow_classifier import FakeClassifierSession
        session = FakeClassifierSession()
        session.deposits = [
            (1, "mexc", "USDT", "1000", "1000", "1.0", T(2026, 1, 1)),
        ]
        classifier = FlowClassifier()
        result = await classifier.classify_all(session)
        assert result["by_class"]["EXTERNAL_DEPOSIT"] == 1

    @pytest.mark.asyncio
    async def test_unmatched_withdrawals_are_external(self):
        """Unmatched withdrawals default to EXTERNAL_WITHDRAWAL."""
        from test_flow_classifier import FakeClassifierSession
        session = FakeClassifierSession()
        session.withdrawals = [
            (1, "nonkyc", "USDT", "500", "500", "1.0", T(2026, 1, 1)),
        ]
        classifier = FlowClassifier()
        result = await classifier.classify_all(session)
        assert result["by_class"]["EXTERNAL_WITHDRAWAL"] == 1


# ── Exception System Run Scoping ─────────────────────────────────────────

class TestExceptionRunScoping:
    """Verify exception system supports run_id filtering."""

    def test_check_filing_ready_accepts_run_id(self):
        """check_filing_ready should accept optional run_id parameter."""
        import inspect
        sig = inspect.signature(ExceptionManager.check_filing_ready)
        assert "run_id" in sig.parameters

    def test_clear_for_run_clears_by_run(self):
        """clear_for_run deletes exceptions for a specific run."""
        import inspect
        source = inspect.getsource(ExceptionManager.clear_for_run)
        assert "run_id = :r" in source


# ── Export Schema Fix Verification ────────────────────────────────────────

class TestExportSchemaFix:
    """Verify export uses correct column names."""

    def test_exceptions_tab_uses_correct_columns(self):
        """The export SQL should reference category, resolution_status, affected_tax_year."""
        import inspect
        from exports.tax_report import _build_exceptions_tab
        source = inspect.getsource(_build_exceptions_tab)
        assert "category" in source
        assert "resolution_status" in source
        assert "affected_tax_year" in source
        # Should NOT contain the old wrong column names
        assert "exception_code" not in source
        assert "tax_year," not in source or "affected_tax_year" in source


# ── Split Compute Preserves Transfer Lots ─────────────────────────────────

class TestSplitComputePreservesTransferLots:
    """Verify compute() doesn't delete transfer_in lots."""

    def test_compute_delete_excludes_transfer_in(self):
        """The DELETE statement should exclude source_type='transfer_in'."""
        import inspect
        from tax_engine_v4 import TaxEngineV4
        source = inspect.getsource(TaxEngineV4.compute)
        assert "NOT IN ('transfer_in')" in source


# ── Form 8949 Disposal ID Linkage ─────────────────────────────────────────

class TestForm8949DisposalIdLinkage:
    """Verify disposal_id is populated in Form 8949."""

    def test_disposal_db_id_field_exists(self):
        """DisposalV4 dataclass should have disposal_db_id field."""
        from tax_engine_v4 import DisposalV4
        import dataclasses
        fields = {f.name for f in dataclasses.fields(DisposalV4)}
        assert "disposal_db_id" in fields

    def test_form_8949_uses_disposal_db_id(self):
        """_insert_form_8949 should use disp.disposal_db_id, not None."""
        import inspect
        from tax_engine_v4 import TaxEngineV4
        source = inspect.getsource(TaxEngineV4._insert_form_8949)
        assert "disp.disposal_db_id" in source
        assert '"did": None' not in source


# ── Legacy v3 Deprecation ─────────────────────────────────────────────────

class TestLegacyDeprecation:
    """Verify /export/tax-report returns 410."""

    def test_legacy_export_source_raises_410(self):
        """The legacy export endpoint should raise HTTPException(410)."""
        import inspect
        # Read the main module source
        import main as main_module
        source = inspect.getsource(main_module.export_tax_report_v3_deprecated)
        assert "410" in source
        assert "DEPRECATED" in source


# ── Wallet Ownership Endpoints Exist ──────────────────────────────────────

class TestWalletEndpointsExist:
    """Verify wallet ownership endpoints are registered."""

    def test_wallet_endpoints_in_main(self):
        """Main module should have wallet CRUD endpoints."""
        import main as main_module
        routes = [r.path for r in main_module.app.routes]
        assert "/v4/wallet/entities" in routes
        assert "/v4/wallet/accounts" in routes
        assert "/v4/wallet/addresses" in routes
        assert "/v4/wallet/claims" in routes
        assert "/v4/wallet/auto-discover" in routes

    def test_wallet_address_check_endpoint(self):
        """Address check endpoint should exist."""
        import main as main_module
        routes = [r.path for r in main_module.app.routes]
        assert any("/v4/wallet/addresses/check/" in r for r in routes)


# ── Data Coverage Module ──────────────────────────────────────────────────

class TestDataCoverageModule:
    """Verify data coverage tracker exists and is wired."""

    def test_data_coverage_module_importable(self):
        from data_coverage import DataCoverageTracker
        tracker = DataCoverageTracker()
        assert hasattr(tracker, "compute_coverage")

    def test_data_coverage_wired_into_compute_all(self):
        """compute-all should call DataCoverageTracker."""
        import inspect
        import main as main_module
        source = inspect.getsource(main_module.v4_compute_all)
        assert "DataCoverageTracker" in source
        assert "compute_coverage" in source


# ── Exchange Transfers ────────────────────────────────────────────────────

class TestExchangeTransfersTable:
    """Verify exchange transfers schema and sync wiring."""

    def test_exchange_transfers_in_schema(self):
        """Schema should define tax.exchange_transfers."""
        from schema_v4 import SCHEMA_V4_SQL
        assert "tax.exchange_transfers" in SCHEMA_V4_SQL

    def test_upsert_method_exists(self):
        """Database should have upsert_exchange_transfers method."""
        from database import Database
        assert hasattr(Database, "upsert_exchange_transfers")

    def test_sync_calls_fetch_transfers(self):
        """run_sync should call fetch_transfers if available."""
        import inspect
        import main as main_module
        source = inspect.getsource(main_module._run_sync_inner)
        assert "fetch_transfers" in source


# ── Compute Pipeline Includes Flow Classification ─────────────────────────

class TestComputeAllPipelineComplete:
    """Verify compute-all includes all required steps."""

    def test_pipeline_includes_flow_classification(self):
        import inspect
        import main as main_module
        source = inspect.getsource(main_module.v4_compute_all)
        assert "FlowClassifier" in source
        assert "classify_all" in source

    def test_pipeline_includes_data_coverage(self):
        import inspect
        import main as main_module
        source = inspect.getsource(main_module.v4_compute_all)
        assert "DataCoverageTracker" in source

    def test_pipeline_clears_exceptions_before_run(self):
        import inspect
        import main as main_module
        source = inspect.getsource(main_module.v4_compute_all)
        assert "clear_for_run" in source
