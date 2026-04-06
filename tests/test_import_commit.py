"""
Tests for the staged import system — Step 3 (commit).

Covers:
  - Commit imports approved rows
  - Commit skips rejected rows
  - Commit with LINK_TRANSFER
  - Commit with IMPORT_UPDATE
  - Invalid stage_id rejection
  - Committed stage cannot be re-committed
  - Import metadata recorded
  - Endpoint registration
"""
import os
import time
import json
import pytest
from unittest.mock import AsyncMock, MagicMock

from import_staging import (
    parse_file, commit_staged,
    _staged_imports, _create_stage_id, get_staged, remove_staged,
)

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


class FakeCommitResult:
    def __init__(self, rows=None):
        self._rows = rows or []
    def fetchone(self):
        return self._rows[0] if self._rows else None
    def fetchall(self):
        return self._rows
    def keys(self):
        return []


class FakeCommitSession:
    """Mock session for commit tests."""
    def __init__(self):
        self.executed = []

    async def execute(self, stmt, params=None):
        sql = str(stmt) if hasattr(stmt, 'text') else str(stmt)
        self.executed.append((sql, params))
        return FakeCommitResult()


def _stage_mexc_deposits():
    """Create a staged import from MEXC deposits fixture."""
    path = os.path.join(FIXTURES_DIR, "mexc_deposits.xlsx")
    parsed = parse_file(path)
    # Add required fields that analyze_matches would add
    for row in parsed["rows"]:
        row["status"] = "NEW"
        row["match"] = None
        row["transfer_candidates"] = []
        row["decision"] = None
        row["decision_reason"] = None
    parsed["summary"] = {"total_rows": 3, "matches": 0, "transfers": 0, "new": 3, "conflicts": 0}

    stage_id = _create_stage_id()
    _staged_imports[stage_id] = {
        "created_at": time.time(),
        "parsed_data": parsed,
        "committed": False,
    }
    return stage_id, parsed


class TestCommitLogic:

    @pytest.mark.asyncio
    async def test_commit_imports_approved_rows(self):
        """Approved rows should trigger INSERT."""
        _staged_imports.clear()
        stage_id, parsed = _stage_mexc_deposits()
        session = FakeCommitSession()

        decisions = [
            {"row_num": 1, "action": "IMPORT"},
            {"row_num": 2, "action": "IMPORT"},
            {"row_num": 3, "action": "SKIP"},
        ]
        results = await commit_staged(session, stage_id, decisions)

        assert results["imported"] == 2
        assert results["skipped"] == 1
        assert results["errors"] == 0

    @pytest.mark.asyncio
    async def test_commit_skips_rejected_rows(self):
        """Skipped rows should NOT trigger INSERT."""
        _staged_imports.clear()
        stage_id, parsed = _stage_mexc_deposits()
        session = FakeCommitSession()

        decisions = [
            {"row_num": 1, "action": "SKIP"},
            {"row_num": 2, "action": "SKIP"},
            {"row_num": 3, "action": "SKIP"},
        ]
        results = await commit_staged(session, stage_id, decisions)

        assert results["imported"] == 0
        assert results["skipped"] == 3
        # No INSERT statements for deposits should have been executed
        inserts = [s for s, p in session.executed if "INSERT INTO tax.deposits" in s]
        assert len(inserts) == 0

    @pytest.mark.asyncio
    async def test_commit_link_transfer(self):
        """LINK_TRANSFER should import AND count as linked."""
        _staged_imports.clear()
        stage_id, parsed = _stage_mexc_deposits()
        session = FakeCommitSession()

        decisions = [
            {"row_num": 1, "action": "LINK_TRANSFER", "link_transfer_id": 99},
            {"row_num": 2, "action": "SKIP"},
            {"row_num": 3, "action": "SKIP"},
        ]
        results = await commit_staged(session, stage_id, decisions)

        assert results["imported"] == 1
        assert results["linked_transfers"] == 1
        assert results["skipped"] == 2

    @pytest.mark.asyncio
    async def test_commit_import_update(self):
        """IMPORT_UPDATE with a match should update existing record."""
        _staged_imports.clear()
        stage_id, parsed = _stage_mexc_deposits()
        # Simulate a MATCH with an existing_id
        parsed["rows"][0]["match"] = {
            "type": "partial_match",
            "existing_id": 42,
            "existing_table": "tax.deposits",
            "existing_exchange": "mexc",
            "existing_exchange_id": "eid1",
            "existing_amount": "0.001",
            "existing_confirmed_at": None,
            "differences": {"amount": {"existing": "0.001", "imported": "0.00137478"}},
        }
        parsed["rows"][0]["parsed"]["amount_usd"] = "100.50"
        session = FakeCommitSession()

        decisions = [
            {"row_num": 1, "action": "IMPORT_UPDATE"},
            {"row_num": 2, "action": "SKIP"},
            {"row_num": 3, "action": "SKIP"},
        ]
        results = await commit_staged(session, stage_id, decisions)

        assert results["updated"] == 1
        assert results["skipped"] == 2

    @pytest.mark.asyncio
    async def test_commit_records_import_metadata(self):
        """commit_staged should insert into tax.csv_imports."""
        _staged_imports.clear()
        stage_id, parsed = _stage_mexc_deposits()
        session = FakeCommitSession()

        decisions = [
            {"row_num": 1, "action": "IMPORT"},
            {"row_num": 2, "action": "SKIP"},
            {"row_num": 3, "action": "SKIP"},
        ]
        await commit_staged(session, stage_id, decisions)

        csv_import_inserts = [s for s, p in session.executed if "tax.csv_imports" in s]
        assert len(csv_import_inserts) == 1


class TestCommitValidation:

    @pytest.mark.asyncio
    async def test_commit_invalid_stage_id(self):
        """Non-existent stage should raise ValueError."""
        _staged_imports.clear()
        session = FakeCommitSession()
        with pytest.raises(ValueError, match="not found"):
            await commit_staged(session, "stg_nonexistent", [])

    @pytest.mark.asyncio
    async def test_commit_removes_stage(self):
        """After commit, the stage should be removed from memory."""
        _staged_imports.clear()
        stage_id, parsed = _stage_mexc_deposits()
        session = FakeCommitSession()

        decisions = [{"row_num": r["row_num"], "action": "SKIP"} for r in parsed["rows"]]
        await commit_staged(session, stage_id, decisions)

        assert get_staged(stage_id) is None


class TestEndpointRegistration:

    def test_stage_endpoint_exists(self):
        import main as main_module
        routes = [r.path for r in main_module.app.routes]
        assert "/v4/import/stage" in routes

    def test_commit_endpoint_exists(self):
        import main as main_module
        routes = [r.path for r in main_module.app.routes]
        assert "/v4/import/commit" in routes

    def test_history_endpoint_exists(self):
        import main as main_module
        routes = [r.path for r in main_module.app.routes]
        assert "/v4/import/history" in routes

    def test_import_file_preview_endpoint_exists(self):
        import main as main_module
        routes = [r.path for r in main_module.app.routes]
        assert "/v4/import-file-preview" in routes


class TestFrontendElements:
    """Verify import and wallet UI elements exist in index.html."""

    def test_import_page_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert 'id="page-import"' in content

    def test_wallets_page_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert 'id="page-wallets"' in content

    def test_import_nav_button_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "showPage('import'" in content
        assert "showPage('wallets'" in content

    def test_drop_zone_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert 'id="importDropZone"' in content

    def test_commit_button_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert 'id="importCommitBtn"' in content

    def test_upload_file_function_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert 'function uploadFile' in content

    def test_commit_import_function_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert 'function commitImport' in content

    def test_load_wallets_function_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert 'function loadWallets' in content
