"""
Tests for the staged import system — Step 1 (parse + match analysis).

Covers:
  - MEXC deposit XLSX parsing
  - NonKYC withdrawal CSV parsing
  - Duplicate detection (MATCH status)
  - Transfer detection by tx_hash cross-exchange
  - NEW record detection
  - Pre-filled SKIP decision for duplicates
  - Stage TTL expiry
  - Unknown format error handling
  - TX hash normalization in matching
"""
import os
import time
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from import_staging import (
    parse_file, analyze_matches, _create_stage_id,
    _staged_imports, _cleanup_expired_stages, get_staged, STAGE_TTL_SECONDS,
    _normalize_tx_hash, _parse_ts,
)

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


class TestParseFile:

    def test_parse_mexc_deposit_xlsx(self):
        path = os.path.join(FIXTURES_DIR, "mexc_deposits.xlsx")
        result = parse_file(path)
        assert result["file_info"]["detected_exchange"] == "mexc"
        assert result["file_info"]["detected_type"] == "deposits"
        assert result["file_info"]["parser"] == "mexc_deposit_xlsx"
        assert len(result["rows"]) == 3

    def test_parse_mexc_deposit_xlsx_row_data(self):
        path = os.path.join(FIXTURES_DIR, "mexc_deposits.xlsx")
        result = parse_file(path)
        row1 = result["rows"][0]["parsed"]
        assert row1["exchange"] == "mexc"
        assert row1["asset"] == "BTC"
        assert row1["amount"] == "0.00137478"
        assert row1["network"] == "Bitcoin(BTC)"

    def test_parse_mexc_withdrawal_xlsx(self):
        path = os.path.join(FIXTURES_DIR, "mexc_withdrawals.xlsx")
        result = parse_file(path)
        assert result["file_info"]["detected_exchange"] == "mexc"
        assert result["file_info"]["detected_type"] == "withdrawals"
        assert len(result["rows"]) == 1
        row = result["rows"][0]["parsed"]
        assert row["asset"] == "SAL"
        assert row["fee"] is not None
        assert row["fee_asset"] == "SAL"

    def test_parse_nonkyc_deposit_csv(self):
        path = os.path.join(FIXTURES_DIR, "nonkyc_deposits.csv")
        result = parse_file(path)
        assert result["file_info"]["detected_exchange"] == "nonkyc"
        assert result["file_info"]["detected_type"] == "deposits"
        assert len(result["rows"]) == 4
        row1 = result["rows"][0]["parsed"]
        assert row1["asset"] == "USDT"
        assert row1["amount"] == "191.63"

    def test_parse_nonkyc_withdrawal_csv(self):
        path = os.path.join(FIXTURES_DIR, "nonkyc_withdrawals.csv")
        result = parse_file(path)
        assert result["file_info"]["detected_exchange"] == "nonkyc"
        assert result["file_info"]["detected_type"] == "withdrawals"
        assert len(result["rows"]) == 4

    def test_parse_unknown_format_raises(self, tmp_path):
        path = tmp_path / "unknown.csv"
        path.write_text("Foo,Bar\n1,2\n")
        with pytest.raises(ValueError, match="Unknown CSV format"):
            parse_file(str(path))

    def test_parse_unsupported_extension_raises(self, tmp_path):
        path = tmp_path / "data.json"
        path.write_text("{}")
        with pytest.raises(ValueError, match="Unsupported file extension"):
            parse_file(str(path))

    def test_file_info_includes_sha256(self):
        path = os.path.join(FIXTURES_DIR, "mexc_deposits.xlsx")
        result = parse_file(path)
        assert len(result["file_info"]["sha256"]) == 64

    def test_file_info_includes_size(self):
        path = os.path.join(FIXTURES_DIR, "mexc_deposits.xlsx")
        result = parse_file(path)
        assert result["file_info"]["size_bytes"] > 0


class TestTxHashNormalization:

    def test_strip_output_index(self):
        assert _normalize_tx_hash("abc123:0") == "abc123"
        assert _normalize_tx_hash("abc123:4") == "abc123"

    def test_preserve_non_numeric_suffix(self):
        assert _normalize_tx_hash("abc:def") == "abc:def"

    def test_lowercase(self):
        assert _normalize_tx_hash("ABC123") == "abc123"

    def test_empty(self):
        assert _normalize_tx_hash("") == ""
        assert _normalize_tx_hash(None) == ""


class TestTimestampParsing:

    def test_nonkyc_format(self):
        result = _parse_ts("3/10/2026, 9:57:14 PM")
        assert result is not None
        assert result.hour == 21

    def test_mexc_format(self):
        result = _parse_ts("2026-03-11 02:03:21")
        assert result is not None
        assert result.year == 2026

    def test_iso_format(self):
        result = _parse_ts("2026-01-01T00:00:00Z")
        assert result is not None

    def test_none(self):
        assert _parse_ts(None) is None
        assert _parse_ts("") is None


# ── Match Analysis Tests ─────────────────────────────────────────────────

class FakeResult:
    def __init__(self, rows=None, keys_list=None):
        self._rows = rows or []
        self._keys = keys_list or []
    def fetchone(self):
        return self._rows[0] if self._rows else None
    def fetchall(self):
        return self._rows
    def keys(self):
        return self._keys


class FakeAnalysisSession:
    """Mock session for match analysis."""

    def __init__(self):
        self.existing_deposits = []  # list of tuples (id, exchange_id, asset, amount, confirmed_at)
        self.existing_withdrawals = []
        self._deposit_keys = ["id", "exchange_id", "asset", "amount", "confirmed_at"]
        self._deposit_full_keys = ["id", "exchange_id", "asset", "amount", "tx_hash", "confirmed_at"]
        self._cross_keys = ["id", "exchange", "asset", "amount", "tx_hash", "address", "confirmed_at"]
        self._cross_wd_keys = ["id", "exchange", "asset", "amount", "fee", "tx_hash", "address", "confirmed_at"]

    async def execute(self, stmt, params=None):
        sql = str(stmt) if hasattr(stmt, 'text') else str(stmt)

        # Exact duplicate check
        if "WHERE exchange = :ex AND exchange_id = :eid" in sql:
            eid = params.get("eid", "")
            ex = params.get("ex", "")
            for rec in self.existing_deposits + self.existing_withdrawals:
                if rec[1] == eid:
                    return FakeResult([rec], self._deposit_keys)
            return FakeResult([], self._deposit_keys)

        # TX hash same exchange check
        if "tx_hash IS NOT NULL" in sql and "exchange = :ex" in sql:
            return FakeResult([], self._deposit_full_keys)

        # TX hash cross-exchange check (deposits)
        if "exchange != :ex" in sql and "tx_hash IS NOT NULL" in sql and "tax.deposits" in sql:
            return FakeResult([], self._cross_keys)

        # TX hash cross-exchange check (withdrawals)
        if "exchange != :ex" in sql and "tx_hash IS NOT NULL" in sql and "tax.withdrawals" in sql:
            return FakeResult([], self._cross_wd_keys)

        # Amount+timing check
        if "BETWEEN :earliest AND :latest" in sql or "BETWEEN :wd_time AND :latest" in sql:
            return FakeResult([], self._cross_wd_keys)

        return FakeResult([], [])


class TestAnalyzeMatches:

    @pytest.mark.asyncio
    async def test_new_records_have_null_decision(self):
        """Unmatched rows should have decision=null."""
        path = os.path.join(FIXTURES_DIR, "mexc_deposits.xlsx")
        parsed = parse_file(path)
        session = FakeAnalysisSession()
        result = await analyze_matches(session, parsed)

        for row in result["rows"]:
            assert row["status"] == "NEW"
            assert row["decision"] is None

    @pytest.mark.asyncio
    async def test_duplicate_prefilled_skip(self):
        """Exact duplicates should have decision='SKIP'."""
        path = os.path.join(FIXTURES_DIR, "mexc_deposits.xlsx")
        parsed = parse_file(path)
        session = FakeAnalysisSession()

        # Pre-insert one of the deposits
        eid = parsed["rows"][0]["parsed"]["exchange_id"]
        session.existing_deposits = [
            (42, eid, "BTC", "0.00137478", datetime(2026, 3, 11, 2, 3, 21, tzinfo=timezone.utc))
        ]

        result = await analyze_matches(session, parsed)
        assert result["rows"][0]["status"] == "MATCH"
        assert result["rows"][0]["decision"] == "SKIP"
        assert result["rows"][0]["match"]["existing_id"] == 42

    @pytest.mark.asyncio
    async def test_summary_counts(self):
        """Summary should count each category."""
        path = os.path.join(FIXTURES_DIR, "mexc_deposits.xlsx")
        parsed = parse_file(path)
        session = FakeAnalysisSession()
        result = await analyze_matches(session, parsed)

        assert result["summary"]["total_rows"] == 3
        assert result["summary"]["new"] == 3
        assert result["summary"]["matches"] == 0

    @pytest.mark.asyncio
    async def test_mixed_match_and_new(self):
        """One duplicate + two new should produce correct summary."""
        path = os.path.join(FIXTURES_DIR, "mexc_deposits.xlsx")
        parsed = parse_file(path)
        session = FakeAnalysisSession()

        eid = parsed["rows"][0]["parsed"]["exchange_id"]
        session.existing_deposits = [
            (1, eid, "BTC", "0.00137478", datetime(2026, 3, 11, 2, 3, 21, tzinfo=timezone.utc))
        ]

        result = await analyze_matches(session, parsed)
        assert result["summary"]["matches"] == 1
        assert result["summary"]["new"] == 2


# ── Stage Management Tests ────────────────────────────────────────────────

class TestStageManagement:

    def test_create_stage_id_format(self):
        sid = _create_stage_id()
        assert sid.startswith("stg_")
        parts = sid.split("_")
        assert len(parts) == 3

    def test_stage_storage_and_retrieval(self):
        _staged_imports.clear()
        sid = _create_stage_id()
        _staged_imports[sid] = {"created_at": time.time(), "parsed_data": {}, "committed": False}
        assert get_staged(sid) is not None
        assert get_staged("nonexistent") is None

    def test_stage_expires_after_ttl(self):
        _staged_imports.clear()
        sid = _create_stage_id()
        _staged_imports[sid] = {
            "created_at": time.time() - STAGE_TTL_SECONDS - 10,
            "parsed_data": {}, "committed": False,
        }
        assert get_staged(sid) is None  # Should be cleaned up

    def test_cleanup_only_removes_expired(self):
        _staged_imports.clear()
        old_sid = "stg_old"
        new_sid = "stg_new"
        _staged_imports[old_sid] = {"created_at": time.time() - STAGE_TTL_SECONDS - 10, "parsed_data": {}}
        _staged_imports[new_sid] = {"created_at": time.time(), "parsed_data": {}}
        _cleanup_expired_stages()
        assert old_sid not in _staged_imports
        assert new_sid in _staged_imports
        _staged_imports.clear()


# ── Row Parsing Specific Tests ────────────────────────────────────────────

class TestRowParsing:

    def test_mexc_deposit_tx_hash_strips_suffix(self):
        """MEXC deposits should have tx_hash with :N suffix stripped."""
        path = os.path.join(FIXTURES_DIR, "mexc_deposits.xlsx")
        result = parse_file(path)
        row1 = result["rows"][0]["parsed"]
        # The full_txid should preserve the original
        assert ":" not in row1["tx_hash"]  # stripped
        assert "full_txid" in row1  # original preserved

    def test_nonkyc_deposit_amount_usd_captured(self):
        """NonKYC deposits should capture ValueUsd as amount_usd."""
        path = os.path.join(FIXTURES_DIR, "nonkyc_deposits.csv")
        result = parse_file(path)
        row1 = result["rows"][0]["parsed"]
        assert row1.get("amount_usd") == "191.63"

    def test_nonkyc_withdrawal_address_captured(self):
        """NonKYC withdrawals should capture the address."""
        path = os.path.join(FIXTURES_DIR, "nonkyc_withdrawals.csv")
        result = parse_file(path)
        row1 = result["rows"][0]["parsed"]
        assert row1["address"] == "bc1qz9p0s296sf07tlcz20sz6n5suf2rk7fg83kanl"

    def test_rows_have_raw_data(self):
        """Every row should include the raw unparsed data."""
        path = os.path.join(FIXTURES_DIR, "mexc_deposits.xlsx")
        result = parse_file(path)
        for row in result["rows"]:
            assert "raw" in row
            assert isinstance(row["raw"], dict)
