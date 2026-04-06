"""
Tests for the format-aware import subsystem (Phase 1).

Tests header fingerprinting, format detection, timestamp parsing,
duplicate detection scoping, and settlement amount validation.
"""
import os
import csv
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from csv_importer import CSVImporter, detect_format


FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


class TestDetectFormat:
    def test_detect_mexc_deposit_xlsx(self):
        path = os.path.join(FIXTURES_DIR, "mexc_deposits.xlsx")
        assert detect_format(path) == ("mexc", "deposits", "mexc_deposit_xlsx")

    def test_detect_mexc_withdrawal_xlsx(self):
        path = os.path.join(FIXTURES_DIR, "mexc_withdrawals.xlsx")
        assert detect_format(path) == ("mexc", "withdrawals", "mexc_withdrawal_xlsx")

    def test_detect_nonkyc_deposit_csv(self):
        path = os.path.join(FIXTURES_DIR, "nonkyc_deposits.csv")
        assert detect_format(path) == ("nonkyc", "deposits", "nonkyc_deposit_csv")

    def test_detect_nonkyc_withdrawal_csv(self):
        path = os.path.join(FIXTURES_DIR, "nonkyc_withdrawals.csv")
        assert detect_format(path) == ("nonkyc", "withdrawals", "nonkyc_withdrawal_csv")

    def test_detect_unknown_raises(self, tmp_path):
        # Create CSV with unknown headers
        path = tmp_path / "unknown.csv"
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Foo", "Bar", "Baz"])
            writer.writerow(["1", "2", "3"])
        with pytest.raises(ValueError, match="Unknown CSV format"):
            detect_format(str(path))

    def test_detect_unknown_xlsx_raises(self, tmp_path):
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.append(["Unknown", "Headers"])
        path = tmp_path / "unknown.xlsx"
        wb.save(str(path))
        with pytest.raises(ValueError, match="Unknown XLSX format"):
            detect_format(str(path))

    def test_unsupported_extension_raises(self, tmp_path):
        path = tmp_path / "data.json"
        path.write_text("{}")
        with pytest.raises(ValueError, match="Unsupported file extension"):
            detect_format(str(path))


class TestTimestampParsing:
    def test_nonkyc_timestamp_format(self):
        importer = CSVImporter()
        result = importer._parse_ts("3/10/2026, 9:57:14 PM")
        assert result is not None
        assert result.year == 2026
        assert result.month == 3
        assert result.day == 10
        assert result.hour == 21
        assert result.minute == 57
        assert result.second == 14
        assert result.tzinfo == timezone.utc

    def test_mexc_xlsx_timestamp_format(self):
        importer = CSVImporter()
        result = importer._parse_ts("2026-03-11 02:03:21")
        assert result is not None
        assert result.year == 2026
        assert result.month == 3
        assert result.day == 11
        assert result.hour == 2

    def test_epoch_ms(self):
        importer = CSVImporter()
        result = importer._parse_ts(1710500000000)
        assert result is not None

    def test_none_returns_none(self):
        importer = CSVImporter()
        assert importer._parse_ts(None) is None
        assert importer._parse_ts("") is None

    def test_datetime_passthrough(self):
        importer = CSVImporter()
        dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert importer._parse_ts(dt) == dt

    def test_naive_datetime_gets_utc(self):
        importer = CSVImporter()
        dt = datetime(2026, 1, 1)
        result = importer._parse_ts(dt)
        assert result.tzinfo == timezone.utc


class TestFileHash:
    def test_hash_consistency(self, tmp_path):
        importer = CSVImporter()
        path = tmp_path / "test.csv"
        path.write_text("hello world")
        h1 = importer._file_hash(str(path))
        h2 = importer._file_hash(str(path))
        assert h1 == h2
        assert len(h1) == 64  # SHA256 hex


class TestSafeDecimal:
    def test_normal(self):
        importer = CSVImporter()
        assert importer._safe_decimal("123.45") == "123.45"

    def test_none(self):
        importer = CSVImporter()
        assert importer._safe_decimal(None) == "0"

    def test_garbage(self):
        importer = CSVImporter()
        assert importer._safe_decimal("not_a_number") == "0"


class TestMEXCSettlementValidation:
    """Verify warning is logged when settlement != request - fee."""

    @pytest.mark.asyncio
    async def test_settlement_mismatch_warning(self, tmp_path, caplog):
        """Import MEXC withdrawal XLSX where settlement doesn't match."""
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.append(["UID", "Status", "Time", "Crypto", "Network",
                    "Request Amount", "Withdrawal Address", "memo", "TxID",
                    "Trading Fee", "Settlement Amount", "Withdrawal Descriptions"])
        # Settlement should be 100 - 5 = 95, but we put 90
        ws.append([12345, "Withdrawal Successful", "2026-01-15 10:00:00",
                    "BTC", "Bitcoin(BTC)", 100, "addr1", "--",
                    "tx_mismatch_001", 5, 90, "test"])
        path = tmp_path / "mexc_wd_mismatch.xlsx"
        wb.save(str(path))

        importer = CSVImporter()
        session = AsyncMock()
        # _check_duplicate returns False
        dup_result = MagicMock()
        dup_result.fetchone.return_value = None
        session.execute.return_value = dup_result

        import logging
        with caplog.at_level(logging.WARNING, logger="tax-collector.csv-importer"):
            await importer.import_mexc_withdrawals_xlsx(session, str(path))

        assert any("settlement mismatch" in rec.message for rec in caplog.records)


class TestDuplicateDetection:
    """Verify duplicate detection is scoped by exchange."""

    @pytest.mark.asyncio
    async def test_check_duplicate_includes_exchange(self):
        importer = CSVImporter()
        session = AsyncMock()
        result_mock = MagicMock()
        result_mock.fetchone.return_value = None
        session.execute.return_value = result_mock

        await importer._check_duplicate(session, "deposits", "mexc", "tx123")

        # Verify the SQL includes exchange scope
        call_args = session.execute.call_args
        sql_text = str(call_args[0][0])
        assert "exchange = :ex" in sql_text
        assert "exchange_id = :eid" in sql_text
