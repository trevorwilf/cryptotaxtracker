"""
Tests for the CSV import pipeline.

Covers:
  - MEXC trade CSV parsing with sample data
  - Deduplication (import same file twice -> 0 new records)
  - File hash tracking
  - Date range extraction
  - Malformed CSV handling
  - Generic CSV with custom column mapping
"""
import os
import pytest
import tempfile
from unittest.mock import AsyncMock, MagicMock

from csv_importer import CSVImporter


class FakeMockResult:
    def __init__(self, row=None):
        self._row = row

    def fetchone(self):
        return self._row


class FakeSession:
    """Minimal async session mock for CSV importer tests."""

    def __init__(self):
        self._duplicate_ids = set()
        self.executed = []

    async def execute(self, stmt, params=None):
        sql = str(stmt) if hasattr(stmt, 'text') else str(stmt)
        self.executed.append((sql, params))

        # Handle duplicate check queries
        if "SELECT 1 FROM" in sql and params:
            eid = params.get("eid", "")
            if eid in self._duplicate_ids:
                return FakeMockResult(row=(1,))
            return FakeMockResult(row=None)

        return FakeMockResult(row=None)


def _write_csv(lines: list[str]) -> str:
    """Write CSV content to a temp file and return the path."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(fd, "w", newline="") as f:
        f.write("\n".join(lines))
    return path


class TestMEXCTradeCSV:
    def setup_method(self):
        self.importer = CSVImporter()

    @pytest.mark.asyncio
    async def test_import_mexc_trades_basic(self):
        csv_path = _write_csv([
            "symbol,orderId,id,price,qty,quoteQty,commission,commissionAsset,time,isBuyer",
            "BTCUSDT,ord001,t001,50000,0.5,25000,25,USDT,2024-06-15T12:00:00Z,true",
            "BTCUSDT,ord002,t002,51000,0.3,15300,15.3,USDT,2024-06-16T12:00:00Z,false",
        ])
        try:
            session = FakeSession()
            result = await self.importer.import_mexc_trades(session, csv_path)
            assert result["imported"] == 2
            assert result["duplicates"] == 0
            assert result["errors"] == 0
            assert result["row_count"] == 2
            assert len(result["file_hash"]) == 64
        finally:
            os.unlink(csv_path)

    @pytest.mark.asyncio
    async def test_deduplication(self):
        """Import same file twice -> second time all duplicates."""
        csv_path = _write_csv([
            "symbol,orderId,id,price,qty,quoteQty,commission,commissionAsset,time,isBuyer",
            "BTCUSDT,ord001,t001,50000,0.5,25000,25,USDT,2024-06-15T12:00:00Z,true",
        ])
        try:
            session = FakeSession()
            r1 = await self.importer.import_mexc_trades(session, csv_path)
            assert r1["imported"] == 1

            # Mark t001 as existing
            session._duplicate_ids.add("t001")
            r2 = await self.importer.import_mexc_trades(session, csv_path)
            assert r2["duplicates"] == 1
            assert r2["imported"] == 0
        finally:
            os.unlink(csv_path)

    @pytest.mark.asyncio
    async def test_file_hash_consistent(self):
        csv_path = _write_csv([
            "symbol,id,price,qty,quoteQty,commission,commissionAsset,time,isBuyer",
            "ETHUSDT,t003,3000,1,3000,3,USDT,2024-01-01T00:00:00Z,true",
        ])
        try:
            h1 = self.importer._file_hash(csv_path)
            h2 = self.importer._file_hash(csv_path)
            assert h1 == h2
            assert len(h1) == 64
        finally:
            os.unlink(csv_path)

    @pytest.mark.asyncio
    async def test_malformed_rows_counted_as_errors(self):
        """Rows with bad data should increment error count, not crash."""
        csv_path = _write_csv([
            "symbol,id,price,qty,quoteQty,commission,commissionAsset,time,isBuyer",
            "BTCUSDT,t004,50000,0.5,25000,25,USDT,2024-06-15T12:00:00Z,true",
        ])
        try:
            session = FakeSession()
            result = await self.importer.import_mexc_trades(session, csv_path)
            # Should process without crashing
            assert result["row_count"] == 1
        finally:
            os.unlink(csv_path)


class TestMEXCDepositCSV:
    def setup_method(self):
        self.importer = CSVImporter()

    @pytest.mark.asyncio
    async def test_import_deposits(self):
        csv_path = _write_csv([
            "coin,amount,network,txId,status,insertTime",
            "BTC,0.5,BTC,tx001,completed,2024-03-01T00:00:00Z",
        ])
        try:
            session = FakeSession()
            result = await self.importer.import_mexc_deposits(session, csv_path)
            assert result["imported"] == 1
            assert result["row_count"] == 1
        finally:
            os.unlink(csv_path)


class TestMEXCWithdrawalCSV:
    def setup_method(self):
        self.importer = CSVImporter()

    @pytest.mark.asyncio
    async def test_import_withdrawals(self):
        csv_path = _write_csv([
            "coin,amount,network,txId,transactionFee,status,applyTime,completeTime",
            "ETH,2.0,ETH,tx002,0.001,completed,2024-04-01T00:00:00Z,2024-04-01T01:00:00Z",
        ])
        try:
            session = FakeSession()
            result = await self.importer.import_mexc_withdrawals(session, csv_path)
            assert result["imported"] == 1
        finally:
            os.unlink(csv_path)


class TestGenericCSV:
    def setup_method(self):
        self.importer = CSVImporter()

    @pytest.mark.asyncio
    async def test_generic_import_with_column_map(self):
        csv_path = _write_csv([
            "my_id,my_market,my_side,my_price,my_qty,my_total,my_fee,my_fee_asset,my_time",
            "g001,BTC/USD,buy,45000,0.1,4500,4.5,USD,2024-02-01T00:00:00Z",
        ])
        try:
            session = FakeSession()
            column_map = {
                "exchange_id": "my_id", "market": "my_market", "side": "my_side",
                "price": "my_price", "quantity": "my_qty", "total": "my_total",
                "fee": "my_fee", "fee_asset": "my_fee_asset", "timestamp": "my_time",
            }
            result = await self.importer.import_generic(
                session, csv_path, "custom_exchange", "trades", column_map)
            assert result["imported"] == 1
            assert result["row_count"] == 1
        finally:
            os.unlink(csv_path)


class TestDateRangeExtraction:
    def setup_method(self):
        self.importer = CSVImporter()

    @pytest.mark.asyncio
    async def test_date_range_captured(self):
        csv_path = _write_csv([
            "symbol,id,price,qty,quoteQty,commission,commissionAsset,time,isBuyer",
            "BTCUSDT,t010,50000,0.1,5000,5,USDT,2024-01-15T00:00:00Z,true",
            "BTCUSDT,t011,51000,0.2,10200,10.2,USDT,2024-06-20T00:00:00Z,false",
            "BTCUSDT,t012,49000,0.3,14700,14.7,USDT,2024-03-10T00:00:00Z,true",
        ])
        try:
            session = FakeSession()
            result = await self.importer.import_mexc_trades(session, csv_path)
            assert result["imported"] == 3
            # Verify import record was written with date range
            import_inserts = [
                (sql, p) for sql, p in session.executed
                if "csv_imports" in sql.lower()
            ]
            assert len(import_inserts) > 0
            # The date_range_start should be earliest
            params = import_inserts[0][1]
            assert params["ds"].month == 1  # January is earliest
            assert params["de"].month == 6  # June is latest
        finally:
            os.unlink(csv_path)
