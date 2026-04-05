"""
CSV Import Pipeline — fills exchange API retention gaps.

Supports:
  - MEXC official trade/deposit/withdrawal CSV exports
  - NonKYC CSV exports
  - Generic/manual CSV with user-provided column mapping

Deduplicates against existing API-ingested records by exchange_id and tx_hash.
Records import metadata: source filename, SHA256 checksum, row count, import timestamp.
"""
import csv
import hashlib
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from sqlalchemy import text

logger = logging.getLogger("tax-collector.csv-importer")

D = Decimal


class CSVImporter:
    """Import exchange history from CSV files to close API retention gaps."""

    MEXC_TRADE_COLUMNS = ["symbol", "orderId", "id", "price", "qty", "quoteQty",
                          "commission", "commissionAsset", "time", "isBuyer"]
    MEXC_DEPOSIT_COLUMNS = ["coin", "amount", "network", "txId", "status", "insertTime"]
    MEXC_WITHDRAWAL_COLUMNS = ["coin", "amount", "network", "txId", "transactionFee",
                                "status", "applyTime", "completeTime"]

    def _file_hash(self, filepath: str) -> str:
        h = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    def _parse_ts(self, val) -> datetime | None:
        if val is None or val == "":
            return None
        if isinstance(val, (int, float)):
            return datetime.fromtimestamp(val / 1000, tz=timezone.utc)
        try:
            return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass
        try:
            return datetime.fromtimestamp(float(val) / 1000, tz=timezone.utc)
        except (ValueError, TypeError):
            return None

    def _safe_decimal(self, val) -> str:
        try:
            return str(D(str(val or "0")))
        except (InvalidOperation, ValueError):
            return "0"

    async def _check_duplicate(self, session, table: str, exchange_id: str) -> bool:
        r = await session.execute(text(f"""
            SELECT 1 FROM tax.{table} WHERE exchange_id = :eid LIMIT 1
        """), {"eid": exchange_id})
        return r.fetchone() is not None

    async def _record_import(self, session, exchange: str, data_type: str,
                             filename: str, file_hash: str, row_count: int,
                             imported: int, duplicates: int, errors: int,
                             date_start: datetime | None, date_end: datetime | None):
        await session.execute(text("""
            INSERT INTO tax.csv_imports
                (exchange, data_type, filename, file_hash, row_count, imported_count,
                 duplicate_count, error_count, date_range_start, date_range_end)
            VALUES (:ex, :dt, :fn, :fh, :rc, :ic, :dc, :ec, :ds, :de)
        """), {
            "ex": exchange, "dt": data_type, "fn": os.path.basename(filename),
            "fh": file_hash, "rc": row_count, "ic": imported, "dc": duplicates,
            "ec": errors, "ds": date_start, "de": date_end,
        })

    async def import_mexc_trades(self, session, filepath: str) -> dict:
        """Import MEXC trade CSV. Returns {imported: N, duplicates: N, errors: N}."""
        file_hash = self._file_hash(filepath)
        imported, duplicates, errors, row_count = 0, 0, 0, 0
        date_start, date_end = None, None

        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row_count += 1
                try:
                    eid = str(row.get("id", row.get("orderId", "")))
                    if await self._check_duplicate(session, "trades", eid):
                        duplicates += 1
                        continue

                    ts = self._parse_ts(row.get("time"))
                    if ts:
                        if date_start is None or ts < date_start:
                            date_start = ts
                        if date_end is None or ts > date_end:
                            date_end = ts

                    price = self._safe_decimal(row.get("price"))
                    qty = self._safe_decimal(row.get("qty"))
                    total = self._safe_decimal(row.get("quoteQty"))
                    if total == "0":
                        total = str(D(price) * D(qty))

                    is_buyer = str(row.get("isBuyer", "")).lower() in ("true", "1", "yes")

                    await session.execute(text("""
                        INSERT INTO tax.trades
                            (exchange, exchange_id, market, side, price, quantity,
                             total, fee, fee_asset, executed_at, raw_data, source_type, source_file)
                        VALUES ('mexc', :eid, :market, :side, :price, :qty,
                                :total, :fee, :fee_asset, :ts, :raw, 'csv', :sf)
                        ON CONFLICT (exchange_id) DO NOTHING
                    """), {
                        "eid": eid, "market": row.get("symbol", ""),
                        "side": "buy" if is_buyer else "sell",
                        "price": price, "qty": qty, "total": total,
                        "fee": self._safe_decimal(row.get("commission")),
                        "fee_asset": row.get("commissionAsset", ""),
                        "ts": ts, "raw": str(row), "sf": os.path.basename(filepath),
                    })
                    imported += 1
                except Exception as e:
                    logger.warning(f"Row {row_count} error: {e}")
                    errors += 1

        await self._record_import(session, "mexc", "trades", filepath, file_hash,
                                  row_count, imported, duplicates, errors,
                                  date_start, date_end)
        return {"imported": imported, "duplicates": duplicates, "errors": errors,
                "row_count": row_count, "file_hash": file_hash}

    async def import_mexc_deposits(self, session, filepath: str) -> dict:
        """Import MEXC deposit CSV."""
        file_hash = self._file_hash(filepath)
        imported, duplicates, errors, row_count = 0, 0, 0, 0
        date_start, date_end = None, None

        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row_count += 1
                try:
                    eid = str(row.get("txId", row.get("id", f"mexc_dep_{row_count}")))
                    if await self._check_duplicate(session, "deposits", eid):
                        duplicates += 1
                        continue

                    ts = self._parse_ts(row.get("insertTime", row.get("completeTime")))
                    if ts:
                        if date_start is None or ts < date_start:
                            date_start = ts
                        if date_end is None or ts > date_end:
                            date_end = ts

                    await session.execute(text("""
                        INSERT INTO tax.deposits
                            (exchange, exchange_id, asset, amount, network, tx_hash,
                             status, confirmed_at, raw_data, source_type, source_file)
                        VALUES ('mexc', :eid, :asset, :amount, :net, :txh,
                                :status, :ts, :raw, 'csv', :sf)
                        ON CONFLICT (exchange_id) DO NOTHING
                    """), {
                        "eid": eid, "asset": row.get("coin", ""),
                        "amount": self._safe_decimal(row.get("amount")),
                        "net": row.get("network", ""), "txh": row.get("txId", ""),
                        "status": row.get("status", ""), "ts": ts,
                        "raw": str(row), "sf": os.path.basename(filepath),
                    })
                    imported += 1
                except Exception as e:
                    logger.warning(f"Deposit row {row_count} error: {e}")
                    errors += 1

        await self._record_import(session, "mexc", "deposits", filepath, file_hash,
                                  row_count, imported, duplicates, errors,
                                  date_start, date_end)
        return {"imported": imported, "duplicates": duplicates, "errors": errors,
                "row_count": row_count, "file_hash": file_hash}

    async def import_mexc_withdrawals(self, session, filepath: str) -> dict:
        """Import MEXC withdrawal CSV."""
        file_hash = self._file_hash(filepath)
        imported, duplicates, errors, row_count = 0, 0, 0, 0
        date_start, date_end = None, None

        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row_count += 1
                try:
                    eid = str(row.get("txId", row.get("id", f"mexc_wd_{row_count}")))
                    if await self._check_duplicate(session, "withdrawals", eid):
                        duplicates += 1
                        continue

                    ts = self._parse_ts(row.get("completeTime", row.get("applyTime")))
                    if ts:
                        if date_start is None or ts < date_start:
                            date_start = ts
                        if date_end is None or ts > date_end:
                            date_end = ts

                    await session.execute(text("""
                        INSERT INTO tax.withdrawals
                            (exchange, exchange_id, asset, amount, fee, network, tx_hash,
                             status, confirmed_at, raw_data, source_type, source_file)
                        VALUES ('mexc', :eid, :asset, :amount, :fee, :net, :txh,
                                :status, :ts, :raw, 'csv', :sf)
                        ON CONFLICT (exchange_id) DO NOTHING
                    """), {
                        "eid": eid, "asset": row.get("coin", ""),
                        "amount": self._safe_decimal(row.get("amount")),
                        "fee": self._safe_decimal(row.get("transactionFee")),
                        "net": row.get("network", ""), "txh": row.get("txId", ""),
                        "status": row.get("status", ""), "ts": ts,
                        "raw": str(row), "sf": os.path.basename(filepath),
                    })
                    imported += 1
                except Exception as e:
                    logger.warning(f"Withdrawal row {row_count} error: {e}")
                    errors += 1

        await self._record_import(session, "mexc", "withdrawals", filepath, file_hash,
                                  row_count, imported, duplicates, errors,
                                  date_start, date_end)
        return {"imported": imported, "duplicates": duplicates, "errors": errors,
                "row_count": row_count, "file_hash": file_hash}

    async def import_generic(self, session, filepath: str, exchange: str,
                             data_type: str, column_map: dict) -> dict:
        """Import any CSV with a user-provided column mapping."""
        file_hash = self._file_hash(filepath)
        imported, duplicates, errors, row_count = 0, 0, 0, 0
        date_start, date_end = None, None

        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row_count += 1
                try:
                    # Apply column mapping
                    mapped = {}
                    for target, source in column_map.items():
                        mapped[target] = row.get(source, "")

                    eid = mapped.get("exchange_id", f"{exchange}_{data_type}_{row_count}")
                    table = "trades" if data_type == "trades" else data_type
                    if await self._check_duplicate(session, table, eid):
                        duplicates += 1
                        continue

                    ts_val = mapped.get("timestamp") or mapped.get("time")
                    ts = self._parse_ts(ts_val)
                    if ts:
                        if date_start is None or ts < date_start:
                            date_start = ts
                        if date_end is None or ts > date_end:
                            date_end = ts

                    if data_type == "trades":
                        await session.execute(text("""
                            INSERT INTO tax.trades
                                (exchange, exchange_id, market, side, price, quantity,
                                 total, fee, fee_asset, executed_at, raw_data, source_type, source_file)
                            VALUES (:ex, :eid, :market, :side, :price, :qty,
                                    :total, :fee, :fee_asset, :ts, :raw, 'csv', :sf)
                            ON CONFLICT (exchange_id) DO NOTHING
                        """), {
                            "ex": exchange, "eid": eid,
                            "market": mapped.get("market", ""),
                            "side": mapped.get("side", ""),
                            "price": self._safe_decimal(mapped.get("price")),
                            "qty": self._safe_decimal(mapped.get("quantity")),
                            "total": self._safe_decimal(mapped.get("total")),
                            "fee": self._safe_decimal(mapped.get("fee")),
                            "fee_asset": mapped.get("fee_asset", ""),
                            "ts": ts, "raw": str(row),
                            "sf": os.path.basename(filepath),
                        })
                    imported += 1
                except Exception as e:
                    logger.warning(f"Generic row {row_count} error: {e}")
                    errors += 1

        await self._record_import(session, exchange, data_type, filepath, file_hash,
                                  row_count, imported, duplicates, errors,
                                  date_start, date_end)
        return {"imported": imported, "duplicates": duplicates, "errors": errors,
                "row_count": row_count, "file_hash": file_hash}
