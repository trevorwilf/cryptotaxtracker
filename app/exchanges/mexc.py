"""
MEXC exchange plugin.

REST API: https://api.mexc.com/api/v3/
Auth: HMAC-SHA256 query string signature.

MEXC signature rules:
  1. Build query string from params IN ORDER (not sorted)
  2. Append timestamp and recvWindow
  3. HMAC-SHA256 the full query string with the secret key
  4. Append signature= to the URL
"""
import hashlib
import hmac
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from urllib.parse import urlencode

import aiohttp

from exchanges import BaseExchange, register

logger = logging.getLogger("tax-collector.mexc")
BASE_URL = "https://api.mexc.com"

MEXC_RETENTION = {
    "myTrades": {"days": 30, "description": "Trade history"},
    "allOrders": {"days": 7, "description": "Order history"},
    "deposit_history": {"days": 90, "description": "Deposit history"},
    "withdraw_history": {"days": 90, "description": "Withdrawal history"},
    "universal_transfer": {"days": 180, "description": "Universal transfer history"},
}


@register
class MEXCExchange(BaseExchange):
    name = "mexc"

    def _sign(self, params: dict) -> str:
        """Build a signed query string. Returns the full query string including signature."""
        params["timestamp"] = str(int(time.time() * 1000))
        params["recvWindow"] = "10000"
        # CRITICAL: urlencode in insertion order — do NOT sort
        query = urlencode(params)
        signature = hmac.new(
            self.api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        return query + "&signature=" + signature

    async def _get(self, path: str, params: dict | None = None, signed: bool = True) -> list | dict:
        params = params or {}
        headers = {"X-MEXC-APIKEY": self.api_key}
        if signed:
            # Build the full signed URL ourselves so param order is guaranteed
            query_string = self._sign(params)
            url = f"{BASE_URL}{path}?{query_string}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning(f"MEXC {path} returned {resp.status}: {body}")
                        resp.raise_for_status()
                    return await resp.json()
        else:
            url = f"{BASE_URL}{path}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, headers=headers) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning(f"MEXC {path} returned {resp.status}: {body}")
                        resp.raise_for_status()
                    return await resp.json()

    def _parse_ts(self, val) -> datetime:
        if val is None:
            logger.warning("MEXC: Missing timestamp on record — defaulting to now()")
            return datetime.now(timezone.utc)
        if isinstance(val, (int, float)):
            return datetime.fromtimestamp(val / 1000, tz=timezone.utc)
        logger.warning("MEXC: Missing timestamp on record — defaulting to now()")
        return datetime.now(timezone.utc)

    async def _get_traded_symbols(self) -> list[str]:
        """Discover symbols from balances AND from env-configured extras.

        Current-balance-only discovery misses fully-exited positions.
        We supplement with any symbols from MEXC_EXTRA_SYMBOLS env var.
        """
        symbols = set()

        # 1. Current balances (existing logic)
        try:
            account = await self._get("/api/v3/account", {})
            if isinstance(account, dict):
                for bal in account.get("balances", []):
                    free = float(bal.get("free", 0))
                    locked = float(bal.get("locked", 0))
                    if (free + locked) > 0:
                        asset = bal.get("asset", "")
                        if asset and asset not in ("USDT", "USDC", "USD"):
                            symbols.add(f"{asset}USDT")
                logger.info(f"MEXC account has {len(symbols)} non-zero assets")
        except Exception as e:
            logger.warning(f"MEXC account lookup failed: {e}")

        # 2. Previously known symbols from env
        extra = os.environ.get("MEXC_EXTRA_SYMBOLS", "")
        if extra:
            for sym in extra.split(","):
                sym = sym.strip().upper()
                if sym:
                    symbols.add(sym)
            logger.info(f"Added extra symbols from MEXC_EXTRA_SYMBOLS")

        if not symbols:
            logger.warning("No traded symbols found on MEXC — skipping trade fetch")
        return list(symbols)

    def get_data_coverage(self, since: datetime | None = None) -> dict:
        """Return metadata about what date ranges the API can actually cover."""
        now = datetime.now(timezone.utc)
        coverage = {}
        for endpoint, info in MEXC_RETENTION.items():
            earliest_available = now - timedelta(days=info["days"])
            requested_start = since or datetime(2020, 1, 1, tzinfo=timezone.utc)
            has_gap = requested_start < earliest_available
            coverage[endpoint] = {
                "description": info["description"],
                "retention_days": info["days"],
                "earliest_available": earliest_available.isoformat(),
                "requested_start": requested_start.isoformat(),
                "has_gap": has_gap,
                "gap_days": (earliest_available - requested_start).days if has_gap else 0,
                "requires_csv_import": has_gap,
            }
        return coverage

    async def fetch_transfers(self, since: datetime | None = None) -> list[dict]:
        """Fetch MEXC universal transfer history. Queries both directions separately.

        MEXC API requires fromAccountType and toAccountType as mandatory params,
        and uses page/size (not limit) for pagination.
        """
        all_transfers = []
        directions = [("SPOT", "FUTURES"), ("FUTURES", "SPOT")]
        for from_acct, to_acct in directions:
            page = 1
            while True:
                params = {
                    "fromAccountType": from_acct,
                    "toAccountType": to_acct,
                    "size": "100",
                    "page": str(page),
                }
                if since:
                    params["startTime"] = str(int(since.timestamp() * 1000))
                try:
                    raw = await self._get("/api/v3/capital/transfer", params)
                except Exception as e:
                    logger.warning(f"Transfer fetch {from_acct}->{to_acct} page {page} failed: {e}")
                    break
                if isinstance(raw, dict):
                    rows = raw.get("rows", raw.get("data", []))
                    total = raw.get("total", 0)
                elif isinstance(raw, list):
                    rows = raw
                    total = len(raw)
                else:
                    break
                if not rows:
                    break
                for t in rows:
                    all_transfers.append({
                        "exchange": self.name,
                        "exchange_id": str(t.get("tranId", "")),
                        "asset": t.get("asset", ""),
                        "amount": str(t.get("amount", "0")),
                        "from_account": t.get("fromAccountType", from_acct),
                        "to_account": t.get("toAccountType", to_acct),
                        "status": t.get("status", ""),
                        "transferred_at": self._parse_ts(t.get("timestamp")),
                        "raw_data": json.dumps(t),
                    })
                if len(all_transfers) >= total or len(rows) < 100:
                    break
                page += 1
        return all_transfers

    async def fetch_trades(self, since: datetime | None = None) -> list[dict]:
        symbols = await self._get_traded_symbols()
        if not symbols:
            return []
        all_trades = []
        for symbol in symbols:
            params = {"symbol": symbol, "limit": 1000}
            if since:
                params["startTime"] = str(int(since.timestamp() * 1000))
            try:
                raw = await self._get("/api/v3/myTrades", params)
                if not isinstance(raw, list):
                    continue
                for t in raw:
                    all_trades.append({
                        "exchange": self.name,
                        "exchange_id": str(t.get("id", t.get("orderId", ""))),
                        "market": t.get("symbol", symbol),
                        "base_asset": None, "quote_asset": None,
                        "side": "buy" if t.get("isBuyer") else "sell",
                        "price": str(t.get("price", "0")),
                        "quantity": str(t.get("qty", "0")),
                        "total": str(Decimal(str(t.get("price", "0"))) * Decimal(str(t.get("qty", "0")))),
                        "fee": str(t.get("commission", "0")),
                        "fee_asset": t.get("commissionAsset", ""),
                        "price_usd": None, "quantity_usd": None, "total_usd": None,
                        "fee_usd": None, "base_price_usd": None, "quote_price_usd": None,
                        "executed_at": self._parse_ts(t.get("time")),
                        "raw_data": json.dumps(t),
                    })
            except Exception as e:
                logger.warning(f"Failed fetching trades for {symbol}: {e}")
        return all_trades

    async def fetch_orders(self, since: datetime | None = None) -> list[dict]:
        symbols = await self._get_traded_symbols()
        if not symbols:
            return []
        status_map = {"NEW": "Active", "PARTIALLY_FILLED": "Partly Filled",
                      "FILLED": "Filled", "CANCELED": "Cancelled", "CANCELLED": "Cancelled"}
        all_orders = []
        for symbol in symbols:
            params = {"symbol": symbol, "limit": 1000}
            if since:
                params["startTime"] = str(int(since.timestamp() * 1000))
            try:
                raw = await self._get("/api/v3/allOrders", params)
                if not isinstance(raw, list):
                    continue
                for o in raw:
                    all_orders.append({
                        "exchange": self.name,
                        "exchange_id": str(o.get("orderId", "")),
                        "market": o.get("symbol", symbol),
                        "base_asset": None, "quote_asset": None,
                        "side": str(o.get("side", "")).lower(),
                        "order_type": str(o.get("type", "LIMIT")).lower(),
                        "price": str(o.get("price", "0")),
                        "quantity": str(o.get("origQty", "0")),
                        "executed_qty": str(o.get("executedQty", "0")),
                        "status": status_map.get(o.get("status", ""), o.get("status", "unknown")),
                        "price_usd": None, "total_usd": None, "fee_usd": None,
                        "created_at_ex": self._parse_ts(o.get("time")),
                        "updated_at_ex": self._parse_ts(o.get("updateTime")),
                        "raw_data": json.dumps(o),
                    })
            except Exception as e:
                logger.warning(f"Failed fetching orders for {symbol}: {e}")
        return all_orders

    async def _fetch_chunked_history(self, endpoint: str, since: datetime | None,
                                     row_mapper, label: str) -> list[dict]:
        """Fetch history in 7-day chunks to respect MEXC API constraints.

        Verified constraints: startTime/endTime diff must be <= 7 days,
        max lookback is 90 days (use 89 to be safe).
        """
        results = []
        now = datetime.now(timezone.utc)
        max_lookback = now - timedelta(days=89)
        chunk_start = max(since, max_lookback) if since else max_lookback

        while chunk_start < now:
            chunk_end = min(chunk_start + timedelta(days=7), now)
            params = {
                "startTime": str(int(chunk_start.timestamp() * 1000)),
                "endTime": str(int(chunk_end.timestamp() * 1000)),
                "limit": "1000",
            }
            try:
                raw = await self._get(endpoint, params)
            except Exception as e:
                logger.warning(f"MEXC {label} chunk {chunk_start.date()}-{chunk_end.date()} failed: {e}")
                chunk_start = chunk_end
                continue
            if not isinstance(raw, list):
                raw = raw.get("data", raw.get("result", []))
            if not isinstance(raw, list):
                raw = []
            for d in raw:
                results.append(row_mapper(d))
            if raw:
                logger.info(f"MEXC {label} chunk {chunk_start.date()}-{chunk_end.date()}: {len(raw)} rows")
            chunk_start = chunk_end

        return results

    async def fetch_deposits(self, since: datetime | None = None) -> list[dict]:
        def mapper(d):
            return {
                "exchange": self.name,
                "exchange_id": str(d.get("id", d.get("txId", ""))),
                "asset": d.get("coin", d.get("currency", "")),
                "amount": str(d.get("amount", "0")),
                "network": d.get("network", ""),
                "tx_hash": d.get("txId", ""),
                "address": d.get("address", ""),
                "status": str(d.get("status", "")),
                "asset_price_usd": None, "amount_usd": None,
                "confirmed_at": self._parse_ts(d.get("insertTime", d.get("completeTime"))),
                "raw_data": json.dumps(d),
            }
        return await self._fetch_chunked_history(
            "/api/v3/capital/deposit/hisrec", since, mapper, "deposits")

    async def fetch_withdrawals(self, since: datetime | None = None) -> list[dict]:
        def mapper(w):
            return {
                "exchange": self.name,
                "exchange_id": str(w.get("id", w.get("txId", ""))),
                "asset": w.get("coin", w.get("currency", "")),
                "amount": str(w.get("amount", "0")),
                "fee": str(w.get("transactionFee", w.get("fee", "0"))),
                "fee_asset": w.get("coin", w.get("currency", "")),
                "network": w.get("network", ""),
                "tx_hash": w.get("txId", ""),
                "address": w.get("address", ""),
                "status": str(w.get("status", "")),
                "asset_price_usd": None, "amount_usd": None, "fee_usd": None,
                "confirmed_at": self._parse_ts(w.get("completeTime", w.get("applyTime"))),
                "raw_data": json.dumps(w),
            }
        return await self._fetch_chunked_history(
            "/api/v3/capital/withdraw/history", since, mapper, "withdrawals")

    async def fetch_pool_activity(self, since: datetime | None = None) -> list[dict]:
        return []
