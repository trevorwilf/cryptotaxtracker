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
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

import aiohttp

from exchanges import BaseExchange, register

logger = logging.getLogger("tax-collector.mexc")
BASE_URL = "https://api.mexc.com"


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
            return datetime.now(timezone.utc)
        if isinstance(val, (int, float)):
            return datetime.fromtimestamp(val / 1000, tz=timezone.utc)
        return datetime.now(timezone.utc)

    async def _get_traded_symbols(self) -> list[str]:
        """Discover symbols by checking account balances for non-zero assets."""
        symbols = set()
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

        if not symbols:
            logger.warning("No traded symbols found on MEXC — skipping trade fetch")
        return list(symbols)

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
                        "total": str(float(t.get("price", 0)) * float(t.get("qty", 0))),
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

    async def fetch_deposits(self, since: datetime | None = None) -> list[dict]:
        params = {"limit": 1000}
        if since:
            params["startTime"] = str(int(since.timestamp() * 1000))
        try:
            raw = await self._get("/api/v3/capital/deposit/hisrec", params)
        except Exception as e:
            logger.warning(f"Deposit history fetch failed: {e}")
            return []
        if not isinstance(raw, list):
            raw = raw.get("data", raw.get("result", []))
        if not isinstance(raw, list):
            return []
        deposits = []
        for d in raw:
            deposits.append({
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
            })
        return deposits

    async def fetch_withdrawals(self, since: datetime | None = None) -> list[dict]:
        params = {"limit": 1000}
        if since:
            params["startTime"] = str(int(since.timestamp() * 1000))
        try:
            raw = await self._get("/api/v3/capital/withdraw/history", params)
        except Exception as e:
            logger.warning(f"Withdrawal history fetch failed: {e}")
            return []
        if not isinstance(raw, list):
            raw = raw.get("data", raw.get("result", []))
        if not isinstance(raw, list):
            return []
        withdrawals = []
        for w in raw:
            withdrawals.append({
                "exchange": self.name,
                "exchange_id": str(w.get("id", w.get("txId", ""))),
                "asset": w.get("coin", w.get("currency", "")),
                "amount": str(w.get("amount", "0")),
                "fee": str(w.get("transactionFee", w.get("fee", "0"))),
                "network": w.get("network", ""),
                "tx_hash": w.get("txId", ""),
                "address": w.get("address", ""),
                "status": str(w.get("status", "")),
                "asset_price_usd": None, "amount_usd": None, "fee_usd": None,
                "confirmed_at": self._parse_ts(w.get("completeTime", w.get("applyTime"))),
                "raw_data": json.dumps(w),
            })
        return withdrawals

    async def fetch_pool_activity(self, since: datetime | None = None) -> list[dict]:
        return []
