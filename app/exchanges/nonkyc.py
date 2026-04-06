"""
NonKYC.io exchange plugin.

REST API: https://api.nonkyc.io/api/v2/
Auth: HMAC-SHA256 with X-API-KEY, X-API-NONCE, X-API-SIGN headers.
"""
import hashlib
import hmac
import json
import logging
import time
from datetime import datetime, timezone

import aiohttp

from exchanges import BaseExchange, register

logger = logging.getLogger("tax-collector.nonkyc")
BASE_URL = "https://api.nonkyc.io/api/v2"


@register
class NonKYCExchange(BaseExchange):
    name = "nonkyc"

    def _sign_get(self, url: str) -> dict:
        nonce = str(int(time.time() * 1000))
        message = self.api_key + url + nonce
        signature = hmac.new(
            self.api_secret.encode(), message.encode(), hashlib.sha256
        ).hexdigest()
        return {"X-API-KEY": self.api_key, "X-API-NONCE": nonce, "X-API-SIGN": signature}

    def _sign_post(self, url: str, body: str) -> dict:
        nonce = str(int(time.time() * 1000))
        message = self.api_key + url + body + nonce
        signature = hmac.new(
            self.api_secret.encode(), message.encode(), hashlib.sha256
        ).hexdigest()
        return {"X-API-KEY": self.api_key, "X-API-NONCE": nonce, "X-API-SIGN": signature,
                "Content-Type": "application/json"}

    async def _get(self, path: str, params: dict | None = None) -> list | dict:
        url = f"{BASE_URL}{path}"
        if params:
            query = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
            if query:
                url = f"{url}?{query}"
        headers = self._sign_get(url)
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                resp.raise_for_status()
                return await resp.json()

    def _parse_ts(self, val) -> datetime:
        if val is None:
            return datetime.now(timezone.utc)
        if isinstance(val, (int, float)):
            return datetime.fromtimestamp(val / 1000, tz=timezone.utc)
        if isinstance(val, str):
            try:
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            except ValueError:
                return datetime.fromtimestamp(float(val) / 1000, tz=timezone.utc)
        return datetime.now(timezone.utc)

    async def fetch_trades(self, since: datetime | None = None) -> list[dict]:
        params = {"limit": 1000, "sort": "ASC"}
        if since:
            params["since"] = str(int(since.timestamp() * 1000))
        raw = await self._get("/account/trades", params)
        if not isinstance(raw, list):
            raw = raw.get("result", raw.get("data", []))
        trades = []
        for t in raw:
            tid = str(t.get("id", t.get("trade_id", "")))
            market = t.get("symbol", t.get("market", ""))
            if isinstance(market, dict):
                market = market.get("symbol", str(market))
            trades.append({
                "exchange": self.name, "exchange_id": tid, "market": market,
                "base_asset": None, "quote_asset": None,
                "side": str(t.get("side", t.get("type", ""))).lower(),
                "price": str(t.get("price", "0")),
                "quantity": str(t.get("quantity", t.get("base_volume", "0"))),
                "total": str(t.get("total", t.get("target_volume", "0"))),
                "fee": str(t.get("fee", "0")),
                "fee_asset": t.get("feeAsset", t.get("fee_asset", "")) or "",
                "price_usd": None, "quantity_usd": None, "total_usd": None,
                "fee_usd": None, "base_price_usd": None, "quote_price_usd": None,
                "executed_at": self._parse_ts(
                    t.get("timestamp", t.get("trade_timestamp", t.get("createdAt")))),
                "raw_data": json.dumps(t),
            })
        return trades

    async def fetch_orders(self, since: datetime | None = None) -> list[dict]:
        params = {"limit": 1000}
        if since:
            params["since"] = str(int(since.timestamp() * 1000))
        raw = await self._get("/account/orders", params)
        if not isinstance(raw, list):
            raw = raw.get("result", raw.get("data", []))
        orders = []
        for o in raw:
            market = o.get("symbol", o.get("market", ""))
            if isinstance(market, dict):
                market = market.get("symbol", str(market))
            orders.append({
                "exchange": self.name,
                "exchange_id": str(o.get("id", o.get("orderId", ""))),
                "market": market, "base_asset": None, "quote_asset": None,
                "side": str(o.get("side", "")).lower(),
                "order_type": str(o.get("type", o.get("orderType", "limit"))).lower(),
                "price": str(o.get("price", "0")),
                "quantity": str(o.get("quantity", "0")),
                "executed_qty": str(o.get("executedQuantity", o.get("executed_qty", "0"))),
                "status": o.get("status", "unknown"),
                "price_usd": None, "total_usd": None, "fee_usd": None,
                "created_at_ex": self._parse_ts(o.get("createdAt", o.get("timestamp"))),
                "updated_at_ex": self._parse_ts(o.get("updatedAt")),
                "raw_data": json.dumps(o),
            })
        return orders

    async def fetch_deposits(self, since: datetime | None = None) -> list[dict]:
        params = {}
        if since:
            params["since"] = str(int(since.timestamp() * 1000))
        raw = await self._get("/getdeposits", params)
        if not isinstance(raw, list):
            raw = raw.get("result", raw.get("data", []))
        deposits = []
        for d in raw:
            deposits.append({
                "exchange": self.name,
                "exchange_id": str(d.get("id", d.get("depositId", ""))),
                "asset": d.get("ticker", d.get("asset", d.get("currency", ""))),
                "child_asset": d.get("childticker", ""),
                "amount": str(d.get("quantity", d.get("amount", "0"))),  # quantity FIRST (official API field)
                "network": d.get("network", ""),
                "tx_hash": d.get("transactionid", d.get("txHash", d.get("txid", ""))),  # transactionid FIRST
                "address": d.get("address", ""),
                "payment_id": d.get("paymentid", ""),
                "status": d.get("status", ""),
                "confirmations": d.get("confirmations"),
                "is_posted": d.get("isposted"),
                "is_reversed": d.get("isreversed"),
                "asset_price_usd": None, "amount_usd": None,
                "confirmed_at": self._parse_ts(
                    d.get("firstseenat", d.get("confirmedAt", d.get("timestamp", d.get("createdAt"))))),  # firstseenat FIRST
                "raw_data": json.dumps(d),
            })
        return deposits

    async def fetch_withdrawals(self, since: datetime | None = None) -> list[dict]:
        params = {}
        if since:
            params["since"] = str(int(since.timestamp() * 1000))
        raw = await self._get("/getwithdrawals", params)
        if not isinstance(raw, list):
            raw = raw.get("result", raw.get("data", []))
        withdrawals = []
        for w in raw:
            withdrawals.append({
                "exchange": self.name,
                "exchange_id": str(w.get("id", w.get("withdrawalId", ""))),
                "asset": w.get("ticker", w.get("asset", w.get("currency", ""))),
                "child_asset": w.get("childticker", ""),
                "amount": str(w.get("quantity", w.get("amount", "0"))),  # quantity FIRST (official API field)
                "fee": str(w.get("fee", "0")),
                "fee_currency": w.get("feecurrency", w.get("fee_asset", "")),
                "fee_asset": w.get("feecurrency", w.get("fee_asset", "")),
                "network": w.get("network", ""),
                "tx_hash": w.get("transactionid", w.get("txHash", w.get("txid", ""))),  # transactionid FIRST
                "address": w.get("address", ""),
                "payment_id": w.get("paymentid", ""),
                "status": w.get("status", ""),
                "is_sent": w.get("issent"),
                "is_confirmed": w.get("isconfirmed"),
                "asset_price_usd": None, "amount_usd": None, "fee_usd": None,
                "confirmed_at": self._parse_ts(
                    w.get("requestedat", w.get("sentat", w.get("confirmedAt", w.get("timestamp", w.get("createdAt")))))),  # requestedat FIRST
                "raw_data": json.dumps(w),
            })
        return withdrawals

    async def fetch_pool_activity(self, since: datetime | None = None) -> list[dict]:
        params = {"limit": 1000}
        if since:
            params["since"] = str(int(since.timestamp() * 1000))
        try:
            raw = await self._get("/pool/trades", params)
        except Exception as e:
            logger.warning(f"Pool trades fetch failed: {e}")
            return []
        if not isinstance(raw, list):
            raw = raw.get("result", raw.get("data", []))
        pools = []
        for p in raw:
            pools.append({
                "exchange": self.name,
                "exchange_id": str(p.get("id", p.get("tradeId", ""))),
                "pool_name": p.get("pool", p.get("market", p.get("symbol", ""))),
                "action": p.get("type", p.get("action", "swap")).lower(),
                "asset_in": p.get("assetIn", p.get("base", "")),
                "amount_in": str(p.get("amountIn", p.get("baseAmount", "0"))),
                "asset_out": p.get("assetOut", p.get("quote", "")),
                "amount_out": str(p.get("amountOut", p.get("quoteAmount", "0"))),
                "fee": str(p.get("fee", "0")),
                "fee_asset": p.get("feeAsset", ""),
                "amount_in_usd": None, "amount_out_usd": None, "fee_usd": None,
                "executed_at": self._parse_ts(p.get("timestamp", p.get("createdAt"))),
                "raw_data": json.dumps(p),
            })
        return pools
