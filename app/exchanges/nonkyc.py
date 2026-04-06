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
            # market is ALWAYS a dict in the current API: {"id": "...", "symbol": "BASE/QUOTE"}
            # The string fallback is retained as a defensive measure only.
            market_raw = t.get("market", t.get("symbol", ""))
            if isinstance(market_raw, dict):
                market = market_raw.get("symbol", "")
            elif isinstance(market_raw, str):
                market = market_raw
            else:
                market = str(market_raw) if market_raw else ""

            price_str = str(t.get("price", "0"))
            quantity_str = str(t.get("quantity", t.get("base_volume", "0")))
            total_with_fee = t.get("totalWithFee", t.get("total", t.get("target_volume", "0")))
            # Fallback: if totalWithFee is zero/missing, compute from price * quantity
            if total_with_fee is not None and str(total_with_fee) not in ("", "0", "0.0", None):
                total_str = str(total_with_fee)
            else:
                try:
                    from decimal import Decimal as _D
                    total_str = str(_D(price_str) * _D(quantity_str))
                except Exception:
                    total_str = "0"

            trades.append({
                "exchange": self.name, "exchange_id": tid, "market": market,
                "base_asset": None, "quote_asset": None,
                "side": str(t.get("side", t.get("type", ""))).lower(),
                "price": price_str,
                "quantity": quantity_str,
                "total": total_str,
                "fee": str(t.get("fee", "0")),
                "fee_asset": t.get("feeAsset", t.get("fee_asset", t.get("alternateFeeAsset"))) or "",
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
            market_raw = o.get("market", o.get("symbol", ""))
            if isinstance(market_raw, dict):
                market = market_raw.get("symbol", "")
            elif isinstance(market_raw, str):
                market = market_raw
            else:
                market = str(market_raw) if market_raw else ""
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
                "external_tx_id": d.get("transactionid", d.get("txHash", d.get("txid", ""))),
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
                "external_tx_id": w.get("transactionid", w.get("txHash", w.get("txid", ""))),
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
        """Fetch PRIVATE account pool trade history (not public market data).

        Uses /getpooltrades which returns trade-style rows:
          {id, pool: {id, symbol}, side, price, quantity, fee, totalWithFee, createdAt}

        The pool.symbol (e.g. "SAL/USDT") + side determines asset flow:
          buy  = spent quote (USDT) to get base (SAL)
          sell = spent base (SAL) to get quote (USDT)
        """
        params = {}
        if since:
            params["since"] = str(int(since.timestamp() * 1000))
        try:
            raw = await self._get("/getpooltrades", params)
        except Exception as e:
            logger.warning(f"Pool trades fetch failed: {e}")
            return []
        if not isinstance(raw, list):
            raw = raw.get("result", raw.get("data", []))
        pools = []
        for p in raw:
            pool_obj = p.get("pool", {})
            pool_name = pool_obj.get("symbol", "") if isinstance(pool_obj, dict) else str(pool_obj)
            if not pool_name:
                logger.debug(f"Skipping pool trade with no pool symbol: {p.get('id', '?')}")
                continue

            parts = pool_name.split("/")
            if len(parts) != 2:
                logger.warning(f"Cannot parse pool symbol '{pool_name}', skipping")
                continue
            base_asset, quote_asset = parts[0], parts[1]

            side = p.get("side", "").lower()
            quantity = str(p.get("quantity", "0"))
            total_with_fee = str(p.get("totalWithFee", "0"))

            if side == "buy":
                asset_in, amount_in = quote_asset, total_with_fee
                asset_out, amount_out = base_asset, quantity
            elif side == "sell":
                asset_in, amount_in = base_asset, quantity
                asset_out, amount_out = quote_asset, total_with_fee
            else:
                logger.warning(f"Unknown pool trade side '{side}' for {p.get('id')}, skipping")
                continue

            # Validate: reject rows with zero amounts
            try:
                from decimal import Decimal
                if Decimal(amount_in) == 0 and Decimal(amount_out) == 0:
                    logger.debug(f"Skipping zero-amount pool trade {p.get('id')}")
                    continue
            except Exception:
                pass

            fee_str = str(p.get("fee", "0"))

            pools.append({
                "exchange": self.name,
                "exchange_id": str(p.get("id", "")),
                "pool_name": pool_name,
                "action": side,
                "asset_in": asset_in,
                "amount_in": amount_in,
                "asset_out": asset_out,
                "amount_out": amount_out,
                "fee": fee_str,
                "fee_asset": quote_asset,
                "amount_in_usd": None, "amount_out_usd": None, "fee_usd": None,
                "executed_at": self._parse_ts(p.get("createdAt")),
                "raw_data": json.dumps(p),
            })
        return pools
