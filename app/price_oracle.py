"""
Price Oracle — historical USD price resolution for crypto assets.

Lookup chain:
  1. Stablecoin shortcut (USDT, USDC, DAI, etc. → $1.00)
  2. PostgreSQL cache (tax.price_cache)
  3. CoinGecko free API (/api/v3/coins/{id}/history)
  4. NonKYC asset/info endpoint (usdValue field)
  5. Falls back to None (marked for manual review)

All resolved prices are cached in tax.price_cache so we never re-fetch
the same (asset, date) pair twice.
"""
import asyncio
import logging
from datetime import datetime, date, timezone
from decimal import Decimal

import aiohttp
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("tax-collector.price-oracle")

# ── Stablecoins that peg to $1.00 ────────────────────────────────────────

STABLECOINS = {
    "USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "GUSD", "FRAX",
    "LUSD", "SUSD", "USDD", "CUSD", "USD",
}
# NOTE: UST removed — depegged May 2022, must use actual market FMV

# ── Common ticker → CoinGecko ID mapping ────────────────────────────────
# CoinGecko uses slugs, not tickers. This covers the most common assets.
# Unknown tickers are resolved via CoinGecko's /search endpoint.

TICKER_TO_COINGECKO: dict[str, str] = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "BNB": "binancecoin",
    "SOL": "solana",
    "XRP": "ripple",
    "ADA": "cardano",
    "DOGE": "dogecoin",
    "DOT": "polkadot",
    "MATIC": "matic-network",
    "POL": "matic-network",
    "AVAX": "avalanche-2",
    "SHIB": "shiba-inu",
    "LTC": "litecoin",
    "LINK": "chainlink",
    "UNI": "uniswap",
    "ATOM": "cosmos",
    "XLM": "stellar",
    "ALGO": "algorand",
    "FIL": "filecoin",
    "VET": "vechain",
    "ICP": "internet-computer",
    "NEAR": "near",
    "APT": "aptos",
    "ARB": "arbitrum",
    "OP": "optimism",
    "SUI": "sui",
    "TRX": "tron",
    "HBAR": "hedera-hashgraph",
    "FTM": "fantom",
    "AAVE": "aave",
    "MKR": "maker",
    "CRV": "curve-dao-token",
    "SAND": "the-sandbox",
    "MANA": "decentraland",
    "AXS": "axie-infinity",
    "PEPE": "pepe",
    "WIF": "dogwifcoin",
    "BONK": "bonk",
    "FLOKI": "floki",
    "INJ": "injective-protocol",
    "TIA": "celestia",
    "SEI": "sei-network",
    "RENDER": "render-token",
    "FET": "fetch-ai",
    "JASMY": "jasmycoin",
    "KAS": "kaspa",
    "XMR": "monero",
    "ETC": "ethereum-classic",
    "BCH": "bitcoin-cash",
    "CRO": "crypto-com-chain",
    "ENS": "ethereum-name-service",
    "GRT": "the-graph",
    "RUNE": "thorchain",
    "STX": "blockstack",
    "IMX": "immutable-x",
    "WLD": "worldcoin-wld",
    "MX": "mx-token",
    "SAL": "salvium",
}

# Runtime cache for CoinGecko ID lookups (avoids repeated /search calls)
_cg_id_cache: dict[str, str | None] = {}

# Rate limit: CoinGecko free tier allows ~10-30 req/min
_CG_DELAY = 2.5  # seconds between requests


class PriceOracle:
    """Resolves historical USD prices for crypto assets."""

    def __init__(self, nonkyc_base_url: str = "https://api.nonkyc.io/api/v2"):
        self.nonkyc_url = nonkyc_base_url

    # ── Public interface ──────────────────────────────────────────────────

    async def get_usd_price(
        self, session: AsyncSession, asset: str, at_time: datetime
    ) -> Decimal | None:
        """
        Get the USD price of `asset` at `at_time`.
        Returns Decimal price or None if unavailable.
        """
        ticker = self._normalize_ticker(asset)
        if not ticker:
            return None

        # 1. Stablecoin shortcut
        if ticker in STABLECOINS:
            return Decimal("1.0")

        # 2. Check DB cache
        price_date = at_time.date() if isinstance(at_time, datetime) else at_time
        cached = await self._get_cached(session, ticker, price_date)
        if cached is not None:
            return cached

        # 3. CoinGecko historical
        price = await self._fetch_coingecko(ticker, price_date)

        # 4. NonKYC asset fallback (current price, not historical — better than nothing)
        used_nonkyc = False
        if price is None:
            price = await self._fetch_nonkyc(ticker)
            if price is not None:
                used_nonkyc = True

        # 5. Cache whatever we got (even None → stored as 0 to avoid re-fetching)
        if price is not None:
            if used_nonkyc:
                await self._set_cached(session, ticker, price_date, price, "nonkyc_current_fallback")
                logger.warning(f"Used NonKYC CURRENT price for {ticker} on {price_date} — not historical")
            else:
                await self._set_cached(session, ticker, price_date, price, "coingecko")
        else:
            logger.warning(f"No USD price found for {ticker} on {price_date}")

        return price

    async def resolve_trade_usd(
        self, session: AsyncSession, market: str, side: str,
        price: str, quantity: str, total: str, fee: str, fee_asset: str,
        at_time: datetime
    ) -> dict:
        """
        Given a trade, resolve all USD values.
        Returns dict with: base_asset, quote_asset, price_usd, quantity_usd,
                          total_usd, fee_usd, base_price_usd, quote_price_usd
        """
        base, quote = self._parse_market(market)

        # Get USD prices for both sides of the pair
        base_usd = await self.get_usd_price(session, base, at_time)
        quote_usd = await self.get_usd_price(session, quote, at_time)

        result = {
            "base_asset": base,
            "quote_asset": quote,
            "base_price_usd": str(base_usd) if base_usd else None,
            "quote_price_usd": str(quote_usd) if quote_usd else None,
            "price_usd": None,
            "quantity_usd": None,
            "total_usd": None,
            "fee_usd": None,
        }

        try:
            qty = Decimal(str(quantity)) if quantity else Decimal("0")
            ttl = Decimal(str(total)) if total else Decimal("0")
            fee_val = Decimal(str(fee)) if fee else Decimal("0")

            # price_usd = what 1 unit of the BASE asset costs in USD
            if base_usd:
                result["price_usd"] = str(base_usd)
                result["quantity_usd"] = str(qty * base_usd)

            # total_usd = quote amount * quote USD price
            if quote_usd and ttl:
                result["total_usd"] = str(ttl * quote_usd)
            elif base_usd and qty:
                result["total_usd"] = str(qty * base_usd)

            # fee_usd depends on what asset the fee is in
            if fee_val and fee_asset:
                fee_ticker = self._normalize_ticker(fee_asset)
                if fee_ticker == base:
                    if base_usd:
                        result["fee_usd"] = str(fee_val * base_usd)
                elif fee_ticker == quote:
                    if quote_usd:
                        result["fee_usd"] = str(fee_val * quote_usd)
                else:
                    fee_price = await self.get_usd_price(session, fee_asset, at_time)
                    if fee_price:
                        result["fee_usd"] = str(fee_val * fee_price)

        except Exception as e:
            logger.warning(f"USD calc error for {market}: {e}")

        return result

    async def resolve_transfer_usd(
        self, session: AsyncSession, asset: str, amount: str,
        fee: str | None, at_time: datetime
    ) -> dict:
        """Resolve USD values for a deposit or withdrawal."""
        price = await self.get_usd_price(session, asset, at_time)
        result = {"asset_price_usd": None, "amount_usd": None, "fee_usd": None}

        if price:
            result["asset_price_usd"] = str(price)
            try:
                amt = Decimal(str(amount)) if amount else Decimal("0")
                result["amount_usd"] = str(amt * price)
                if fee:
                    fee_val = Decimal(str(fee))
                    result["fee_usd"] = str(fee_val * price)
            except Exception as e:
                logger.warning(f"USD calc error for {asset}: {e}")

        return result

    # ── CoinGecko ─────────────────────────────────────────────────────────

    async def _resolve_coingecko_id(self, ticker: str) -> str | None:
        """Map a ticker to a CoinGecko coin ID."""
        if ticker in TICKER_TO_COINGECKO:
            return TICKER_TO_COINGECKO[ticker]

        if ticker in _cg_id_cache:
            return _cg_id_cache[ticker]

        # Search CoinGecko
        try:
            await asyncio.sleep(_CG_DELAY)
            async with aiohttp.ClientSession() as http:
                async with http.get(
                    f"https://api.coingecko.com/api/v3/search",
                    params={"query": ticker},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        _cg_id_cache[ticker] = None
                        return None
                    data = await resp.json()
                    coins = data.get("coins", [])
                    # Find exact ticker match
                    for coin in coins:
                        if coin.get("symbol", "").upper() == ticker:
                            cg_id = coin["id"]
                            _cg_id_cache[ticker] = cg_id
                            TICKER_TO_COINGECKO[ticker] = cg_id
                            logger.info(f"Resolved {ticker} → CoinGecko ID: {cg_id}")
                            return cg_id
                    _cg_id_cache[ticker] = None
                    return None
        except Exception as e:
            logger.warning(f"CoinGecko search failed for {ticker}: {e}")
            _cg_id_cache[ticker] = None
            return None

    async def _fetch_coingecko(self, ticker: str, price_date: date) -> Decimal | None:
        """Fetch historical daily price from CoinGecko."""
        cg_id = await self._resolve_coingecko_id(ticker)
        if not cg_id:
            return None

        date_str = price_date.strftime("%d-%m-%Y")
        try:
            await asyncio.sleep(_CG_DELAY)
            async with aiohttp.ClientSession() as http:
                async with http.get(
                    f"https://api.coingecko.com/api/v3/coins/{cg_id}/history",
                    params={"date": date_str, "localization": "false"},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status == 429:
                        logger.warning("CoinGecko rate limited — will retry later")
                        return None
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    market_data = data.get("market_data", {})
                    usd_price = market_data.get("current_price", {}).get("usd")
                    if usd_price is not None:
                        return Decimal(str(usd_price))
                    return None
        except Exception as e:
            logger.warning(f"CoinGecko history failed for {ticker} on {date_str}: {e}")
            return None

    # ── NonKYC fallback ───────────────────────────────────────────────────

    async def _fetch_nonkyc(self, ticker: str) -> Decimal | None:
        """Fetch current USD value from NonKYC asset/info endpoint."""
        try:
            async with aiohttp.ClientSession() as http:
                async with http.get(
                    f"{self.nonkyc_url}/asset/info",
                    params={"ticker": ticker},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    usd_val = data.get("usdValue")
                    if usd_val:
                        return Decimal(str(usd_val))
                    return None
        except Exception as e:
            logger.debug(f"NonKYC asset fallback failed for {ticker}: {e}")
            return None

    # ── DB cache ──────────────────────────────────────────────────────────

    async def _get_cached(self, session: AsyncSession, ticker: str, price_date: date) -> Decimal | None:
        result = await session.execute(
            text("SELECT price_usd FROM tax.price_cache WHERE asset = :a AND price_date = :d"),
            {"a": ticker, "d": price_date},
        )
        row = result.fetchone()
        if row and row[0]:
            return Decimal(str(row[0]))
        return None

    async def _set_cached(self, session: AsyncSession, ticker: str, price_date: date,
                          price: Decimal, source: str):
        await session.execute(
            text("""
                INSERT INTO tax.price_cache (asset, price_date, price_usd, source)
                VALUES (:a, :d, :p, :s)
                ON CONFLICT (asset, price_date) DO UPDATE SET
                    price_usd = EXCLUDED.price_usd,
                    source = EXCLUDED.source,
                    updated_at = NOW()
            """),
            {"a": ticker, "d": price_date, "p": str(price), "s": source},
        )

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _normalize_ticker(raw: str) -> str:
        """Clean up a ticker symbol."""
        if not raw:
            return ""
        return raw.strip().upper().replace(" ", "")

    @staticmethod
    def _parse_market(market: str) -> tuple[str, str]:
        """
        Parse a market symbol into (base, quote).
        Handles: BTC/USDT, BTC_USDT, BTCUSDT
        """
        market = market.strip().upper()
        for sep in ("/", "_", "-"):
            if sep in market:
                parts = market.split(sep, 1)
                return parts[0].strip(), parts[1].strip()

        # No separator — try common quote suffixes
        for quote in ("USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "BNB"):
            if market.endswith(quote) and len(market) > len(quote):
                return market[:-len(quote)], quote

        return market, "USD"
