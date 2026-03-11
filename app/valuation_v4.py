"""
Audit-Grade Valuation — v4 pricing with full provenance.

Every price lookup creates a valuation_log record.
UST removed from stablecoin list (depegged).
Current-price fallback NEVER silently used for historical data.
Missing historical price → BLOCKING exception, price_usd = NULL.

Price source hierarchy:
  1. Stablecoin controlled-peg (not UST) → $1.00 with source='stablecoin_peg'
  2. CoinGecko historical daily → source='coingecko'
  3. Missing → MISSING_PRICE BLOCKING exception, price_usd = NULL
  4. Manual override → source='manual', is_manual=TRUE
"""
import logging
from datetime import datetime, date, timezone
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from exceptions import ExceptionManager, BLOCKING, WARNING
from exceptions import MISSING_PRICE, VALUATION_FALLBACK

logger = logging.getLogger("tax-collector.valuation-v4")

D = Decimal
ZERO = D("0")

# Stablecoins — UST intentionally REMOVED (it depegged May 2022)
STABLECOINS_V4 = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "GUSD", "FRAX",
                  "LUSD", "SUSD", "USDD", "CUSD", "USD"}

# Reuse CoinGecko mapping from price_oracle
from price_oracle import TICKER_TO_COINGECKO, PriceOracle


class ValuationV4:
    """Audit-grade valuation with full provenance tracking."""

    def __init__(self, exc_manager: ExceptionManager):
        self.exc = exc_manager
        self._oracle = PriceOracle()

    async def get_price(self, session: AsyncSession, asset: str,
                        event_at: datetime, run_id: int = None,
                        source_event_id: int = None) -> tuple[Decimal | None, int | None]:
        """
        Get USD price for an asset at a specific time.
        Returns (price, valuation_log_id).
        Creates a valuation_log record for every lookup.
        """
        ticker = asset.strip().upper() if asset else ""
        if not ticker:
            return None, None

        price_date = event_at.date() if isinstance(event_at, datetime) else event_at

        # 1. Stablecoin controlled-peg (not UST)
        if ticker in STABLECOINS_V4:
            price = D("1.0")
            val_id = await self._log_valuation(
                session, ticker, event_at, price_date, price,
                source_name="stablecoin_peg", granularity="fixed",
                is_estimated=True, run_id=run_id)
            return price, val_id

        # 2. Check DB cache first (from price_oracle)
        cached = await self._get_cached(session, ticker, price_date)
        if cached is not None:
            val_id = await self._log_valuation(
                session, ticker, event_at, price_date, cached,
                source_name="coingecko", granularity="daily",
                is_estimated=False, run_id=run_id)
            return cached, val_id

        # 3. CoinGecko historical daily
        price = await self._oracle._fetch_coingecko(ticker, price_date)
        if price is not None:
            # Cache it
            await self._oracle._set_cached(session, ticker, price_date, price, "coingecko")
            val_id = await self._log_valuation(
                session, ticker, event_at, price_date, price,
                source_name="coingecko", granularity="daily",
                is_estimated=False, run_id=run_id)
            return price, val_id

        # 4. Missing → BLOCKING exception, price_usd = NULL
        # NEVER use current price as fallback for historical data
        self.exc.log(BLOCKING, MISSING_PRICE,
                     f"No historical USD price for {ticker} on {price_date}",
                     detail=f"CoinGecko lookup failed. Manual price entry required.",
                     source_event_id=source_event_id,
                     tax_year=event_at.year if isinstance(event_at, datetime) else None,
                     run_id=run_id)

        val_id = await self._log_valuation(
            session, ticker, event_at, price_date, None,
            source_name="missing", granularity=None,
            is_estimated=False, fallback_reason="No historical price available",
            run_id=run_id)

        return None, val_id

    async def get_manual_price(self, session: AsyncSession, asset: str,
                               event_at: datetime, price_usd: Decimal,
                               notes: str = None, run_id: int = None) -> int:
        """Record a manually-set price. Returns valuation_log_id."""
        ticker = asset.strip().upper()
        price_date = event_at.date() if isinstance(event_at, datetime) else event_at

        val_id = await self._log_valuation(
            session, ticker, event_at, price_date, price_usd,
            source_name="manual", granularity="manual",
            is_estimated=False, is_manual=True,
            fallback_reason=notes, run_id=run_id)

        # Also update the cache so future lookups find it
        await self._oracle._set_cached(session, ticker, price_date, price_usd, "manual")

        return val_id

    async def _log_valuation(self, session: AsyncSession, asset: str,
                             event_at: datetime, price_date, price_usd: Decimal | None,
                             source_name: str, source_id: str = None,
                             source_timestamp: datetime = None,
                             granularity: str = None, is_estimated: bool = False,
                             is_manual: bool = False, fallback_reason: str = None,
                             run_id: int = None) -> int:
        """Insert a valuation_log record and return its ID."""
        result = await session.execute(text("""
            INSERT INTO tax.valuation_log
                (asset, event_at, price_date, price_usd, source_name, source_id,
                 source_timestamp, granularity, is_estimated, is_manual,
                 fallback_reason, run_id)
            VALUES
                (:asset, :event_at, :price_date, :price_usd, :source_name, :source_id,
                 :source_timestamp, :granularity, :is_estimated, :is_manual,
                 :fallback_reason, :run_id)
            RETURNING id
        """), {
            "asset": asset,
            "event_at": event_at,
            "price_date": price_date,
            "price_usd": str(price_usd) if price_usd is not None else None,
            "source_name": source_name,
            "source_id": source_id,
            "source_timestamp": source_timestamp,
            "granularity": granularity,
            "is_estimated": is_estimated,
            "is_manual": is_manual,
            "fallback_reason": fallback_reason,
            "run_id": run_id,
        })
        row = result.fetchone()
        return row[0] if row else 0

    async def _get_cached(self, session: AsyncSession, ticker: str, price_date) -> Decimal | None:
        """Check the price cache."""
        result = await session.execute(
            text("SELECT price_usd FROM tax.price_cache WHERE asset = :a AND price_date = :d"),
            {"a": ticker, "d": price_date},
        )
        row = result.fetchone()
        if row and row[0]:
            return D(str(row[0]))
        return None
