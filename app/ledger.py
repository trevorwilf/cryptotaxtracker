"""
Normalized Event Ledger — double-entry decomposition of all raw records.

Addresses reviewer Issue 1: crypto-to-crypto trades must be decomposed
into a disposal of the given asset AND an acquisition of the received asset.

Every raw record produces one or more normalized events:
  - Trade (buy BTC/USDT):
      → DISPOSAL of USDT (quote asset given up)
      → ACQUISITION of BTC (base asset received)
      → FEE_DISPOSAL if fee is in a separate crypto asset
  - Trade (sell BTC/USDT):
      → DISPOSAL of BTC (base asset given up)
      → ACQUISITION of USDT (quote asset received)
  - Deposit (not matched as transfer):
      → UNRESOLVED (queued for manual classification)
  - Deposit (matched as transfer):
      → TRANSFER_IN
  - Deposit (confirmed income):
      → INCOME
  - Withdrawal (matched as transfer):
      → TRANSFER_OUT
  - Withdrawal (unmatched):
      → UNRESOLVED
  - Pool activity:
      → UNSUPPORTED (unless reward, which → INCOME candidate)

This module does NOT compute taxes. It normalizes raw data into a
consistent double-entry ledger that the tax engine then processes.
"""
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from exceptions import ExceptionManager, BLOCKING, WARNING, INFO
from exceptions import (TIMESTAMP_INVALID, UNSUPPORTED_TX, AMBIGUOUS_DEPOSIT,
                        CRYPTO_TO_CRYPTO, VALUATION_FALLBACK)

logger = logging.getLogger("tax-collector.ledger")

D = Decimal
ZERO = D("0")

# Stablecoins — treated as crypto (NOT fiat) for tax purposes,
# but flagged so the engine knows they're low-volatility
STABLECOINS = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "GUSD", "FRAX"}
# Note: UST intentionally REMOVED — it depegged and must use actual FMV


class NormalizedLedger:
    """Decomposes raw exchange data into double-entry normalized events."""

    def __init__(self, exc_manager: ExceptionManager):
        self.exc = exc_manager

    async def decompose_all(self, session: AsyncSession, run_id: int) -> dict:
        """Decompose all raw records into normalized events."""
        logger.info("Starting ledger normalization (double-entry decomposition)")

        # Clear previous normalized events for this run
        await session.execute(text("DELETE FROM tax.normalized_events WHERE run_id = :r"), {"r": run_id})

        stats = {"trades": 0, "deposits": 0, "withdrawals": 0, "pools": 0,
                 "events_created": 0, "errors": 0}

        # 1. Decompose trades
        stats["trades"], tc = await self._decompose_trades(session, run_id)
        stats["events_created"] += tc

        # 2. Decompose deposits (initially UNRESOLVED — classified later)
        stats["deposits"], dc = await self._decompose_deposits(session, run_id)
        stats["events_created"] += dc

        # 3. Decompose withdrawals (initially UNRESOLVED — classified later)
        stats["withdrawals"], wc = await self._decompose_withdrawals(session, run_id)
        stats["events_created"] += wc

        # 4. Decompose pool activity
        stats["pools"], pc = await self._decompose_pools(session, run_id)
        stats["events_created"] += pc

        logger.info(f"Ledger normalization complete: {stats}")
        return stats

    # ── Trades ────────────────────────────────────────────────────────────

    async def _decompose_trades(self, session: AsyncSession, run_id: int) -> tuple[int, int]:
        """Decompose each trade into disposal + acquisition events."""
        result = await session.execute(text("""
            SELECT id, exchange, market, base_asset, quote_asset, side,
                   price::text, quantity::text, total::text,
                   fee::text, fee_asset,
                   price_usd::text, quantity_usd::text, total_usd::text, fee_usd::text,
                   base_price_usd::text, quote_price_usd::text,
                   executed_at, raw_data
            FROM tax.trades
            ORDER BY executed_at ASC, id ASC
        """))
        rows = result.fetchall()
        cols = list(result.keys())

        trade_count = 0
        event_count = 0

        for row in rows:
            t = dict(zip(cols, row))
            trade_id = t["id"]
            wallet = t["exchange"]
            base = (t["base_asset"] or "").upper()
            quote = (t["quote_asset"] or "").upper()
            side = (t["side"] or "").lower()
            executed_at = t["executed_at"]

            # Validate timestamp
            if executed_at is None:
                self.exc.log(BLOCKING, TIMESTAMP_INVALID,
                             f"Trade {trade_id} has no timestamp — cannot be processed",
                             source_trade_id=trade_id, run_id=run_id)
                continue

            # Parse quantities safely
            qty = self._safe_decimal(t["quantity"])
            total = self._safe_decimal(t["total"])
            fee = self._safe_decimal(t["fee"])
            fee_asset = (t["fee_asset"] or "").upper()
            base_usd = self._safe_decimal(t["base_price_usd"])
            quote_usd = self._safe_decimal(t["quote_price_usd"])
            total_usd = self._safe_decimal(t["total_usd"])
            fee_usd = self._safe_decimal(t["fee_usd"])

            if side == "buy":
                # BUY base with quote:
                # Leg 1: ACQUISITION of base asset
                acq_id = await self._insert_event(session, run_id,
                    source_trade_id=trade_id, event_type="ACQUISITION",
                    wallet=wallet, asset=base,
                    quantity=qty, unit_price_usd=base_usd,
                    total_usd=self._safe_decimal(t["quantity_usd"]),
                    event_at=executed_at, raw_market=t["market"], raw_side=side,
                    classification_rule="buy-side base asset acquisition")

                # Leg 2: DISPOSAL of quote asset (the crypto/stablecoin you spent)
                disp_id = await self._insert_event(session, run_id,
                    source_trade_id=trade_id, event_type="DISPOSAL",
                    wallet=wallet, asset=quote,
                    quantity=total, unit_price_usd=quote_usd,
                    total_usd=total_usd,
                    event_at=executed_at, raw_market=t["market"], raw_side=side,
                    classification_rule="buy-side quote asset disposal (crypto-to-crypto)")

                # Pair the two legs
                await self._pair_events(session, acq_id, disp_id)
                event_count += 2

            elif side == "sell":
                # SELL base for quote:
                # Leg 1: DISPOSAL of base asset
                disp_id = await self._insert_event(session, run_id,
                    source_trade_id=trade_id, event_type="DISPOSAL",
                    wallet=wallet, asset=base,
                    quantity=qty, unit_price_usd=base_usd,
                    total_usd=self._safe_decimal(t["quantity_usd"]),
                    event_at=executed_at, raw_market=t["market"], raw_side=side,
                    classification_rule="sell-side base asset disposal")

                # Leg 2: ACQUISITION of quote asset (the crypto/stablecoin received)
                acq_id = await self._insert_event(session, run_id,
                    source_trade_id=trade_id, event_type="ACQUISITION",
                    wallet=wallet, asset=quote,
                    quantity=total, unit_price_usd=quote_usd,
                    total_usd=total_usd,
                    event_at=executed_at, raw_market=t["market"], raw_side=side,
                    classification_rule="sell-side quote asset acquisition")

                await self._pair_events(session, acq_id, disp_id)
                event_count += 2

            # Leg 3 (optional): Fee paid in crypto — separate disposal
            if fee and fee > ZERO and fee_asset and fee_asset not in ("", "USD"):
                fee_event_id = await self._insert_event(session, run_id,
                    source_trade_id=trade_id, event_type="FEE_DISPOSAL",
                    wallet=wallet, asset=fee_asset,
                    quantity=fee, unit_price_usd=None,
                    total_usd=fee_usd,
                    event_at=executed_at, raw_market=t["market"], raw_side=side,
                    classification_rule=f"crypto fee disposal ({fee} {fee_asset})")
                event_count += 1

            trade_count += 1

        return trade_count, event_count

    # ── Deposits ──────────────────────────────────────────────────────────

    async def _decompose_deposits(self, session: AsyncSession, run_id: int) -> tuple[int, int]:
        """All deposits start as UNRESOLVED — transfer matcher and income
        classifier will reclassify them later."""
        result = await session.execute(text("""
            SELECT id, exchange, asset, amount::text, amount_usd::text,
                   asset_price_usd::text, confirmed_at, tx_hash, raw_data
            FROM tax.deposits
            WHERE amount > 0
            ORDER BY confirmed_at ASC, id ASC
        """))
        rows = result.fetchall()
        cols = list(result.keys())
        count = 0
        events = 0

        for row in rows:
            d = dict(zip(cols, row))
            dep_id = d["id"]
            confirmed_at = d["confirmed_at"]

            if confirmed_at is None:
                self.exc.log(WARNING, TIMESTAMP_INVALID,
                             f"Deposit {dep_id} has no timestamp — using import time",
                             source_deposit_id=dep_id, run_id=run_id)
                confirmed_at = datetime.now(timezone.utc)

            await self._insert_event(session, run_id,
                source_deposit_id=dep_id, event_type="UNRESOLVED",
                wallet=d["exchange"], asset=(d["asset"] or "").upper(),
                quantity=self._safe_decimal(d["amount"]),
                unit_price_usd=self._safe_decimal(d["asset_price_usd"]),
                total_usd=self._safe_decimal(d["amount_usd"]),
                event_at=confirmed_at,
                classification_rule="deposit — pending classification")

            self.exc.log(INFO, AMBIGUOUS_DEPOSIT,
                         f"Deposit {dep_id}: {d['amount']} {d['asset']} on {d['exchange']} — needs classification",
                         source_deposit_id=dep_id, run_id=run_id)

            count += 1
            events += 1

        return count, events

    # ── Withdrawals ───────────────────────────────────────────────────────

    async def _decompose_withdrawals(self, session: AsyncSession, run_id: int) -> tuple[int, int]:
        """Withdrawals start as UNRESOLVED — transfer matcher classifies them."""
        result = await session.execute(text("""
            SELECT id, exchange, asset, amount::text, fee::text,
                   amount_usd::text, fee_usd::text,
                   asset_price_usd::text, confirmed_at, tx_hash, raw_data
            FROM tax.withdrawals
            WHERE amount > 0
            ORDER BY confirmed_at ASC, id ASC
        """))
        rows = result.fetchall()
        cols = list(result.keys())
        count = 0
        events = 0

        for row in rows:
            w = dict(zip(cols, row))
            wd_id = w["id"]
            confirmed_at = w["confirmed_at"]

            if confirmed_at is None:
                self.exc.log(WARNING, TIMESTAMP_INVALID,
                             f"Withdrawal {wd_id} has no timestamp",
                             source_withdrawal_id=wd_id, run_id=run_id)
                confirmed_at = datetime.now(timezone.utc)

            await self._insert_event(session, run_id,
                source_withdrawal_id=wd_id, event_type="UNRESOLVED",
                wallet=w["exchange"], asset=(w["asset"] or "").upper(),
                quantity=self._safe_decimal(w["amount"]),
                unit_price_usd=self._safe_decimal(w["asset_price_usd"]),
                total_usd=self._safe_decimal(w["amount_usd"]),
                event_at=confirmed_at,
                classification_rule="withdrawal — pending classification")
            events += 1

            # Fix 13: Create FEE_DISPOSAL for crypto withdrawal fees
            fee = self._safe_decimal(w.get("fee", "0"))
            if fee > ZERO:
                fee_asset = (w.get("fee_asset") or w["asset"] or "").upper()
                await self._insert_event(session, run_id,
                    source_withdrawal_id=wd_id, event_type="FEE_DISPOSAL",
                    wallet=w["exchange"], asset=fee_asset,
                    quantity=fee,
                    total_usd=self._safe_decimal(w.get("fee_usd")),
                    event_at=confirmed_at,
                    classification_rule=f"network fee on withdrawal ({fee} {fee_asset})")
                events += 1

            count += 1

        return count, events

    # ── Pool Activity ─────────────────────────────────────────────────────

    async def _decompose_pools(self, session: AsyncSession, run_id: int) -> tuple[int, int]:
        """Pool activity is mostly UNSUPPORTED except rewards."""
        result = await session.execute(text("""
            SELECT id, exchange, pool_name, action, asset_in, amount_in::text,
                   asset_out, amount_out::text, fee::text, fee_asset,
                   amount_in_usd::text, amount_out_usd::text, fee_usd::text,
                   executed_at
            FROM tax.pool_activity
            ORDER BY executed_at ASC, id ASC
        """))
        rows = result.fetchall()
        cols = list(result.keys())
        count = 0
        events = 0

        for row in rows:
            p = dict(zip(cols, row))
            pool_id = p["id"]
            action = (p["action"] or "").lower()

            if action == "reward":
                # Pool rewards → INCOME candidate (still needs review)
                await self._insert_event(session, run_id,
                    source_pool_id=pool_id, event_type="INCOME",
                    wallet=p["exchange"], asset=(p["asset_out"] or "").upper(),
                    quantity=self._safe_decimal(p["amount_out"]),
                    total_usd=self._safe_decimal(p["amount_out_usd"]),
                    event_at=p["executed_at"],
                    classification_rule=f"pool reward from {p['pool_name']}")
                events += 1
            else:
                # Swaps, adds, removes → UNSUPPORTED
                await self._insert_event(session, run_id,
                    source_pool_id=pool_id, event_type="UNSUPPORTED",
                    wallet=p["exchange"], asset=(p["asset_in"] or p["asset_out"] or "").upper(),
                    quantity=self._safe_decimal(p["amount_in"] or p["amount_out"] or "0"),
                    total_usd=self._safe_decimal(p["amount_in_usd"] or p["amount_out_usd"]),
                    event_at=p["executed_at"],
                    classification_rule=f"pool {action} — unsupported tx type")

                self.exc.log(BLOCKING, UNSUPPORTED_TX,
                             f"Pool {action} on {p['pool_name']} — tax treatment not implemented",
                             source_event_id=pool_id, run_id=run_id,
                             blocks_filing=True)
                events += 1

            count += 1

        return count, events

    # ── Internal helpers ──────────────────────────────────────────────────

    async def _insert_event(self, session, run_id, source_trade_id=None,
                            source_deposit_id=None, source_withdrawal_id=None,
                            source_pool_id=None, event_type="UNRESOLVED",
                            wallet="", asset="", quantity=ZERO,
                            unit_price_usd=None, total_usd=None,
                            event_at=None, raw_market=None, raw_side=None,
                            classification_rule=None) -> int:
        """Insert a normalized event and return its ID."""
        result = await session.execute(text("""
            INSERT INTO tax.normalized_events
                (source_trade_id, source_deposit_id, source_withdrawal_id, source_pool_id,
                 event_type, wallet, asset, quantity,
                 unit_price_usd, total_usd, event_at,
                 raw_market, raw_side, classification_rule, run_id)
            VALUES
                (:stid, :sdid, :swid, :spid,
                 :etype, :wallet, :asset, :qty,
                 :upu, :tusd, :eat,
                 :rm, :rs, :cr, :rid)
            RETURNING id
        """), {
            "stid": source_trade_id, "sdid": source_deposit_id,
            "swid": source_withdrawal_id, "spid": source_pool_id,
            "etype": event_type, "wallet": wallet, "asset": asset,
            "qty": str(quantity), "upu": str(unit_price_usd) if unit_price_usd else None,
            "tusd": str(total_usd) if total_usd else None,
            "eat": event_at, "rm": raw_market, "rs": raw_side,
            "cr": classification_rule, "rid": run_id,
        })
        row = result.fetchone()
        return row[0] if row else 0

    async def _pair_events(self, session, event_id_a: int, event_id_b: int):
        """Link two events as paired legs of the same trade."""
        await session.execute(text(
            "UPDATE tax.normalized_events SET paired_event_id = :b WHERE id = :a"),
            {"a": event_id_a, "b": event_id_b})
        await session.execute(text(
            "UPDATE tax.normalized_events SET paired_event_id = :a WHERE id = :b"),
            {"a": event_id_a, "b": event_id_b})

    @staticmethod
    def _safe_decimal(val) -> Decimal:
        if val is None:
            return ZERO
        try:
            return D(str(val))
        except (InvalidOperation, ValueError):
            return ZERO
