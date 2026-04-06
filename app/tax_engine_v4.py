"""
Tax Engine v4 — Wallet-Aware FIFO with Filing-Grade Output.

Processes the normalized event ledger (not raw trades).
Lots tracked per (wallet, asset) — FIFO within each wallet.

Addresses reviewer Issues 1-3:
  1. Double-entry events (from ledger.py)
  2. Lot identity preserved through transfers (from transfer_matcher_v4.py)
  3. Per-wallet FIFO (this module)

Key IRS rules:
  - Long-term = held MORE THAN one year (>365 days, NOT >=365)
  - Zero basis = BLOCKING exception
  - Deterministic sort: ORDER BY original_acquired_at ASC, id ASC
  - Form 8949 box selection configurable (B/D default for pre-2025)
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from exceptions import ExceptionManager, BLOCKING, WARNING, INFO
from exceptions import UNKNOWN_BASIS, OVERSOLD
from valuation_v4 import ValuationV4

logger = logging.getLogger("tax-collector.tax-engine-v4")

D = Decimal
ZERO = D("0")
TWO_PLACES = D("0.01")


@dataclass
class LotV4:
    """In-memory lot for FIFO processing."""
    id: int
    asset: str
    wallet: str
    original_quantity: Decimal
    remaining: Decimal
    cost_per_unit_usd: Decimal | None
    original_acquired_at: datetime
    source_type: str = ""
    parent_lot_id: int | None = None

    @property
    def is_depleted(self) -> bool:
        return self.remaining <= ZERO


@dataclass
class DisposalV4:
    """Record of a lot consumption for a disposal event."""
    asset: str
    wallet: str
    quantity: Decimal
    proceeds_usd: Decimal | None
    fee_usd: Decimal | None
    cost_basis_usd: Decimal | None
    gain_loss_usd: Decimal | None
    original_acquired_at: datetime
    disposed_at: datetime
    holding_days: int
    term: str  # 'short' or 'long'
    disposal_event_id: int
    lot_id: int
    source_trade_id: int | None = None
    market: str = ""
    exchange: str = ""
    disposal_db_id: int | None = None


class TaxEngineV4:
    """Wallet-aware FIFO engine processing normalized events."""

    def __init__(self, exc_manager: ExceptionManager,
                 valuation: ValuationV4,
                 default_box_short: str = "B",
                 default_box_long: str = "D"):
        self.exc = exc_manager
        self.valuation = valuation
        self.default_box_short = default_box_short
        self.default_box_long = default_box_long

    async def compute(self, session: AsyncSession,
                      run_id: int, year: int = None) -> dict:
        """
        Run the full FIFO computation pipeline.

        1. Create lots from ACQUISITION events
        2. Create lots from confirmed INCOME events
        3. TRANSFER_IN lots already created by transfer_matcher_v4
        4. Process DISPOSAL + FEE_DISPOSAL events in chronological order
        5. Generate Form 8949 lines
        6. Check filing readiness
        """
        stats = {"lots_created": 0, "disposals_processed": 0,
                 "form_8949_lines": 0, "short_term_gains": ZERO,
                 "short_term_losses": ZERO, "long_term_gains": ZERO,
                 "long_term_losses": ZERO}

        # Clear previous v4 computation for this run
        # IMPORTANT: preserve transfer_in lots created by transfer matcher
        await session.execute(text(
            "DELETE FROM tax.form_8949_v4 WHERE run_id = :r"), {"r": run_id})
        await session.execute(text(
            "DELETE FROM tax.disposals_v4 WHERE run_id = :r"), {"r": run_id})
        await session.execute(text(
            "DELETE FROM tax.lots_v4 WHERE run_id = :r AND source_type NOT IN ('transfer_in')"),
            {"r": run_id})

        # 1. Create lots from ACQUISITION events
        acq_count = await self._create_acquisition_lots(session, run_id)
        stats["lots_created"] += acq_count

        # 2. Create lots from confirmed INCOME events
        inc_count = await self._create_income_lots(session, run_id)
        stats["lots_created"] += inc_count

        # 3. TRANSFER_IN lots already created by transfer_matcher_v4
        # (they have source_type='transfer_in' in lots_v4)

        # 4. Process DISPOSAL + FEE_DISPOSAL events
        disposals = await self._process_disposals(session, run_id, year)
        stats["disposals_processed"] = len(disposals)

        # 5. Generate Form 8949 lines
        for disp in disposals:
            await self._insert_form_8949(session, disp, run_id)
            stats["form_8949_lines"] += 1

            if disp.gain_loss_usd is not None:
                if disp.term == "short":
                    if disp.gain_loss_usd >= ZERO:
                        stats["short_term_gains"] += disp.gain_loss_usd
                    else:
                        stats["short_term_losses"] += disp.gain_loss_usd
                else:
                    if disp.gain_loss_usd >= ZERO:
                        stats["long_term_gains"] += disp.gain_loss_usd
                    else:
                        stats["long_term_losses"] += disp.gain_loss_usd

        # Convert to serializable strings
        result = {
            "lots_created": stats["lots_created"],
            "disposals_processed": stats["disposals_processed"],
            "form_8949_lines": stats["form_8949_lines"],
            "short_term_gains": str(stats["short_term_gains"]),
            "short_term_losses": str(stats["short_term_losses"]),
            "long_term_gains": str(stats["long_term_gains"]),
            "long_term_losses": str(stats["long_term_losses"]),
            "net_total": str(stats["short_term_gains"] + stats["short_term_losses"] +
                            stats["long_term_gains"] + stats["long_term_losses"]),
            "filing_ready": not self.exc.has_blocking,
        }

        logger.info(f"Tax computation complete: {result}")
        return result

    async def _create_acquisition_lots(self, session: AsyncSession,
                                       run_id: int) -> int:
        """Create lots from ACQUISITION normalized events."""
        # Don't recreate lots that already exist for this run
        result = await session.execute(text("""
            SELECT ne.id, ne.wallet, ne.asset, ne.quantity::text,
                   ne.unit_price_usd::text, ne.total_usd::text,
                   ne.event_at, ne.source_trade_id
            FROM tax.normalized_events ne
            WHERE ne.event_type = 'ACQUISITION'
              AND ne.run_id = :run_id
              AND NOT EXISTS (
                  SELECT 1 FROM tax.lots_v4 l
                  WHERE l.source_event_id = ne.id AND l.run_id = :run_id
              )
            ORDER BY ne.event_at ASC, ne.id ASC
        """), {"run_id": run_id})
        events = [dict(zip(result.keys(), row)) for row in result.fetchall()]

        count = 0
        for ev in events:
            qty = D(ev["quantity"])
            if qty <= ZERO:
                continue

            unit_price = D(ev["unit_price_usd"]) if ev["unit_price_usd"] else None
            total_usd = D(ev["total_usd"]) if ev["total_usd"] else None

            # Derive cost_per_unit if not directly available
            if unit_price is None and total_usd and qty > ZERO:
                unit_price = total_usd / qty
            total_cost = total_usd or (unit_price * qty if unit_price else None)

            await session.execute(text("""
                INSERT INTO tax.lots_v4
                    (asset, wallet, original_quantity, remaining,
                     cost_per_unit_usd, total_cost_usd,
                     original_acquired_at, lot_created_at,
                     source_event_id, source_type, run_id)
                VALUES
                    (:asset, :wallet, :qty, :remaining,
                     :cpu, :tc,
                     :oaa, NOW(),
                     :seid, 'trade', :rid)
            """), {
                "asset": ev["asset"], "wallet": ev["wallet"],
                "qty": str(qty), "remaining": str(qty),
                "cpu": str(unit_price) if unit_price else None,
                "tc": str(total_cost) if total_cost else None,
                "oaa": ev["event_at"],
                "seid": ev["id"], "rid": run_id,
            })
            count += 1

        return count

    async def _create_income_lots(self, session: AsyncSession,
                                  run_id: int) -> int:
        """Create lots from confirmed income events."""
        result = await session.execute(text("""
            SELECT id, wallet, asset, quantity::text, fmv_per_unit_usd::text,
                   total_fmv_usd::text, dominion_at, source_event_id
            FROM tax.income_events_v4
            WHERE review_status = 'confirmed'
              AND lot_id IS NULL
              AND run_id = :run_id
        """), {"run_id": run_id})
        incomes = [dict(zip(result.keys(), row)) for row in result.fetchall()]

        count = 0
        for ie in incomes:
            qty = D(ie["quantity"])
            if qty <= ZERO:
                continue

            lot_result = await session.execute(text("""
                INSERT INTO tax.lots_v4
                    (asset, wallet, original_quantity, remaining,
                     cost_per_unit_usd, total_cost_usd,
                     original_acquired_at, lot_created_at,
                     source_event_id, source_type, run_id)
                VALUES
                    (:asset, :wallet, :qty, :remaining,
                     :cpu, :tc,
                     :oaa, NOW(),
                     :seid, 'income', :rid)
                RETURNING id
            """), {
                "asset": ie["asset"], "wallet": ie["wallet"],
                "qty": str(qty), "remaining": str(qty),
                "cpu": ie["fmv_per_unit_usd"],
                "tc": ie["total_fmv_usd"],
                "oaa": ie["dominion_at"],
                "seid": ie["source_event_id"],
                "rid": run_id,
            })
            lot_row = lot_result.fetchone()
            lot_id = lot_row[0] if lot_row else None

            if lot_id:
                await session.execute(text(
                    "UPDATE tax.income_events_v4 SET lot_id = :lid WHERE id = :id"),
                    {"lid": lot_id, "id": ie["id"]})
                count += 1

        return count

    async def _process_disposals(self, session: AsyncSession,
                                 run_id: int, year: int = None) -> list[DisposalV4]:
        """Process DISPOSAL and FEE_DISPOSAL events using per-wallet FIFO."""
        where_year = ""
        params = {"run_id": run_id}
        if year:
            where_year = "AND EXTRACT(YEAR FROM ne.event_at) = :year"
            params["year"] = year

        result = await session.execute(text(f"""
            SELECT ne.id, ne.wallet, ne.asset, ne.quantity::text,
                   ne.unit_price_usd::text, ne.total_usd::text,
                   ne.event_at, ne.event_type, ne.source_trade_id,
                   ne.raw_market
            FROM tax.normalized_events ne
            WHERE ne.event_type IN ('DISPOSAL', 'FEE_DISPOSAL')
              AND ne.run_id = :run_id
              {where_year}
            ORDER BY ne.event_at ASC, ne.id ASC
        """), params)
        events = [dict(zip(result.keys(), row)) for row in result.fetchall()]

        all_disposals = []

        for ev in events:
            disposal_qty = D(ev["quantity"])
            if disposal_qty <= ZERO:
                continue

            wallet = ev["wallet"]
            asset = ev["asset"]
            disposed_at = ev["event_at"]

            # Calculate proceeds
            unit_price = D(ev["unit_price_usd"]) if ev["unit_price_usd"] else None
            total_usd = D(ev["total_usd"]) if ev["total_usd"] else None
            proceeds = total_usd or (unit_price * disposal_qty if unit_price else None)

            # Fee disposal proceeds = FMV of the fee (what you "received" was the service)
            if ev["event_type"] == "FEE_DISPOSAL":
                proceeds = D(str(ev["total_usd"])) if ev["total_usd"] else ZERO
                if proceeds == ZERO and disposal_qty > ZERO:
                    self.exc.log(WARNING, "MISSING_PRICE",
                                 f"FEE_DISPOSAL event {ev['id']} has no USD valuation",
                                 source_event_id=ev["id"], run_id=run_id)

            # Find lots for (wallet, asset) with remaining > 0, scoped to run_id
            # Temporal constraint: only lots acquired on or before the disposal date
            lot_result = await session.execute(text("""
                SELECT id, remaining::text, cost_per_unit_usd::text,
                       original_acquired_at, source_type
                FROM tax.lots_v4
                WHERE wallet = :wallet AND asset = :asset AND remaining > 0 AND run_id = :run_id
                  AND original_acquired_at <= :disposed_at
                ORDER BY original_acquired_at ASC, id ASC
            """), {"wallet": wallet, "asset": asset, "run_id": run_id,
                   "disposed_at": disposed_at})
            lots = [dict(zip(lot_result.keys(), row))
                    for row in lot_result.fetchall()]

            remaining_to_sell = disposal_qty
            portion_used = ZERO

            for lot in lots:
                if remaining_to_sell <= ZERO:
                    break

                lot_remaining = D(lot["remaining"])
                consume = min(lot_remaining, remaining_to_sell)
                cost_per_unit = D(lot["cost_per_unit_usd"]) if lot["cost_per_unit_usd"] else None

                if cost_per_unit is None:
                    # Unknown basis — BLOCKING
                    self.exc.log(BLOCKING, UNKNOWN_BASIS,
                                 f"Lot {lot['id']} for {asset} on {wallet} has no cost basis",
                                 lot_id=lot["id"], source_event_id=ev["id"],
                                 tax_year=disposed_at.year, run_id=run_id)

                cost_basis = (cost_per_unit * consume) if cost_per_unit else None

                # Proportional proceeds for this slice
                if proceeds is not None and disposal_qty > ZERO:
                    slice_proceeds = proceeds * (consume / disposal_qty)
                else:
                    slice_proceeds = None

                # Gain/loss
                if slice_proceeds is not None and cost_basis is not None:
                    gain_loss = slice_proceeds - cost_basis
                else:
                    gain_loss = None

                # Holding period: >365 for long-term (NOT >=365)
                holding_days = (disposed_at - lot["original_acquired_at"]).days
                if holding_days < 0:
                    self.exc.log(BLOCKING, "FUTURE_LOT_USED",
                                 f"Disposal {ev['id']} would consume lot {lot['id']} acquired "
                                 f"{lot['original_acquired_at']} for disposal at {disposed_at} "
                                 f"(holding_days={holding_days}). Skipping this lot.",
                                 source_event_id=ev["id"], lot_id=lot["id"],
                                 tax_year=disposed_at.year, run_id=run_id)
                    continue  # Skip future-dated lots
                term = "long" if holding_days > 365 else "short"

                disp = DisposalV4(
                    asset=asset, wallet=wallet, quantity=consume,
                    proceeds_usd=slice_proceeds, fee_usd=None,
                    cost_basis_usd=cost_basis, gain_loss_usd=gain_loss,
                    original_acquired_at=lot["original_acquired_at"],
                    disposed_at=disposed_at, holding_days=holding_days,
                    term=term, disposal_event_id=ev["id"],
                    lot_id=lot["id"],
                    source_trade_id=ev.get("source_trade_id"),
                    market=ev.get("raw_market", ""),
                    exchange=wallet,
                )
                all_disposals.append(disp)

                # Update lot remaining
                new_remaining = lot_remaining - consume
                await session.execute(text("""
                    UPDATE tax.lots_v4
                    SET remaining = :remaining,
                        is_depleted = :depleted
                    WHERE id = :id
                """), {"remaining": str(new_remaining),
                       "depleted": new_remaining <= ZERO,
                       "id": lot["id"]})

                # Insert disposal record
                disp_result = await session.execute(text("""
                    INSERT INTO tax.disposals_v4
                        (asset, wallet, quantity, proceeds_usd, net_proceeds_usd,
                         cost_basis_usd, gain_loss_usd,
                         original_acquired_at, disposed_at, holding_days, term,
                         disposal_event_id, lot_id, source_trade_id,
                         market, exchange, run_id)
                    VALUES
                        (:asset, :wallet, :qty, :proceeds, :net_proceeds,
                         :cost_basis, :gain_loss,
                         :oaa, :da, :hd, :term,
                         :deid, :lid, :stid,
                         :market, :exchange, :rid)
                    RETURNING id
                """), {
                    "asset": asset, "wallet": wallet,
                    "qty": str(consume),
                    "proceeds": str(slice_proceeds) if slice_proceeds is not None else None,
                    "net_proceeds": str(slice_proceeds) if slice_proceeds is not None else None,
                    "cost_basis": str(cost_basis) if cost_basis is not None else None,
                    "gain_loss": str(gain_loss) if gain_loss is not None else None,
                    "oaa": lot["original_acquired_at"],
                    "da": disposed_at, "hd": holding_days, "term": term,
                    "deid": ev["id"], "lid": lot["id"],
                    "stid": ev.get("source_trade_id"),
                    "market": ev.get("raw_market", ""),
                    "exchange": wallet, "rid": run_id,
                })
                disp_row = disp_result.fetchone()
                disp.disposal_db_id = disp_row[0] if disp_row else None

                remaining_to_sell -= consume
                portion_used += consume

            # Oversold — no lots left but still have quantity to sell
            if remaining_to_sell > ZERO:
                self.exc.log(BLOCKING, OVERSOLD,
                             f"Oversold {asset} on {wallet}: tried to sell "
                             f"{disposal_qty} but only {portion_used} available in lots",
                             source_event_id=ev["id"],
                             tax_year=disposed_at.year, run_id=run_id)

        return all_disposals

    # ── Public methods for split pipeline (Fix 2) ────────────────────────

    async def create_acquisition_lots(self, session: AsyncSession,
                                      run_id: int) -> int:
        """Public wrapper: create lots from ACQUISITION events."""
        return await self._create_acquisition_lots(session, run_id)

    async def create_income_lots(self, session: AsyncSession,
                                 run_id: int) -> int:
        """Public wrapper: create lots from confirmed income events."""
        return await self._create_income_lots(session, run_id)

    async def process_disposals_and_report(self, session: AsyncSession,
                                           run_id: int, year: int = None) -> dict:
        """Process disposals + generate Form 8949 (steps 4-5 of pipeline)."""
        stats = {"disposals_processed": 0, "form_8949_lines": 0,
                 "short_term_gains": ZERO, "short_term_losses": ZERO,
                 "long_term_gains": ZERO, "long_term_losses": ZERO}

        # Clear previous disposals/form for this run
        await session.execute(text(
            "DELETE FROM tax.form_8949_v4 WHERE run_id = :r"), {"r": run_id})
        await session.execute(text(
            "DELETE FROM tax.disposals_v4 WHERE run_id = :r"), {"r": run_id})

        disposals = await self._process_disposals(session, run_id, year)
        stats["disposals_processed"] = len(disposals)

        for disp in disposals:
            await self._insert_form_8949(session, disp, run_id)
            stats["form_8949_lines"] += 1
            if disp.gain_loss_usd is not None:
                if disp.term == "short":
                    if disp.gain_loss_usd >= ZERO:
                        stats["short_term_gains"] += disp.gain_loss_usd
                    else:
                        stats["short_term_losses"] += disp.gain_loss_usd
                else:
                    if disp.gain_loss_usd >= ZERO:
                        stats["long_term_gains"] += disp.gain_loss_usd
                    else:
                        stats["long_term_losses"] += disp.gain_loss_usd

        return {
            "disposals_processed": stats["disposals_processed"],
            "form_8949_lines": stats["form_8949_lines"],
            "short_term_gains": str(stats["short_term_gains"]),
            "short_term_losses": str(stats["short_term_losses"]),
            "long_term_gains": str(stats["long_term_gains"]),
            "long_term_losses": str(stats["long_term_losses"]),
            "net_total": str(stats["short_term_gains"] + stats["short_term_losses"] +
                            stats["long_term_gains"] + stats["long_term_losses"]),
            "filing_ready": not self.exc.has_blocking,
        }

    async def _insert_form_8949(self, session: AsyncSession,
                                disp: DisposalV4, run_id: int):
        """Generate a Form 8949 line from a disposal."""
        # Description
        desc = f"{disp.quantity.normalize()} {disp.asset}"
        if disp.market:
            desc += f" ({disp.market})"

        # Date formatting: MM/DD/YYYY
        date_acquired = disp.original_acquired_at.strftime("%m/%d/%Y")
        date_sold = disp.disposed_at.strftime("%m/%d/%Y")

        # Round to 2 decimal places
        proceeds = disp.proceeds_usd.quantize(TWO_PLACES, ROUND_HALF_UP) if disp.proceeds_usd is not None else None
        cost_basis = disp.cost_basis_usd.quantize(TWO_PLACES, ROUND_HALF_UP) if disp.cost_basis_usd is not None else None
        gain_loss = disp.gain_loss_usd.quantize(TWO_PLACES, ROUND_HALF_UP) if disp.gain_loss_usd is not None else None

        # Box selection
        tax_year = disp.disposed_at.year
        if disp.term == "short":
            box = self.default_box_short
        else:
            box = self.default_box_long

        await session.execute(text("""
            INSERT INTO tax.form_8949_v4
                (description, date_acquired, date_sold,
                 proceeds, cost_basis, gain_loss,
                 term, box, asset, wallet, exchange,
                 holding_days, tax_year, disposal_id, run_id)
            VALUES
                (:desc, :da, :ds,
                 :proceeds, :cost_basis, :gain_loss,
                 :term, :box, :asset, :wallet, :exchange,
                 :hd, :ty, :did, :rid)
        """), {
            "desc": desc, "da": date_acquired, "ds": date_sold,
            "proceeds": str(proceeds) if proceeds is not None else None,
            "cost_basis": str(cost_basis) if cost_basis is not None else None,
            "gain_loss": str(gain_loss) if gain_loss is not None else None,
            "term": disp.term, "box": box,
            "asset": disp.asset, "wallet": disp.wallet,
            "exchange": disp.exchange,
            "hd": disp.holding_days, "ty": tax_year,
            "did": disp.disposal_db_id,
            "rid": run_id,
        })
