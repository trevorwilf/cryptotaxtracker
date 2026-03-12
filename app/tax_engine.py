"""
Tax Engine — FIFO cost basis lot tracking and capital gains computation.

Processes all trades chronologically:
  - BUYs create acquisition lots (asset, qty, cost_basis_usd, date)
  - SELLs consume lots in FIFO order, recording gain/loss per disposal
  - Partial lot consumption splits the lot
  - Holding period determines short-term (<=365 days) vs long-term (>365 days)
  - Fees are added to cost basis (buys) or subtracted from proceeds (sells)
  - Produces Form 8949 lines ready for Schedule D
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("tax-collector.tax-engine")

D = Decimal
ZERO = D("0")
ONE_YEAR = timedelta(days=365)


@dataclass
class Lot:
    """An acquisition lot of a single asset."""
    id: int | None = None
    asset: str = ""
    quantity: D = ZERO
    remaining: D = ZERO
    cost_per_unit_usd: D = ZERO
    total_cost_usd: D = ZERO
    acquired_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    exchange: str = ""
    source: str = ""          # "trade", "deposit", "staking", "airdrop", "transfer_in"
    source_trade_id: int | None = None


@dataclass
class Disposal:
    """A disposal event matched to one or more lots."""
    asset: str = ""
    quantity: D = ZERO
    proceeds_usd: D = ZERO
    cost_basis_usd: D = ZERO
    gain_loss_usd: D = ZERO
    fee_usd: D = ZERO
    acquired_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    disposed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    holding_days: int = 0
    term: str = ""            # "short" or "long"
    exchange: str = ""
    market: str = ""
    lot_id: int | None = None
    trade_id: int | None = None
    is_futures: bool = False
    # Form 8949 fields
    description: str = ""
    box: str = ""             # "A" (short, reported), "B" (short, not reported),
                              # "C" (long, reported), "D" (long, not reported)


class TaxEngine:
    """FIFO cost basis engine."""

    async def compute(self, session: AsyncSession, year: int | None = None) -> dict:
        """Run the full tax computation. Returns summary stats."""
        logger.info(f"Starting tax computation (year={year or 'all'}, method=FIFO)")

        # Clear previous computation
        await self._clear_computed(session, year)

        # Load all buy-side events as lots (chronological)
        lots_by_asset: dict[str, list[Lot]] = {}
        await self._load_buy_lots(session, lots_by_asset)
        await self._load_deposit_lots(session, lots_by_asset)
        await self._load_income_lots(session, lots_by_asset)
        await self._load_transfer_in_lots(session, lots_by_asset)

        total_lots = sum(len(v) for v in lots_by_asset.values())
        logger.info(f"Loaded {total_lots} acquisition lots across {len(lots_by_asset)} assets")

        # Fix 9: Sort each asset's lots globally by acquired_at for correct FIFO
        for asset, lot_list in lots_by_asset.items():
            lot_list.sort(key=lambda lot: (lot.acquired_at, lot.source_trade_id or 0))

        # Process all sells/disposals in chronological order
        disposals = await self._process_sells(session, lots_by_asset, year)
        logger.info(f"Processed {len(disposals)} disposals")

        # Fix 10: Save lots first (gets IDs from DB), then backfill disposal lot_ids
        await self._save_lots(session, lots_by_asset)

        # Backfill disposal lot_ids from saved lots
        for d in disposals:
            if d.lot_id is None and d.asset and d.acquired_at:
                for lot in lots_by_asset.get(d.asset, []):
                    if (lot.id and lot.acquired_at == d.acquired_at
                            and lot.exchange == d.exchange):
                        d.lot_id = lot.id
                        break

        await self._save_disposals(session, disposals)

        # Generate Form 8949 lines
        form_lines = self._generate_form_8949(disposals, year)
        await self._save_form_8949(session, form_lines)

        # Compute summary
        summary = self._compute_summary(disposals, year)
        await session.commit()

        logger.info(f"Tax computation complete: {summary}")
        return summary

    # ── Load acquisition lots ─────────────────────────────────────────────

    async def _load_buy_lots(self, session: AsyncSession, lots: dict):
        """Create lots from buy-side trades."""
        result = await session.execute(text("""
            SELECT id, exchange, base_asset, quantity, total_usd, fee_usd, executed_at
            FROM tax.trades
            WHERE side = 'buy' AND quantity > 0
            ORDER BY executed_at ASC, id ASC
        """))
        for row in result.fetchall():
            asset = row[2] or ""
            if not asset:
                continue
            qty = D(str(row[3]))
            cost = D(str(row[4] or 0))
            fee = D(str(row[5] or 0))
            total_cost = cost + fee  # fees increase cost basis on buys

            lot = Lot(
                asset=asset, quantity=qty, remaining=qty,
                cost_per_unit_usd=(total_cost / qty) if qty > 0 else ZERO,
                total_cost_usd=total_cost,
                acquired_at=row[6], exchange=row[1],
                source="trade", source_trade_id=row[0],
            )
            lots.setdefault(asset, []).append(lot)

    async def _load_deposit_lots(self, session: AsyncSession, lots: dict):
        """Create lots from deposits (non-transfer, non-income deposits get cost basis from USD value)."""
        result = await session.execute(text("""
            SELECT id, exchange, asset, amount, amount_usd, confirmed_at
            FROM tax.deposits
            WHERE amount > 0
              AND id NOT IN (SELECT deposit_id FROM tax.transfer_matches WHERE deposit_id IS NOT NULL)
              AND id NOT IN (SELECT deposit_id FROM tax.income_events WHERE deposit_id IS NOT NULL)
            ORDER BY confirmed_at ASC, id ASC
        """))
        for row in result.fetchall():
            asset = row[2]
            qty = D(str(row[3]))
            cost = D(str(row[4] or 0))
            lot = Lot(
                asset=asset, quantity=qty, remaining=qty,
                cost_per_unit_usd=(cost / qty) if qty > 0 else ZERO,
                total_cost_usd=cost,
                acquired_at=row[5] or datetime.now(timezone.utc),
                exchange=row[1], source="deposit",
            )
            lots.setdefault(asset, []).append(lot)

    async def _load_income_lots(self, session: AsyncSession, lots: dict):
        """Create lots from income events (staking rewards etc.) — FMV at receipt is the cost basis."""
        result = await session.execute(text("""
            SELECT id, exchange, asset, amount, amount_usd, received_at
            FROM tax.income_events
            WHERE amount > 0
            ORDER BY received_at ASC, id ASC
        """))
        for row in result.fetchall():
            asset = row[2]
            qty = D(str(row[3]))
            fmv = D(str(row[4] or 0))
            lot = Lot(
                asset=asset, quantity=qty, remaining=qty,
                cost_per_unit_usd=(fmv / qty) if qty > 0 else ZERO,
                total_cost_usd=fmv,
                acquired_at=row[5], exchange=row[1],
                source="staking",
            )
            lots.setdefault(asset, []).append(lot)

    async def _load_transfer_in_lots(self, session: AsyncSession, lots: dict):
        """Create lots from matched transfer-ins — preserves cost basis from the sending side."""
        result = await session.execute(text("""
            SELECT tm.id, tm.asset, tm.amount, tm.cost_basis_usd, tm.transferred_at,
                   d.exchange
            FROM tax.transfer_matches tm
            JOIN tax.deposits d ON d.id = tm.deposit_id
            WHERE tm.amount > 0
            ORDER BY tm.transferred_at ASC, tm.id ASC
        """))
        for row in result.fetchall():
            asset = row[1]
            qty = D(str(row[2]))
            cost = D(str(row[3] or 0))
            lot = Lot(
                asset=asset, quantity=qty, remaining=qty,
                cost_per_unit_usd=(cost / qty) if qty > 0 else ZERO,
                total_cost_usd=cost,
                acquired_at=row[4], exchange=row[5],
                source="transfer_in",
            )
            lots.setdefault(asset, []).append(lot)

    # ── Process sells ─────────────────────────────────────────────────────

    async def _process_sells(self, session: AsyncSession, lots: dict,
                             year: int | None) -> list[Disposal]:
        """Match sell trades to lots using FIFO."""
        year_filter = ""
        params: dict = {}
        if year:
            year_filter = "AND EXTRACT(YEAR FROM executed_at) = :year"
            params["year"] = year

        result = await session.execute(text(f"""
            SELECT id, exchange, market, base_asset, quantity, total_usd, fee_usd,
                   executed_at, side
            FROM tax.trades
            WHERE side = 'sell' AND quantity > 0 {year_filter}
            ORDER BY executed_at ASC, id ASC
        """), params)

        disposals = []
        for row in result.fetchall():
            trade_id = row[0]
            exchange = row[1]
            market = row[2]
            asset = row[3] or ""
            qty_to_sell = D(str(row[4]))
            proceeds = D(str(row[5] or 0))
            sell_fee = D(str(row[6] or 0))
            disposed_at = row[7]

            if not asset or asset not in lots:
                # No lots found — record as unknown cost basis
                disposals.append(Disposal(
                    asset=asset, quantity=qty_to_sell,
                    proceeds_usd=proceeds - sell_fee,
                    cost_basis_usd=ZERO, gain_loss_usd=proceeds - sell_fee,
                    fee_usd=sell_fee, disposed_at=disposed_at,
                    acquired_at=disposed_at, holding_days=0,
                    term="short", exchange=exchange, market=market,
                    trade_id=trade_id,
                    description=f"{qty_to_sell} {asset} (unknown cost basis)",
                ))
                continue

            asset_lots = lots[asset]
            remaining_sell = qty_to_sell
            proceeds_per_unit = (proceeds / qty_to_sell) if qty_to_sell > 0 else ZERO

            for lot in asset_lots:
                if remaining_sell <= 0:
                    break
                if lot.remaining <= 0:
                    continue

                consumed = min(remaining_sell, lot.remaining)
                lot.remaining -= consumed
                remaining_sell -= consumed

                portion_proceeds = consumed * proceeds_per_unit
                portion_cost = consumed * lot.cost_per_unit_usd
                portion_fee = sell_fee * (consumed / qty_to_sell) if qty_to_sell > 0 else ZERO
                net_proceeds = portion_proceeds - portion_fee
                gain_loss = net_proceeds - portion_cost

                holding = (disposed_at - lot.acquired_at) if lot.acquired_at else timedelta(0)
                holding_days = holding.days

                disposals.append(Disposal(
                    asset=asset, quantity=consumed,
                    proceeds_usd=net_proceeds,
                    cost_basis_usd=portion_cost,
                    gain_loss_usd=gain_loss,
                    fee_usd=portion_fee,
                    acquired_at=lot.acquired_at,
                    disposed_at=disposed_at,
                    holding_days=holding_days,
                    term="long" if holding_days > 365 else "short",
                    exchange=exchange, market=market,
                    lot_id=lot.id, trade_id=trade_id,
                    description=f"{consumed} {asset}",
                ))

            if remaining_sell > 0:
                # Oversold — no lots left
                disposals.append(Disposal(
                    asset=asset, quantity=remaining_sell,
                    proceeds_usd=remaining_sell * proceeds_per_unit,
                    cost_basis_usd=ZERO,
                    gain_loss_usd=remaining_sell * proceeds_per_unit,
                    fee_usd=ZERO, disposed_at=disposed_at,
                    acquired_at=disposed_at, holding_days=0,
                    term="short", exchange=exchange, market=market,
                    trade_id=trade_id,
                    description=f"{remaining_sell} {asset} (no lots remaining)",
                ))

        return disposals

    # ── Form 8949 ─────────────────────────────────────────────────────────

    def _generate_form_8949(self, disposals: list[Disposal], year: int | None) -> list[dict]:
        """Generate Form 8949 lines from disposals."""
        lines = []
        for d in disposals:
            if year and d.disposed_at.year != year:
                continue

            # Box determination:
            # A = short-term, basis reported to IRS (1099-B)
            # B = short-term, basis NOT reported
            # C = long-term, basis reported
            # D = long-term, basis NOT reported
            # Crypto exchanges generally don't report to IRS, so B and D
            box = "D" if d.term == "long" else "B"

            lines.append({
                "description": d.description,
                "date_acquired": d.acquired_at.strftime("%m/%d/%Y") if d.acquired_at else "Various",
                "date_sold": d.disposed_at.strftime("%m/%d/%Y"),
                "proceeds": str(d.proceeds_usd.quantize(D("0.01"), ROUND_HALF_UP)),
                "cost_basis": str(d.cost_basis_usd.quantize(D("0.01"), ROUND_HALF_UP)),
                "adjustment_code": "",
                "adjustment_amount": "0.00",
                "gain_loss": str(d.gain_loss_usd.quantize(D("0.01"), ROUND_HALF_UP)),
                "term": d.term,
                "box": box,
                "asset": d.asset,
                "exchange": d.exchange,
                "holding_days": d.holding_days,
                "is_futures": d.is_futures,
                "tax_year": d.disposed_at.year,
            })
        return lines

    def _compute_summary(self, disposals: list[Disposal], year: int | None) -> dict:
        """Compute aggregate tax summary."""
        st_gains = ZERO
        st_losses = ZERO
        lt_gains = ZERO
        lt_losses = ZERO
        total_proceeds = ZERO
        total_cost = ZERO
        total_fees = ZERO

        for d in disposals:
            if year and d.disposed_at.year != year:
                continue
            total_proceeds += d.proceeds_usd
            total_cost += d.cost_basis_usd
            total_fees += d.fee_usd
            if d.term == "short":
                if d.gain_loss_usd >= 0:
                    st_gains += d.gain_loss_usd
                else:
                    st_losses += d.gain_loss_usd
            else:
                if d.gain_loss_usd >= 0:
                    lt_gains += d.gain_loss_usd
                else:
                    lt_losses += d.gain_loss_usd

        return {
            "year": year or "all",
            "method": "FIFO",
            "total_disposals": len([d for d in disposals if not year or d.disposed_at.year == year]),
            "total_proceeds_usd": str(total_proceeds.quantize(D("0.01"))),
            "total_cost_basis_usd": str(total_cost.quantize(D("0.01"))),
            "total_fees_usd": str(total_fees.quantize(D("0.01"))),
            "short_term_gains": str(st_gains.quantize(D("0.01"))),
            "short_term_losses": str(st_losses.quantize(D("0.01"))),
            "long_term_gains": str(lt_gains.quantize(D("0.01"))),
            "long_term_losses": str(lt_losses.quantize(D("0.01"))),
            "net_short_term": str((st_gains + st_losses).quantize(D("0.01"))),
            "net_long_term": str((lt_gains + lt_losses).quantize(D("0.01"))),
            "net_total": str((st_gains + st_losses + lt_gains + lt_losses).quantize(D("0.01"))),
        }

    # ── Persistence ───────────────────────────────────────────────────────

    async def _clear_computed(self, session: AsyncSession, year: int | None):
        if year:
            await session.execute(text("DELETE FROM tax.form_8949 WHERE tax_year = :y"), {"y": year})
            await session.execute(text(
                "DELETE FROM tax.disposals WHERE EXTRACT(YEAR FROM disposed_at) = :y"), {"y": year})
        else:
            await session.execute(text("DELETE FROM tax.form_8949"))
            await session.execute(text("DELETE FROM tax.disposals"))
            await session.execute(text("DELETE FROM tax.lots"))

    async def _save_lots(self, session: AsyncSession, lots_by_asset: dict):
        for asset, lot_list in lots_by_asset.items():
            for lot in lot_list:
                result = await session.execute(text("""
                    INSERT INTO tax.lots
                        (asset, quantity, remaining, cost_per_unit_usd, total_cost_usd,
                         acquired_at, exchange, source, source_trade_id)
                    VALUES (:asset, :qty, :rem, :cpu, :tc, :acq, :ex, :src, :stid)
                    ON CONFLICT (asset, exchange, acquired_at, source_trade_id) DO UPDATE SET
                        remaining = EXCLUDED.remaining
                    RETURNING id
                """), {
                    "asset": lot.asset, "qty": str(lot.quantity), "rem": str(lot.remaining),
                    "cpu": str(lot.cost_per_unit_usd), "tc": str(lot.total_cost_usd),
                    "acq": lot.acquired_at, "ex": lot.exchange, "src": lot.source,
                    "stid": lot.source_trade_id,
                })
                row = result.fetchone()
                if row:
                    lot.id = row[0]

    async def _save_disposals(self, session: AsyncSession, disposals: list[Disposal]):
        for d in disposals:
            await session.execute(text("""
                INSERT INTO tax.disposals
                    (asset, quantity, proceeds_usd, cost_basis_usd, gain_loss_usd,
                     fee_usd, acquired_at, disposed_at, holding_days, term,
                     exchange, market, lot_id, trade_id)
                VALUES
                    (:asset, :qty, :proceeds, :cost, :gl, :fee,
                     :acq, :disp, :hd, :term, :ex, :mkt, :lid, :tid)
            """), {
                "asset": d.asset, "qty": str(d.quantity),
                "proceeds": str(d.proceeds_usd), "cost": str(d.cost_basis_usd),
                "gl": str(d.gain_loss_usd), "fee": str(d.fee_usd),
                "acq": d.acquired_at, "disp": d.disposed_at,
                "hd": d.holding_days, "term": d.term,
                "ex": d.exchange, "mkt": d.market,
                "lid": d.lot_id, "tid": d.trade_id,
            })

    async def _save_form_8949(self, session: AsyncSession, lines: list[dict]):
        for line in lines:
            await session.execute(text("""
                INSERT INTO tax.form_8949
                    (description, date_acquired, date_sold, proceeds, cost_basis,
                     adjustment_code, adjustment_amount, gain_loss, term, box,
                     asset, exchange, holding_days, is_futures, tax_year)
                VALUES
                    (:description, :date_acquired, :date_sold, :proceeds, :cost_basis,
                     :adjustment_code, :adjustment_amount, :gain_loss, :term, :box,
                     :asset, :exchange, :holding_days, :is_futures, :tax_year)
            """), line)
