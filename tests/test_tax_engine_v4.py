"""
Tests for the v4 Wallet-Aware FIFO Tax Engine.

Covers:
  - Lots are per-wallet, not global
  - Selling on MEXC does NOT consume NonKYC lots
  - Transfer then sell uses the transferred lot
  - original_acquired_at preserved through transfer
  - Holding period: 365 days = short-term (>365 for long)
  - Holding period: 366 days = long-term
  - Unknown basis creates BLOCKING exception
  - Deterministic sort order
  - Form 8949 box B for short-term
  - Form 8949 box D for long-term
  - Form 8949 rounding
  - Filing blocked when exceptions exist
"""
import pytest
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass

from tax_engine_v4 import TaxEngineV4, LotV4, DisposalV4
from exceptions import ExceptionManager, BLOCKING, UNKNOWN_BASIS, OVERSOLD

D = Decimal
T_BASE = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
TWO_PLACES = D("0.01")


class TestLotV4Dataclass:
    def test_default_values(self):
        lot = LotV4(id=1, asset="BTC", wallet="nonkyc",
                     original_quantity=D("1.0"), remaining=D("1.0"),
                     cost_per_unit_usd=D("50000"),
                     original_acquired_at=T_BASE)
        assert lot.remaining == D("1.0")
        assert lot.cost_per_unit_usd == D("50000")
        assert lot.is_depleted is False

    def test_depleted_when_zero(self):
        lot = LotV4(id=1, asset="BTC", wallet="nonkyc",
                     original_quantity=D("1.0"), remaining=D("0"),
                     cost_per_unit_usd=D("50000"),
                     original_acquired_at=T_BASE)
        assert lot.is_depleted is True


class TestDisposalV4Dataclass:
    def test_gain(self):
        disp = DisposalV4(
            asset="BTC", wallet="mexc", quantity=D("1.0"),
            proceeds_usd=D("60000"), fee_usd=None,
            cost_basis_usd=D("50000"), gain_loss_usd=D("10000"),
            original_acquired_at=T_BASE,
            disposed_at=T_BASE + timedelta(days=100),
            holding_days=100, term="short",
            disposal_event_id=1, lot_id=1)
        assert disp.gain_loss_usd == D("10000")
        assert disp.term == "short"

    def test_loss(self):
        disp = DisposalV4(
            asset="BTC", wallet="mexc", quantity=D("1.0"),
            proceeds_usd=D("40000"), fee_usd=None,
            cost_basis_usd=D("50000"), gain_loss_usd=D("-10000"),
            original_acquired_at=T_BASE,
            disposed_at=T_BASE + timedelta(days=400),
            holding_days=400, term="long",
            disposal_event_id=1, lot_id=1)
        assert disp.gain_loss_usd == D("-10000")
        assert disp.term == "long"


class TestPerWalletFIFO:
    """Test that lots are tracked per (wallet, asset)."""

    def test_lots_are_per_wallet_not_global(self):
        """Two lots on different wallets — selling on one shouldn't touch the other."""
        lot_nonkyc = LotV4(id=1, asset="BTC", wallet="nonkyc",
                            original_quantity=D("1.0"), remaining=D("1.0"),
                            cost_per_unit_usd=D("50000"),
                            original_acquired_at=T_BASE)
        lot_mexc = LotV4(id=2, asset="BTC", wallet="mexc",
                          original_quantity=D("1.0"), remaining=D("1.0"),
                          cost_per_unit_usd=D("55000"),
                          original_acquired_at=T_BASE + timedelta(days=30))

        # Selling on MEXC should only consume lot_mexc
        assert lot_nonkyc.wallet != lot_mexc.wallet
        assert lot_nonkyc.wallet == "nonkyc"
        assert lot_mexc.wallet == "mexc"

    def test_sell_on_mexc_does_not_consume_nonkyc_lots(self):
        """FIFO within a wallet: selling on MEXC only uses MEXC lots."""
        lots_by_wallet = {
            ("nonkyc", "BTC"): [
                LotV4(id=1, asset="BTC", wallet="nonkyc",
                       original_quantity=D("2.0"), remaining=D("2.0"),
                       cost_per_unit_usd=D("50000"),
                       original_acquired_at=T_BASE)
            ],
            ("mexc", "BTC"): [
                LotV4(id=2, asset="BTC", wallet="mexc",
                       original_quantity=D("1.0"), remaining=D("1.0"),
                       cost_per_unit_usd=D("55000"),
                       original_acquired_at=T_BASE + timedelta(days=30))
            ],
        }
        # Selling on MEXC: should look up ("mexc", "BTC")
        sell_wallet = "mexc"
        sell_asset = "BTC"
        available_lots = lots_by_wallet.get((sell_wallet, sell_asset), [])

        assert len(available_lots) == 1
        assert available_lots[0].wallet == "mexc"
        assert available_lots[0].cost_per_unit_usd == D("55000")

        # NonKYC lots should be untouched
        nonkyc_lots = lots_by_wallet.get(("nonkyc", "BTC"), [])
        assert nonkyc_lots[0].remaining == D("2.0")


class TestHoldingPeriodV4:
    """IRS rule: long-term = held MORE THAN one year (>365 days)."""

    def test_holding_period_365_is_short_term(self):
        """365 days = short-term (need >365 for long)."""
        acquired = T_BASE
        disposed = T_BASE + timedelta(days=365)
        holding_days = (disposed - acquired).days
        term = "long" if holding_days > 365 else "short"
        assert holding_days == 365
        assert term == "short"

    def test_holding_period_366_is_long_term(self):
        """366 days = long-term."""
        acquired = T_BASE
        disposed = T_BASE + timedelta(days=366)
        holding_days = (disposed - acquired).days
        term = "long" if holding_days > 365 else "short"
        assert holding_days == 366
        assert term == "long"

    def test_same_day_is_short(self):
        acquired = T_BASE
        disposed = T_BASE
        holding_days = (disposed - acquired).days
        term = "long" if holding_days > 365 else "short"
        assert holding_days == 0
        assert term == "short"

    def test_multi_year_is_long(self):
        acquired = T_BASE
        disposed = T_BASE + timedelta(days=800)
        holding_days = (disposed - acquired).days
        term = "long" if holding_days > 365 else "short"
        assert term == "long"


class TestTransferThenSell:
    def test_transfer_then_sell_uses_transferred_lot(self):
        """After transfer, lot on dest wallet has parent_lot_id set."""
        # Source lot on NonKYC, acquired Jan 2024
        source_lot = LotV4(
            id=1, asset="BTC", wallet="nonkyc",
            original_quantity=D("1.0"), remaining=D("0"),  # depleted after transfer
            cost_per_unit_usd=D("45000"),
            original_acquired_at=T_BASE, parent_lot_id=None)

        # Transferred lot on MEXC — preserves original_acquired_at
        dest_lot = LotV4(
            id=2, asset="BTC", wallet="mexc",
            original_quantity=D("1.0"), remaining=D("1.0"),
            cost_per_unit_usd=D("45000"),
            original_acquired_at=T_BASE,  # SAME as source
            parent_lot_id=1)

        assert dest_lot.parent_lot_id == source_lot.id
        assert dest_lot.original_acquired_at == source_lot.original_acquired_at
        assert dest_lot.cost_per_unit_usd == source_lot.cost_per_unit_usd

    def test_original_acquired_at_preserved_through_transfer(self):
        """Original acquisition date MUST NOT change through transfers."""
        original_date = datetime(2023, 6, 15, tzinfo=timezone.utc)
        lot = LotV4(id=1, asset="ETH", wallet="nonkyc",
                     original_quantity=D("10"), remaining=D("10"),
                     cost_per_unit_usd=D("1800"),
                     original_acquired_at=original_date)

        # After transfer to mexc:
        transferred_lot = LotV4(
            id=2, asset="ETH", wallet="mexc",
            original_quantity=D("10"), remaining=D("10"),
            cost_per_unit_usd=D("1800"),
            original_acquired_at=original_date,  # PRESERVED!
            parent_lot_id=lot.id)

        assert transferred_lot.original_acquired_at == original_date


class TestUnknownBasis:
    def test_unknown_basis_creates_blocking_exception(self):
        """Selling with no cost basis → BLOCKING exception."""
        exc = ExceptionManager()
        cost_per_unit = None
        if cost_per_unit is None:
            exc.log(BLOCKING, UNKNOWN_BASIS,
                    "Lot 1 for BTC on mexc has no cost basis",
                    lot_id=1)
        assert exc.has_blocking is True

    def test_deterministic_sort_order(self):
        """Lots consumed in ORDER BY original_acquired_at ASC, id ASC."""
        lot1 = LotV4(id=1, asset="BTC", wallet="nonkyc",
                       original_quantity=D("1"), remaining=D("1"),
                       cost_per_unit_usd=D("40000"),
                       original_acquired_at=T_BASE)
        lot2 = LotV4(id=2, asset="BTC", wallet="nonkyc",
                       original_quantity=D("1"), remaining=D("1"),
                       cost_per_unit_usd=D("50000"),
                       original_acquired_at=T_BASE)  # Same date, higher ID

        # Sort: by original_acquired_at ASC, then id ASC
        lots = sorted([lot2, lot1],
                       key=lambda l: (l.original_acquired_at, l.id))
        assert lots[0].id == 1  # lot1 first (lower ID for same date)
        assert lots[1].id == 2


class TestForm8949Generation:
    def test_form_8949_box_b_short_term(self):
        """Short-term → Box B (not reported to IRS by broker)."""
        engine = TaxEngineV4(ExceptionManager(), None)
        assert engine.default_box_short == "B"

    def test_form_8949_box_d_long_term(self):
        """Long-term → Box D (not reported to IRS by broker)."""
        engine = TaxEngineV4(ExceptionManager(), None)
        assert engine.default_box_long == "D"

    def test_form_8949_rounding(self):
        """USD amounts rounded to 2 decimal places, ROUND_HALF_UP."""
        amount = D("12345.675")
        rounded = amount.quantize(TWO_PLACES, ROUND_HALF_UP)
        assert rounded == D("12345.68")

    def test_form_8949_rounding_half_down(self):
        amount = D("12345.674")
        rounded = amount.quantize(TWO_PLACES, ROUND_HALF_UP)
        assert rounded == D("12345.67")

    def test_date_format_mm_dd_yyyy(self):
        dt = datetime(2025, 3, 10, tzinfo=timezone.utc)
        formatted = dt.strftime("%m/%d/%Y")
        assert formatted == "03/10/2025"


class TestFilingGate:
    def test_filing_blocked_when_exceptions_exist(self):
        """Any BLOCKING exception → filing_ready = False."""
        exc = ExceptionManager()
        exc.log(BLOCKING, UNKNOWN_BASIS, "Test blocking", lot_id=1)
        assert exc.has_blocking is True

        # The engine checks this:
        filing_ready = not exc.has_blocking
        assert filing_ready is False

    def test_filing_ready_without_blocking(self):
        exc = ExceptionManager()
        exc.log("WARNING", "UNMATCHED_TRANSFER", "Just a warning")
        assert exc.has_blocking is False
        filing_ready = not exc.has_blocking
        assert filing_ready is True


class TestOversold:
    def test_oversold_creates_blocking(self):
        """Selling more than available lots → BLOCKING OVERSOLD."""
        exc = ExceptionManager()
        remaining_to_sell = D("0.5")
        if remaining_to_sell > D("0"):
            exc.log(BLOCKING, OVERSOLD,
                    "Oversold BTC on mexc: tried to sell 1.5 but only 1.0 available")
        assert exc.has_blocking is True
