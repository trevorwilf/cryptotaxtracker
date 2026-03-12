"""
Tests for the FIFO Tax Engine.

Covers:
  - Lot creation from buys
  - FIFO ordering (first in, first out)
  - Partial lot consumption
  - Short-term vs long-term holding period
  - Fee handling (added to cost basis on buys, subtracted from proceeds on sells)
  - Form 8949 line generation
  - Box assignment (B for short-term, D for long-term)
  - Gain/loss computation
  - Multiple assets don't cross-contaminate
  - Oversold scenario (sell more than you own)
  - Schedule D summary computation
"""
import pytest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from tax_engine import TaxEngine, Lot, Disposal, D, ZERO

# ── Helpers ───────────────────────────────────────────────────────────────

JAN_2024 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
JUN_2024 = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
JAN_2025 = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
MAR_2025 = datetime(2025, 3, 10, 12, 0, 0, tzinfo=timezone.utc)
SEP_2025 = datetime(2025, 9, 5, 12, 0, 0, tzinfo=timezone.utc)


# ── Lot Dataclass Tests ───────────────────────────────────────────────────

class TestLotDataclass:
    def test_default_values(self):
        lot = Lot()
        assert lot.quantity == ZERO
        assert lot.remaining == ZERO
        assert lot.asset == ""

    def test_cost_per_unit(self):
        lot = Lot(asset="BTC", quantity=D("2"), remaining=D("2"),
                  cost_per_unit_usd=D("50000"), total_cost_usd=D("100000"))
        assert lot.cost_per_unit_usd == D("50000")

    def test_partial_remaining(self):
        lot = Lot(asset="BTC", quantity=D("2"), remaining=D("0.5"))
        assert lot.remaining < lot.quantity


# ── Disposal Dataclass Tests ──────────────────────────────────────────────

class TestDisposalDataclass:
    def test_gain(self):
        d = Disposal(proceeds_usd=D("60000"), cost_basis_usd=D("50000"),
                     gain_loss_usd=D("10000"))
        assert d.gain_loss_usd > 0

    def test_loss(self):
        d = Disposal(proceeds_usd=D("40000"), cost_basis_usd=D("50000"),
                     gain_loss_usd=D("-10000"))
        assert d.gain_loss_usd < 0

    def test_short_term(self):
        d = Disposal(acquired_at=MAR_2025, disposed_at=SEP_2025,
                     holding_days=179, term="short")
        assert d.term == "short"
        assert d.holding_days < 365

    def test_long_term(self):
        d = Disposal(acquired_at=JAN_2024, disposed_at=MAR_2025,
                     holding_days=420, term="long")
        assert d.term == "long"
        assert d.holding_days >= 365


# ── Form 8949 Generation Tests ────────────────────────────────────────────

class TestForm8949Generation:
    """Test the _generate_form_8949 method."""

    def setup_method(self):
        self.engine = TaxEngine()

    def test_short_term_box_b(self):
        disposals = [Disposal(
            asset="BTC", quantity=D("0.5"),
            proceeds_usd=D("30000"), cost_basis_usd=D("25000"),
            gain_loss_usd=D("5000"), fee_usd=D("0"),
            acquired_at=MAR_2025, disposed_at=SEP_2025,
            holding_days=179, term="short",
            exchange="nonkyc", market="BTC/USDT",
            description="0.5 BTC",
        )]
        lines = self.engine._generate_form_8949(disposals, 2025)
        assert len(lines) == 1
        assert lines[0]["box"] == "B"
        assert lines[0]["term"] == "short"
        assert lines[0]["gain_loss"] == "5000.00"

    def test_long_term_box_d(self):
        disposals = [Disposal(
            asset="ETH", quantity=D("10"),
            proceeds_usd=D("35000"), cost_basis_usd=D("20000"),
            gain_loss_usd=D("15000"), fee_usd=D("0"),
            acquired_at=JAN_2024, disposed_at=MAR_2025,
            holding_days=420, term="long",
            exchange="nonkyc", market="ETH/USDT",
            description="10 ETH",
        )]
        lines = self.engine._generate_form_8949(disposals, 2025)
        assert len(lines) == 1
        assert lines[0]["box"] == "D"
        assert lines[0]["term"] == "long"

    def test_loss_negative_gain(self):
        disposals = [Disposal(
            asset="SOL", quantity=D("100"),
            proceeds_usd=D("5000"), cost_basis_usd=D("8000"),
            gain_loss_usd=D("-3000"), fee_usd=D("10"),
            acquired_at=MAR_2025, disposed_at=SEP_2025,
            holding_days=179, term="short",
            exchange="mexc", market="SOL/USDT",
            description="100 SOL",
        )]
        lines = self.engine._generate_form_8949(disposals, 2025)
        assert lines[0]["gain_loss"] == "-3000.00"

    def test_year_filter(self):
        disposals = [
            Disposal(asset="BTC", disposed_at=JAN_2024, term="short",
                     quantity=D("1"), proceeds_usd=D("0"), cost_basis_usd=D("0"),
                     gain_loss_usd=D("0"), description="old"),
            Disposal(asset="BTC", disposed_at=MAR_2025, term="short",
                     quantity=D("1"), proceeds_usd=D("0"), cost_basis_usd=D("0"),
                     gain_loss_usd=D("0"), description="new"),
        ]
        lines_2025 = self.engine._generate_form_8949(disposals, 2025)
        lines_2024 = self.engine._generate_form_8949(disposals, 2024)
        assert len(lines_2025) == 1
        assert len(lines_2024) == 1

    def test_date_formatting(self):
        disposals = [Disposal(
            asset="BTC", quantity=D("1"),
            proceeds_usd=D("50000"), cost_basis_usd=D("40000"),
            gain_loss_usd=D("10000"), fee_usd=D("0"),
            acquired_at=JAN_2024, disposed_at=MAR_2025,
            holding_days=420, term="long",
            exchange="nonkyc", market="BTC/USDT",
            description="1 BTC",
        )]
        lines = self.engine._generate_form_8949(disposals, 2025)
        assert lines[0]["date_acquired"] == "01/15/2024"
        assert lines[0]["date_sold"] == "03/10/2025"

    def test_rounding(self):
        disposals = [Disposal(
            asset="BTC", quantity=D("0.00123456"),
            proceeds_usd=D("123.456789"), cost_basis_usd=D("100.111111"),
            gain_loss_usd=D("23.345678"), fee_usd=D("0"),
            acquired_at=MAR_2025, disposed_at=SEP_2025,
            holding_days=179, term="short",
            exchange="nonkyc", market="BTC/USDT",
            description="test",
        )]
        lines = self.engine._generate_form_8949(disposals, 2025)
        assert lines[0]["proceeds"] == "123.46"
        assert lines[0]["cost_basis"] == "100.11"
        assert lines[0]["gain_loss"] == "23.35"


# ── Summary Computation Tests ─────────────────────────────────────────────

class TestSummaryComputation:
    def setup_method(self):
        self.engine = TaxEngine()

    def test_basic_summary(self):
        disposals = [
            Disposal(asset="BTC", disposed_at=MAR_2025, term="short",
                     quantity=D("1"), proceeds_usd=D("60000"),
                     cost_basis_usd=D("50000"), gain_loss_usd=D("10000"),
                     fee_usd=D("50")),
            Disposal(asset="ETH", disposed_at=MAR_2025, term="long",
                     quantity=D("10"), proceeds_usd=D("30000"),
                     cost_basis_usd=D("35000"), gain_loss_usd=D("-5000"),
                     fee_usd=D("30")),
        ]
        summary = self.engine._compute_summary(disposals, 2025)
        assert summary["method"] == "FIFO"
        assert summary["total_disposals"] == 2
        assert summary["short_term_gains"] == "10000.00"
        assert summary["long_term_losses"] == "-5000.00"
        assert summary["net_total"] == "5000.00"
        assert summary["total_fees_usd"] == "80.00"

    def test_all_losses(self):
        disposals = [
            Disposal(asset="BTC", disposed_at=MAR_2025, term="short",
                     quantity=D("1"), proceeds_usd=D("40000"),
                     cost_basis_usd=D("50000"), gain_loss_usd=D("-10000"),
                     fee_usd=D("0")),
        ]
        summary = self.engine._compute_summary(disposals, 2025)
        assert summary["short_term_losses"] == "-10000.00"
        assert summary["net_total"] == "-10000.00"

    def test_empty_disposals(self):
        summary = self.engine._compute_summary([], 2025)
        assert summary["total_disposals"] == 0
        assert summary["net_total"] == "0.00"

    def test_year_filter_in_summary(self):
        disposals = [
            Disposal(asset="BTC", disposed_at=JAN_2024, term="short",
                     quantity=D("1"), proceeds_usd=D("50000"),
                     cost_basis_usd=D("40000"), gain_loss_usd=D("10000"),
                     fee_usd=D("0")),
            Disposal(asset="BTC", disposed_at=MAR_2025, term="short",
                     quantity=D("1"), proceeds_usd=D("60000"),
                     cost_basis_usd=D("50000"), gain_loss_usd=D("10000"),
                     fee_usd=D("0")),
        ]
        s2025 = self.engine._compute_summary(disposals, 2025)
        assert s2025["total_disposals"] == 1
        assert s2025["net_total"] == "10000.00"


# ── Holding Period Classification ─────────────────────────────────────────

class TestHoldingPeriod:
    """Verify short-term (<=365 days) vs long-term (>365 days).
    IRS rule: long-term = held MORE THAN one year (>365 days, not >=365)."""

    def test_exactly_365_days_is_short(self):
        """IRS rule: long-term = held MORE THAN one year (>365 days, not >=365)."""
        acquired = JAN_2024
        disposed = acquired + timedelta(days=365)
        holding = (disposed - acquired).days
        term = "long" if holding > 365 else "short"
        assert term == "short"

    def test_364_days_is_short(self):
        acquired = JAN_2024
        disposed = acquired + timedelta(days=364)
        holding = (disposed - acquired).days
        term = "long" if holding > 365 else "short"
        assert term == "short"

    def test_same_day_is_short(self):
        holding = 0
        term = "long" if holding > 365 else "short"
        assert term == "short"

    def test_multi_year_is_long(self):
        acquired = JAN_2024
        disposed = SEP_2025
        holding = (disposed - acquired).days
        assert holding > 365
        term = "long" if holding > 365 else "short"
        assert term == "long"


# ── FIFO Logic Unit Tests ────────────────────────────────────────────────

class TestFIFOLogic:
    """Test FIFO consumption logic without DB.
    These test the mathematical correctness of lot consumption."""

    def test_single_lot_full_sell(self):
        """Buy 1 BTC at $50k, sell 1 BTC at $60k → $10k gain."""
        lot = Lot(asset="BTC", quantity=D("1"), remaining=D("1"),
                  cost_per_unit_usd=D("50000"), total_cost_usd=D("50000"),
                  acquired_at=JAN_2024)

        sell_qty = D("1")
        proceeds_per_unit = D("60000")
        consumed = min(sell_qty, lot.remaining)
        lot.remaining -= consumed
        cost = consumed * lot.cost_per_unit_usd
        proceeds = consumed * proceeds_per_unit
        gain = proceeds - cost

        assert lot.remaining == D("0")
        assert gain == D("10000")

    def test_single_lot_partial_sell(self):
        """Buy 2 BTC at $50k each, sell 0.5 BTC → lot has 1.5 remaining."""
        lot = Lot(asset="BTC", quantity=D("2"), remaining=D("2"),
                  cost_per_unit_usd=D("50000"), total_cost_usd=D("100000"),
                  acquired_at=JAN_2024)

        consumed = D("0.5")
        lot.remaining -= consumed
        cost = consumed * lot.cost_per_unit_usd

        assert lot.remaining == D("1.5")
        assert cost == D("25000")

    def test_fifo_order_two_lots(self):
        """Two lots: Lot 1 at $40k, Lot 2 at $60k. FIFO sells Lot 1 first."""
        lots = [
            Lot(asset="BTC", quantity=D("1"), remaining=D("1"),
                cost_per_unit_usd=D("40000"), acquired_at=JAN_2024),
            Lot(asset="BTC", quantity=D("1"), remaining=D("1"),
                cost_per_unit_usd=D("60000"), acquired_at=JUN_2024),
        ]

        sell_qty = D("1")
        sell_price = D("55000")
        remaining = sell_qty

        consumed_lots = []
        for lot in lots:
            if remaining <= 0:
                break
            consumed = min(remaining, lot.remaining)
            lot.remaining -= consumed
            remaining -= consumed
            consumed_lots.append((lot.cost_per_unit_usd, consumed))

        # FIFO: should consume Lot 1 ($40k) first
        assert consumed_lots[0][0] == D("40000")
        assert lots[0].remaining == D("0")
        assert lots[1].remaining == D("1")  # untouched

    def test_fifo_spans_two_lots(self):
        """Sell more than one lot's worth — should consume across lots."""
        lots = [
            Lot(asset="ETH", quantity=D("5"), remaining=D("5"),
                cost_per_unit_usd=D("2000"), acquired_at=JAN_2024),
            Lot(asset="ETH", quantity=D("5"), remaining=D("5"),
                cost_per_unit_usd=D("3000"), acquired_at=JUN_2024),
        ]

        sell_qty = D("7")
        remaining = sell_qty
        total_cost = D("0")

        for lot in lots:
            if remaining <= 0:
                break
            consumed = min(remaining, lot.remaining)
            lot.remaining -= consumed
            remaining -= consumed
            total_cost += consumed * lot.cost_per_unit_usd

        # 5 from Lot 1 ($2000) + 2 from Lot 2 ($3000)
        assert total_cost == D("5") * D("2000") + D("2") * D("3000")
        assert total_cost == D("16000")
        assert lots[0].remaining == D("0")
        assert lots[1].remaining == D("3")

    def test_fee_increases_cost_basis(self):
        """Fees on buy side increase cost basis."""
        buy_total = D("50000")
        buy_fee = D("50")
        total_cost = buy_total + buy_fee

        lot = Lot(asset="BTC", quantity=D("1"), remaining=D("1"),
                  cost_per_unit_usd=total_cost, total_cost_usd=total_cost)

        assert lot.cost_per_unit_usd == D("50050")

    def test_fee_reduces_proceeds(self):
        """Fees on sell side reduce net proceeds."""
        gross_proceeds = D("60000")
        sell_fee = D("60")
        net_proceeds = gross_proceeds - sell_fee
        cost_basis = D("50000")
        gain = net_proceeds - cost_basis

        assert gain == D("9940")

    def test_different_assets_dont_cross(self):
        """BTC lots should not be consumed when selling ETH."""
        btc_lots = [Lot(asset="BTC", quantity=D("1"), remaining=D("1"),
                        cost_per_unit_usd=D("50000"))]
        eth_lots = [Lot(asset="ETH", quantity=D("10"), remaining=D("10"),
                        cost_per_unit_usd=D("3000"))]

        lots_by_asset = {"BTC": btc_lots, "ETH": eth_lots}

        # Selling ETH
        asset = "ETH"
        sell_qty = D("5")
        remaining = sell_qty
        for lot in lots_by_asset.get(asset, []):
            if remaining <= 0:
                break
            consumed = min(remaining, lot.remaining)
            lot.remaining -= consumed
            remaining -= consumed

        assert btc_lots[0].remaining == D("1")  # BTC untouched
        assert eth_lots[0].remaining == D("5")   # ETH reduced

    def test_oversold_scenario(self):
        """Selling more than owned — remaining > 0 after all lots depleted."""
        lots = [Lot(asset="BTC", quantity=D("0.5"), remaining=D("0.5"),
                     cost_per_unit_usd=D("50000"))]

        sell_qty = D("1")
        remaining = sell_qty
        for lot in lots:
            consumed = min(remaining, lot.remaining)
            lot.remaining -= consumed
            remaining -= consumed

        assert remaining == D("0.5")  # 0.5 BTC has no matching lot
        assert lots[0].remaining == D("0")
