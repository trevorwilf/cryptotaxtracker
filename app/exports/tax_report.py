"""
Full Tax Report XLSX Generator — accountant-ready output.

Tabs:
  1. Summary         — high-level numbers, Schedule D totals
  2. Form 8949 (ST)  — short-term capital gains/losses (Box B)
  3. Form 8949 (LT)  — long-term capital gains/losses (Box D)
  4. Income Schedule  — staking rewards, airdrops (ordinary income)
  5. Transfer Recon   — matched cross-exchange transfers (non-taxable)
  6. Fee Summary      — deductible trading fees by exchange
  7. Lot Inventory    — all acquisition lots with remaining quantities
  8+  Per-exchange raw data tabs (from original export)

This function is called via GET /export/tax-report?year=2025
"""
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("tax-collector.export")

EXPORT_DIR = os.environ.get("TAX_EXPORT_DIR", "/data/exports")

# Styles
H_FONT = Font(name="Arial", bold=True, size=11, color="FFFFFF")
H_FILL = PatternFill("solid", fgColor="2F5496")
H2_FONT = Font(name="Arial", bold=True, size=12, color="2F5496")
TITLE_FONT = Font(name="Arial", bold=True, size=14, color="2F5496")
DATA_FONT = Font(name="Arial", size=10)
USD_FONT = Font(name="Arial", size=10, color="006100")
LOSS_FONT = Font(name="Arial", size=10, color="CC0000")
DIM_FONT = Font(name="Arial", size=10, color="999999", italic=True)
NOTE_FONT = Font(name="Arial", size=9, color="666666")
BOLD_FONT = Font(name="Arial", bold=True, size=10)
USD_FMT = '$#,##0.00'
CRYPTO_FMT = '#,##0.00000000'
DATE_FMT = "YYYY-MM-DD HH:MM:SS"
BORDER = Border(bottom=Side(style="thin", color="D9D9D9"))

D = Decimal


def _hdr(ws, row, headers):
    for ci, h in enumerate(headers, 1):
        c = ws.cell(row=row, column=ci, value=h)
        c.font = H_FONT
        c.fill = H_FILL
        c.alignment = Alignment(horizontal="center", vertical="center")


def _auto(ws, mn=10, mx=35):
    for col_cells in ws.columns:
        ml = max((len(str(c.value or "")) for c in col_cells), default=0)
        ws.column_dimensions[get_column_letter(col_cells[0].column)].width = min(max(ml + 2, mn), mx)


def _usd_cell(ws, row, col, val):
    c = ws.cell(row=row, column=col)
    try:
        fv = float(val) if val else 0
        c.value = fv
        c.number_format = USD_FMT
        c.font = LOSS_FONT if fv < 0 else USD_FONT
    except (ValueError, TypeError):
        c.value = "N/A"
        c.font = DIM_FONT
    c.border = BORDER


def _data_cell(ws, row, col, val, is_date=False):
    c = ws.cell(row=row, column=col)
    c.border = BORDER
    c.font = DATA_FONT
    if is_date and isinstance(val, datetime):
        c.value = val
        c.number_format = DATE_FMT
    else:
        c.value = str(val) if val is not None else ""


async def generate_full_tax_report(session: AsyncSession, year: int) -> str:
    """Generate the full accountant-ready XLSX tax report."""
    os.makedirs(EXPORT_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"tax_report_{year}_full_{ts}.xlsx"
    filepath = os.path.join(EXPORT_DIR, filename)
    wb = Workbook()

    await _build_summary(wb, session, year)
    await _build_form_8949(wb, session, year, "short", "Form 8949 (ST)")
    await _build_form_8949(wb, session, year, "long", "Form 8949 (LT)")
    await _build_income_schedule(wb, session, year)
    await _build_transfer_recon(wb, session)
    await _build_fee_summary(wb, session, year)
    await _build_lot_inventory(wb, session)
    await _build_raw_trades(wb, session, year)

    wb.save(filepath)
    logger.info(f"Full tax report exported: {filepath}")
    return filepath


# ── Tab 1: Summary ────────────────────────────────────────────────────────

async def _build_summary(wb, session, year):
    ws = wb.active
    ws.title = "Summary"

    ws.cell(row=1, column=1, value=f"Cryptocurrency Tax Report — {year}").font = TITLE_FONT
    ws.cell(row=2, column=1, value=f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}").font = NOTE_FONT
    ws.cell(row=3, column=1, value="Cost basis method: FIFO (First In, First Out)").font = NOTE_FONT

    # Schedule D summary
    ws.cell(row=5, column=1, value="SCHEDULE D SUMMARY").font = H2_FONT
    r = 6
    _hdr(ws, r, ["Category", "Proceeds (USD)", "Cost Basis (USD)", "Net Gain/Loss (USD)"])
    r += 1

    res = await session.execute(text("""
        SELECT
            COALESCE(SUM(CASE WHEN term='short' THEN proceeds END), 0),
            COALESCE(SUM(CASE WHEN term='short' THEN cost_basis END), 0),
            COALESCE(SUM(CASE WHEN term='short' THEN gain_loss END), 0),
            COALESCE(SUM(CASE WHEN term='long' THEN proceeds END), 0),
            COALESCE(SUM(CASE WHEN term='long' THEN cost_basis END), 0),
            COALESCE(SUM(CASE WHEN term='long' THEN gain_loss END), 0),
            COUNT(*)
        FROM tax.form_8949 WHERE tax_year = :y
    """), {"y": year})
    d = res.fetchone()

    for label, proc, cost, gl in [
        ("Short-Term (< 1 year)", d[0], d[1], d[2]),
        ("Long-Term (≥ 1 year)", d[3], d[4], d[5]),
        ("TOTAL", float(d[0]) + float(d[3]), float(d[1]) + float(d[4]), float(d[2]) + float(d[5])),
    ]:
        _data_cell(ws, r, 1, label)
        ws.cell(row=r, column=1).font = BOLD_FONT if label == "TOTAL" else DATA_FONT
        _usd_cell(ws, r, 2, proc)
        _usd_cell(ws, r, 3, cost)
        _usd_cell(ws, r, 4, gl)
        r += 1

    # Ordinary income summary
    r += 1
    ws.cell(row=r, column=1, value="ORDINARY INCOME (Staking, Rewards)").font = H2_FONT
    r += 1
    _hdr(ws, r, ["Income Type", "Count", "Total (USD)"])
    r += 1

    res = await session.execute(text("""
        SELECT income_type, COUNT(*), COALESCE(SUM(amount_usd), 0)
        FROM tax.income_events
        WHERE EXTRACT(YEAR FROM received_at) = :y
        GROUP BY income_type ORDER BY income_type
    """), {"y": year})
    income_total = D("0")
    for row_data in res.fetchall():
        _data_cell(ws, r, 1, row_data[0].replace("_", " ").title())
        _data_cell(ws, r, 2, row_data[1])
        _usd_cell(ws, r, 3, row_data[2])
        income_total += D(str(row_data[2]))
        r += 1
    _data_cell(ws, r, 1, "TOTAL INCOME")
    ws.cell(row=r, column=1).font = BOLD_FONT
    _usd_cell(ws, r, 3, str(income_total))
    r += 2

    # Fee summary
    ws.cell(row=r, column=1, value="DEDUCTIBLE TRADING FEES").font = H2_FONT
    r += 1
    res = await session.execute(text("""
        SELECT exchange, COALESCE(SUM(fee_usd), 0)
        FROM tax.trades
        WHERE fee_usd > 0 AND EXTRACT(YEAR FROM executed_at) = :y
        GROUP BY exchange
    """), {"y": year})
    fee_total = D("0")
    for row_data in res.fetchall():
        _data_cell(ws, r, 1, row_data[0].upper())
        _usd_cell(ws, r, 2, row_data[1])
        fee_total += D(str(row_data[1]))
        r += 1
    _data_cell(ws, r, 1, "TOTAL FEES")
    ws.cell(row=r, column=1).font = BOLD_FONT
    _usd_cell(ws, r, 2, str(fee_total))
    r += 2

    # Transfer summary
    res = await session.execute(text("SELECT COUNT(*) FROM tax.transfer_matches"))
    tm_count = res.scalar() or 0
    ws.cell(row=r, column=1, value=f"Cross-exchange transfers matched: {tm_count} (non-taxable)").font = NOTE_FONT

    _auto(ws)


# ── Tab 2/3: Form 8949 ───────────────────────────────────────────────────

async def _build_form_8949(wb, session, year, term, tab_name):
    ws = wb.create_sheet(title=tab_name)

    box = "B" if term == "short" else "D"
    label = "Short-Term" if term == "short" else "Long-Term"

    ws.cell(row=1, column=1, value=f"Form 8949 — {label} Capital Gains and Losses").font = TITLE_FONT
    ws.cell(row=2, column=1, value=f"Tax Year: {year} | Box {box} (basis NOT reported to IRS)").font = NOTE_FONT

    r = 4
    headers = [
        "(a) Description", "(b) Date Acquired", "(c) Date Sold",
        "(d) Proceeds", "(e) Cost Basis",
        "(f) Code", "(g) Adjustment",
        "(h) Gain or Loss", "Asset", "Exchange", "Holding Days"
    ]
    _hdr(ws, r, headers)
    r += 1

    res = await session.execute(text("""
        SELECT description, date_acquired, date_sold, proceeds, cost_basis,
               adjustment_code, adjustment_amount, gain_loss, asset, exchange, holding_days
        FROM tax.form_8949
        WHERE tax_year = :y AND term = :t
        ORDER BY date_sold, asset
    """), {"y": year, "t": term})

    total_proceeds = D("0")
    total_cost = D("0")
    total_gl = D("0")
    count = 0

    for row_data in res.fetchall():
        _data_cell(ws, r, 1, row_data[0])
        _data_cell(ws, r, 2, row_data[1])
        _data_cell(ws, r, 3, row_data[2])
        _usd_cell(ws, r, 4, row_data[3])
        _usd_cell(ws, r, 5, row_data[4])
        _data_cell(ws, r, 6, row_data[5] or "")
        _usd_cell(ws, r, 7, row_data[6])
        _usd_cell(ws, r, 8, row_data[7])
        _data_cell(ws, r, 9, row_data[8])
        _data_cell(ws, r, 10, row_data[9])
        _data_cell(ws, r, 11, row_data[10])
        total_proceeds += D(str(row_data[3] or 0))
        total_cost += D(str(row_data[4] or 0))
        total_gl += D(str(row_data[7] or 0))
        count += 1
        r += 1

    # Totals row
    r += 1
    ws.cell(row=r, column=1, value=f"TOTALS ({count} disposals)").font = BOLD_FONT
    _usd_cell(ws, r, 4, str(total_proceeds))
    ws.cell(row=r, column=4).font = Font(name="Arial", bold=True, size=10, color="006100")
    _usd_cell(ws, r, 5, str(total_cost))
    ws.cell(row=r, column=5).font = Font(name="Arial", bold=True, size=10, color="006100")
    _usd_cell(ws, r, 8, str(total_gl))
    ws.cell(row=r, column=8).font = Font(name="Arial", bold=True, size=10,
                                          color="CC0000" if total_gl < 0 else "006100")

    _auto(ws)


# ── Tab 4: Income Schedule ────────────────────────────────────────────────

async def _build_income_schedule(wb, session, year):
    ws = wb.create_sheet(title="Income Schedule")

    ws.cell(row=1, column=1, value=f"Ordinary Income Schedule — {year}").font = TITLE_FONT
    ws.cell(row=2, column=1,
            value="Staking rewards and airdrops are taxed as ordinary income at FMV when received").font = NOTE_FONT

    r = 4
    _hdr(ws, r, ["Date Received", "Type", "Asset", "Amount", "FMV (USD)", "Exchange", "Description"])
    r += 1

    res = await session.execute(text("""
        SELECT received_at, income_type, asset, amount, amount_usd, exchange, description
        FROM tax.income_events
        WHERE EXTRACT(YEAR FROM received_at) = :y
        ORDER BY received_at ASC
    """), {"y": year})

    total = D("0")
    for row_data in res.fetchall():
        _data_cell(ws, r, 1, row_data[0], is_date=True)
        _data_cell(ws, r, 2, row_data[1].replace("_", " ").title())
        _data_cell(ws, r, 3, row_data[2])
        _data_cell(ws, r, 4, row_data[3])
        ws.cell(row=r, column=4).number_format = CRYPTO_FMT
        _usd_cell(ws, r, 5, row_data[4])
        _data_cell(ws, r, 6, row_data[5])
        _data_cell(ws, r, 7, row_data[6])
        total += D(str(row_data[4] or 0))
        r += 1

    r += 1
    ws.cell(row=r, column=1, value="TOTAL ORDINARY INCOME").font = BOLD_FONT
    _usd_cell(ws, r, 5, str(total))

    _auto(ws)


# ── Tab 5: Transfer Reconciliation ───────────────────────────────────────

async def _build_transfer_recon(wb, session):
    ws = wb.create_sheet(title="Transfer Recon")

    ws.cell(row=1, column=1, value="Cross-Exchange Transfer Reconciliation").font = TITLE_FONT
    ws.cell(row=2, column=1,
            value="Matched transfers are NOT taxable events — cost basis carries over").font = NOTE_FONT

    r = 4
    _hdr(ws, r, ["Date", "Asset", "Amount", "From Exchange", "To Exchange",
                  "TX Hash", "Confidence", "Cost Basis (USD)"])
    r += 1

    res = await session.execute(text("""
        SELECT transferred_at, asset, amount, from_exchange, to_exchange,
               tx_hash, match_confidence, cost_basis_usd
        FROM tax.transfer_matches
        ORDER BY transferred_at ASC
    """))

    for row_data in res.fetchall():
        _data_cell(ws, r, 1, row_data[0], is_date=True)
        _data_cell(ws, r, 2, row_data[1])
        _data_cell(ws, r, 3, row_data[2])
        ws.cell(row=r, column=3).number_format = CRYPTO_FMT
        _data_cell(ws, r, 4, (row_data[3] or "").upper())
        _data_cell(ws, r, 5, (row_data[4] or "").upper())
        _data_cell(ws, r, 6, row_data[5] or "")
        _data_cell(ws, r, 7, row_data[6] or "")
        _usd_cell(ws, r, 8, row_data[7])
        r += 1

    if r == 5:
        _data_cell(ws, r, 1, "No matched transfers found")
        ws.cell(row=r, column=1).font = DIM_FONT

    _auto(ws)


# ── Tab 6: Fee Summary ───────────────────────────────────────────────────

async def _build_fee_summary(wb, session, year):
    ws = wb.create_sheet(title="Fee Summary")

    ws.cell(row=1, column=1, value=f"Trading Fee Summary — {year}").font = TITLE_FONT
    ws.cell(row=2, column=1,
            value="Trading fees may be deductible — consult your tax professional").font = NOTE_FONT

    r = 4
    _hdr(ws, r, ["Exchange", "Total Trades", "Trades with Fees", "Total Fees (USD)"])
    r += 1

    res = await session.execute(text("""
        SELECT exchange,
               COUNT(*),
               COUNT(CASE WHEN fee_usd > 0 THEN 1 END),
               COALESCE(SUM(fee_usd), 0)
        FROM tax.trades
        WHERE EXTRACT(YEAR FROM executed_at) = :y
        GROUP BY exchange ORDER BY exchange
    """), {"y": year})

    grand_total = D("0")
    for row_data in res.fetchall():
        _data_cell(ws, r, 1, row_data[0].upper())
        _data_cell(ws, r, 2, row_data[1])
        _data_cell(ws, r, 3, row_data[2])
        _usd_cell(ws, r, 4, row_data[3])
        grand_total += D(str(row_data[3]))
        r += 1

    r += 1
    ws.cell(row=r, column=1, value="TOTAL DEDUCTIBLE FEES").font = BOLD_FONT
    _usd_cell(ws, r, 4, str(grand_total))

    # Per-asset fee breakdown
    r += 2
    ws.cell(row=r, column=1, value="Fees by Asset").font = H2_FONT
    r += 1
    _hdr(ws, r, ["Fee Asset", "Total Fees", "Total Fees (USD)"])
    r += 1

    res = await session.execute(text("""
        SELECT COALESCE(fee_asset, 'Unknown'), SUM(fee)::text, COALESCE(SUM(fee_usd), 0)
        FROM tax.trades
        WHERE fee > 0 AND EXTRACT(YEAR FROM executed_at) = :y
        GROUP BY fee_asset ORDER BY SUM(fee_usd) DESC NULLS LAST
    """), {"y": year})

    for row_data in res.fetchall():
        _data_cell(ws, r, 1, row_data[0])
        _data_cell(ws, r, 2, row_data[1])
        ws.cell(row=r, column=2).number_format = CRYPTO_FMT
        _usd_cell(ws, r, 3, row_data[2])
        r += 1

    _auto(ws)


# ── Tab 7: Lot Inventory ─────────────────────────────────────────────────

async def _build_lot_inventory(wb, session):
    ws = wb.create_sheet(title="Lot Inventory")

    ws.cell(row=1, column=1, value="FIFO Lot Inventory — Current Holdings").font = TITLE_FONT
    ws.cell(row=2, column=1, value="Shows remaining acquisition lots and their cost basis").font = NOTE_FONT

    r = 4
    _hdr(ws, r, ["Asset", "Acquired", "Source", "Exchange",
                  "Original Qty", "Remaining", "Cost/Unit (USD)", "Total Cost (USD)"])
    r += 1

    res = await session.execute(text("""
        SELECT asset, acquired_at, source, exchange,
               quantity, remaining, cost_per_unit_usd, total_cost_usd
        FROM tax.lots
        WHERE remaining > 0
        ORDER BY asset, acquired_at
    """))

    for row_data in res.fetchall():
        _data_cell(ws, r, 1, row_data[0])
        _data_cell(ws, r, 2, row_data[1], is_date=True)
        _data_cell(ws, r, 3, (row_data[2] or "").replace("_", " ").title())
        _data_cell(ws, r, 4, (row_data[3] or "").upper())
        _data_cell(ws, r, 5, row_data[4])
        ws.cell(row=r, column=5).number_format = CRYPTO_FMT
        _data_cell(ws, r, 6, row_data[5])
        ws.cell(row=r, column=6).number_format = CRYPTO_FMT
        _usd_cell(ws, r, 7, row_data[6])
        _usd_cell(ws, r, 8, row_data[7])
        r += 1

    _auto(ws)


# ── Tab 8+: Raw trade data per exchange ──────────────────────────────────

async def _build_raw_trades(wb, session, year):
    """Add per-exchange raw trade data tabs."""
    res = await session.execute(text("""
        SELECT DISTINCT exchange FROM tax.trades
        WHERE EXTRACT(YEAR FROM executed_at) = :y
        ORDER BY 1
    """), {"y": year})
    exchanges = [row[0] for row in res.fetchall()]

    for ex in exchanges:
        tab_name = f"{ex.upper()} Trades"[:31]
        ws = wb.create_sheet(title=tab_name)

        ws.cell(row=1, column=1, value=f"{ex.upper()} — All Trades {year}").font = TITLE_FONT
        r = 3
        headers = ["Date", "Market", "Side", "Price", "Qty", "Total",
                   "Fee", "Fee Asset", "Total (USD)", "Fee (USD)", "Exchange ID"]
        _hdr(ws, r, headers)
        r += 1

        trades = await session.execute(text("""
            SELECT executed_at, market, side, price, quantity, total,
                   fee, fee_asset, total_usd, fee_usd, exchange_id
            FROM tax.trades
            WHERE exchange = :ex AND EXTRACT(YEAR FROM executed_at) = :y
            ORDER BY executed_at ASC
        """), {"ex": ex, "y": year})

        for t in trades.fetchall():
            _data_cell(ws, r, 1, t[0], is_date=True)
            _data_cell(ws, r, 2, t[1])
            c = ws.cell(row=r, column=3, value=(t[2] or "").upper())
            c.font = Font(name="Arial", size=10, bold=True,
                          color="006100" if t[2] == "buy" else "CC0000")
            _data_cell(ws, r, 4, t[3])
            ws.cell(row=r, column=4).number_format = CRYPTO_FMT
            _data_cell(ws, r, 5, t[4])
            ws.cell(row=r, column=5).number_format = CRYPTO_FMT
            _data_cell(ws, r, 6, t[5])
            ws.cell(row=r, column=6).number_format = CRYPTO_FMT
            _data_cell(ws, r, 7, t[6])
            ws.cell(row=r, column=7).number_format = CRYPTO_FMT
            _data_cell(ws, r, 8, t[7])
            _usd_cell(ws, r, 9, t[8])
            _usd_cell(ws, r, 10, t[9])
            _data_cell(ws, r, 11, t[10])
            r += 1

        _auto(ws)
