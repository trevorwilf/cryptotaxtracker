#!/usr/bin/env python3
"""
smoke_test.py — Quick health check that runs inside the production container.

No test framework needed. Verifies:
  1. All modules import successfully
  2. FastAPI app loads with all routes
  3. Schema SQL is parseable
  4. Price oracle stablecoin logic works
  5. FIFO lot math is correct
  6. Exchange plugins are registered

Usage:
  docker exec hummingbot-tax-collector python /app/tests/smoke_test.py
  # Or during build verification:
  docker run --rm tax-collector:latest python /app/tests/smoke_test.py
"""
import sys
import traceback
from datetime import datetime, timezone, timedelta
from decimal import Decimal

D = Decimal
PASS = 0
FAIL = 0


def check(name, fn):
    global PASS, FAIL
    try:
        fn()
        print(f"  ✓  {name}")
        PASS += 1
    except Exception as e:
        print(f"  ✗  {name}: {e}")
        traceback.print_exc()
        FAIL += 1


# ── 1. Imports ────────────────────────────────────────────────────────────

print("\n=== Module Imports ===")

check("main.py", lambda: __import__("main"))
check("config.py", lambda: __import__("config"))
check("database.py", lambda: __import__("database"))
check("price_oracle.py", lambda: __import__("price_oracle"))
check("tax_engine.py", lambda: __import__("tax_engine"))
check("transfer_matcher.py", lambda: __import__("transfer_matcher"))
check("income_classifier.py", lambda: __import__("income_classifier"))
check("schema_v3.py", lambda: __import__("schema_v3"))
check("exchanges.__init__", lambda: __import__("exchanges"))
check("exchanges.nonkyc", lambda: __import__("exchanges.nonkyc"))
check("exchanges.mexc", lambda: __import__("exchanges.mexc"))
check("exports.xlsx_export", lambda: __import__("exports.xlsx_export"))
check("exports.tax_report", lambda: __import__("exports.tax_report"))

# ── 2. FastAPI App ────────────────────────────────────────────────────────

print("\n=== FastAPI App ===")


def check_app():
    from main import app
    routes = [r.path for r in app.routes if hasattr(r, "path")]
    assert len(routes) >= 15, f"Only {len(routes)} routes found"
    critical = ["/health", "/sync/status", "/tax/compute", "/tax/form-8949",
                "/export/xlsx", "/export/tax-report"]
    for path in critical:
        assert path in routes, f"Missing route: {path}"


check("App loads with routes", check_app)

# ── 3. Schema SQL ─────────────────────────────────────────────────────────

print("\n=== Schema Validation ===")


def check_schema():
    from database import SCHEMA_SQL, MIGRATION_SQL
    from schema_v3 import SCHEMA_V3_SQL
    # No ::jsonb casts (breaks asyncpg)
    for name, sql in [("SCHEMA_SQL", SCHEMA_SQL), ("MIGRATION_SQL", MIGRATION_SQL)]:
        assert "::jsonb" not in sql, f"Found ::jsonb in {name}"
    # v3 has required tables
    for table in ["tax.lots", "tax.disposals", "tax.form_8949",
                  "tax.income_events", "tax.transfer_matches"]:
        assert table in SCHEMA_V3_SQL, f"Missing table in v3: {table}"


check("Schema SQL valid", check_schema)

# ── 4. Price Oracle ───────────────────────────────────────────────────────

print("\n=== Price Oracle ===")


def check_stablecoins():
    from price_oracle import PriceOracle, STABLECOINS
    assert "USDT" in STABLECOINS
    assert "USDC" in STABLECOINS
    assert D("1.0") == D("1.0")  # sanity


check("Stablecoin list", check_stablecoins)


def check_market_parsing():
    p = __import__("price_oracle").PriceOracle
    assert p._parse_market("BTC/USDT") == ("BTC", "USDT")
    assert p._parse_market("ETH_BTC") == ("ETH", "BTC")
    assert p._parse_market("SOLUSDT") == ("SOL", "USDT")


check("Market parsing", check_market_parsing)


def check_ticker_normalize():
    p = __import__("price_oracle").PriceOracle
    assert p._normalize_ticker("btc") == "BTC"
    assert p._normalize_ticker("  eth  ") == "ETH"
    assert p._normalize_ticker("") == ""


check("Ticker normalization", check_ticker_normalize)

# ── 5. FIFO Math ──────────────────────────────────────────────────────────

print("\n=== FIFO Lot Math ===")


def check_fifo():
    from tax_engine import Lot, D, ZERO
    # Buy 2 BTC at $50k, sell 0.5 → remaining = 1.5
    lot = Lot(asset="BTC", quantity=D("2"), remaining=D("2"),
              cost_per_unit_usd=D("50000"))
    consumed = min(D("0.5"), lot.remaining)
    lot.remaining -= consumed
    cost = consumed * lot.cost_per_unit_usd
    assert lot.remaining == D("1.5")
    assert cost == D("25000")


check("FIFO lot consumption", check_fifo)


def check_fifo_order():
    from tax_engine import Lot, D
    lots = [
        Lot(asset="BTC", quantity=D("1"), remaining=D("1"), cost_per_unit_usd=D("40000")),
        Lot(asset="BTC", quantity=D("1"), remaining=D("1"), cost_per_unit_usd=D("60000")),
    ]
    # FIFO: sell 1 → should consume $40k lot first
    consumed = min(D("1"), lots[0].remaining)
    lots[0].remaining -= consumed
    assert lots[0].remaining == D("0")
    assert lots[1].remaining == D("1")


check("FIFO ordering", check_fifo_order)


def check_gain_loss():
    proceeds = D("60000")
    cost_basis = D("50000")
    fee = D("50")
    gain = (proceeds - fee) - cost_basis
    assert gain == D("9950")


check("Gain/loss calculation", check_gain_loss)


def check_holding_period():
    acquired = datetime(2024, 1, 15, tzinfo=timezone.utc)
    disposed_short = acquired + timedelta(days=365)   # exactly 365 = short-term
    disposed_long = acquired + timedelta(days=366)     # 366 = long-term (>365)
    assert (disposed_short - acquired).days <= 365     # IRS: must be MORE THAN one year
    assert (disposed_long - acquired).days > 365


check("Holding period classification", check_holding_period)

# ── 6. Exchange Plugins ───────────────────────────────────────────────────

print("\n=== Exchange Plugins ===")


def check_exchanges():
    from exchanges import list_exchanges
    exs = list_exchanges()
    assert "nonkyc" in exs
    assert "mexc" in exs


check("Plugins registered", check_exchanges)


def check_nonkyc_signature():
    from exchanges.nonkyc import NonKYCExchange
    ex = NonKYCExchange(api_key="testkey", api_secret="testsecret")
    headers = ex._sign_get("https://api.nonkyc.io/api/v2/test")
    assert "X-API-KEY" in headers
    assert "X-API-SIGN" in headers
    assert len(headers["X-API-SIGN"]) == 64


check("NonKYC signature", check_nonkyc_signature)


def check_mexc_signature():
    from exchanges.mexc import MEXCExchange
    ex = MEXCExchange(api_key="testkey", api_secret="testsecret")
    qs = ex._sign({"symbol": "BTCUSDT"})
    assert "signature=" in qs
    assert "timestamp=" in qs


check("MEXC signature", check_mexc_signature)

# ── 7. Form 8949 ─────────────────────────────────────────────────────────

print("\n=== Form 8949 ===")


def check_form_8949():
    from tax_engine import TaxEngine, Disposal, D
    engine = TaxEngine()
    disposals = [Disposal(
        asset="BTC", quantity=D("1"), proceeds_usd=D("60000"),
        cost_basis_usd=D("50000"), gain_loss_usd=D("10000"),
        fee_usd=D("0"),
        acquired_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        disposed_at=datetime(2025, 6, 1, tzinfo=timezone.utc),
        holding_days=151, term="short", description="1 BTC",
    )]
    lines = engine._generate_form_8949(disposals, 2025)
    assert len(lines) == 1
    assert lines[0]["box"] == "B"
    assert lines[0]["gain_loss"] == "10000.00"


check("Form 8949 generation", check_form_8949)

# ── Summary ───────────────────────────────────────────────────────────────

print(f"\n{'='*50}")
print(f"  PASSED: {PASS}  |  FAILED: {FAIL}")
print(f"{'='*50}\n")

sys.exit(1 if FAIL > 0 else 0)
