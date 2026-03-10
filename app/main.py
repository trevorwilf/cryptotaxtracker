"""
Tax Collector — Unified trade data aggregator for tax reporting.

FastAPI server with APScheduler for scheduled + on-demand syncs.
Pulls trades, deposits, withdrawals, orders, and pool activity from
configured exchanges, resolves USD values via CoinGecko + NonKYC,
and stores everything in PostgreSQL (tax schema).
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from config import Settings
from database import Database
from exchanges import get_exchange, list_exchanges
from exports.xlsx_export import generate_tax_xlsx
from price_oracle import PriceOracle
from tax_engine import TaxEngine
from transfer_matcher import TransferMatcher
from income_classifier import IncomeClassifier

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("tax-collector")

settings = Settings()
db = Database(settings.database_url)
oracle = PriceOracle()
tax_engine = TaxEngine()
transfer_matcher = TransferMatcher()
income_classifier = IncomeClassifier()
scheduler = AsyncIOScheduler(timezone="UTC")

sync_status: dict[str, dict] = {}
backfill_status: dict = {"status": "idle"}


# ── Sync logic ────────────────────────────────────────────────────────────

async def run_sync(exchange_name: str, full: bool = False):
    """Pull all tax-relevant data from an exchange and resolve USD values."""
    logger.info(f"Starting {'full' if full else 'incremental'} sync for {exchange_name}")
    sync_status[exchange_name] = {"status": "running", "started": datetime.now(timezone.utc).isoformat()}

    try:
        ex = get_exchange(exchange_name, settings)
        if ex is None:
            raise ValueError(f"Exchange '{exchange_name}' not configured or unknown")

        async with db.get_session() as session:
            last_ts = {}
            if not full:
                last_ts = await db.get_last_sync_timestamps(session, exchange_name)

            counts = {}

            # ── Trades ────────────────────────────────────────────────
            logger.info(f"  [{exchange_name}] Pulling trades...")
            trades = await ex.fetch_trades(since=last_ts.get("trades"))
            if trades:
                # Resolve USD for each trade
                for t in trades:
                    usd = await oracle.resolve_trade_usd(
                        session, t["market"], t["side"],
                        t["price"], t["quantity"], t["total"],
                        t["fee"], t["fee_asset"], t["executed_at"],
                    )
                    t["base_asset"] = usd["base_asset"]
                    t["quote_asset"] = usd["quote_asset"]
                    t["price_usd"] = usd["price_usd"]
                    t["quantity_usd"] = usd["quantity_usd"]
                    t["total_usd"] = usd["total_usd"]
                    t["fee_usd"] = usd["fee_usd"]
                    t["base_price_usd"] = usd["base_price_usd"]
                    t["quote_price_usd"] = usd["quote_price_usd"]
                await db.upsert_trades(session, exchange_name, trades)
                counts["trades"] = len(trades)
                logger.info(f"  [{exchange_name}] Stored {len(trades)} trades with USD values")

            # ── Orders ────────────────────────────────────────────────
            logger.info(f"  [{exchange_name}] Pulling orders...")
            orders = await ex.fetch_orders(since=last_ts.get("orders"))
            if orders:
                for o in orders:
                    usd = await oracle.resolve_trade_usd(
                        session, o["market"], o["side"],
                        o["price"], o["quantity"], "0",
                        "0", "", o["created_at_ex"],
                    )
                    o["base_asset"] = usd["base_asset"]
                    o["quote_asset"] = usd["quote_asset"]
                    o["price_usd"] = usd["price_usd"]
                    # total_usd for orders = executed_qty * price_usd
                    try:
                        exec_qty = float(o.get("executed_qty", 0) or 0)
                        p_usd = float(usd["price_usd"]) if usd["price_usd"] else 0
                        o["total_usd"] = str(exec_qty * p_usd) if p_usd else None
                    except (ValueError, TypeError):
                        o["total_usd"] = None
                    o["fee_usd"] = None  # order-level fees tracked via trades
                await db.upsert_orders(session, exchange_name, orders)
                counts["orders"] = len(orders)
                logger.info(f"  [{exchange_name}] Stored {len(orders)} orders")

            # ── Deposits ──────────────────────────────────────────────
            logger.info(f"  [{exchange_name}] Pulling deposits...")
            deposits = await ex.fetch_deposits(since=last_ts.get("deposits"))
            if deposits:
                for d in deposits:
                    ts = d.get("confirmed_at") or datetime.now(timezone.utc)
                    usd = await oracle.resolve_transfer_usd(
                        session, d["asset"], d["amount"], None, ts,
                    )
                    d["asset_price_usd"] = usd["asset_price_usd"]
                    d["amount_usd"] = usd["amount_usd"]
                await db.upsert_deposits(session, exchange_name, deposits)
                counts["deposits"] = len(deposits)
                logger.info(f"  [{exchange_name}] Stored {len(deposits)} deposits with USD")

            # ── Withdrawals ───────────────────────────────────────────
            logger.info(f"  [{exchange_name}] Pulling withdrawals...")
            withdrawals = await ex.fetch_withdrawals(since=last_ts.get("withdrawals"))
            if withdrawals:
                for w in withdrawals:
                    ts = w.get("confirmed_at") or datetime.now(timezone.utc)
                    usd = await oracle.resolve_transfer_usd(
                        session, w["asset"], w["amount"], w.get("fee"), ts,
                    )
                    w["asset_price_usd"] = usd["asset_price_usd"]
                    w["amount_usd"] = usd["amount_usd"]
                    w["fee_usd"] = usd["fee_usd"]
                await db.upsert_withdrawals(session, exchange_name, withdrawals)
                counts["withdrawals"] = len(withdrawals)
                logger.info(f"  [{exchange_name}] Stored {len(withdrawals)} withdrawals with USD")

            # ── Pool activity ─────────────────────────────────────────
            logger.info(f"  [{exchange_name}] Pulling pool activity...")
            pools = await ex.fetch_pool_activity(since=last_ts.get("pools"))
            if pools:
                for p in pools:
                    ts = p["executed_at"]
                    if p.get("asset_in") and p.get("amount_in"):
                        u_in = await oracle.resolve_transfer_usd(
                            session, p["asset_in"], p["amount_in"], None, ts)
                        p["amount_in_usd"] = u_in["amount_usd"]
                    else:
                        p["amount_in_usd"] = None
                    if p.get("asset_out") and p.get("amount_out"):
                        u_out = await oracle.resolve_transfer_usd(
                            session, p["asset_out"], p["amount_out"], None, ts)
                        p["amount_out_usd"] = u_out["amount_usd"]
                    else:
                        p["amount_out_usd"] = None
                    if p.get("fee") and p.get("fee_asset"):
                        u_fee = await oracle.resolve_transfer_usd(
                            session, p["fee_asset"], p["fee"], None, ts)
                        p["fee_usd"] = u_fee["amount_usd"]
                    else:
                        p["fee_usd"] = None
                await db.upsert_pool_activity(session, exchange_name, pools)
                counts["pools"] = len(pools)
                logger.info(f"  [{exchange_name}] Stored {len(pools)} pool records with USD")

            await session.commit()

        sync_status[exchange_name] = {
            "status": "success",
            "finished": datetime.now(timezone.utc).isoformat(),
            **{k: v for k, v in counts.items()},
        }
        logger.info(f"Sync complete for {exchange_name}")

    except Exception as e:
        logger.exception(f"Sync failed for {exchange_name}: {e}")
        sync_status[exchange_name] = {
            "status": "error",
            "finished": datetime.now(timezone.utc).isoformat(),
            "error": str(e),
        }


async def run_sync_all(full: bool = False):
    for name in settings.enabled_exchanges:
        await run_sync(name, full=full)


# ── Backfill logic ────────────────────────────────────────────────────────

async def run_backfill_usd(batch_size: int = 100):
    """Backfill USD values for any records that are missing them."""
    global backfill_status
    backfill_status = {"status": "running", "started": datetime.now(timezone.utc).isoformat()}
    filled = {"trades": 0, "deposits": 0, "withdrawals": 0}

    try:
        async with db.get_session() as session:
            # Backfill trades
            trades = await db.get_trades_missing_usd(session, limit=batch_size)
            for t in trades:
                usd = await oracle.resolve_trade_usd(
                    session, t["market"], t["side"],
                    t["price"], t["quantity"], t["total"],
                    t["fee"], t.get("fee_asset", ""), t["executed_at"],
                )
                await db.update_trade_usd(session, t["id"], usd)
                filled["trades"] += 1

            # Backfill deposits
            deps = await db.get_deposits_missing_usd(session, limit=batch_size)
            for d in deps:
                ts = d.get("confirmed_at") or datetime.now(timezone.utc)
                usd = await oracle.resolve_transfer_usd(session, d["asset"], d["amount"], None, ts)
                await db.update_deposit_usd(session, d["id"], usd)
                filled["deposits"] += 1

            # Backfill withdrawals
            wds = await db.get_withdrawals_missing_usd(session, limit=batch_size)
            for w in wds:
                ts = w.get("confirmed_at") or datetime.now(timezone.utc)
                usd = await oracle.resolve_transfer_usd(
                    session, w["asset"], w["amount"], w.get("fee"), ts)
                await db.update_withdrawal_usd(session, w["id"], usd)
                filled["withdrawals"] += 1

            await session.commit()

        backfill_status = {
            "status": "success",
            "finished": datetime.now(timezone.utc).isoformat(),
            "filled": filled,
        }
        logger.info(f"Backfill complete: {filled}")

    except Exception as e:
        logger.exception(f"Backfill failed: {e}")
        backfill_status = {
            "status": "error",
            "finished": datetime.now(timezone.utc).isoformat(),
            "error": str(e),
            "filled_before_error": filled,
        }


# ── App lifecycle ─────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init()
    logger.info("Database initialized (tax schema + USD columns + price cache)")

    cron = settings.sync_cron
    scheduler.add_job(run_sync_all, CronTrigger.from_crontab(cron),
                      id="daily_sync", replace_existing=True)
    # Run backfill after each sync to catch any stragglers
    scheduler.add_job(run_backfill_usd, CronTrigger.from_crontab(cron.replace("3", "4", 1)),
                      id="daily_backfill", replace_existing=True)
    scheduler.start()
    logger.info(f"Scheduler started (sync: {cron}, backfill: 1h after sync)")
    logger.info(f"Configured exchanges: {', '.join(settings.enabled_exchanges)}")

    yield

    scheduler.shutdown(wait=False)
    await db.close()


app = FastAPI(
    title="Tax Collector",
    description="Unified trade data aggregator for tax reporting — with USD valuations",
    version="2.0.0",
    lifespan=lifespan,
)


# ── Health ────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "version": "2.0.0", "exchanges": settings.enabled_exchanges}


# ── Sync endpoints ───────────────────────────────────────────────────────

@app.post("/sync/{exchange}")
async def sync_exchange(exchange: str, full: bool = Query(False)):
    if exchange not in settings.enabled_exchanges:
        raise HTTPException(404, f"Exchange '{exchange}' not configured")
    asyncio.create_task(run_sync(exchange, full=full))
    return {"message": f"Sync started for {exchange}", "full": full}


@app.post("/sync")
async def sync_all(full: bool = Query(False)):
    asyncio.create_task(run_sync_all(full=full))
    return {"message": "Sync started for all exchanges", "full": full}


@app.get("/sync/status")
async def get_sync_status():
    return sync_status


# ── Backfill endpoint ────────────────────────────────────────────────────

@app.post("/backfill-usd")
async def backfill_usd(batch_size: int = Query(100, le=1000)):
    """Backfill USD values for records missing them. Runs in background.
    Use batch_size to control how many records per table per run
    (CoinGecko rate limits ~10-30 req/min on free tier)."""
    asyncio.create_task(run_backfill_usd(batch_size=batch_size))
    return {"message": "USD backfill started", "batch_size": batch_size}


@app.get("/backfill-usd/status")
async def get_backfill_status():
    return backfill_status


# ── Export endpoints ─────────────────────────────────────────────────────

@app.get("/export/xlsx")
async def export_xlsx(year: int = Query(None)):
    """Generate and download xlsx tax report with per-exchange tabs + USD columns."""
    try:
        async with db.get_session() as session:
            filepath = await generate_tax_xlsx(session, year=year)
        return FileResponse(
            filepath,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=os.path.basename(filepath),
        )
    except Exception as e:
        logger.exception(f"Export failed: {e}")
        raise HTTPException(500, f"Export failed: {str(e)}")


# ── Data query endpoints ─────────────────────────────────────────────────

@app.get("/trades/{exchange}")
async def get_trades(
    exchange: str,
    year: int = Query(None),
    limit: int = Query(100, le=10000),
    offset: int = Query(0),
):
    async with db.get_session() as session:
        rows = await db.query_trades(session, exchange, year=year, limit=limit, offset=offset)
    return {"exchange": exchange, "count": len(rows), "trades": rows}


@app.get("/summary")
async def get_summary(year: int = Query(None)):
    async with db.get_session() as session:
        summary = await db.get_summary(session, year=year)
    return summary


# ── Price cache stats ────────────────────────────────────────────────────

@app.get("/prices/stats")
async def price_stats():
    """Show how many prices are cached and coverage."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("SELECT COUNT(*), COUNT(DISTINCT asset) FROM tax.price_cache"))
        row = r.fetchone()
        total_prices = row[0] if row else 0
        unique_assets = row[1] if row else 0

        r = await session.execute(t(
            "SELECT asset, COUNT(*), MIN(price_date), MAX(price_date) "
            "FROM tax.price_cache GROUP BY asset ORDER BY COUNT(*) DESC LIMIT 20"))
        assets = [{"asset": row[0], "cached_days": row[1],
                    "earliest": str(row[2]), "latest": str(row[3])} for row in r.fetchall()]

    return {
        "total_cached_prices": total_prices,
        "unique_assets": unique_assets,
        "top_assets": assets,
    }


# ══════════════════════════════════════════════════════════════════════════
# TAX COMPUTATION ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════

tax_compute_status: dict = {"status": "idle"}


@app.post("/tax/match-transfers")
async def match_transfers():
    """Step 1: Match cross-exchange withdrawal→deposit pairs as non-taxable transfers."""
    async with db.get_session() as session:
        result = await transfer_matcher.match_transfers(session)
    return result


@app.get("/tax/unmatched-transfers")
async def get_unmatched():
    """View withdrawals/deposits that weren't matched as transfers."""
    async with db.get_session() as session:
        return await transfer_matcher.get_unmatched(session)


@app.post("/tax/classify-income")
async def classify_income():
    """Step 2: Identify staking rewards, airdrops, and other ordinary income."""
    async with db.get_session() as session:
        result = await income_classifier.classify(session)
    return result


@app.get("/tax/income")
async def get_income(year: int = Query(None)):
    """Get income schedule (staking rewards, airdrops, etc.)."""
    async with db.get_session() as session:
        return await income_classifier.get_income_summary(session, year=year)


@app.post("/tax/compute")
async def compute_taxes(year: int = Query(None)):
    """Step 3: Run FIFO cost basis computation and generate Form 8949.
    
    Recommended order:
      1. POST /tax/match-transfers
      2. POST /tax/classify-income
      3. POST /tax/compute?year=2025
    """
    global tax_compute_status
    tax_compute_status = {"status": "running", "started": datetime.now(timezone.utc).isoformat()}
    try:
        async with db.get_session() as session:
            result = await tax_engine.compute(session, year=year)
        tax_compute_status = {"status": "success", **result}
        return result
    except Exception as e:
        logger.exception(f"Tax computation failed: {e}")
        tax_compute_status = {"status": "error", "error": str(e)}
        raise HTTPException(500, f"Tax computation failed: {str(e)}")


@app.post("/tax/compute-all")
async def compute_all(year: int = Query(None)):
    """Run the full tax pipeline: transfers → income → FIFO → Form 8949."""
    results = {}
    async with db.get_session() as session:
        results["transfers"] = await transfer_matcher.match_transfers(session)
    async with db.get_session() as session:
        results["income"] = await income_classifier.classify(session)
    async with db.get_session() as session:
        results["tax"] = await tax_engine.compute(session, year=year)
    return results


@app.get("/tax/compute/status")
async def get_tax_status():
    return tax_compute_status


@app.get("/tax/form-8949")
async def get_form_8949(year: int = Query(..., description="Tax year (required)")):
    """Get Form 8949 data — one line per disposal."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        result = await session.execute(t("""
            SELECT description, date_acquired, date_sold, proceeds::text,
                   cost_basis::text, adjustment_code, adjustment_amount::text,
                   gain_loss::text, term, box, asset, exchange, holding_days,
                   is_futures
            FROM tax.form_8949
            WHERE tax_year = :year
            ORDER BY date_sold, asset
        """), {"year": year})
        lines = [dict(zip(result.keys(), row)) for row in result.fetchall()]

    # Compute totals
    from decimal import Decimal as D
    st_total = sum(D(l["gain_loss"] or "0") for l in lines if l["term"] == "short")
    lt_total = sum(D(l["gain_loss"] or "0") for l in lines if l["term"] == "long")

    return {
        "year": year,
        "total_lines": len(lines),
        "short_term_net": str(st_total),
        "long_term_net": str(lt_total),
        "net_total": str(st_total + lt_total),
        "lines": lines,
    }


@app.get("/tax/schedule-d")
async def get_schedule_d(year: int = Query(...)):
    """Schedule D summary — aggregated short-term and long-term totals."""
    async with db.get_session() as session:
        from sqlalchemy import text as t

        r = await session.execute(t("""
            SELECT
                COALESCE(SUM(CASE WHEN term='short' THEN proceeds END), 0)::text AS st_proceeds,
                COALESCE(SUM(CASE WHEN term='short' THEN cost_basis END), 0)::text AS st_cost,
                COALESCE(SUM(CASE WHEN term='short' THEN gain_loss END), 0)::text AS st_gain_loss,
                COALESCE(SUM(CASE WHEN term='long' THEN proceeds END), 0)::text AS lt_proceeds,
                COALESCE(SUM(CASE WHEN term='long' THEN cost_basis END), 0)::text AS lt_cost,
                COALESCE(SUM(CASE WHEN term='long' THEN gain_loss END), 0)::text AS lt_gain_loss,
                COUNT(*) AS total_disposals
            FROM tax.form_8949
            WHERE tax_year = :year
        """), {"year": year})
        row = r.fetchone()

    return {
        "year": year,
        "short_term": {
            "proceeds": row[0], "cost_basis": row[1], "gain_loss": row[2]
        },
        "long_term": {
            "proceeds": row[3], "cost_basis": row[4], "gain_loss": row[5]
        },
        "net_gain_loss": str(
            (D(row[2]) + D(row[5])) if row else D("0")
        ),
        "total_disposals": row[6] if row else 0,
    }


@app.get("/tax/fee-summary")
async def get_fee_summary(year: int = Query(None)):
    """Total deductible trading fees by exchange and year."""
    yf = ""
    params: dict = {}
    if year:
        yf = "AND EXTRACT(YEAR FROM executed_at) = :year"
        params["year"] = year

    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t(f"""
            SELECT exchange,
                   COUNT(*) AS trade_count,
                   COALESCE(SUM(fee_usd), 0)::text AS total_fees_usd
            FROM tax.trades
            WHERE fee_usd > 0 {yf}
            GROUP BY exchange
            ORDER BY exchange
        """), params)
        rows = [{"exchange": row[0], "trade_count": row[1], "total_fees_usd": row[2]}
                for row in r.fetchall()]

        total = sum(D(row["total_fees_usd"]) for row in rows)

    return {
        "year": year or "all",
        "by_exchange": rows,
        "total_deductible_fees_usd": str(total),
    }


@app.get("/tax/lots")
async def get_lots(asset: str = Query(None), show_depleted: bool = Query(False)):
    """View acquisition lots. Filter by asset, optionally show fully depleted lots."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        where = "WHERE 1=1"
        params: dict = {}
        if asset:
            where += " AND asset = :asset"
            params["asset"] = asset.upper()
        if not show_depleted:
            where += " AND remaining > 0"

        r = await session.execute(t(f"""
            SELECT asset, quantity::text, remaining::text, cost_per_unit_usd::text,
                   total_cost_usd::text, acquired_at, exchange, source
            FROM tax.lots {where}
            ORDER BY asset, acquired_at
        """), params)
        lots = [dict(zip(r.keys(), row)) for row in r.fetchall()]

    return {"count": len(lots), "lots": lots}


@app.get("/export/tax-report")
async def export_tax_report(year: int = Query(..., description="Tax year (required)")):
    """Generate the full accountant-ready tax report XLSX."""
    try:
        async with db.get_session() as session:
            from exports.tax_report import generate_full_tax_report
            filepath = await generate_full_tax_report(session, year=year)
        return FileResponse(
            filepath,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=os.path.basename(filepath),
        )
    except Exception as e:
        logger.exception(f"Tax report export failed: {e}")
        raise HTTPException(500, f"Export failed: {str(e)}")
