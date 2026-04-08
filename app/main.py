"""
Tax Collector — Unified trade data aggregator for tax reporting.

FastAPI server with APScheduler for scheduled + on-demand syncs.
Pulls trades, deposits, withdrawals, orders, and pool activity from
configured exchanges, resolves USD values via CoinGecko + NonKYC,
and stores everything in PostgreSQL (tax schema).
"""
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse

from config import Settings
from database import Database
from exchanges import get_exchange, list_exchanges
from exports.xlsx_export import generate_tax_xlsx
from price_oracle import PriceOracle
from tax_engine import TaxEngine
from transfer_matcher import TransferMatcher
from income_classifier import IncomeClassifier

from logging.handlers import TimedRotatingFileHandler

LOG_DIR = os.environ.get("LOG_DIR", "/data/logs")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = "[%(asctime)s] %(levelname)s %(name)s: %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging():
    """Configure logging to stdout + rotating log files."""
    os.makedirs(LOG_DIR, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    # Console handler (stdout)
    console = logging.StreamHandler()
    console.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    console.setFormatter(formatter)
    root.addHandler(console)

    # Main log file — rotates daily, keeps 90 days
    try:
        main_handler = TimedRotatingFileHandler(
            os.path.join(LOG_DIR, "tax-collector.log"),
            when="midnight", interval=1, backupCount=90, utc=True,
        )
        main_handler.setLevel(logging.INFO)
        main_handler.setFormatter(formatter)
        main_handler.suffix = "%Y-%m-%d"
        root.addHandler(main_handler)
    except (PermissionError, OSError) as e:
        root.warning(f"Could not create main log file: {e} — logging to stdout only")

    # API log file — DEBUG+ for exchange API loggers
    try:
        api_handler = TimedRotatingFileHandler(
            os.path.join(LOG_DIR, "api.log"),
            when="midnight", interval=1, backupCount=30, utc=True,
        )
        api_handler.setLevel(logging.DEBUG)
        api_handler.setFormatter(formatter)
        api_handler.suffix = "%Y-%m-%d"
        for api_logger_name in ["tax-collector.api", "tax-collector.nonkyc",
                                 "tax-collector.mexc", "tax-collector.parser",
                                 "tax-collector.schema-registry",
                                 "tax-collector.price-oracle"]:
            logging.getLogger(api_logger_name).addHandler(api_handler)
    except (PermissionError, OSError) as e:
        root.warning(f"Could not create API log file: {e}")

    # Error log file — WARNING+ only
    try:
        error_handler = TimedRotatingFileHandler(
            os.path.join(LOG_DIR, "errors.log"),
            when="midnight", interval=1, backupCount=180, utc=True,
        )
        error_handler.setLevel(logging.WARNING)
        error_handler.setFormatter(formatter)
        error_handler.suffix = "%Y-%m-%d"
        root.addHandler(error_handler)
    except (PermissionError, OSError) as e:
        root.warning(f"Could not create error log file: {e}")

    # Import log file — tracks file imports
    try:
        import_handler = TimedRotatingFileHandler(
            os.path.join(LOG_DIR, "imports.log"),
            when="midnight", interval=1, backupCount=365, utc=True,
        )
        import_handler.setLevel(logging.DEBUG)
        import_handler.setFormatter(formatter)
        import_handler.suffix = "%Y-%m-%d"
        logging.getLogger("tax-collector.csv-importer").addHandler(import_handler)
        logging.getLogger("tax-collector.transfer-preview").addHandler(import_handler)
    except (PermissionError, OSError) as e:
        root.warning(f"Could not create import log file: {e}")

    root.info(f"Logging initialized: level={LOG_LEVEL}, dir={LOG_DIR}")


setup_logging()
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
recompute_status: dict = {}
_sync_lock = asyncio.Lock()


# ── Sync logic ────────────────────────────────────────────────────────────

async def run_sync(exchange_name: str, full: bool = False, recompute: bool = True):
    """Pull all tax-relevant data from an exchange and resolve USD values."""
    async with _sync_lock:
        await _run_sync_inner(exchange_name, full)
    if recompute:
        await _run_recompute_after_sync()


async def _run_sync_inner(exchange_name: str, full: bool = False):
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

            # ── Exchange internal transfers ────────────────────────────
            if hasattr(ex, 'fetch_transfers'):
                logger.info(f"  [{exchange_name}] Pulling internal transfers...")
                transfers = await ex.fetch_transfers(since=last_ts.get("transfers"))
                if transfers:
                    for t in transfers:
                        ts = t.get("transferred_at") or datetime.now(timezone.utc)
                        usd = await oracle.resolve_transfer_usd(
                            session, t["asset"], t["amount"], None, ts,
                        )
                        t["asset_price_usd"] = usd["asset_price_usd"]
                        t["amount_usd"] = usd["amount_usd"]
                    await db.upsert_exchange_transfers(session, exchange_name, transfers)
                    counts["exchange_transfers"] = len(transfers)
                    logger.info(f"  [{exchange_name}] Stored {len(transfers)} internal transfers")

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


async def _run_recompute_after_sync():
    """Trigger a v4 recompute for the current tax year after sync completes."""
    global recompute_status
    if not settings.auto_recompute:
        logger.debug("Auto-recompute disabled (TAX_AUTO_RECOMPUTE=false)")
        return

    year = datetime.now(timezone.utc).year
    logger.info(f"Auto-recompute: triggering v4 compute-all for tax year {year}")
    recompute_status = {
        "status": "running",
        "started": datetime.now(timezone.utc).isoformat(),
        "year": year,
    }
    try:
        result = await _compute_v4_full(year)
        net = result.get("compute", {}).get("net_total", "?")
        run_id = result.get("run_id", "?")
        recompute_status.update({
            "status": "success",
            "finished": datetime.now(timezone.utc).isoformat(),
            "run_id": run_id,
            "net_total": net,
        })
        logger.info(f"Auto-recompute complete: run_id={run_id}  net={net}")
    except Exception as e:
        recompute_status.update({
            "status": "error",
            "finished": datetime.now(timezone.utc).isoformat(),
            "error": str(e),
        })
        logger.error(f"Auto-recompute failed (sync data is still saved): {e}")


async def run_sync_all(full: bool = False):
    for name in settings.enabled_exchanges:
        await run_sync(name, full=full, recompute=False)  # Don't recompute per-exchange

    # Auto-recompute once after all exchanges have synced
    await _run_recompute_after_sync()


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

# ── Dashboard ─────────────────────────────────────────────────────────────

STATIC_DIR = Path(__file__).parent / "static"


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the Tax Collector web dashboard."""
    html_path = STATIC_DIR / "index.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text(), status_code=200)
    return HTMLResponse(content="<h1>Dashboard not found</h1><p>static/index.html missing</p>", status_code=404)


# ── Health ────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "version": "2.0.0", "exchanges": settings.enabled_exchanges}


# ── Log viewer endpoints ──────────────────────────────────────────────────

@app.get("/logs")
async def get_logs(file: str = Query("tax-collector.log"),
                   lines: int = Query(200),
                   level: str = Query(None)):
    """View recent log entries."""
    allowed_files = {"tax-collector.log", "api.log", "errors.log", "imports.log"}
    if file not in allowed_files:
        raise HTTPException(400, f"Invalid log file. Choose from: {allowed_files}")

    log_path = os.path.join(LOG_DIR, file)
    if not os.path.exists(log_path):
        return {"file": file, "lines": [], "total_lines": 0, "returned": 0,
                "note": "Log file not created yet"}

    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()

        if level:
            all_lines = [l for l in all_lines if f"] {level.upper()} " in l]

        recent = all_lines[-lines:]
        return {
            "file": file,
            "total_lines": len(all_lines),
            "returned": len(recent),
            "lines": [l.rstrip() for l in recent],
        }
    except Exception as e:
        raise HTTPException(500, f"Error reading log: {e}")


@app.get("/logs/files")
async def list_log_files():
    """List available log files with sizes."""
    files = []
    if os.path.isdir(LOG_DIR):
        for f in sorted(os.listdir(LOG_DIR)):
            fp = os.path.join(LOG_DIR, f)
            if os.path.isfile(fp):
                files.append({
                    "name": f,
                    "size_bytes": os.path.getsize(fp),
                    "size_human": f"{os.path.getsize(fp)/1024:.1f} KB",
                    "modified": os.path.getmtime(fp),
                })
    return {"log_dir": LOG_DIR, "files": files}


# ── Sync endpoints ───────────────────────────────────────────────────────

@app.post("/sync/{exchange}")
async def sync_exchange(exchange: str, full: bool = Query(False)):
    if exchange not in settings.enabled_exchanges:
        raise HTTPException(404, f"Exchange '{exchange}' not configured")
    if _sync_lock.locked():
        raise HTTPException(409, "Sync already in progress. Please wait.")
    asyncio.create_task(run_sync(exchange, full=full))
    return {"message": f"Sync started for {exchange}", "full": full}


@app.post("/sync")
async def sync_all(full: bool = Query(False)):
    asyncio.create_task(run_sync_all(full=full))
    return {"message": "Sync started for all exchanges", "full": full}


@app.get("/sync/status")
async def get_sync_status():
    return {
        "exchanges": sync_status,
        "auto_recompute_enabled": settings.auto_recompute,
        "last_recompute": recompute_status,
    }


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
        result["WARNING"] = "v3 pipeline is NOT filing-safe. Use /v4/compute-all and /export/v4-tax-report for tax filing."
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
    results["WARNING"] = "v3 pipeline is NOT filing-safe. Use /v4/compute-all and /export/v4-tax-report for tax filing."
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
        "WARNING": "v3 pipeline is NOT filing-safe. Use /v4/compute-all and /export/v4-tax-report for tax filing.",
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
        "WARNING": "v3 pipeline is NOT filing-safe. Use /v4/compute-all and /export/v4-tax-report for tax filing.",
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
async def export_tax_report_v3_deprecated(year: int = Query(..., description="Tax year (required)")):
    """DEPRECATED: v3 tax report is not filing-safe."""
    raise HTTPException(
        410,
        detail="DEPRECATED: The v3 tax report is not filing-safe. "
               "Use GET /export/v4-tax-report for accountant-grade output. "
               "See /v4/compute-all to run the v4 pipeline first."
    )


# ══════════════════════════════════════════════════════════════════════════
# V4 — FILING-GRADE TAX COMPUTATION ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════

from exceptions import ExceptionManager
from ledger import NormalizedLedger
from valuation_v4 import ValuationV4
from transfer_matcher_v4 import TransferMatcherV4
from income_classifier_v4 import IncomeClassifierV4
from tax_engine_v4 import TaxEngineV4


async def _resolve_run_id(session, requested_run_id: int | None = None, year: int | None = None) -> int | None:
    """Return the latest completed/filing_blocked run_id for the given year."""
    if requested_run_id is not None:
        return requested_run_id
    from sqlalchemy import text as t
    r = await session.execute(t("""
        SELECT id FROM tax.run_manifest
        WHERE status IN ('completed', 'filing_blocked')
          AND (:year IS NULL OR tax_year = :year)
        ORDER BY completed_at DESC NULLS LAST, id DESC
        LIMIT 1
    """), {"year": year})
    row = r.fetchone()
    return row[0] if row else None


async def _create_run_manifest(session, run_type: str, tax_year: int = None,
                               config_snapshot: dict = None) -> int:
    """Create a run_manifest record and return its ID."""
    from sqlalchemy import text as t
    r = await session.execute(t("""
        INSERT INTO tax.run_manifest
            (run_type, tax_year, basis_method, wallet_aware, code_version, config_snapshot)
        VALUES
            (:rt, :ty, 'FIFO', TRUE, '4.0.0', CAST(:cfg AS jsonb))
        RETURNING id
    """), {"rt": run_type, "ty": tax_year,
           "cfg": json.dumps(config_snapshot) if config_snapshot else None})
    row = r.fetchone()
    return row[0] if row else 0


async def _update_run_manifest(session, run_id: int, status: str,
                               stats: dict = None, error: str = None):
    """Update a run_manifest with final status."""
    from sqlalchemy import text as t
    await session.execute(t("""
        UPDATE tax.run_manifest SET
            completed_at = NOW(), status = :status,
            total_events = :te, total_disposals = :td,
            total_exceptions = :tex, blocking_exceptions = :be,
            filing_ready = :fr, error_message = :err
        WHERE id = :id
    """), {
        "id": run_id, "status": status,
        "te": stats.get("total_events") if stats else None,
        "td": stats.get("total_disposals") if stats else None,
        "tex": stats.get("total_exceptions") if stats else None,
        "be": stats.get("blocking_exceptions") if stats else None,
        "fr": stats.get("filing_ready", False) if stats else False,
        "err": error,
    })


async def _compute_v4_full(year: int) -> dict:
    """Core v4 pipeline logic. Called from endpoint and auto-recompute."""
    async with db.get_session() as session:
        run_id = await _create_run_manifest(session, "full", year)
        await session.commit()

    results = {}
    exc = ExceptionManager()

    # Step 0: Clear stale exceptions from any prior run with same run_id
    async with db.get_session() as session:
        await ExceptionManager.clear_for_run(session, run_id)
        await session.commit()

    # Step 0.5: Data quality validation
    async with db.get_session() as session:
        from data_quality import validate_data_quality
        await validate_data_quality(session, exc, run_id)

    # Step 1: Normalize
    async with db.get_session() as session:
        ledger = NormalizedLedger(exc)
        results["normalize"] = await ledger.decompose_all(session, run_id)
        await session.commit()

    # Step 2: Create acquisition lots first (so transfer matcher can find them)
    async with db.get_session() as session:
        val = ValuationV4(exc)
        engine = TaxEngineV4(exc, val)
        acq_count = await engine.create_acquisition_lots(session, run_id)
        results["acquisition_lots"] = acq_count
        await session.commit()

    # Step 3: Match and relocate transfers (lots now exist)
    async with db.get_session() as session:
        matcher = TransferMatcherV4()
        results["transfers"] = await matcher.match_and_relocate(session, exc, run_id)
        await session.commit()

    # Step 4: Classify income + create income lots
    async with db.get_session() as session:
        val = ValuationV4(exc)
        classifier = IncomeClassifierV4(exc, val)
        results["income"] = await classifier.classify(session, run_id)
        await session.commit()

    async with db.get_session() as session:
        val = ValuationV4(exc)
        engine = TaxEngineV4(exc, val)
        inc_count = await engine.create_income_lots(session, run_id)
        results["income_lots"] = inc_count
        await session.commit()

    # Step 4.5: Reclassify remaining UNRESOLVED deposits as ACQUISITION
    async with db.get_session() as session:
        from sqlalchemy import text as t
        reclassified = await session.execute(t("""
            UPDATE tax.normalized_events
            SET event_type = 'ACQUISITION',
                classification_rule = 'external deposit — reclassified from UNRESOLVED'
            WHERE run_id = :run_id
              AND event_type = 'UNRESOLVED'
              AND source_deposit_id IS NOT NULL
            RETURNING id
        """), {"run_id": run_id})
        reclass_ids = reclassified.fetchall()
        results["deposits_reclassified_to_acquisition"] = len(reclass_ids)
        await session.commit()
        if reclass_ids:
            logger.info(f"Reclassified {len(reclass_ids)} unresolved deposits as ACQUISITION")

    # Step 4.6: Create lots from newly reclassified ACQUISITION deposits
    async with db.get_session() as session:
        val = ValuationV4(exc)
        engine = TaxEngineV4(exc, val)
        dep_lot_count = await engine.create_acquisition_lots(session, run_id)
        results["deposit_acquisition_lots"] = dep_lot_count
        await session.commit()

    # Step 5: Process disposals + Form 8949
    async with db.get_session() as session:
        val = ValuationV4(exc)
        engine = TaxEngineV4(exc, val)
        results["compute"] = await engine.process_disposals_and_report(session, run_id, year=year)
        await session.commit()

    # Step 6: Classify funding flows
    async with db.get_session() as session:
        from flow_classifier import FlowClassifier
        fc = FlowClassifier()
        results["funding_flows"] = await fc.classify_all(session, run_id)
        await session.commit()

    # Step 7: Compute data coverage
    async with db.get_session() as session:
        from data_coverage import DataCoverageTracker
        tracker = DataCoverageTracker()
        results["data_coverage"] = await tracker.compute_coverage(session, run_id, year)
        await session.commit()

    # Step 8: Check data coverage gaps (stop-ship)
    blockers = []
    warnings_list = []
    try:
        from exchanges.mexc import MEXCExchange, MEXC_RETENTION
        mexc_ex = get_exchange("mexc", settings)
        if mexc_ex and isinstance(mexc_ex, MEXCExchange):
            tax_year_start = datetime(year, 1, 1, tzinfo=timezone.utc)
            coverage = mexc_ex.get_data_coverage(since=tax_year_start)
            for endpoint, info in coverage.items():
                if info["has_gap"]:
                    async with db.get_session() as session:
                        from sqlalchemy import text as t
                        r = await session.execute(t("""
                            SELECT COUNT(*) FROM tax.csv_imports
                            WHERE exchange = 'mexc' AND date_range_start <= :start
                        """), {"start": tax_year_start})
                        csv_covers = (r.scalar() or 0) > 0
                    if not csv_covers:
                        msg = (f"BLOCKING: MEXC {info['description']} only covers last "
                               f"{info['retention_days']} days. Tax year {year} requires "
                               f"data from {year}-01-01. Import official MEXC CSV exports.")
                        blockers.append(msg)
                        exc.log("BLOCKING", "DATA_COVERAGE_GAP", msg,
                                tax_year=year, blocks_filing=True, run_id=run_id)
    except Exception as e:
        logger.warning(f"Data coverage check failed: {e}")

    # Flush exceptions
    async with db.get_session() as session:
        exc_count = await exc.flush(session)
        filing_status = await ExceptionManager.check_filing_ready(session, year, run_id=run_id)
        await session.commit()

    if blockers:
        filing_status["filing_ready"] = False
        filing_status["blockers"] = blockers

    # Update manifest
    async with db.get_session() as session:
        await _update_run_manifest(session, run_id,
            status="completed" if filing_status["filing_ready"] else "filing_blocked",
            stats={
                "total_events": results["normalize"].get("events_created", 0),
                "total_disposals": results["compute"].get("disposals_processed", 0),
                "total_exceptions": exc_count,
                "blocking_exceptions": filing_status.get("blocking_count", 0),
                "filing_ready": filing_status["filing_ready"],
            })
        await session.commit()

    results["run_id"] = run_id
    results["exceptions_logged"] = exc_count
    results["filing_status"] = filing_status
    results["filing_ready"] = filing_status["filing_ready"]
    results["blockers"] = blockers
    results["warnings"] = warnings_list
    return results


@app.post("/v4/compute-all")
async def v4_compute_all(year: int = Query(None)):
    """Full v4 pipeline: normalize -> match transfers -> classify income -> FIFO -> Form 8949."""
    if year is None:
        year = datetime.now(timezone.utc).year
    try:
        return await _compute_v4_full(year)
    except Exception as e:
        logger.exception(f"v4 compute-all failed: {e}")
        raise HTTPException(500, str(e))


@app.post("/v4/normalize")
async def v4_normalize():
    """Step 1: Decompose raw data into normalized events."""
    try:
        exc = ExceptionManager()
        async with db.get_session() as session:
            run_id = await _create_run_manifest(session, "normalize")
            await session.commit()
        async with db.get_session() as session:
            ledger = NormalizedLedger(exc)
            result = await ledger.decompose_all(session, run_id)
            await exc.flush(session)
            await session.commit()
        result["run_id"] = run_id
        return result
    except Exception as e:
        logger.exception(f"v4 normalize failed: {e}")
        raise HTTPException(500, str(e))


@app.post("/v4/match-transfers")
async def v4_match_transfers(run_id: int = Query(None)):
    """Step 2: Match transfers, relocate lots."""
    if not run_id:
        raise HTTPException(400, "run_id is required (from normalize step)")
    try:
        exc = ExceptionManager()
        async with db.get_session() as session:
            matcher = TransferMatcherV4()
            result = await matcher.match_and_relocate(session, exc, run_id)
            await exc.flush(session)
            await session.commit()
        return result
    except Exception as e:
        logger.exception(f"v4 match-transfers failed: {e}")
        raise HTTPException(500, str(e))


@app.post("/v4/classify-income")
async def v4_classify_income(run_id: int = Query(None)):
    """Step 3: Queue income for review."""
    if not run_id:
        raise HTTPException(400, "run_id is required")
    try:
        exc = ExceptionManager()
        async with db.get_session() as session:
            val = ValuationV4(exc)
            classifier = IncomeClassifierV4(exc, val)
            result = await classifier.classify(session, run_id)
            await exc.flush(session)
            await session.commit()
        return result
    except Exception as e:
        logger.exception(f"v4 classify-income failed: {e}")
        raise HTTPException(500, str(e))


@app.post("/v4/compute")
async def v4_compute(year: int = Query(2025), run_id: int = Query(None)):
    """Step 4: FIFO computation. DEPRECATED — use /v4/compute-all for filing-grade output."""
    if not run_id:
        raise HTTPException(400, "run_id is required")
    try:
        exc = ExceptionManager()
        async with db.get_session() as session:
            val = ValuationV4(exc)
            engine = TaxEngineV4(exc, val)
            result = await engine.compute(session, run_id, year=year)
            await exc.flush(session)
            await session.commit()
        result["_deprecation_warning"] = (
            "DEPRECATED: /v4/compute is not filing-safe. "
            "Use /v4/compute-all for deterministic, run-scoped computation."
        )
        return result
    except Exception as e:
        logger.exception(f"v4 compute failed: {e}")
        raise HTTPException(500, str(e))


@app.get("/v4/filing-status")
async def v4_filing_status(year: int = Query(2025)):
    """Check if filing-ready (any blocking exceptions?)."""
    async with db.get_session() as session:
        return await ExceptionManager.check_filing_ready(session, year)


@app.get("/v4/exceptions")
async def v4_exceptions(year: int = Query(None), severity: str = Query(None),
                        status: str = Query("open")):
    """List all exceptions."""
    async with db.get_session() as session:
        return await ExceptionManager.get_all(session, tax_year=year,
                                               severity=severity, status=status)


@app.post("/v4/exceptions/{exception_id}/resolve")
async def v4_resolve_exception(exception_id: int,
                               status: str = Query("resolved"),
                               notes: str = Query("")):
    """Resolve/accept-risk an exception."""
    async with db.get_session() as session:
        await ExceptionManager.resolve(session, exception_id, status, notes)
        await session.commit()
    return {"resolved": exception_id, "status": status}


@app.get("/v4/events")
async def v4_events(wallet: str = Query(None), asset: str = Query(None),
                    event_type: str = Query(None), run_id: int = Query(None),
                    limit: int = Query(100, le=1000),
                    offset: int = Query(0)):
    """Browse normalized event ledger."""
    where = ["1=1"]
    params: dict = {"limit": limit, "offset": offset}
    if wallet:
        where.append("wallet = :wallet")
        params["wallet"] = wallet
    if asset:
        where.append("asset = :asset")
        params["asset"] = asset.upper()
    if event_type:
        where.append("event_type = :etype")
        params["etype"] = event_type.upper()

    async with db.get_session() as session:
        from sqlalchemy import text as t
        resolved = await _resolve_run_id(session, run_id)
        if resolved:
            where.append("run_id = :run_id")
            params["run_id"] = resolved
        r = await session.execute(t(f"""
            SELECT id, event_type, wallet, asset, quantity::text,
                   unit_price_usd::text, total_usd::text, event_at,
                   paired_event_id, classification_rule,
                   source_trade_id, source_deposit_id, source_withdrawal_id, source_pool_id
            FROM tax.normalized_events
            WHERE {' AND '.join(where)}
            ORDER BY event_at ASC, id ASC
            LIMIT :limit OFFSET :offset
        """), params)
        events = [dict(zip(r.keys(), row)) for row in r.fetchall()]
    return {"count": len(events), "events": events, "run_id": resolved}


@app.get("/v4/lots")
async def v4_lots(wallet: str = Query(None), asset: str = Query(None),
                  run_id: int = Query(None), show_depleted: bool = Query(False)):
    """Browse lots per wallet."""
    where = ["1=1"]
    params: dict = {}
    if wallet:
        where.append("wallet = :wallet")
        params["wallet"] = wallet
    if asset:
        where.append("asset = :asset")
        params["asset"] = asset.upper()
    if not show_depleted:
        where.append("remaining > 0")

    async with db.get_session() as session:
        from sqlalchemy import text as t
        resolved = await _resolve_run_id(session, run_id)
        if resolved:
            where.append("run_id = :run_id")
            params["run_id"] = resolved
        r = await session.execute(t(f"""
            SELECT id, asset, wallet, original_quantity::text, remaining::text,
                   cost_per_unit_usd::text, total_cost_usd::text,
                   original_acquired_at, lot_created_at, source_type,
                   parent_lot_id, is_depleted
            FROM tax.lots_v4
            WHERE {' AND '.join(where)}
            ORDER BY asset, original_acquired_at ASC, id ASC
        """), params)
        lots = [dict(zip(r.keys(), row)) for row in r.fetchall()]
    return {"count": len(lots), "lots": lots}


@app.get("/v4/form-8949")
async def v4_form_8949(year: int = Query(...), run_id: int = Query(None)):
    """Form 8949 from v4 engine."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        resolved = await _resolve_run_id(session, run_id, year)
        params: dict = {"year": year}
        run_filter = ""
        if resolved:
            run_filter = " AND run_id = :run_id"
            params["run_id"] = resolved
        result = await session.execute(t(f"""
            SELECT description, date_acquired, date_sold, proceeds::text,
                   cost_basis::text, adjustment_code, adjustment_amount::text,
                   gain_loss::text, term, box, asset, wallet, exchange,
                   holding_days, is_futures
            FROM tax.form_8949_v4
            WHERE tax_year = :year{run_filter}
            ORDER BY date_sold, asset
        """), params)
        lines = [dict(zip(result.keys(), row)) for row in result.fetchall()]

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


@app.get("/v4/schedule-d")
async def v4_schedule_d(year: int = Query(...), run_id: int = Query(None)):
    """Schedule D summary from v4 engine."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        resolved = await _resolve_run_id(session, run_id, year)
        params: dict = {"year": year}
        run_filter = ""
        if resolved:
            run_filter = " AND run_id = :run_id"
            params["run_id"] = resolved
        r = await session.execute(t(f"""
            SELECT
                COALESCE(SUM(CASE WHEN term='short' THEN proceeds END), 0)::text,
                COALESCE(SUM(CASE WHEN term='short' THEN cost_basis END), 0)::text,
                COALESCE(SUM(CASE WHEN term='short' THEN gain_loss END), 0)::text,
                COALESCE(SUM(CASE WHEN term='long' THEN proceeds END), 0)::text,
                COALESCE(SUM(CASE WHEN term='long' THEN cost_basis END), 0)::text,
                COALESCE(SUM(CASE WHEN term='long' THEN gain_loss END), 0)::text,
                COUNT(*)
            FROM tax.form_8949_v4
            WHERE tax_year = :year{run_filter}
        """), params)
        row = r.fetchone()

    from decimal import Decimal as D
    return {
        "year": year,
        "short_term": {"proceeds": row[0], "cost_basis": row[1], "gain_loss": row[2]},
        "long_term": {"proceeds": row[3], "cost_basis": row[4], "gain_loss": row[5]},
        "net_gain_loss": str(D(row[2]) + D(row[5])) if row else "0",
        "total_disposals": row[6] if row else 0,
    }


@app.get("/v4/income")
async def v4_income(year: int = Query(None), run_id: int = Query(None)):
    """Income events with review status."""
    where = "WHERE 1=1"
    params: dict = {}
    if year:
        where += " AND EXTRACT(YEAR FROM dominion_at) = :year"
        params["year"] = year

    async with db.get_session() as session:
        from sqlalchemy import text as t
        resolved = await _resolve_run_id(session, run_id, year)
        if resolved:
            where += " AND run_id = :run_id"
            params["run_id"] = resolved
        r = await session.execute(t(f"""
            SELECT id, wallet, asset, quantity::text, fmv_per_unit_usd::text,
                   total_fmv_usd::text, income_type, classification_source,
                   review_status, dominion_at, reviewer_notes
            FROM tax.income_events_v4
            {where}
            ORDER BY dominion_at ASC
        """), params)
        events = [dict(zip(r.keys(), row)) for row in r.fetchall()]

    from decimal import Decimal as D
    total = sum(D(e["total_fmv_usd"] or "0") for e in events)
    pending = sum(1 for e in events if e["review_status"] == "pending")
    confirmed = sum(1 for e in events if e["review_status"] == "confirmed")

    return {
        "total_income_events": len(events),
        "total_income_usd": str(total),
        "pending_review": pending,
        "confirmed": confirmed,
        "events": events,
    }


@app.get("/v4/transfers")
async def v4_transfers(run_id: int = Query(None)):
    """Transfer carryover records."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        resolved = await _resolve_run_id(session, run_id)
        params: dict = {}
        run_filter = ""
        if resolved:
            run_filter = "WHERE run_id = :run_id"
            params["run_id"] = resolved
        r = await session.execute(t(f"""
            SELECT DISTINCT ON (tc.id)
                tc.id, tc.asset, tc.quantity::text, tc.source_wallet, tc.dest_wallet,
                tc.original_acquired_at, tc.carryover_basis_usd::text,
                tc.cost_per_unit_usd::text, tc.transferred_at, tc.tx_hash,
                tc.transfer_fee::text, tc.match_confidence,
                w.address AS wd_address, d.address AS dep_address,
                wd_claim.claim_type AS wd_claim_type,
                wd_ent.entity_type AS wd_entity_type,
                wd_ent.label AS wd_entity_label,
                dep_claim.claim_type AS dep_claim_type,
                dep_ent.entity_type AS dep_entity_type,
                dep_ent.label AS dep_entity_label
            FROM tax.transfer_carryover tc
            LEFT JOIN tax.withdrawals w ON w.id = tc.withdrawal_id
            LEFT JOIN tax.deposits d ON d.id = tc.deposit_id
            LEFT JOIN tax.wallet_addresses wd_wa ON wd_wa.address = w.address
            LEFT JOIN tax.wallet_address_claims wd_claim ON wd_claim.address_id = wd_wa.id
                AND wd_claim.claim_type = 'self_owned'
            LEFT JOIN tax.wallet_accounts wd_acct ON wd_wa.account_id = wd_acct.id
            LEFT JOIN tax.wallet_entities wd_ent ON wd_acct.entity_id = wd_ent.id
            LEFT JOIN tax.wallet_addresses dep_wa ON dep_wa.address = d.address
            LEFT JOIN tax.wallet_address_claims dep_claim ON dep_claim.address_id = dep_wa.id
                AND dep_claim.claim_type = 'self_owned'
            LEFT JOIN tax.wallet_accounts dep_acct ON dep_wa.account_id = dep_acct.id
            LEFT JOIN tax.wallet_entities dep_ent ON dep_acct.entity_id = dep_ent.id
            {"WHERE tc.run_id = :run_id" if resolved else ""}
            ORDER BY tc.id, tc.transferred_at DESC
            LIMIT 200
        """), params)
        raw = [dict(zip(r.keys(), row)) for row in r.fetchall()]
        # Re-sort by transferred_at DESC after DISTINCT ON
        transfers = sorted(raw, key=lambda x: x.get("transferred_at") or "", reverse=True)
        for t_row in transfers:
            t_row["both_owned"] = bool(t_row.get("wd_claim_type")) and bool(t_row.get("dep_claim_type"))
    return {"count": len(transfers), "transfers": transfers}


@app.get("/v4/run-history")
async def v4_run_history():
    """Past computation runs."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("""
            SELECT id, run_type, tax_year, basis_method, wallet_aware,
                   code_version, started_at, completed_at, status,
                   total_events, total_disposals, total_exceptions,
                   blocking_exceptions, filing_ready, error_message
            FROM tax.run_manifest
            ORDER BY started_at DESC
            LIMIT 50
        """))
        runs = [dict(zip(r.keys(), row)) for row in r.fetchall()]
    return {"count": len(runs), "runs": runs}


@app.post("/v4/activity-start")
async def set_activity_start(exchange: str = Query(...), start_date: str = Query(...),
                              notes: str = Query(None)):
    """Record the first activity date for an exchange to suppress phantom coverage gaps."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        await session.execute(t("""
            INSERT INTO tax.activity_start (exchange, start_date, notes)
            VALUES (:ex, :sd, :notes)
            ON CONFLICT (exchange) DO UPDATE SET start_date = EXCLUDED.start_date,
                notes = EXCLUDED.notes, updated_at = NOW()
        """), {"ex": exchange.lower(), "sd": start_date, "notes": notes})
        await session.commit()
    return {"exchange": exchange, "start_date": start_date}


@app.get("/v4/activity-start")
async def get_activity_starts():
    """List recorded activity start dates."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("SELECT exchange, start_date, notes FROM tax.activity_start ORDER BY exchange"))
        return [dict(zip(r.keys(), row)) for row in r.fetchall()]


# ══════════════════════════════════════════════════════════════════════════
# DATABASE DIAGNOSTIC EXPORT (for debugging)
# ══════════════════════════════════════════════════════════════════════════

import zipfile

@app.post("/v4/export-tables")
async def export_tables():
    """Export complete database diagnostic package for debugging."""
    from db_export import DatabaseExporter
    export_dir = os.path.join(LOG_DIR, "tables")
    exporter = DatabaseExporter(export_dir, settings.database_url)
    async with db.get_session() as session:
        result = await exporter.export_all(session)
    return result


@app.get("/v4/export-tables/download")
async def download_table_export(table: str = Query(None)):
    """Download exported table CSVs or full diagnostic zip.

    Args:
        table: Specific table name (without tax. prefix) to download as CSV.
               If omitted, downloads a zip of all tables + diagnostic files.
    """
    export_dir = os.path.join(LOG_DIR, "tables")

    if not os.path.isdir(export_dir) or not os.listdir(export_dir):
        raise HTTPException(404, "No table export found. Run POST /v4/export-tables first.")

    if table:
        csv_path = os.path.join(export_dir, f"{table}.csv")
        if not os.path.exists(csv_path):
            available = [f.replace(".csv", "") for f in os.listdir(export_dir) if f.endswith(".csv")]
            raise HTTPException(404, f"Table '{table}' not found. Available: {available}")
        return FileResponse(csv_path, filename=f"tax_{table}.csv", media_type="text/csv")

    zip_path = os.path.join(export_dir, "_all_tables.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(os.listdir(export_dir)):
            if f.startswith("_all_tables"):
                continue
            fp = os.path.join(export_dir, f)
            if os.path.isfile(fp):
                zf.write(fp, f"tax_tables/{f}")

    return FileResponse(zip_path, filename="tax_tables_export.zip", media_type="application/zip")


# ══════════════════════════════════════════════════════════════════════════
# STAGED IMPORT / RECONCILIATION ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════

from fastapi import UploadFile, File
import tempfile
import shutil
from import_staging import (
    parse_file, analyze_matches, commit_staged,
    _staged_imports, _create_stage_id, _cleanup_expired_stages, get_staged,
)


@app.post("/v4/import/stage")
async def import_stage(file: UploadFile = File(...)):
    """Upload and parse a file without importing. Returns parsed rows with match analysis."""
    _cleanup_expired_stages()

    # Save uploaded file to temp location
    upload_dir = os.path.join(tempfile.gettempdir(), "tax-collector-uploads")
    os.makedirs(upload_dir, exist_ok=True)
    temp_path = os.path.join(upload_dir, file.filename or "upload")
    try:
        with open(temp_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
    finally:
        await file.close()

    try:
        # Parse the file
        parsed_data = parse_file(temp_path)
    except ValueError as e:
        raise HTTPException(400, detail=str(e))
    finally:
        # Cleanup temp file
        if os.path.exists(temp_path):
            os.unlink(temp_path)

    # Run match analysis against the database
    async with db.get_session() as session:
        parsed_data = await analyze_matches(session, parsed_data)

    # Store in memory
    stage_id = _create_stage_id()
    import time as _time
    _staged_imports[stage_id] = {
        "created_at": _time.time(),
        "parsed_data": parsed_data,
        "committed": False,
    }

    return {
        "stage_id": stage_id,
        "file_info": parsed_data["file_info"],
        "summary": parsed_data["summary"],
        "rows": parsed_data["rows"],
    }


@app.post("/v4/import/commit")
async def import_commit(payload: dict):
    """Commit a staged import. Only approved rows are written to DB."""
    stage_id = payload.get("stage_id")
    decisions = payload.get("decisions", [])

    if not stage_id:
        raise HTTPException(400, "stage_id is required")

    stage = get_staged(stage_id)
    if not stage:
        raise HTTPException(404, f"Stage {stage_id} not found or expired")

    if stage.get("committed"):
        raise HTTPException(409, "This import has already been committed")

    # Check for undecided rows
    decision_map = {d["row_num"]: d for d in decisions}
    undecided = [r for r in stage["parsed_data"]["rows"]
                 if r["row_num"] not in decision_map]
    if undecided:
        raise HTTPException(400, f"{len(undecided)} rows have no decision. "
                                 "Every row must have an action (IMPORT, SKIP, etc.)")

    async with db.get_session() as session:
        try:
            results = await commit_staged(session, stage_id, decisions)
            await session.commit()
        except ValueError as e:
            raise HTTPException(404, str(e))
        except Exception as e:
            logger.exception(f"Import commit failed: {e}")
            raise HTTPException(500, str(e))

    return {
        "stage_id": stage_id,
        "committed": True,
        "results": results,
    }


@app.get("/v4/import/history")
async def import_history():
    """List past imports with metadata."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("""
            SELECT id, exchange, data_type, filename, file_hash, row_count,
                   imported_count, duplicate_count, error_count,
                   date_range_start, date_range_end, imported_at
            FROM tax.csv_imports ORDER BY imported_at DESC LIMIT 100
        """))
        imports = [dict(zip(r.keys(), row)) for row in r.fetchall()]
    return {"count": len(imports), "imports": imports}


# ══════════════════════════════════════════════════════════════════════════
# ACCOUNTANT HANDOFF ENDPOINTS (Phases 2-7)
# ══════════════════════════════════════════════════════════════════════════

@app.get("/v4/data-coverage")
async def v4_data_coverage(year: int = Query(None)):
    """Show what date ranges each exchange's API covers and identify gaps."""
    from exchanges.mexc import MEXCExchange, MEXC_RETENTION
    since = datetime(year, 1, 1, tzinfo=timezone.utc) if year else None
    mexc_ex = get_exchange("mexc", settings)
    coverage = {}
    if mexc_ex and isinstance(mexc_ex, MEXCExchange):
        coverage["mexc"] = mexc_ex.get_data_coverage(since=since)

    # Check if CSV imports cover any gaps
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("""
            SELECT exchange, data_type, date_range_start, date_range_end
            FROM tax.csv_imports
            ORDER BY exchange, data_type
        """))
        csv_imports = [dict(zip(r.keys(), row)) for row in r.fetchall()]
    coverage["csv_imports"] = csv_imports
    return coverage


@app.post("/v4/import-file")
@app.post("/v4/import-csv")
async def import_file(filepath: str = Query(...),
                      exchange: str = Query(None), data_type: str = Query(None),
                      preview_matches: bool = Query(False)):
    """Import a CSV/XLSX file to supplement API data.

    Auto-detects format by extension + header fingerprinting.
    Optional exchange/data_type overrides skip auto-detection.
    Set preview_matches=true to scan for potential transfer matches after import.
    Accepts a server-side filepath (upload the file to the server first).
    """
    from csv_importer import CSVImporter, detect_format

    if not os.path.exists(filepath):
        raise HTTPException(400, f"File not found: {filepath}")

    # Auto-detect format if not provided
    if not exchange or not data_type:
        try:
            det_exchange, det_type, _ = detect_format(filepath)
            exchange = exchange or det_exchange
            data_type = data_type or det_type
        except ValueError:
            pass  # will fall through to generic import

    importer = CSVImporter()
    async with db.get_session() as session:
        result = await importer.import_file(session, filepath,
                                            exchange=exchange, data_type=data_type)
        await session.commit()

    # Transfer match preview
    if preview_matches and result.get("imported", 0) > 0 and exchange and data_type:
        from transfer_preview import TransferPreview
        preview = TransferPreview(time_window_hours=72, fee_tolerance_pct=10.0)

        async with db.get_session() as session:
            from sqlalchemy import text as t
            if data_type == "deposits":
                id_result = await session.execute(t("""
                    SELECT id FROM tax.deposits
                    WHERE exchange = :ex AND source_file = :sf
                    ORDER BY id DESC LIMIT :limit
                """), {"ex": exchange, "sf": os.path.basename(filepath),
                       "limit": result["imported"]})
                imported_ids = [r[0] for r in id_result.fetchall()]

                result["transfer_matches"] = await preview.scan_imported_deposits(
                    session, imported_ids, exchange
                )
                result["address_suggestions"] = await preview.scan_address_overlaps(
                    session, imported_ids, "deposits"
                )

            elif data_type == "withdrawals":
                id_result = await session.execute(t("""
                    SELECT id FROM tax.withdrawals
                    WHERE exchange = :ex AND source_file = :sf
                    ORDER BY id DESC LIMIT :limit
                """), {"ex": exchange, "sf": os.path.basename(filepath),
                       "limit": result["imported"]})
                imported_ids = [r[0] for r in id_result.fetchall()]

                result["transfer_matches"] = await preview.scan_imported_withdrawals(
                    session, imported_ids, exchange
                )
                result["address_suggestions"] = await preview.scan_address_overlaps(
                    session, imported_ids, "withdrawals"
                )

        match_count = len(result.get("transfer_matches", []))
        addr_count = len(result.get("address_suggestions", []))
        if match_count > 0:
            logger.info(f"[import] Found {match_count} potential transfer matches for imported {data_type}")
        if addr_count > 0:
            logger.info(f"[import] Found {addr_count} address overlap suggestions for imported {data_type}")

    return result


@app.post("/v4/import-file-preview")
async def import_file_preview(filepath: str = Query(...),
                               exchange: str = Query(None),
                               data_type: str = Query(None)):
    """Import a file AND preview potential transfer matches.

    Convenience alias for import-file with preview_matches=true.
    """
    from csv_importer import CSVImporter, detect_format
    from transfer_preview import TransferPreview

    if not os.path.exists(filepath):
        raise HTTPException(400, f"File not found: {filepath}")

    if not exchange or not data_type:
        try:
            det_exchange, det_type, _ = detect_format(filepath)
            exchange = exchange or det_exchange
            data_type = data_type or det_type
        except ValueError:
            pass

    importer = CSVImporter()
    async with db.get_session() as session:
        result = await importer.import_file(session, filepath,
                                            exchange=exchange, data_type=data_type)
        await session.commit()

    if result.get("imported", 0) > 0 and exchange and data_type:
        preview = TransferPreview(time_window_hours=72, fee_tolerance_pct=10.0)
        async with db.get_session() as session:
            from sqlalchemy import text as t
            table = "deposits" if data_type == "deposits" else "withdrawals"
            id_result = await session.execute(t(f"""
                SELECT id FROM tax.{table}
                WHERE exchange = :ex AND source_file = :sf
                ORDER BY id DESC LIMIT :limit
            """), {"ex": exchange, "sf": os.path.basename(filepath),
                   "limit": result["imported"]})
            imported_ids = [r[0] for r in id_result.fetchall()]

            if data_type == "deposits":
                result["transfer_matches"] = await preview.scan_imported_deposits(
                    session, imported_ids, exchange)
                result["address_suggestions"] = await preview.scan_address_overlaps(
                    session, imported_ids, "deposits")
            elif data_type == "withdrawals":
                result["transfer_matches"] = await preview.scan_imported_withdrawals(
                    session, imported_ids, exchange)
                result["address_suggestions"] = await preview.scan_address_overlaps(
                    session, imported_ids, "withdrawals")

    return result


@app.get("/v4/transfer-preview")
async def transfer_preview_scan(exchange: str = Query(None),
                                 asset: str = Query(None),
                                 days: int = Query(90)):
    """Scan existing deposits and withdrawals for potential transfer matches
    that haven't been matched yet.

    Useful for finding matches BEFORE running compute-all, or for
    discovering matches that the compute pipeline missed due to
    wider time windows or looser tolerances.
    """
    from transfer_preview import TransferPreview
    preview = TransferPreview(time_window_hours=72, fee_tolerance_pct=10.0)

    async with db.get_session() as session:
        from sqlalchemy import text as t

        dep_where = "WHERE 1=1"
        wd_where = "WHERE 1=1"
        params = {}
        if exchange:
            dep_where += " AND d.exchange = :ex"
            wd_where += " AND w.exchange = :ex"
            params["ex"] = exchange
        if asset:
            dep_where += " AND d.asset = :asset"
            wd_where += " AND w.asset = :asset"
            params["asset"] = asset.upper()

        # Unmatched deposits
        dep_result = await session.execute(t(f"""
            SELECT d.id FROM tax.deposits d
            LEFT JOIN tax.normalized_events ne
                ON ne.source_deposit_id = d.id AND ne.event_type = 'TRANSFER_IN'
            {dep_where}
              AND ne.id IS NULL
            ORDER BY d.confirmed_at DESC
            LIMIT 500
        """), params)
        unmatched_dep_ids = [r[0] for r in dep_result.fetchall()]

        # Unmatched withdrawals
        wd_result = await session.execute(t(f"""
            SELECT w.id FROM tax.withdrawals w
            LEFT JOIN tax.normalized_events ne
                ON ne.source_withdrawal_id = w.id AND ne.event_type = 'TRANSFER_OUT'
            {wd_where}
              AND ne.id IS NULL
            ORDER BY w.confirmed_at DESC
            LIMIT 500
        """), params)
        unmatched_wd_ids = [r[0] for r in wd_result.fetchall()]

        results = {
            "unmatched_deposits": len(unmatched_dep_ids),
            "unmatched_withdrawals": len(unmatched_wd_ids),
        }

        # Scan for matches
        all_matches = []
        for dep_id in unmatched_dep_ids:
            dep_matches = await preview.scan_imported_deposits(
                session, [dep_id], exchange or ""
            )
            all_matches.extend(dep_matches)

        for wd_id in unmatched_wd_ids:
            wd_matches = await preview.scan_imported_withdrawals(
                session, [wd_id], exchange or ""
            )
            all_matches.extend(wd_matches)

        # Deduplicate (same pair can be found from both sides)
        seen_pairs = set()
        deduped = []
        for m in all_matches:
            pair_key = tuple(sorted([
                f"{m['imported_record']['exchange']}:{m['imported_record']['id']}",
                f"{m['existing_record']['exchange']}:{m['existing_record']['id']}"
            ]))
            if pair_key not in seen_pairs:
                seen_pairs.add(pair_key)
                deduped.append(m)

        results["potential_matches"] = deduped
        results["match_count"] = len(deduped)

        # Address overlap suggestions
        if unmatched_dep_ids:
            results["address_suggestions"] = await preview.scan_address_overlaps(
                session, unmatched_dep_ids, "deposits"
            )
        else:
            results["address_suggestions"] = []

    return results


@app.get("/v4/csv-imports")
async def list_csv_imports():
    """List all CSV imports with metadata."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("""
            SELECT id, exchange, data_type, filename, file_hash, row_count,
                   imported_count, duplicate_count, error_count,
                   date_range_start, date_range_end, imported_at, imported_by
            FROM tax.csv_imports ORDER BY imported_at DESC
        """))
        imports = [dict(zip(r.keys(), row)) for row in r.fetchall()]
    return {"count": len(imports), "imports": imports}


@app.post("/v4/classify-flows")
async def v4_classify_flows(run_id: int = Query(None)):
    """Classify all deposits/withdrawals into funding flow categories."""
    from flow_classifier import FlowClassifier
    classifier = FlowClassifier()
    async with db.get_session() as session:
        result = await classifier.classify_all(session, run_id)
        await session.commit()
    return result


@app.get("/v4/funding-by-exchange")
async def v4_funding_by_exchange(year: int = Query(None), run_id: int = Query(None)):
    """Return classified funding flows grouped by exchange."""
    where = "WHERE 1=1"
    params: dict = {}
    if year:
        where += " AND EXTRACT(YEAR FROM event_at) = :year"
        params["year"] = year

    async with db.get_session() as session:
        from sqlalchemy import text as t
        resolved = await _resolve_run_id(session, run_id, year)
        if resolved:
            where += " AND run_id = :run_id"
            params["run_id"] = resolved
        r = await session.execute(t(f"""
            SELECT exchange,
                COALESCE(SUM(CASE WHEN flow_class = 'EXTERNAL_DEPOSIT' THEN total_usd END), 0)::text AS external_in_usd,
                COALESCE(SUM(CASE WHEN flow_class = 'EXTERNAL_WITHDRAWAL' THEN total_usd END), 0)::text AS external_out_usd,
                COALESCE(SUM(CASE WHEN flow_class IN ('EXTERNAL_DEPOSIT') THEN total_usd ELSE 0 END)
                    - SUM(CASE WHEN flow_class IN ('EXTERNAL_WITHDRAWAL') THEN total_usd ELSE 0 END), 0)::text AS net_external_funding_usd,
                COALESCE(SUM(CASE WHEN flow_class = 'INTERNAL_TRANSFER_IN' THEN total_usd END), 0)::text AS internal_in_usd,
                COALESCE(SUM(CASE WHEN flow_class = 'INTERNAL_TRANSFER_OUT' THEN total_usd END), 0)::text AS internal_out_usd,
                COALESCE(SUM(CASE WHEN flow_class = 'INCOME_RECEIPT' THEN total_usd END), 0)::text AS income_in_usd,
                COALESCE(SUM(CASE WHEN flow_class = 'UNCLASSIFIED' THEN total_usd END), 0)::text AS unclassified_usd
            FROM tax.classified_flows
            {where}
            GROUP BY exchange ORDER BY exchange
        """), params)
        rows = [dict(zip(r.keys(), row)) for row in r.fetchall()]
    return rows


@app.get("/v4/pnl-by-exchange")
async def v4_pnl_by_exchange(year: int = Query(...), run_id: int = Query(None)):
    """Realized P&L summary grouped by exchange for accountant review."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        resolved = await _resolve_run_id(session, run_id, year)
        params: dict = {"year": year}
        run_filter = ""
        if resolved:
            run_filter = " AND run_id = :run_id"
            params["run_id"] = resolved
        r = await session.execute(t(f"""
            SELECT
                exchange,
                COUNT(*) AS disposal_count,
                COALESCE(SUM(proceeds), 0)::text AS total_proceeds_usd,
                COALESCE(SUM(cost_basis), 0)::text AS total_basis_usd,
                COALESCE(SUM(CASE WHEN term = 'short' THEN proceeds END), 0)::text AS st_proceeds,
                COALESCE(SUM(CASE WHEN term = 'short' THEN cost_basis END), 0)::text AS st_basis,
                COALESCE(SUM(CASE WHEN term = 'short' THEN gain_loss END), 0)::text AS st_net,
                COALESCE(SUM(CASE WHEN term = 'long' THEN proceeds END), 0)::text AS lt_proceeds,
                COALESCE(SUM(CASE WHEN term = 'long' THEN cost_basis END), 0)::text AS lt_basis,
                COALESCE(SUM(CASE WHEN term = 'long' THEN gain_loss END), 0)::text AS lt_net,
                COALESCE(SUM(gain_loss), 0)::text AS total_net_usd
            FROM tax.form_8949_v4
            WHERE tax_year = :year{run_filter}
            GROUP BY exchange
            ORDER BY exchange
        """), params)
        rows = [dict(zip(r.keys(), row)) for row in r.fetchall()]
    return rows


# ══════════════════════════════════════════════════════════════════════════
# WALLET OWNERSHIP / ADDRESS CLAIMS
# ══════════════════════════════════════════════════════════════════════════

VALID_ENTITY_TYPES = {"taxpayer", "spouse", "business", "third_party"}
VALID_ACCOUNT_TYPES = {"exchange", "hardware_wallet", "software_wallet", "custodial", "other"}


@app.post("/v4/wallet/entities")
async def wallet_create_entity(entity_type: str = Query(...), label: str = Query(...),
                                notes: str = Query(None)):
    """Create a wallet entity."""
    if entity_type not in VALID_ENTITY_TYPES:
        raise HTTPException(400, f"entity_type must be one of: {', '.join(sorted(VALID_ENTITY_TYPES))}")
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("""
            INSERT INTO tax.wallet_entities (entity_type, label, notes)
            VALUES (:et, :label, :notes) RETURNING id
        """), {"et": entity_type, "label": label, "notes": notes})
        await session.commit()
        return {"id": r.fetchone()[0], "entity_type": entity_type, "label": label}


@app.get("/v4/wallet/entities")
async def wallet_list_entities():
    """List all wallet entities with accounts and addresses."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("""
            SELECT e.id, e.entity_type, e.label, e.notes,
                   a.id AS account_id, a.account_type, a.exchange_name,
                   a.label AS account_label, a.notes AS account_notes,
                   wa.id AS address_id, wa.address, wa.chain,
                   wa.label AS address_label, wa.is_active,
                   wac.id AS claim_id, wac.claim_type,
                   wac.confidence AS claim_confidence,
                   wac.review_status AS claim_review_status
            FROM tax.wallet_entities e
            LEFT JOIN tax.wallet_accounts a ON a.entity_id = e.id
            LEFT JOIN tax.wallet_addresses wa ON wa.account_id = a.id
            LEFT JOIN LATERAL (
                SELECT wac.id, wac.claim_type, wac.confidence, wac.review_status
                FROM tax.wallet_address_claims wac
                WHERE wac.address_id = wa.id
                ORDER BY wac.claimed_at DESC
                LIMIT 1
            ) wac ON TRUE
            ORDER BY e.id, a.id, wa.id
        """))
        rows = [dict(zip(r.keys(), row)) for row in r.fetchall()]

    # Group into nested structure
    entities = {}
    for row in rows:
        eid = row["id"]
        if eid not in entities:
            entities[eid] = {
                "id": eid, "entity_type": row["entity_type"],
                "label": row["label"], "notes": row["notes"], "accounts": {}
            }
        if row["account_id"]:
            aid = row["account_id"]
            if aid not in entities[eid]["accounts"]:
                entities[eid]["accounts"][aid] = {
                    "id": aid, "account_type": row["account_type"],
                    "exchange_name": row["exchange_name"],
                    "label": row["account_label"],
                    "notes": row.get("account_notes"), "addresses": []
                }
            if row["address_id"]:
                entities[eid]["accounts"][aid]["addresses"].append({
                    "id": row["address_id"], "address": row["address"],
                    "chain": row["chain"], "label": row["address_label"],
                    "is_active": row.get("is_active", True),
                    "claim_id": row.get("claim_id"),
                    "claim_type": row.get("claim_type"),
                    "claim_confidence": row.get("claim_confidence"),
                    "claim_review_status": row.get("claim_review_status"),
                })

    result = []
    for e in entities.values():
        e["accounts"] = list(e["accounts"].values())
        result.append(e)
    return result


@app.get("/v4/wallet/entities/{entity_id}")
async def wallet_get_entity(entity_id: int):
    """Get a single entity with nested accounts and addresses."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("""
            SELECT e.id, e.entity_type, e.label, e.notes, e.created_at,
                   a.id AS account_id, a.account_type, a.exchange_name,
                   a.label AS account_label, a.notes AS account_notes,
                   wa.id AS address_id, wa.address, wa.chain, wa.network,
                   wa.token_contract, wa.label AS address_label, wa.is_active
            FROM tax.wallet_entities e
            LEFT JOIN tax.wallet_accounts a ON a.entity_id = e.id
            LEFT JOIN tax.wallet_addresses wa ON wa.account_id = a.id
            WHERE e.id = :eid ORDER BY a.id, wa.id
        """), {"eid": entity_id})
        rows = [dict(zip(r.keys(), row)) for row in r.fetchall()]
    if not rows:
        raise HTTPException(404, f"Entity {entity_id} not found")
    first = rows[0]
    entity = {"id": first["id"], "entity_type": first["entity_type"],
              "label": first["label"], "notes": first["notes"],
              "created_at": str(first["created_at"]) if first["created_at"] else None,
              "accounts": []}
    accounts = {}
    for row in rows:
        if row["account_id"] and row["account_id"] not in accounts:
            accounts[row["account_id"]] = {
                "id": row["account_id"], "account_type": row["account_type"],
                "exchange_name": row["exchange_name"], "label": row["account_label"],
                "notes": row["account_notes"], "addresses": []}
        if row["address_id"] and row["account_id"]:
            accounts[row["account_id"]]["addresses"].append({
                "id": row["address_id"], "address": row["address"],
                "chain": row["chain"], "network": row["network"],
                "label": row["address_label"], "is_active": row["is_active"]})
    entity["accounts"] = list(accounts.values())
    return entity


@app.patch("/v4/wallet/entities/{entity_id}")
async def wallet_update_entity(entity_id: int, entity_type: str = Query(None),
                                label: str = Query(None), notes: str = Query(None)):
    """Update a wallet entity."""
    updates, params = [], {"id": entity_id}
    if entity_type is not None:
        if entity_type not in VALID_ENTITY_TYPES:
            raise HTTPException(400, f"entity_type must be one of: {', '.join(sorted(VALID_ENTITY_TYPES))}")
        updates.append("entity_type = :et"); params["et"] = entity_type
    if label is not None:
        if not label.strip():
            raise HTTPException(400, "label cannot be empty")
        updates.append("label = :label"); params["label"] = label.strip()
    if notes is not None:
        updates.append("notes = :notes"); params["notes"] = notes if notes.strip() else None
    if not updates:
        raise HTTPException(400, "No fields to update")
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t(f"UPDATE tax.wallet_entities SET {', '.join(updates)} WHERE id = :id RETURNING id"), params)
        if not r.fetchone():
            raise HTTPException(404, f"Entity {entity_id} not found")
        await session.commit()
    return {"updated": entity_id}


@app.post("/v4/wallet/accounts")
async def wallet_create_account(entity_id: int = Query(...), account_type: str = Query(...),
                                 exchange_name: str = Query(None), label: str = Query(...)):
    """Create a wallet account under an entity."""
    if account_type not in VALID_ACCOUNT_TYPES:
        raise HTTPException(400, f"account_type must be one of: {', '.join(sorted(VALID_ACCOUNT_TYPES))}")
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("""
            INSERT INTO tax.wallet_accounts (entity_id, account_type, exchange_name, label)
            VALUES (:eid, :at, :en, :label) RETURNING id
        """), {"eid": entity_id, "at": account_type, "en": exchange_name, "label": label})
        await session.commit()
        return {"id": r.fetchone()[0], "entity_id": entity_id, "label": label}


@app.post("/v4/wallet/addresses")
async def wallet_register_address(account_id: int = Query(...), address: str = Query(...),
                                   chain: str = Query(None), network: str = Query(None),
                                   label: str = Query(None)):
    """Register an address under a wallet account."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("""
            INSERT INTO tax.wallet_addresses (account_id, address, chain, network, label)
            VALUES (:aid, :addr, :chain, :net, :label)
            ON CONFLICT (address, chain) DO UPDATE SET
                label = COALESCE(EXCLUDED.label, tax.wallet_addresses.label),
                last_seen_at = NOW()
            RETURNING id
        """), {"aid": account_id, "addr": address, "chain": chain, "net": network, "label": label})
        await session.commit()
        return {"id": r.fetchone()[0], "address": address, "chain": chain}


@app.post("/v4/wallet/claims")
async def wallet_create_claim(address_id: int = Query(...), claim_type: str = Query(...),
                               confidence: str = Query("verified"),
                               evidence_summary: str = Query(None)):
    """Claim ownership of an address."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("""
            INSERT INTO tax.wallet_address_claims
                (address_id, claim_type, confidence, evidence_summary)
            VALUES (:aid, :ct, :conf, :es) RETURNING id
        """), {"aid": address_id, "ct": claim_type, "conf": confidence, "es": evidence_summary})
        await session.commit()
        return {"id": r.fetchone()[0], "claim_type": claim_type, "confidence": confidence}


@app.get("/v4/wallet/addresses/check/{address}")
async def wallet_check_address(address: str):
    """Check if an address is claimed as self-owned."""
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("""
            SELECT wa.id, wa.address, wa.chain, wa.label,
                   wac.claim_type, wac.confidence, wac.review_status,
                   wacct.exchange_name, wacct.label AS account_label,
                   we.entity_type, we.label AS entity_label
            FROM tax.wallet_addresses wa
            JOIN tax.wallet_address_claims wac ON wac.address_id = wa.id
            JOIN tax.wallet_accounts wacct ON wacct.id = wa.account_id
            JOIN tax.wallet_entities we ON we.id = wacct.entity_id
            WHERE wa.address = :addr
            ORDER BY wac.claimed_at DESC
            LIMIT 1
        """), {"addr": address})
        row = r.fetchone()

    if not row:
        return {"address": address, "is_claimed": False}

    return {
        "address": address, "is_claimed": True,
        "address_id": row[0], "chain": row[2], "label": row[3],
        "claim_type": row[4], "confidence": row[5], "review_status": row[6],
        "exchange_name": row[7], "account_label": row[8],
        "entity_type": row[9], "entity_label": row[10],
    }


@app.post("/v4/wallet/auto-discover")
async def wallet_auto_discover():
    """Scan all deposit/withdrawal addresses and show claim status."""
    async with db.get_session() as session:
        from sqlalchemy import text as t

        # 1. All unique addresses from deposits and withdrawals
        r = await session.execute(t("""
            SELECT address, exchange, asset, network, 'deposit' AS direction,
                   COUNT(*) AS tx_count,
                   MIN(confirmed_at)::text AS first_seen,
                   MAX(confirmed_at)::text AS last_seen
            FROM tax.deposits
            WHERE address IS NOT NULL AND address != ''
            GROUP BY address, exchange, asset, network
            UNION ALL
            SELECT address, exchange, asset, network, 'withdrawal' AS direction,
                   COUNT(*) AS tx_count,
                   MIN(confirmed_at)::text AS first_seen,
                   MAX(confirmed_at)::text AS last_seen
            FROM tax.withdrawals
            WHERE address IS NOT NULL AND address != ''
            GROUP BY address, exchange, asset, network
        """))
        all_rows = [dict(zip(r.keys(), row)) for row in r.fetchall()]

        # 2. Claimed addresses with registration info
        r = await session.execute(t("""
            SELECT wa.address, wac.claim_type, we.label AS entity_label,
                   wacct.label AS account_label, wacct.id AS account_id
            FROM tax.wallet_addresses wa
            JOIN tax.wallet_address_claims wac ON wac.address_id = wa.id
            JOIN tax.wallet_accounts wacct ON wa.account_id = wacct.id
            JOIN tax.wallet_entities we ON wacct.entity_id = we.id
        """))
        claimed_map = {}
        for row in r.fetchall():
            claimed_map[row[0]] = {
                "entity_label": row[1] if len(row) < 4 else row[2],
                "account_label": row[3] if len(row) >= 4 else "",
                "account_id": row[4] if len(row) >= 5 else None,
                "claim_type": row[1],
            }
        # Fix: use dict(zip()) for clarity
        r2 = await session.execute(t("""
            SELECT wa.address, wac.claim_type, we.label AS entity_label,
                   wacct.label AS account_label, wacct.id AS account_id
            FROM tax.wallet_addresses wa
            JOIN tax.wallet_address_claims wac ON wac.address_id = wa.id
            JOIN tax.wallet_accounts wacct ON wa.account_id = wacct.id
            JOIN tax.wallet_entities we ON wacct.entity_id = we.id
        """))
        claimed_map = {}
        for row in r2.fetchall():
            rd = dict(zip(r2.keys(), row))
            claimed_map[rd["address"]] = {
                "entity_label": rd["entity_label"],
                "account_label": rd["account_label"],
                "account_id": rd["account_id"],
                "claim_type": rd["claim_type"],
            }

        # 3. Entity list for the "Add to Wallet" dropdowns
        ent_r = await session.execute(t("""
            SELECT e.id, e.label, a.id AS account_id, a.label AS account_label,
                   a.account_type
            FROM tax.wallet_entities e
            LEFT JOIN tax.wallet_accounts a ON a.entity_id = e.id
            ORDER BY e.id, a.id
        """))
        ent_rows = [dict(zip(ent_r.keys(), row)) for row in ent_r.fetchall()]

    # Group addresses
    # 4. Registered but unclaimed addresses
    async with db.get_session() as session:
        from sqlalchemy import text as t
        reg_r = await session.execute(t("""
            SELECT wa.address, we.label AS entity_label, wacct.label AS account_label,
                   wacct.id AS account_id
            FROM tax.wallet_addresses wa
            JOIN tax.wallet_accounts wacct ON wa.account_id = wacct.id
            JOIN tax.wallet_entities we ON wacct.entity_id = we.id
            WHERE wa.id NOT IN (
                SELECT address_id FROM tax.wallet_address_claims
                WHERE claim_type = 'self_owned'
            )
        """))
        registered_no_claim_map = {}
        for row in reg_r.fetchall():
            rd = dict(zip(reg_r.keys(), row))
            registered_no_claim_map[rd["address"]] = {
                "entity_label": rd["entity_label"],
                "account_label": rd["account_label"],
                "account_id": rd["account_id"],
            }

        # 5. Transfer pairs
        tp_r = await session.execute(t("""
            SELECT tc.source_wallet AS from_exchange, tc.dest_wallet AS to_exchange,
                   tc.asset, tc.match_confidence,
                   w.address AS withdrawal_address, d.address AS deposit_address,
                   COUNT(*) AS tx_count
            FROM tax.transfer_carryover tc
            LEFT JOIN tax.withdrawals w ON w.id = tc.withdrawal_id
            LEFT JOIN tax.deposits d ON d.id = tc.deposit_id
            WHERE w.address IS NOT NULL OR d.address IS NOT NULL
            GROUP BY tc.source_wallet, tc.dest_wallet, tc.asset, tc.match_confidence,
                     w.address, d.address
            ORDER BY tx_count DESC
            LIMIT 500
        """))
        transfer_pairs_raw = [dict(zip(tp_r.keys(), row)) for row in tp_r.fetchall()]

    # Group addresses
    addr_groups: dict[str, dict] = {}
    for row in all_rows:
        addr = row["address"]
        if addr not in addr_groups:
            addr_groups[addr] = {"address": addr, "exchanges": [], "assets": set(),
                                 "networks": set(), "total_tx_count": 0,
                                 "first_seen": row["first_seen"], "last_seen": row["last_seen"]}
        g = addr_groups[addr]
        g["exchanges"].append({"exchange": row["exchange"], "direction": row["direction"],
                                "tx_count": row["tx_count"]})
        g["assets"].add(row["asset"] or "")
        if row["network"]:
            g["networks"].add(row["network"])
        g["total_tx_count"] += row["tx_count"]
        if row["first_seen"] and (not g["first_seen"] or row["first_seen"] < g["first_seen"]):
            g["first_seen"] = row["first_seen"]
        if row["last_seen"] and (not g["last_seen"] or row["last_seen"] > g["last_seen"]):
            g["last_seen"] = row["last_seen"]

    addresses = []
    for addr, g in addr_groups.items():
        g["assets"] = sorted(g["assets"] - {""})
        g["networks"] = sorted(g["networks"])
        if addr in claimed_map:
            g["status"] = "claimed"
            g["registered_in"] = claimed_map[addr]
        elif addr in registered_no_claim_map:
            g["status"] = "registered"
            g["registered_in"] = registered_no_claim_map[addr]
        else:
            g["status"] = "unclaimed"
            g["registered_in"] = None
        addresses.append(g)

    addresses.sort(key=lambda a: ({"unclaimed": 0, "registered": 1, "claimed": 2}.get(a["status"], 3),
                                   -a["total_tx_count"]))

    # Build entity list
    ent_map: dict[int, dict] = {}
    for row in ent_rows:
        eid = row["id"]
        if eid not in ent_map:
            ent_map[eid] = {"id": eid, "label": row["label"], "accounts": []}
        if row["account_id"]:
            ent_map[eid]["accounts"].append({
                "id": row["account_id"], "label": row["account_label"],
                "account_type": row["account_type"]})

    # Build transfer pairs with claim info
    transfer_pairs = []
    for tp in transfer_pairs_raw:
        wd_addr = tp.get("withdrawal_address")
        dep_addr = tp.get("deposit_address")
        transfer_pairs.append({
            "withdrawal_address": wd_addr,
            "deposit_address": dep_addr,
            "from_exchange": tp["from_exchange"],
            "to_exchange": tp["to_exchange"],
            "asset": tp["asset"],
            "tx_count": tp["tx_count"],
            "match_confidence": tp["match_confidence"],
            "wd_claimed": wd_addr in claimed_map,
            "dep_claimed": dep_addr in claimed_map,
            "both_owned": (wd_addr in claimed_map) and (dep_addr in claimed_map),
        })

    claimed_count = sum(1 for a in addresses if a["status"] == "claimed")
    registered_count = sum(1 for a in addresses if a["status"] == "registered")
    return {
        "summary": {
            "total_unique_addresses": len(addresses),
            "already_registered": claimed_count + registered_count,
            "claimed": claimed_count,
            "registered_no_claim": registered_count,
            "unclaimed": len(addresses) - claimed_count - registered_count,
        },
        "addresses": addresses,
        "entities": list(ent_map.values()),
        "transfer_pairs": transfer_pairs,
    }


@app.patch("/v4/wallet/accounts/{account_id}")
async def wallet_update_account(account_id: int, account_type: str = Query(None),
                                 exchange_name: str = Query(None), label: str = Query(None),
                                 notes: str = Query(None)):
    """Update a wallet account."""
    updates, params = [], {"id": account_id}
    if account_type is not None:
        if account_type not in VALID_ACCOUNT_TYPES:
            raise HTTPException(400, f"account_type must be one of: {', '.join(sorted(VALID_ACCOUNT_TYPES))}")
        updates.append("account_type = :at"); params["at"] = account_type
    if exchange_name is not None:
        updates.append("exchange_name = :en"); params["en"] = exchange_name if exchange_name.strip() else None
    if label is not None:
        if not label.strip():
            raise HTTPException(400, "label cannot be empty")
        updates.append("label = :label"); params["label"] = label.strip()
    if notes is not None:
        updates.append("notes = :notes"); params["notes"] = notes if notes.strip() else None
    if not updates:
        raise HTTPException(400, "No fields to update")
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t(f"UPDATE tax.wallet_accounts SET {', '.join(updates)} WHERE id = :id RETURNING id"), params)
        if not r.fetchone():
            raise HTTPException(404, f"Account {account_id} not found")
        await session.commit()
    return {"updated": account_id}


@app.patch("/v4/wallet/addresses/{address_id}")
async def wallet_update_address(address_id: int, chain: str = Query(None),
                                 network: str = Query(None), label: str = Query(None),
                                 token_contract: str = Query(None), is_active: bool = Query(None)):
    """Update a wallet address's metadata. The address string itself is immutable."""
    updates, params = [], {"id": address_id}
    if chain is not None:
        updates.append("chain = :chain"); params["chain"] = chain if chain.strip() else None
    if network is not None:
        updates.append("network = :net"); params["net"] = network if network.strip() else None
    if label is not None:
        updates.append("label = :label"); params["label"] = label if label.strip() else None
    if token_contract is not None:
        updates.append("token_contract = :tc"); params["tc"] = token_contract if token_contract.strip() else None
    if is_active is not None:
        updates.append("is_active = :active"); params["active"] = is_active
    if not updates:
        raise HTTPException(400, "No fields to update")
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t(f"UPDATE tax.wallet_addresses SET {', '.join(updates)} WHERE id = :id RETURNING id"), params)
        if not r.fetchone():
            raise HTTPException(404, f"Address {address_id} not found")
        await session.commit()
    return {"updated": address_id}


@app.delete("/v4/wallet/entities/{entity_id}")
async def wallet_delete_entity(entity_id: int):
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("DELETE FROM tax.wallet_entities WHERE id = :id RETURNING id"), {"id": entity_id})
        if not r.fetchone():
            raise HTTPException(404, f"Entity {entity_id} not found")
        await session.commit()
    return {"deleted": entity_id}


@app.delete("/v4/wallet/accounts/{account_id}")
async def wallet_delete_account(account_id: int):
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("DELETE FROM tax.wallet_accounts WHERE id = :id RETURNING id"), {"id": account_id})
        if not r.fetchone():
            raise HTTPException(404, f"Account {account_id} not found")
        await session.commit()
    return {"deleted": account_id}


@app.delete("/v4/wallet/addresses/{address_id}")
async def wallet_delete_address(address_id: int):
    async with db.get_session() as session:
        from sqlalchemy import text as t
        r = await session.execute(t("DELETE FROM tax.wallet_addresses WHERE id = :id RETURNING id"), {"id": address_id})
        if not r.fetchone():
            raise HTTPException(404, f"Address {address_id} not found")
        await session.commit()
    return {"deleted": address_id}


@app.patch("/v4/wallet/claims/{claim_id}")
async def wallet_update_claim(claim_id: int,
                               review_status: str = Query(None),
                               claim_type: str = Query(None)):
    async with db.get_session() as session:
        from sqlalchemy import text as t
        updates = []
        params = {"id": claim_id}
        if review_status:
            updates.append("review_status = :rs")
            params["rs"] = review_status
        if claim_type:
            updates.append("claim_type = :ct")
            params["ct"] = claim_type
        if updates:
            await session.execute(t(f"UPDATE tax.wallet_address_claims SET {', '.join(updates)} WHERE id = :id"), params)
            await session.commit()
    return {"updated": claim_id}


@app.delete("/v4/wallet/claims/{claim_id}")
async def wallet_delete_claim(claim_id: int):
    async with db.get_session() as session:
        from sqlalchemy import text as t
        await session.execute(t("DELETE FROM tax.wallet_address_claims WHERE id = :id"), {"id": claim_id})
        await session.commit()
    return {"deleted": claim_id}


# ══════════════════════════════════════════════════════════════════════════
# SALVIUM WALLET ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════

from salvium_staking import SalviumStakingTracker

salvium_staking = SalviumStakingTracker()


@app.get("/salvium/status")
async def salvium_status():
    """Salvium wallet balance, sync height, staking summary."""
    try:
        status = {}
        # Get staking DB summary
        async with db.get_session() as session:
            status = await salvium_staking.get_status(session)

        # Try to get live wallet balance from the exchange plugin
        try:
            ex = get_exchange("salvium", settings)
            if ex:
                staking_summary = await ex.get_staking_summary()
                status["wallet_balance_sal"] = staking_summary.get("wallet_balance_sal", "0")
                status["unlocked_balance_sal"] = staking_summary.get("unlocked_balance_sal", "0")
                status["wallet_connected"] = True
            else:
                status["wallet_connected"] = False
        except Exception as e:
            status["wallet_connected"] = False
            status["wallet_error"] = str(e)

        return status
    except Exception as e:
        logger.exception(f"Salvium status failed: {e}")
        raise HTTPException(500, str(e))


@app.get("/salvium/stakes")
async def salvium_stakes():
    """List all staking lock/unlock pairs with yield."""
    async with db.get_session() as session:
        stakes = await salvium_staking.get_stakes(session)
    return {"count": len(stakes), "stakes": stakes}


@app.get("/salvium/income")
async def salvium_income(year: int = Query(None)):
    """Staking income for a tax year."""
    async with db.get_session() as session:
        events = await salvium_staking.get_income(session, year=year)

    from decimal import Decimal as D
    total = sum(D(e.get("total_fmv_usd") or "0") for e in events)
    return {
        "year": year or "all",
        "total_income_events": len(events),
        "total_staking_income_usd": str(total),
        "events": events,
    }


@app.get("/salvium/accounts")
async def salvium_accounts():
    """Get per-account balances from Salvium wallet."""
    ex = get_exchange("salvium", settings)
    if not ex:
        raise HTTPException(503, "Salvium not configured")
    accounts = await ex._rpc("get_accounts")
    if "error" in accounts:
        raise HTTPException(502, f"Wallet RPC error: {accounts['error']}")
    accts = accounts.get("subaddress_accounts", [])

    from decimal import Decimal as D
    ATOMIC = D("100000000")
    result = []
    for a in accts:
        result.append({
            "index": a["account_index"],
            "label": a.get("label", ""),
            "address": a.get("base_address", ""),
            "balance_sal": str(D(str(a.get("balance", 0))) / ATOMIC),
            "unlocked_sal": str(D(str(a.get("unlocked_balance", 0))) / ATOMIC),
            "locked_sal": str((D(str(a.get("balance", 0))) - D(str(a.get("unlocked_balance", 0)))) / ATOMIC),
        })

    return {
        "accounts": result,
        "total_balance_sal": str(D(str(accounts.get("total_balance", 0))) / ATOMIC),
        "total_unlocked_sal": str(D(str(accounts.get("total_unlocked_balance", 0))) / ATOMIC),
        "total_locked_sal": str((D(str(accounts.get("total_balance", 0))) - D(str(accounts.get("total_unlocked_balance", 0)))) / ATOMIC),
    }


@app.post("/salvium/sweep")
async def salvium_sweep(
    from_account: int = Query(..., description="Source account index"),
    to_account: int = Query(..., description="Destination account index"),
):
    """Sweep all unlocked SAL from one account to another."""
    ex = get_exchange("salvium", settings)
    if not ex:
        raise HTTPException(503, "Salvium not configured")

    # Get destination address from accounts
    accounts = await ex._rpc("get_accounts")
    if "error" in accounts:
        raise HTTPException(502, f"Wallet RPC error: {accounts['error']}")
    accts = accounts.get("subaddress_accounts", [])
    dest_addr = None
    for a in accts:
        if a["account_index"] == to_account:
            dest_addr = a["base_address"]
            break
    if not dest_addr:
        raise HTTPException(400, f"Account {to_account} not found")

    result = await ex._rpc("sweep_all", {
        "address": dest_addr,
        "asset_type": "SAL1",
        "account_index": from_account,
    })

    if not result or "tx_hash_list" not in result:
        error = result.get("error", {}).get("message", "Unknown error") if isinstance(result, dict) else "RPC call failed"
        raise HTTPException(400, f"Sweep failed: {error}")

    from decimal import Decimal as D
    ATOMIC = D("100000000")
    amounts = [str(D(str(a)) / ATOMIC) for a in result.get("amount_list", [])]
    fees = [str(D(str(f)) / ATOMIC) for f in result.get("fee_list", [])]

    return {
        "status": "success",
        "from_account": from_account,
        "to_account": to_account,
        "tx_hashes": result.get("tx_hash_list", []),
        "amounts_sal": amounts,
        "fees_sal": fees,
    }


@app.post("/salvium/stake")
async def salvium_stake(
    amount: float = Query(..., description="Amount of SAL to stake"),
    account_index: int = Query(0, description="Account to stake from"),
):
    """Stake SAL tokens (~30 day lock period)."""
    ex = get_exchange("salvium", settings)
    if not ex:
        raise HTTPException(503, "Salvium not configured")

    if amount <= 0:
        raise HTTPException(400, "Amount must be positive")

    from decimal import Decimal as D
    # Convert SAL to atomic units (1e8)
    atomic_amount = int(D(str(amount)) * D("100000000"))

    # Get own address for the staking account
    accounts = await ex._rpc("get_accounts")
    if "error" in accounts:
        raise HTTPException(502, f"Wallet RPC error: {accounts['error']}")
    accts = accounts.get("subaddress_accounts", [])
    own_addr = None
    for a in accts:
        if a["account_index"] == account_index:
            own_addr = a["base_address"]
            break
    if not own_addr:
        raise HTTPException(400, f"Account {account_index} not found")

    result = await ex._rpc("transfer", {
        "destinations": [{"amount": atomic_amount, "address": own_addr}],
        "source_asset": "SAL1",
        "dest_asset": "SAL1",
        "tx_type": 6,  # STAKE
        "account_index": account_index,
        "get_tx_key": True,
    })

    if not result or "tx_hash" not in result:
        error_msg = "Unknown error"
        if isinstance(result, dict) and "error" in result:
            error_msg = result["error"].get("message", error_msg)
        raise HTTPException(400, f"Stake failed: {error_msg}")

    ATOMIC = D("100000000")
    fee_sal = str(D(str(result.get("fee", 0))) / ATOMIC)

    return {
        "status": "success",
        "amount_sal": str(amount),
        "account_index": account_index,
        "tx_hash": result.get("tx_hash", ""),
        "fee_sal": fee_sal,
        "lock_period": "~30 days (21,600 blocks)",
    }


@app.post("/salvium/stake-max")
async def salvium_stake_max(account_index: int = Query(0)):
    """Stake the maximum possible SAL by estimating fees first."""
    ex = get_exchange("salvium", settings)
    if not ex:
        raise HTTPException(503, "Salvium not configured")

    from decimal import Decimal as D
    ATOMIC = D("100000000")

    # 1. Get unlocked balance for this account
    accounts = await ex._rpc("get_accounts")
    if "error" in accounts:
        raise HTTPException(502, f"Wallet RPC error: {accounts['error']}")
    accts = accounts.get("subaddress_accounts", [])
    own_addr = None
    unlocked_atomic = 0
    for a in accts:
        if a["account_index"] == account_index:
            own_addr = a["base_address"]
            unlocked_atomic = a.get("unlocked_balance", 0)
            break
    if not own_addr:
        raise HTTPException(400, f"Account {account_index} not found")
    if unlocked_atomic <= 0:
        raise HTTPException(400, "No unlocked funds available to stake")

    # 2. Estimate fee with a dry-run stake of the full unlocked amount
    estimate = await ex._rpc("transfer", {
        "destinations": [{"amount": unlocked_atomic, "address": own_addr}],
        "source_asset": "SAL1",
        "dest_asset": "SAL1",
        "tx_type": 6,
        "account_index": account_index,
        "do_not_relay": True,
        "get_tx_key": True,
    })

    # The dry run may fail because amount + fee > balance
    estimated_fee = 0
    if isinstance(estimate, dict) and "fee" in estimate:
        estimated_fee = estimate["fee"]
    else:
        # Dry run failed — try with half the balance to get a fee estimate
        half_estimate = await ex._rpc("transfer", {
            "destinations": [{"amount": unlocked_atomic // 2, "address": own_addr}],
            "source_asset": "SAL1",
            "dest_asset": "SAL1",
            "tx_type": 6,
            "account_index": account_index,
            "do_not_relay": True,
            "get_tx_key": True,
        })
        if isinstance(half_estimate, dict) and "fee" in half_estimate:
            estimated_fee = int(half_estimate["fee"] * 1.5)
        else:
            estimated_fee = 1000000  # 0.01 SAL fallback

    # 3. Calculate max stakeable amount
    max_stake_atomic = unlocked_atomic - estimated_fee
    if max_stake_atomic <= 0:
        raise HTTPException(400, f"Unlocked balance ({D(str(unlocked_atomic)) / ATOMIC} SAL) is less than estimated fee ({D(str(estimated_fee)) / ATOMIC} SAL)")

    # 4. Actually stake it
    result = await ex._rpc("transfer", {
        "destinations": [{"amount": max_stake_atomic, "address": own_addr}],
        "source_asset": "SAL1",
        "dest_asset": "SAL1",
        "tx_type": 6,
        "account_index": account_index,
        "get_tx_key": True,
    })

    if not isinstance(result, dict) or "tx_hash" not in result:
        error_msg = "Unknown error"
        if isinstance(result, dict) and "error" in result:
            error_msg = result["error"].get("message", error_msg)
        raise HTTPException(400, f"Stake-max failed: {error_msg}")

    actual_fee = D(str(result.get("fee", 0))) / ATOMIC
    staked_sal = D(str(max_stake_atomic)) / ATOMIC

    return {
        "status": "success",
        "staked_sal": str(staked_sal),
        "fee_sal": str(actual_fee),
        "account_index": account_index,
        "tx_hash": result.get("tx_hash", ""),
        "lock_period": "~30 days (21,600 blocks)",
    }


@app.post("/salvium/consolidate")
async def salvium_consolidate(account_index: int = Query(0)):
    """Consolidate all outputs in an account by sweeping to self."""
    ex = get_exchange("salvium", settings)
    if not ex:
        raise HTTPException(503, "Salvium not configured")

    # Get own address for this account
    accounts = await ex._rpc("get_accounts")
    if "error" in accounts:
        raise HTTPException(502, f"Wallet RPC error: {accounts['error']}")
    accts = accounts.get("subaddress_accounts", [])
    own_addr = None
    for a in accts:
        if a["account_index"] == account_index:
            own_addr = a["base_address"]
            break
    if not own_addr:
        raise HTTPException(400, f"Account {account_index} not found")

    result = await ex._rpc("sweep_all", {
        "address": own_addr,
        "asset_type": "SAL1",
        "account_index": account_index,
    })

    if not isinstance(result, dict) or "tx_hash_list" not in result:
        error_msg = "No usable outputs to consolidate"
        if isinstance(result, dict) and "error" in result:
            error_msg = result["error"].get("message", error_msg)
        raise HTTPException(400, f"Consolidate failed: {error_msg}")

    from decimal import Decimal as D
    ATOMIC = D("100000000")
    amounts = [str(D(str(a)) / ATOMIC) for a in result.get("amount_list", [])]
    fees = [str(D(str(f)) / ATOMIC) for f in result.get("fee_list", [])]

    return {
        "status": "success",
        "account_index": account_index,
        "tx_hashes": result.get("tx_hash_list", []),
        "amounts_sal": amounts,
        "fees_sal": fees,
        "note": "Outputs consolidated into a single UTXO",
    }


@app.get("/salvium/outputs")
async def salvium_outputs(account_index: int = Query(0)):
    """Get all unspent outputs for an account."""
    ex = get_exchange("salvium", settings)
    if not ex:
        raise HTTPException(503, "Salvium not configured")

    from decimal import Decimal as D
    ATOMIC = D("100000000")

    # Get available (unlocked) transfers
    result = await ex._rpc("incoming_transfers", {
        "transfer_type": "available",
        "account_index": account_index,
    })
    available = result.get("transfers", []) if isinstance(result, dict) else []

    # Get unavailable (locked/spent) transfers
    locked_result = await ex._rpc("incoming_transfers", {
        "transfer_type": "unavailable",
        "account_index": account_index,
    })
    locked = locked_result.get("transfers", []) if isinstance(locked_result, dict) else []

    outputs = []
    total_available = D("0")
    total_locked = D("0")

    for t in available:
        amount_sal = D(str(t.get("amount", 0))) / ATOMIC
        total_available += amount_sal
        outputs.append({
            "amount_sal": str(amount_sal),
            "amount_atomic": t.get("amount", 0),
            "key_image": t.get("key_image", ""),
            "tx_hash": t.get("tx_hash", ""),
            "subaddr_index": t.get("subaddr_index", 0),
            "block_height": t.get("block_height", 0),
            "frozen": t.get("frozen", False),
            "unlocked": True,
            "spent": False,
        })

    for t in locked:
        amount_sal = D(str(t.get("amount", 0))) / ATOMIC
        total_locked += amount_sal
        outputs.append({
            "amount_sal": str(amount_sal),
            "amount_atomic": t.get("amount", 0),
            "key_image": t.get("key_image", ""),
            "tx_hash": t.get("tx_hash", ""),
            "subaddr_index": t.get("subaddr_index", 0),
            "block_height": t.get("block_height", 0),
            "frozen": t.get("frozen", False),
            "unlocked": False,
            "spent": True,
        })

    outputs.sort(key=lambda x: float(x["amount_sal"]), reverse=True)

    return {
        "account_index": account_index,
        "available_count": len(available),
        "locked_count": len(locked),
        "total_count": len(outputs),
        "total_available_sal": str(total_available),
        "total_locked_sal": str(total_locked),
        "outputs": outputs,
    }


@app.post("/salvium/sync")
async def salvium_sync():
    """Manual sync of Salvium wallet transactions + staking pair detection."""
    try:
        # Step 1: Sync wallet transactions via normal exchange sync
        await run_sync("salvium", full=True)

        # Step 2: Scan for staking lock/unlock pairs
        async with db.get_session() as session:
            result = await salvium_staking.scan_and_match(session)

        return {"sync": "completed", "staking": result}
    except Exception as e:
        logger.exception(f"Salvium sync failed: {e}")
        raise HTTPException(500, str(e))


@app.get("/export/v4-tax-report")
async def export_v4_tax_report(year: int = Query(...), run_id: int = Query(None)):
    """Full accountant-ready XLSX from v4 engine."""
    try:
        async with db.get_session() as session:
            from exports.tax_report import generate_full_tax_report_v4
            filepath = await generate_full_tax_report_v4(session, year=year, run_id=run_id)
        return FileResponse(
            filepath,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=os.path.basename(filepath),
        )
    except Exception as e:
        logger.exception(f"v4 tax report export failed: {e}")
        raise HTTPException(500, f"Export failed: {str(e)}")
