"""
Database layer — PostgreSQL 'tax' schema with normalized tables + USD valuations.

Tables:
  tax.trades         — buy/sell trades with USD values
  tax.orders         — order history with USD values
  tax.deposits       — incoming transfers with USD values
  tax.withdrawals    — outgoing transfers with USD values
  tax.pool_activity  — liquidity pool events with USD values
  tax.price_cache    — cached historical USD prices per asset/date
  tax.sync_log       — tracks last sync timestamps per exchange/type
"""
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

logger = logging.getLogger("tax-collector.db")

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS tax;

CREATE TABLE IF NOT EXISTS tax.price_cache (
    id              SERIAL PRIMARY KEY,
    asset           VARCHAR(50)    NOT NULL,
    price_date      DATE           NOT NULL,
    price_usd       NUMERIC(36,18) NOT NULL,
    source          VARCHAR(50),
    updated_at      TIMESTAMPTZ    DEFAULT NOW(),
    UNIQUE(asset, price_date)
);
CREATE INDEX IF NOT EXISTS idx_price_cache_asset ON tax.price_cache(asset, price_date);

CREATE TABLE IF NOT EXISTS tax.trades (
    id              SERIAL PRIMARY KEY,
    exchange        VARCHAR(50)   NOT NULL,
    exchange_id     VARCHAR(200)  NOT NULL,
    market          VARCHAR(100)  NOT NULL,
    base_asset      VARCHAR(50),
    quote_asset     VARCHAR(50),
    side            VARCHAR(10)   NOT NULL,
    price           NUMERIC(36,18) NOT NULL,
    quantity        NUMERIC(36,18) NOT NULL,
    total           NUMERIC(36,18),
    fee             NUMERIC(36,18),
    fee_asset       VARCHAR(50),
    price_usd       NUMERIC(36,18),
    quantity_usd    NUMERIC(36,18),
    total_usd       NUMERIC(36,18),
    fee_usd         NUMERIC(36,18),
    base_price_usd  NUMERIC(36,18),
    quote_price_usd NUMERIC(36,18),
    executed_at     TIMESTAMPTZ   NOT NULL,
    raw_data        JSONB,
    created_at      TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE(exchange, exchange_id)
);

CREATE TABLE IF NOT EXISTS tax.orders (
    id              SERIAL PRIMARY KEY,
    exchange        VARCHAR(50)   NOT NULL,
    exchange_id     VARCHAR(200)  NOT NULL,
    market          VARCHAR(100)  NOT NULL,
    base_asset      VARCHAR(50),
    quote_asset     VARCHAR(50),
    side            VARCHAR(10)   NOT NULL,
    order_type      VARCHAR(20),
    price           NUMERIC(36,18),
    quantity        NUMERIC(36,18) NOT NULL,
    executed_qty    NUMERIC(36,18),
    status          VARCHAR(30)   NOT NULL,
    price_usd       NUMERIC(36,18),
    total_usd       NUMERIC(36,18),
    fee_usd         NUMERIC(36,18),
    created_at_ex   TIMESTAMPTZ   NOT NULL,
    updated_at_ex   TIMESTAMPTZ,
    raw_data        JSONB,
    created_at      TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE(exchange, exchange_id)
);

CREATE TABLE IF NOT EXISTS tax.deposits (
    id              SERIAL PRIMARY KEY,
    exchange        VARCHAR(50)   NOT NULL,
    exchange_id     VARCHAR(200)  NOT NULL,
    asset           VARCHAR(50)   NOT NULL,
    amount          NUMERIC(36,18) NOT NULL,
    network         VARCHAR(100),
    tx_hash         VARCHAR(500),
    address         VARCHAR(500),
    status          VARCHAR(30),
    asset_price_usd NUMERIC(36,18),
    amount_usd      NUMERIC(36,18),
    confirmed_at    TIMESTAMPTZ,
    raw_data        JSONB,
    created_at      TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE(exchange, exchange_id)
);

CREATE TABLE IF NOT EXISTS tax.withdrawals (
    id              SERIAL PRIMARY KEY,
    exchange        VARCHAR(50)   NOT NULL,
    exchange_id     VARCHAR(200)  NOT NULL,
    asset           VARCHAR(50)   NOT NULL,
    amount          NUMERIC(36,18) NOT NULL,
    fee             NUMERIC(36,18),
    network         VARCHAR(100),
    tx_hash         VARCHAR(500),
    address         VARCHAR(500),
    status          VARCHAR(30),
    asset_price_usd NUMERIC(36,18),
    amount_usd      NUMERIC(36,18),
    fee_usd         NUMERIC(36,18),
    confirmed_at    TIMESTAMPTZ,
    raw_data        JSONB,
    created_at      TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE(exchange, exchange_id)
);

CREATE TABLE IF NOT EXISTS tax.pool_activity (
    id              SERIAL PRIMARY KEY,
    exchange        VARCHAR(50)   NOT NULL,
    exchange_id     VARCHAR(200)  NOT NULL,
    pool_name       VARCHAR(200)  NOT NULL,
    action          VARCHAR(30)   NOT NULL,
    asset_in        VARCHAR(50),
    amount_in       NUMERIC(36,18),
    asset_out       VARCHAR(50),
    amount_out      NUMERIC(36,18),
    fee             NUMERIC(36,18),
    fee_asset       VARCHAR(50),
    amount_in_usd   NUMERIC(36,18),
    amount_out_usd  NUMERIC(36,18),
    fee_usd         NUMERIC(36,18),
    executed_at     TIMESTAMPTZ   NOT NULL,
    raw_data        JSONB,
    created_at      TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE(exchange, exchange_id)
);

CREATE TABLE IF NOT EXISTS tax.sync_log (
    id              SERIAL PRIMARY KEY,
    exchange        VARCHAR(50)   NOT NULL,
    data_type       VARCHAR(30)   NOT NULL,
    last_timestamp  TIMESTAMPTZ,
    last_id         VARCHAR(200),
    records_synced  INTEGER       DEFAULT 0,
    synced_at       TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE(exchange, data_type)
);

CREATE INDEX IF NOT EXISTS idx_trades_exchange_date ON tax.trades(exchange, executed_at);
CREATE INDEX IF NOT EXISTS idx_orders_exchange_date ON tax.orders(exchange, created_at_ex);
CREATE INDEX IF NOT EXISTS idx_deposits_exchange    ON tax.deposits(exchange, confirmed_at);
CREATE INDEX IF NOT EXISTS idx_withdrawals_exchange ON tax.withdrawals(exchange, confirmed_at);
CREATE INDEX IF NOT EXISTS idx_pool_exchange        ON tax.pool_activity(exchange, executed_at);
CREATE INDEX IF NOT EXISTS idx_trades_no_usd ON tax.trades(id) WHERE total_usd IS NULL;
CREATE INDEX IF NOT EXISTS idx_deposits_no_usd ON tax.deposits(id) WHERE amount_usd IS NULL;
CREATE INDEX IF NOT EXISTS idx_withdrawals_no_usd ON tax.withdrawals(id) WHERE amount_usd IS NULL;
"""

# v1→v2 migration: adds USD columns to existing tables (safe to re-run)
MIGRATION_SQL = """
ALTER TABLE tax.trades ADD COLUMN IF NOT EXISTS base_asset VARCHAR(50);
ALTER TABLE tax.trades ADD COLUMN IF NOT EXISTS quote_asset VARCHAR(50);
ALTER TABLE tax.trades ADD COLUMN IF NOT EXISTS price_usd NUMERIC(36,18);
ALTER TABLE tax.trades ADD COLUMN IF NOT EXISTS quantity_usd NUMERIC(36,18);
ALTER TABLE tax.trades ADD COLUMN IF NOT EXISTS total_usd NUMERIC(36,18);
ALTER TABLE tax.trades ADD COLUMN IF NOT EXISTS fee_usd NUMERIC(36,18);
ALTER TABLE tax.trades ADD COLUMN IF NOT EXISTS base_price_usd NUMERIC(36,18);
ALTER TABLE tax.trades ADD COLUMN IF NOT EXISTS quote_price_usd NUMERIC(36,18);
ALTER TABLE tax.orders ADD COLUMN IF NOT EXISTS base_asset VARCHAR(50);
ALTER TABLE tax.orders ADD COLUMN IF NOT EXISTS quote_asset VARCHAR(50);
ALTER TABLE tax.orders ADD COLUMN IF NOT EXISTS price_usd NUMERIC(36,18);
ALTER TABLE tax.orders ADD COLUMN IF NOT EXISTS total_usd NUMERIC(36,18);
ALTER TABLE tax.orders ADD COLUMN IF NOT EXISTS fee_usd NUMERIC(36,18);
ALTER TABLE tax.deposits ADD COLUMN IF NOT EXISTS asset_price_usd NUMERIC(36,18);
ALTER TABLE tax.deposits ADD COLUMN IF NOT EXISTS amount_usd NUMERIC(36,18);
ALTER TABLE tax.withdrawals ADD COLUMN IF NOT EXISTS asset_price_usd NUMERIC(36,18);
ALTER TABLE tax.withdrawals ADD COLUMN IF NOT EXISTS amount_usd NUMERIC(36,18);
ALTER TABLE tax.withdrawals ADD COLUMN IF NOT EXISTS fee_usd NUMERIC(36,18);
ALTER TABLE tax.pool_activity ADD COLUMN IF NOT EXISTS amount_in_usd NUMERIC(36,18);
ALTER TABLE tax.pool_activity ADD COLUMN IF NOT EXISTS amount_out_usd NUMERIC(36,18);
ALTER TABLE tax.pool_activity ADD COLUMN IF NOT EXISTS fee_usd NUMERIC(36,18);
"""


class Database:
    def __init__(self, url: str):
        self.engine = create_async_engine(url, echo=False, pool_size=5, max_overflow=5)
        self.session_factory = async_sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)

    async def init(self):
        async with self.engine.begin() as conn:
            for statement in SCHEMA_SQL.split(";"):
                stmt = statement.strip()
                if stmt:
                    await conn.execute(text(stmt))
            for statement in MIGRATION_SQL.split(";"):
                stmt = statement.strip()
                if stmt:
                    await conn.execute(text(stmt))

            # v3: tax computation tables (lots, disposals, form_8949, etc.)
            from schema_v3 import SCHEMA_V3_SQL
            for statement in SCHEMA_V3_SQL.split(";"):
                stmt = statement.strip()
                if stmt:
                    await conn.execute(text(stmt))

        logger.info("Tax schema ready (v3 — includes lots, disposals, form_8949)")

    async def close(self):
        await self.engine.dispose()

    @asynccontextmanager
    async def get_session(self):
        async with self.session_factory() as session:
            yield session

    # ── Sync timestamps ───────────────────────────────────────────────────

    async def get_last_sync_timestamps(self, session: AsyncSession, exchange: str) -> dict:
        result = await session.execute(
            text("SELECT data_type, last_timestamp FROM tax.sync_log WHERE exchange = :ex"),
            {"ex": exchange},
        )
        return {row[0]: row[1] for row in result.fetchall()}

    async def _update_sync_log(self, session: AsyncSession, exchange: str, data_type: str,
                               last_ts: datetime | None, count: int):
        await session.execute(
            text("""
                INSERT INTO tax.sync_log (exchange, data_type, last_timestamp, records_synced, synced_at)
                VALUES (:ex, :dt, :ts, :cnt, NOW())
                ON CONFLICT (exchange, data_type)
                DO UPDATE SET last_timestamp = EXCLUDED.last_timestamp,
                              records_synced = EXCLUDED.records_synced,
                              synced_at = NOW()
            """),
            {"ex": exchange, "dt": data_type, "ts": last_ts, "cnt": count},
        )

    # ── Upserts ───────────────────────────────────────────────────────────

    async def upsert_trades(self, session: AsyncSession, exchange: str, trades: list[dict]):
        if not trades:
            return
        for t in trades:
            await session.execute(
                text("""
                    INSERT INTO tax.trades
                        (exchange, exchange_id, market, base_asset, quote_asset, side,
                         price, quantity, total, fee, fee_asset,
                         price_usd, quantity_usd, total_usd, fee_usd,
                         base_price_usd, quote_price_usd,
                         executed_at, raw_data)
                    VALUES
                        (:exchange, :exchange_id, :market, :base_asset, :quote_asset, :side,
                         :price, :quantity, :total, :fee, :fee_asset,
                         :price_usd, :quantity_usd, :total_usd, :fee_usd,
                         :base_price_usd, :quote_price_usd,
                         :executed_at, CAST(:raw_data AS jsonb))
                    ON CONFLICT (exchange, exchange_id) DO UPDATE SET
                        price = EXCLUDED.price, quantity = EXCLUDED.quantity,
                        total = EXCLUDED.total, fee = EXCLUDED.fee,
                        price_usd = COALESCE(EXCLUDED.price_usd, tax.trades.price_usd),
                        quantity_usd = COALESCE(EXCLUDED.quantity_usd, tax.trades.quantity_usd),
                        total_usd = COALESCE(EXCLUDED.total_usd, tax.trades.total_usd),
                        fee_usd = COALESCE(EXCLUDED.fee_usd, tax.trades.fee_usd),
                        base_price_usd = COALESCE(EXCLUDED.base_price_usd, tax.trades.base_price_usd),
                        quote_price_usd = COALESCE(EXCLUDED.quote_price_usd, tax.trades.quote_price_usd),
                        raw_data = EXCLUDED.raw_data
                """),
                t,
            )
        last_ts = max((t["executed_at"] for t in trades), default=None)
        await self._update_sync_log(session, exchange, "trades", last_ts, len(trades))

    async def upsert_orders(self, session: AsyncSession, exchange: str, orders: list[dict]):
        if not orders:
            return
        for o in orders:
            await session.execute(
                text("""
                    INSERT INTO tax.orders
                        (exchange, exchange_id, market, base_asset, quote_asset, side,
                         order_type, price, quantity, executed_qty, status,
                         price_usd, total_usd, fee_usd,
                         created_at_ex, updated_at_ex, raw_data)
                    VALUES
                        (:exchange, :exchange_id, :market, :base_asset, :quote_asset, :side,
                         :order_type, :price, :quantity, :executed_qty, :status,
                         :price_usd, :total_usd, :fee_usd,
                         :created_at_ex, :updated_at_ex, CAST(:raw_data AS jsonb))
                    ON CONFLICT (exchange, exchange_id) DO UPDATE SET
                        executed_qty = EXCLUDED.executed_qty, status = EXCLUDED.status,
                        price_usd = COALESCE(EXCLUDED.price_usd, tax.orders.price_usd),
                        total_usd = COALESCE(EXCLUDED.total_usd, tax.orders.total_usd),
                        fee_usd = COALESCE(EXCLUDED.fee_usd, tax.orders.fee_usd),
                        updated_at_ex = EXCLUDED.updated_at_ex,
                        raw_data = EXCLUDED.raw_data
                """),
                o,
            )
        last_ts = max((o["created_at_ex"] for o in orders), default=None)
        await self._update_sync_log(session, exchange, "orders", last_ts, len(orders))

    async def upsert_deposits(self, session: AsyncSession, exchange: str, deposits: list[dict]):
        if not deposits:
            return
        for d in deposits:
            await session.execute(
                text("""
                    INSERT INTO tax.deposits
                        (exchange, exchange_id, asset, amount, network, tx_hash, address, status,
                         asset_price_usd, amount_usd, confirmed_at, raw_data)
                    VALUES
                        (:exchange, :exchange_id, :asset, :amount, :network, :tx_hash, :address, :status,
                         :asset_price_usd, :amount_usd, :confirmed_at, CAST(:raw_data AS jsonb))
                    ON CONFLICT (exchange, exchange_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        asset_price_usd = COALESCE(EXCLUDED.asset_price_usd, tax.deposits.asset_price_usd),
                        amount_usd = COALESCE(EXCLUDED.amount_usd, tax.deposits.amount_usd),
                        confirmed_at = EXCLUDED.confirmed_at, raw_data = EXCLUDED.raw_data
                """),
                d,
            )
        last_ts = max((d.get("confirmed_at") for d in deposits if d.get("confirmed_at")), default=None)
        await self._update_sync_log(session, exchange, "deposits", last_ts, len(deposits))

    async def upsert_withdrawals(self, session: AsyncSession, exchange: str, withdrawals: list[dict]):
        if not withdrawals:
            return
        for w in withdrawals:
            await session.execute(
                text("""
                    INSERT INTO tax.withdrawals
                        (exchange, exchange_id, asset, amount, fee, network, tx_hash, address, status,
                         asset_price_usd, amount_usd, fee_usd, confirmed_at, raw_data)
                    VALUES
                        (:exchange, :exchange_id, :asset, :amount, :fee, :network, :tx_hash, :address, :status,
                         :asset_price_usd, :amount_usd, :fee_usd, :confirmed_at, CAST(:raw_data AS jsonb))
                    ON CONFLICT (exchange, exchange_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        asset_price_usd = COALESCE(EXCLUDED.asset_price_usd, tax.withdrawals.asset_price_usd),
                        amount_usd = COALESCE(EXCLUDED.amount_usd, tax.withdrawals.amount_usd),
                        fee_usd = COALESCE(EXCLUDED.fee_usd, tax.withdrawals.fee_usd),
                        confirmed_at = EXCLUDED.confirmed_at, raw_data = EXCLUDED.raw_data
                """),
                w,
            )
        last_ts = max((w.get("confirmed_at") for w in withdrawals if w.get("confirmed_at")), default=None)
        await self._update_sync_log(session, exchange, "withdrawals", last_ts, len(withdrawals))

    async def upsert_pool_activity(self, session: AsyncSession, exchange: str, pools: list[dict]):
        if not pools:
            return
        for p in pools:
            await session.execute(
                text("""
                    INSERT INTO tax.pool_activity
                        (exchange, exchange_id, pool_name, action, asset_in, amount_in,
                         asset_out, amount_out, fee, fee_asset,
                         amount_in_usd, amount_out_usd, fee_usd, executed_at, raw_data)
                    VALUES
                        (:exchange, :exchange_id, :pool_name, :action, :asset_in, :amount_in,
                         :asset_out, :amount_out, :fee, :fee_asset,
                         :amount_in_usd, :amount_out_usd, :fee_usd, :executed_at, CAST(:raw_data AS jsonb))
                    ON CONFLICT (exchange, exchange_id) DO UPDATE SET
                        amount_in_usd = COALESCE(EXCLUDED.amount_in_usd, tax.pool_activity.amount_in_usd),
                        amount_out_usd = COALESCE(EXCLUDED.amount_out_usd, tax.pool_activity.amount_out_usd),
                        fee_usd = COALESCE(EXCLUDED.fee_usd, tax.pool_activity.fee_usd),
                        raw_data = EXCLUDED.raw_data
                """),
                p,
            )
        last_ts = max((p["executed_at"] for p in pools), default=None)
        await self._update_sync_log(session, exchange, "pools", last_ts, len(pools))

    # ── Backfill: find records missing USD values ─────────────────────────

    async def get_trades_missing_usd(self, session: AsyncSession, limit: int = 200) -> list[dict]:
        result = await session.execute(
            text("""SELECT id, exchange, market, side, price::text, quantity::text,
                       total::text, fee::text, fee_asset, executed_at
                FROM tax.trades WHERE total_usd IS NULL
                ORDER BY executed_at ASC LIMIT :limit"""),
            {"limit": limit},
        )
        return [dict(zip(result.keys(), row)) for row in result.fetchall()]

    async def update_trade_usd(self, session: AsyncSession, trade_id: int, usd: dict):
        await session.execute(
            text("""UPDATE tax.trades SET
                    base_asset = :base_asset, quote_asset = :quote_asset,
                    price_usd = :price_usd, quantity_usd = :quantity_usd,
                    total_usd = :total_usd, fee_usd = :fee_usd,
                    base_price_usd = :base_price_usd, quote_price_usd = :quote_price_usd
                WHERE id = :id"""),
            {"id": trade_id, **usd},
        )

    async def get_deposits_missing_usd(self, session: AsyncSession, limit: int = 200) -> list[dict]:
        result = await session.execute(
            text("""SELECT id, exchange, asset, amount::text, confirmed_at
                FROM tax.deposits WHERE amount_usd IS NULL
                ORDER BY confirmed_at ASC LIMIT :limit"""),
            {"limit": limit},
        )
        return [dict(zip(result.keys(), row)) for row in result.fetchall()]

    async def update_deposit_usd(self, session: AsyncSession, dep_id: int, usd: dict):
        await session.execute(
            text("""UPDATE tax.deposits SET asset_price_usd = :asset_price_usd,
                    amount_usd = :amount_usd WHERE id = :id"""),
            {"id": dep_id, **usd},
        )

    async def get_withdrawals_missing_usd(self, session: AsyncSession, limit: int = 200) -> list[dict]:
        result = await session.execute(
            text("""SELECT id, exchange, asset, amount::text, fee::text, confirmed_at
                FROM tax.withdrawals WHERE amount_usd IS NULL
                ORDER BY confirmed_at ASC LIMIT :limit"""),
            {"limit": limit},
        )
        return [dict(zip(result.keys(), row)) for row in result.fetchall()]

    async def update_withdrawal_usd(self, session: AsyncSession, wd_id: int, usd: dict):
        await session.execute(
            text("""UPDATE tax.withdrawals SET asset_price_usd = :asset_price_usd,
                    amount_usd = :amount_usd, fee_usd = :fee_usd WHERE id = :id"""),
            {"id": wd_id, **usd},
        )

    # ── Queries ───────────────────────────────────────────────────────────

    async def query_trades(self, session: AsyncSession, exchange: str,
                           year: int | None = None, limit: int = 100, offset: int = 0) -> list[dict]:
        where = "WHERE exchange = :ex"
        params: dict = {"ex": exchange, "limit": limit, "offset": offset}
        if year:
            where += " AND EXTRACT(YEAR FROM executed_at) = :year"
            params["year"] = year
        result = await session.execute(
            text(f"""SELECT exchange, exchange_id, market, base_asset, quote_asset, side,
                       price::text, quantity::text, total::text, fee::text, fee_asset,
                       price_usd::text, quantity_usd::text, total_usd::text, fee_usd::text,
                       base_price_usd::text, quote_price_usd::text, executed_at
                FROM tax.trades {where}
                ORDER BY executed_at DESC LIMIT :limit OFFSET :offset"""),
            params,
        )
        return [dict(zip(result.keys(), row)) for row in result.fetchall()]

    async def get_summary(self, session: AsyncSession, year: int | None = None) -> dict:
        yf = ""
        params: dict = {}
        if year:
            yf = "AND EXTRACT(YEAR FROM executed_at) = :year"
            params["year"] = year
        yf_dep = yf.replace("executed_at", "confirmed_at")

        r = await session.execute(text(
            f"SELECT exchange, COUNT(*), COALESCE(SUM(total_usd),0)::text, COALESCE(SUM(fee_usd),0)::text "
            f"FROM tax.trades WHERE 1=1 {yf} GROUP BY exchange"), params)
        trades = {row[0]: {"count": row[1], "volume_usd": row[2], "fees_usd": row[3]} for row in r.fetchall()}

        r = await session.execute(text(
            f"SELECT exchange, COUNT(*), COALESCE(SUM(amount_usd),0)::text "
            f"FROM tax.deposits WHERE 1=1 {yf_dep} GROUP BY exchange"), params)
        deps = {row[0]: {"count": row[1], "total_usd": row[2]} for row in r.fetchall()}

        r = await session.execute(text(
            f"SELECT exchange, COUNT(*), COALESCE(SUM(amount_usd),0)::text, COALESCE(SUM(fee_usd),0)::text "
            f"FROM tax.withdrawals WHERE 1=1 {yf_dep} GROUP BY exchange"), params)
        wds = {row[0]: {"count": row[1], "total_usd": row[2], "fees_usd": row[3]} for row in r.fetchall()}

        r = await session.execute(text("SELECT COUNT(*) FROM tax.trades WHERE total_usd IS NULL"))
        t_miss = r.scalar() or 0
        r = await session.execute(text("SELECT COUNT(*) FROM tax.deposits WHERE amount_usd IS NULL"))
        d_miss = r.scalar() or 0
        r = await session.execute(text("SELECT COUNT(*) FROM tax.withdrawals WHERE amount_usd IS NULL"))
        w_miss = r.scalar() or 0

        return {
            "year": year or "all", "trades": trades, "deposits": deps, "withdrawals": wds,
            "missing_usd": {"trades": t_miss, "deposits": d_miss, "withdrawals": w_miss},
        }
