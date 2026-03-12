"""
Income Classifier — identifies staking rewards, airdrops, and other
events that are taxed as ordinary income (not capital gains).

IRS rules:
  - Staking rewards: ordinary income at FMV when received
  - Airdrops: ordinary income at FMV when you gain control
  - Mining: ordinary income at FMV when mined

Detection heuristics:
  1. Deposits with no matching withdrawal (not a transfer) and no trade
     context → possible airdrop/staking
  2. Exchange-specific API fields that identify staking rewards
  3. Small, periodic deposits of the same asset → likely staking
  4. Manual classification via the API
"""
import logging
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("tax-collector.income-classifier")

D = Decimal
ZERO = D("0")

# Assets commonly staked (extend as needed)
KNOWN_STAKING_ASSETS = {
    "ETH", "SOL", "ADA", "DOT", "ATOM", "MATIC", "AVAX", "NEAR",
    "XTZ", "ALGO", "FTM", "ONE", "HBAR", "VET", "EGLD", "MINA",
    "OSMO", "JUNO", "SCRT", "ROSE", "CRO", "BNB", "TRX",
}


class IncomeClassifier:

    async def classify(self, session: AsyncSession) -> dict:
        """Scan deposits and pool activity for income events."""
        logger.info("Starting income classification")

        # Clear previous classifications
        await session.execute(text("DELETE FROM tax.income_events"))

        events = []

        # 1. Find deposits that aren't transfers and look like staking/airdrops
        await self._classify_deposits(session, events)

        # 2. Find pool rewards
        await self._classify_pool_rewards(session, events)

        # Save events
        for e in events:
            await session.execute(text("""
                INSERT INTO tax.income_events
                    (exchange, asset, amount, amount_usd, income_type,
                     received_at, description, deposit_id, pool_activity_id)
                VALUES
                    (:exchange, :asset, :amount, :amount_usd, :income_type,
                     :received_at, :description, :deposit_id, :pool_activity_id)
            """), e)

        await session.commit()

        # Summary
        by_type = defaultdict(lambda: {"count": 0, "total_usd": ZERO})
        for e in events:
            t = e["income_type"]
            by_type[t]["count"] += 1
            by_type[t]["total_usd"] += D(str(e["amount_usd"] or 0))

        summary = {
            "total_events": len(events),
            "by_type": {k: {"count": v["count"], "total_usd": str(v["total_usd"])}
                        for k, v in by_type.items()},
        }
        logger.info(f"Income classification complete: {summary}")
        return summary

    async def _classify_deposits(self, session: AsyncSession, events: list):
        """Find deposits that look like staking rewards or airdrops."""
        # Get deposits not matched as transfers
        result = await session.execute(text("""
            SELECT d.id, d.exchange, d.asset, d.amount, d.amount_usd, d.confirmed_at, d.tx_hash
            FROM tax.deposits d
            WHERE d.id NOT IN (
                SELECT deposit_id FROM tax.transfer_matches WHERE deposit_id IS NOT NULL
            )
            AND d.amount > 0
            ORDER BY d.confirmed_at ASC, d.id ASC
        """))
        deposits = [dict(zip(result.keys(), row)) for row in result.fetchall()]

        # Group by asset to detect periodic patterns (staking)
        by_asset: dict[str, list] = defaultdict(list)
        for d in deposits:
            by_asset[d["asset"]].append(d)

        for asset, deps in by_asset.items():
            if len(deps) >= 3:
                # Multiple deposits of same asset, no matching trades = likely staking
                income_type = "staking" if asset.upper() in KNOWN_STAKING_ASSETS else "airdrop_or_reward"

                for d in deps:
                    events.append({
                        "exchange": d["exchange"],
                        "asset": d["asset"],
                        "amount": str(d["amount"]),
                        "amount_usd": str(d["amount_usd"] or 0),
                        "income_type": income_type,
                        "received_at": d["confirmed_at"],
                        "description": f"{income_type.replace('_', ' ').title()}: "
                                       f"{d['amount']} {d['asset']} on {d['exchange']}",
                        "deposit_id": d["id"],
                        "pool_activity_id": None,
                    })
            elif len(deps) == 1:
                # Single deposit, no transfer match — could be airdrop
                d = deps[0]
                events.append({
                    "exchange": d["exchange"],
                    "asset": d["asset"],
                    "amount": str(d["amount"]),
                    "amount_usd": str(d["amount_usd"] or 0),
                    "income_type": "deposit_unclassified",
                    "received_at": d["confirmed_at"],
                    "description": f"Unclassified deposit: {d['amount']} {d['asset']} on {d['exchange']}",
                    "deposit_id": d["id"],
                    "pool_activity_id": None,
                })

    async def _classify_pool_rewards(self, session: AsyncSession, events: list):
        """Identify pool reward/yield events."""
        result = await session.execute(text("""
            SELECT id, exchange, pool_name, asset_out, amount_out, amount_out_usd, executed_at
            FROM tax.pool_activity
            WHERE action = 'reward' AND amount_out > 0
            ORDER BY executed_at ASC, id ASC
        """))
        for row in result.fetchall():
            events.append({
                "exchange": row[1],
                "asset": row[3],
                "amount": str(row[4]),
                "amount_usd": str(row[5] or 0),
                "income_type": "pool_reward",
                "received_at": row[6],
                "description": f"Pool reward: {row[4]} {row[3]} from {row[2]}",
                "deposit_id": None,
                "pool_activity_id": row[0],
            })

    async def get_income_summary(self, session: AsyncSession, year: int | None = None) -> dict:
        """Get income summary for tax reporting."""
        yf = ""
        params: dict = {}
        if year:
            yf = "AND EXTRACT(YEAR FROM received_at) = :year"
            params["year"] = year

        result = await session.execute(text(f"""
            SELECT income_type, COUNT(*), COALESCE(SUM(amount_usd), 0)::text
            FROM tax.income_events
            WHERE 1=1 {yf}
            GROUP BY income_type
            ORDER BY income_type
        """), params)

        types = {}
        total_usd = ZERO
        total_count = 0
        for row in result.fetchall():
            types[row[0]] = {"count": row[1], "total_usd": row[2]}
            total_usd += D(row[2])
            total_count += row[1]

        # Get individual events for the schedule
        events_result = await session.execute(text(f"""
            SELECT exchange, asset, amount::text, amount_usd::text, income_type,
                   received_at, description
            FROM tax.income_events
            WHERE 1=1 {yf}
            ORDER BY received_at ASC, id ASC
        """), params)
        events = [dict(zip(events_result.keys(), row)) for row in events_result.fetchall()]

        return {
            "year": year or "all",
            "total_income_events": total_count,
            "total_income_usd": str(total_usd),
            "by_type": types,
            "events": events,
        }
