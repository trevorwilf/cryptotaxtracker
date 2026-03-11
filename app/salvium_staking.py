"""
Salvium Staking Income Detector.

Matches staking lock/unlock pairs and computes taxable yield.

Tax rules (IRS Rev. Rul. 2023-14):
  - Staking rewards are ordinary income when dominion and control is gained
  - For Salvium: dominion = when the staking unlock occurs (coins become spendable)
  - FMV at the TIME of unlock, not the time of lock
  - The yield creates a new cost basis lot: basis = FMV at receipt
  - The original staked SAL retains its original cost basis (unchanged)
  - The lock itself is NOT a taxable event (you still own the SAL)
  - The return of principal is NOT income (only the yield is)

How Salvium staking works:
  - Lock:   user sends SAL to staking (outgoing tx with unlock_time > 0, ~21,600 blocks)
  - Unlock: after ~30 days, protocol mints stake + yield back to wallet (incoming tx)
  - Yield = unlock_amount - lock_amount (the difference is taxable income)
"""
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from price_oracle import PriceOracle

logger = logging.getLogger("tax-collector.salvium-staking")

D = Decimal

# ~21,600 blocks is the standard Salvium staking period (~30 days)
STAKING_BLOCK_WINDOW = 21600
# Allow some tolerance for matching (blocks can vary slightly)
BLOCK_TOLERANCE = 2000


class SalviumStakingTracker:
    """Detects staking lock/unlock pairs and computes taxable yield."""

    def __init__(self):
        self._oracle = PriceOracle()

    async def scan_and_match(self, session: AsyncSession) -> dict:
        """Scan deposits/withdrawals for staking lock/unlock pairs.

        Returns summary of matched/unmatched stakes.
        """
        locks = await self._find_staking_locks(session)
        unlocks = await self._find_staking_unlocks(session)

        matched = 0
        new_income = 0
        unmatched_locks = 0

        for lock in locks:
            # Check if this lock already has a record
            existing = await self._get_existing_stake(session, lock["tx_hash"])
            if existing and existing["status"] == "unlocked":
                continue  # Already matched

            # Try to find a matching unlock
            unlock = self._find_matching_unlock(lock, unlocks)

            if unlock:
                yield_amount = D(unlock["amount"]) - D(lock["amount"])
                if yield_amount < 0:
                    yield_amount = D("0")

                # Get SAL/USD price at unlock time
                sal_price = await self._oracle.get_usd_price(
                    session, "SAL", unlock["confirmed_at"]
                )
                yield_usd = yield_amount * sal_price if sal_price else None

                await self._upsert_stake(session, {
                    "lock_tx_hash": lock["tx_hash"],
                    "lock_amount": lock["amount"],
                    "lock_height": lock["height"],
                    "lock_at": lock["confirmed_at"],
                    "unlock_tx_hash": unlock["tx_hash"],
                    "unlock_amount": unlock["amount"],
                    "unlock_height": unlock["height"],
                    "unlock_at": unlock["confirmed_at"],
                    "yield_amount": str(yield_amount),
                    "yield_usd": str(yield_usd) if yield_usd else None,
                    "sal_price_usd": str(sal_price) if sal_price else None,
                    "status": "unlocked",
                })
                matched += 1

                # Create income event for the yield portion
                if yield_amount > 0 and yield_usd:
                    await self._record_yield_income(
                        session, unlock, yield_amount, yield_usd, sal_price
                    )
                    new_income += 1

                # Remove from unlocks pool so it can't be double-matched
                unlocks = [u for u in unlocks if u["tx_hash"] != unlock["tx_hash"]]
            else:
                # No matching unlock yet — record as locked
                if not existing:
                    await self._upsert_stake(session, {
                        "lock_tx_hash": lock["tx_hash"],
                        "lock_amount": lock["amount"],
                        "lock_height": lock["height"],
                        "lock_at": lock["confirmed_at"],
                        "unlock_tx_hash": None,
                        "unlock_amount": None,
                        "unlock_height": None,
                        "unlock_at": None,
                        "yield_amount": None,
                        "yield_usd": None,
                        "sal_price_usd": None,
                        "status": "locked",
                    })
                unmatched_locks += 1

        await session.commit()

        return {
            "locks_found": len(locks),
            "unlocks_found": len(unlocks),
            "matched_pairs": matched,
            "new_income_events": new_income,
            "unmatched_locks": unmatched_locks,
        }

    async def _find_staking_locks(self, session: AsyncSession) -> list[dict]:
        """Find all outgoing transactions tagged as staking locks."""
        result = await session.execute(text("""
            SELECT id, exchange_id, asset, amount::text, fee::text, tx_hash,
                   confirmed_at, raw_data
            FROM tax.withdrawals
            WHERE exchange = 'salvium'
            ORDER BY confirmed_at ASC
        """))
        locks = []
        for row in result.fetchall():
            row_dict = dict(zip(result.keys(), row))
            raw = row_dict.get("raw_data")
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    raw = {}
            elif raw is None:
                raw = {}

            subtype = raw.get("_salvium_subtype", "")
            if subtype == "staking_lock":
                row_dict["height"] = raw.get("_salvium_height", 0)
                row_dict["unlock_time"] = raw.get("_salvium_unlock_time", 0)
                locks.append(row_dict)

        return locks

    async def _find_staking_unlocks(self, session: AsyncSession) -> list[dict]:
        """Find all incoming transactions that could be staking unlocks."""
        result = await session.execute(text("""
            SELECT id, exchange_id, asset, amount::text, tx_hash,
                   confirmed_at, raw_data
            FROM tax.deposits
            WHERE exchange = 'salvium'
            ORDER BY confirmed_at ASC
        """))
        unlocks = []
        for row in result.fetchall():
            row_dict = dict(zip(result.keys(), row))
            raw = row_dict.get("raw_data")
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    raw = {}
            elif raw is None:
                raw = {}

            subtype = raw.get("_salvium_subtype", "")
            # Staking unlock candidates or incoming transfers with unlock_time
            if subtype in ("staking_unlock_candidate", "incoming"):
                height = raw.get("_salvium_height", 0)
                is_coinbase = raw.get("_salvium_coinbase", False)
                if not is_coinbase:  # Mining rewards are not staking unlocks
                    row_dict["height"] = height
                    unlocks.append(row_dict)

        return unlocks

    def _find_matching_unlock(self, lock: dict, unlocks: list[dict]) -> dict | None:
        """Find the unlock that matches a given lock by timing and amount.

        Matching criteria:
          1. Unlock height > lock height
          2. Height difference is within the staking window (~21,600 blocks +/- tolerance)
          3. Unlock amount >= lock amount (must include at least the principal)
        """
        lock_height = lock.get("height", 0)
        lock_amount = D(lock["amount"])

        best_match = None
        best_height_diff = float("inf")

        for unlock in unlocks:
            unlock_height = unlock.get("height", 0)

            # Unlock must come after lock
            if unlock_height <= lock_height:
                continue

            height_diff = unlock_height - lock_height

            # Must be within the staking window (with tolerance)
            if height_diff > STAKING_BLOCK_WINDOW + BLOCK_TOLERANCE:
                continue

            unlock_amount = D(unlock["amount"])
            # Unlock amount must be >= lock amount (principal + yield)
            if unlock_amount < lock_amount:
                continue

            # Pick the closest match by height difference to expected staking period
            diff_from_expected = abs(height_diff - STAKING_BLOCK_WINDOW)
            if diff_from_expected < best_height_diff:
                best_height_diff = diff_from_expected
                best_match = unlock

        return best_match

    async def _get_existing_stake(self, session: AsyncSession,
                                   lock_tx_hash: str) -> dict | None:
        """Check if a stake record already exists for this lock."""
        result = await session.execute(text("""
            SELECT id, status FROM tax.salvium_stakes
            WHERE lock_tx_hash = :hash
        """), {"hash": lock_tx_hash})
        row = result.fetchone()
        if row:
            return {"id": row[0], "status": row[1]}
        return None

    async def _upsert_stake(self, session: AsyncSession, stake: dict):
        """Insert or update a stake record."""
        await session.execute(text("""
            INSERT INTO tax.salvium_stakes
                (lock_tx_hash, lock_amount, lock_height, lock_at,
                 unlock_tx_hash, unlock_amount, unlock_height, unlock_at,
                 yield_amount, yield_usd, sal_price_usd, status)
            VALUES
                (:lock_tx_hash, :lock_amount, :lock_height, :lock_at,
                 :unlock_tx_hash, :unlock_amount, :unlock_height, :unlock_at,
                 :yield_amount, :yield_usd, :sal_price_usd, :status)
            ON CONFLICT (lock_tx_hash) DO UPDATE SET
                unlock_tx_hash = EXCLUDED.unlock_tx_hash,
                unlock_amount = EXCLUDED.unlock_amount,
                unlock_height = EXCLUDED.unlock_height,
                unlock_at = EXCLUDED.unlock_at,
                yield_amount = EXCLUDED.yield_amount,
                yield_usd = EXCLUDED.yield_usd,
                sal_price_usd = EXCLUDED.sal_price_usd,
                status = EXCLUDED.status
        """), stake)

    async def _record_yield_income(self, session: AsyncSession, unlock: dict,
                                    yield_amount: Decimal, yield_usd: Decimal,
                                    sal_price: Decimal):
        """Record the staking yield as an ordinary income event."""
        await session.execute(text("""
            INSERT INTO tax.income_events_v4
                (wallet, asset, quantity, fmv_per_unit_usd, total_fmv_usd,
                 income_type, classification_source, review_status, dominion_at)
            VALUES
                ('salvium', 'SAL', :qty, :fmv, :total,
                 'staking', 'salvium_staking_tracker', 'pending', :at)
        """), {
            "qty": str(yield_amount),
            "fmv": str(sal_price),
            "total": str(yield_usd),
            "at": unlock["confirmed_at"],
        })

    # ── Query methods for API endpoints ─────────────────────────────────

    async def get_stakes(self, session: AsyncSession) -> list[dict]:
        """Get all staking lock/unlock pairs."""
        result = await session.execute(text("""
            SELECT id, lock_tx_hash, lock_amount::text, lock_height, lock_at,
                   unlock_tx_hash, unlock_amount::text, unlock_height, unlock_at,
                   yield_amount::text, yield_usd::text, sal_price_usd::text,
                   status, income_event_id, created_at
            FROM tax.salvium_stakes
            ORDER BY lock_at DESC
        """))
        return [dict(zip(result.keys(), row)) for row in result.fetchall()]

    async def get_income(self, session: AsyncSession,
                         year: int | None = None) -> list[dict]:
        """Get staking income events for a tax year."""
        where = "WHERE income_type = 'staking' AND wallet = 'salvium'"
        params: dict = {}
        if year:
            where += " AND EXTRACT(YEAR FROM dominion_at) = :year"
            params["year"] = year

        result = await session.execute(text(f"""
            SELECT id, wallet, asset, quantity::text, fmv_per_unit_usd::text,
                   total_fmv_usd::text, income_type, classification_source,
                   review_status, dominion_at
            FROM tax.income_events_v4
            {where}
            ORDER BY dominion_at ASC
        """), params)
        return [dict(zip(result.keys(), row)) for row in result.fetchall()]

    async def get_status(self, session: AsyncSession) -> dict:
        """Get Salvium wallet status summary."""
        # Stake summary from DB
        r = await session.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'locked') AS active_locks,
                COUNT(*) FILTER (WHERE status = 'unlocked') AS completed,
                COALESCE(SUM(lock_amount) FILTER (WHERE status = 'locked'), 0)::text AS locked_sal,
                COALESCE(SUM(yield_amount) FILTER (WHERE status = 'unlocked'), 0)::text AS total_yield_sal,
                COALESCE(SUM(yield_usd) FILTER (WHERE status = 'unlocked'), 0)::text AS total_yield_usd
            FROM tax.salvium_stakes
        """))
        row = r.fetchone()

        # Wallet transaction counts
        r2 = await session.execute(text("""
            SELECT
                (SELECT COUNT(*) FROM tax.deposits WHERE exchange = 'salvium') AS deposits,
                (SELECT COUNT(*) FROM tax.withdrawals WHERE exchange = 'salvium') AS withdrawals
        """))
        tx_row = r2.fetchone()

        return {
            "active_locks": row[0] if row else 0,
            "completed_stakes": row[1] if row else 0,
            "locked_sal": row[2] if row else "0",
            "total_yield_sal": row[3] if row else "0",
            "total_yield_usd": row[4] if row else "0",
            "total_deposits": tx_row[0] if tx_row else 0,
            "total_withdrawals": tx_row[1] if tx_row else 0,
        }
