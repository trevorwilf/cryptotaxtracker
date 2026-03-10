"""
Transfer Matcher — pairs cross-exchange withdrawals with deposits.

A withdrawal from exchange A followed by a deposit to exchange B of the
same asset within a configurable time window is a non-taxable transfer.
The cost basis from the original lots carries over.

Matching criteria:
  1. Same asset
  2. Deposit amount ≈ withdrawal amount (within fee tolerance)
  3. Deposit occurs AFTER the withdrawal
  4. Time gap < configurable window (default 48 hours)
  5. Neither record is already matched
"""
import logging
from datetime import timedelta
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("tax-collector.transfer-matcher")

D = Decimal
ZERO = D("0")


class TransferMatcher:
    def __init__(self, time_window_hours: int = 48, fee_tolerance_pct: float = 5.0):
        self.time_window = timedelta(hours=time_window_hours)
        self.fee_tolerance = D(str(fee_tolerance_pct / 100))

    async def match_transfers(self, session: AsyncSession) -> dict:
        """Find and record matching withdrawal→deposit pairs."""
        logger.info(f"Starting transfer matching (window={self.time_window}, tolerance={self.fee_tolerance*100}%)")

        # Clear previous matches
        await session.execute(text("DELETE FROM tax.transfer_matches"))

        # Load unmatched withdrawals
        wd_result = await session.execute(text("""
            SELECT id, exchange, asset, amount, fee, confirmed_at, tx_hash
            FROM tax.withdrawals
            WHERE confirmed_at IS NOT NULL AND amount > 0
            ORDER BY confirmed_at ASC
        """))
        withdrawals = [dict(zip(wd_result.keys(), row)) for row in wd_result.fetchall()]

        # Load unmatched deposits
        dep_result = await session.execute(text("""
            SELECT id, exchange, asset, amount, confirmed_at, tx_hash
            FROM tax.deposits
            WHERE confirmed_at IS NOT NULL AND amount > 0
            ORDER BY confirmed_at ASC
        """))
        deposits = [dict(zip(dep_result.keys(), row)) for row in dep_result.fetchall()]

        matched_wd_ids = set()
        matched_dep_ids = set()
        matches = []

        for wd in withdrawals:
            if wd["id"] in matched_wd_ids:
                continue

            wd_asset = wd["asset"]
            wd_amount = D(str(wd["amount"]))
            wd_fee = D(str(wd["fee"] or 0))
            wd_net = wd_amount - wd_fee
            wd_time = wd["confirmed_at"]
            wd_tx = wd.get("tx_hash", "")

            for dep in deposits:
                if dep["id"] in matched_dep_ids:
                    continue
                if dep["asset"] != wd_asset:
                    continue
                if dep["exchange"] == wd["exchange"]:
                    continue  # same exchange = not a transfer

                dep_amount = D(str(dep["amount"]))
                dep_time = dep["confirmed_at"]

                # Must be after withdrawal
                if dep_time < wd_time:
                    continue

                # Within time window
                if (dep_time - wd_time) > self.time_window:
                    continue

                # Amount check: deposit should be close to withdrawal minus fee
                if wd_net > 0:
                    diff_pct = abs(dep_amount - wd_net) / wd_net
                else:
                    diff_pct = abs(dep_amount - wd_amount) / wd_amount if wd_amount > 0 else D("1")

                if diff_pct > self.fee_tolerance:
                    continue

                # TX hash match (strongest signal if available)
                tx_match = bool(wd_tx and dep.get("tx_hash") and wd_tx == dep.get("tx_hash"))

                # Match found
                matches.append({
                    "withdrawal_id": wd["id"],
                    "deposit_id": dep["id"],
                    "asset": wd_asset,
                    "amount": str(wd_net if wd_net > 0 else wd_amount),
                    "from_exchange": wd["exchange"],
                    "to_exchange": dep["exchange"],
                    "transferred_at": wd_time,
                    "tx_hash": wd_tx or dep.get("tx_hash", ""),
                    "match_confidence": "high" if tx_match else "medium",
                    "cost_basis_usd": None,  # will be filled by tax engine from lots
                })

                matched_wd_ids.add(wd["id"])
                matched_dep_ids.add(dep["id"])
                break  # move to next withdrawal

        # Save matches
        for m in matches:
            await session.execute(text("""
                INSERT INTO tax.transfer_matches
                    (withdrawal_id, deposit_id, asset, amount, from_exchange, to_exchange,
                     transferred_at, tx_hash, match_confidence, cost_basis_usd)
                VALUES
                    (:withdrawal_id, :deposit_id, :asset, :amount, :from_exchange, :to_exchange,
                     :transferred_at, :tx_hash, :match_confidence, :cost_basis_usd)
            """), m)

        await session.commit()
        logger.info(f"Transfer matching complete: {len(matches)} pairs matched")

        return {
            "matched_pairs": len(matches),
            "unmatched_withdrawals": len(withdrawals) - len(matched_wd_ids),
            "unmatched_deposits": len(deposits) - len(matched_dep_ids),
            "matches": [
                {"asset": m["asset"], "amount": m["amount"],
                 "from": m["from_exchange"], "to": m["to_exchange"],
                 "confidence": m["match_confidence"]}
                for m in matches
            ],
        }

    async def get_unmatched(self, session: AsyncSession) -> dict:
        """Get withdrawals and deposits that weren't matched."""
        wd = await session.execute(text("""
            SELECT w.id, w.exchange, w.asset, w.amount::text, w.confirmed_at
            FROM tax.withdrawals w
            WHERE w.id NOT IN (SELECT withdrawal_id FROM tax.transfer_matches WHERE withdrawal_id IS NOT NULL)
              AND w.amount > 0
            ORDER BY w.confirmed_at
        """))
        dep = await session.execute(text("""
            SELECT d.id, d.exchange, d.asset, d.amount::text, d.confirmed_at
            FROM tax.deposits d
            WHERE d.id NOT IN (SELECT deposit_id FROM tax.transfer_matches WHERE deposit_id IS NOT NULL)
              AND d.amount > 0
            ORDER BY d.confirmed_at
        """))
        return {
            "unmatched_withdrawals": [dict(zip(wd.keys(), row)) for row in wd.fetchall()],
            "unmatched_deposits": [dict(zip(dep.keys(), row)) for row in dep.fetchall()],
        }
