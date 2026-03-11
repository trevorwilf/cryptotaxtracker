"""
Transfer Matcher v4 — Lot-Slice Relocation.

Instead of creating synthetic new lots, this module RELOCATES existing
lot slices from the source wallet to the destination wallet, preserving:
  - original_acquired_at (holding period preserved)
  - cost_per_unit_usd (basis preserved)
  - parent_lot_id (full lineage tracking)

Addresses reviewer Issue 2: transfers must preserve lot identity.
Also fixes v3 bug: same-exchange transfers ARE now supported.
"""
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from exceptions import ExceptionManager, WARNING, INFO
from exceptions import UNMATCHED_TRANSFER

logger = logging.getLogger("tax-collector.transfer-matcher-v4")

D = Decimal
ZERO = D("0")


class TransferMatcherV4:
    """Match withdrawal→deposit pairs and relocate lot slices."""

    def __init__(self, time_window_hours: int = 48,
                 fee_tolerance_pct: float = 5.0):
        self.time_window = timedelta(hours=time_window_hours)
        self.fee_tolerance = D(str(fee_tolerance_pct / 100))

    async def match_and_relocate(self, session: AsyncSession,
                                 exc: ExceptionManager,
                                 run_id: int) -> dict:
        """
        Match withdrawal→deposit pairs from normalized events,
        then relocate lot slices for each match.
        """
        stats = {"matched_pairs": 0, "lots_relocated": 0,
                 "unmatched_withdrawals": 0, "unmatched_deposits": 0}

        # Get unresolved withdrawal events
        wd_result = await session.execute(text("""
            SELECT ne.id, ne.wallet, ne.asset, ne.quantity::text, ne.event_at,
                   w.tx_hash, w.fee::text, w.fee_asset, ne.source_withdrawal_id
            FROM tax.normalized_events ne
            LEFT JOIN tax.withdrawals w ON w.id = ne.source_withdrawal_id
            WHERE ne.event_type = 'UNRESOLVED'
              AND ne.source_withdrawal_id IS NOT NULL
              AND ne.run_id = :run_id
            ORDER BY ne.event_at ASC, ne.id ASC
        """), {"run_id": run_id})
        withdrawals = [dict(zip(wd_result.keys(), row)) for row in wd_result.fetchall()]

        # Get unresolved deposit events
        dep_result = await session.execute(text("""
            SELECT ne.id, ne.wallet, ne.asset, ne.quantity::text, ne.event_at,
                   d.tx_hash, ne.source_deposit_id
            FROM tax.normalized_events ne
            LEFT JOIN tax.deposits d ON d.id = ne.source_deposit_id
            WHERE ne.event_type = 'UNRESOLVED'
              AND ne.source_deposit_id IS NOT NULL
              AND ne.run_id = :run_id
            ORDER BY ne.event_at ASC, ne.id ASC
        """), {"run_id": run_id})
        deposits = [dict(zip(dep_result.keys(), row)) for row in dep_result.fetchall()]

        matched_dep_ids = set()
        matched_wd_ids = set()

        for wd in withdrawals:
            if wd["id"] in matched_wd_ids:
                continue

            best_match = None
            best_confidence = None

            for dep in deposits:
                if dep["id"] in matched_dep_ids:
                    continue

                confidence = self._check_match(wd, dep)
                if confidence is None:
                    continue

                # TX hash match is highest confidence
                if confidence == "tx_hash":
                    best_match = dep
                    best_confidence = confidence
                    break

                if best_match is None:
                    best_match = dep
                    best_confidence = confidence

            if best_match:
                # Relocate lot slices
                relocated = await self._relocate_lots(
                    session, wd, best_match, best_confidence, exc, run_id)

                # Reclassify events
                await session.execute(text("""
                    UPDATE tax.normalized_events
                    SET event_type = 'TRANSFER_OUT',
                        classification_rule = :rule
                    WHERE id = :id
                """), {"id": wd["id"],
                       "rule": f"matched to deposit {best_match['id']} ({best_confidence})"})

                await session.execute(text("""
                    UPDATE tax.normalized_events
                    SET event_type = 'TRANSFER_IN',
                        classification_rule = :rule
                    WHERE id = :id
                """), {"id": best_match["id"],
                       "rule": f"matched to withdrawal {wd['id']} ({best_confidence})"})

                matched_wd_ids.add(wd["id"])
                matched_dep_ids.add(best_match["id"])
                stats["matched_pairs"] += 1
                stats["lots_relocated"] += relocated

        # Log unmatched
        for wd in withdrawals:
            if wd["id"] not in matched_wd_ids:
                stats["unmatched_withdrawals"] += 1
                exc.log(WARNING, UNMATCHED_TRANSFER,
                        f"Withdrawal on {wd['wallet']}: {wd['quantity']} {wd['asset']} — no matching deposit found",
                        source_withdrawal_id=wd.get("source_withdrawal_id"),
                        source_event_id=wd["id"], run_id=run_id)

        for dep in deposits:
            if dep["id"] not in matched_dep_ids:
                stats["unmatched_deposits"] += 1
                # Don't log here — income_classifier_v4 will handle unmatched deposits

        logger.info(f"Transfer matching complete: {stats}")
        return stats

    def _check_match(self, wd: dict, dep: dict) -> str | None:
        """Check if a withdrawal-deposit pair matches. Returns confidence or None."""
        # Same asset required
        if dep["asset"] != wd["asset"]:
            return None

        # Deposit must be after withdrawal
        if dep["event_at"] < wd["event_at"]:
            return None

        # Within time window
        if (dep["event_at"] - wd["event_at"]) > self.time_window:
            return None

        # Amount within fee tolerance
        wd_amount = D(str(wd["quantity"]))
        wd_fee = D(str(wd.get("fee") or 0))
        wd_net = wd_amount - wd_fee
        dep_amount = D(str(dep["quantity"]))

        if wd_net > 0:
            diff_pct = abs(dep_amount - wd_net) / wd_net
        elif wd_amount > 0:
            diff_pct = abs(dep_amount - wd_amount) / wd_amount
        else:
            return None

        if diff_pct > self.fee_tolerance:
            return None

        # TX hash match = high confidence
        if (wd.get("tx_hash") and dep.get("tx_hash")
                and wd["tx_hash"] == dep["tx_hash"]):
            return "tx_hash"

        return "amount_timing"

    async def _relocate_lots(self, session: AsyncSession,
                             wd: dict, dep: dict,
                             confidence: str,
                             exc: ExceptionManager,
                             run_id: int) -> int:
        """
        Relocate lot slices from source wallet to destination wallet.
        Consumes lots in FIFO order and creates new lots on the dest wallet.
        Returns count of lots relocated.
        """
        source_wallet = wd["wallet"]
        dest_wallet = dep["wallet"]
        asset = wd["asset"]
        transfer_amount = D(str(dep["quantity"]))
        wd_fee = D(str(wd.get("fee") or 0))
        remaining_to_transfer = transfer_amount
        relocated = 0

        # Find lots on source wallet in FIFO order
        lot_result = await session.execute(text("""
            SELECT id, original_quantity::text, remaining::text,
                   cost_per_unit_usd::text, original_acquired_at,
                   source_type
            FROM tax.lots_v4
            WHERE wallet = :wallet AND asset = :asset AND remaining > 0
            ORDER BY original_acquired_at ASC, id ASC
        """), {"wallet": source_wallet, "asset": asset})
        lots = [dict(zip(lot_result.keys(), row)) for row in lot_result.fetchall()]

        for lot in lots:
            if remaining_to_transfer <= ZERO:
                break

            lot_remaining = D(lot["remaining"])
            consume = min(lot_remaining, remaining_to_transfer)
            cost_per_unit = D(lot["cost_per_unit_usd"]) if lot["cost_per_unit_usd"] else None

            # Reduce source lot
            new_remaining = lot_remaining - consume
            await session.execute(text("""
                UPDATE tax.lots_v4
                SET remaining = :remaining,
                    is_depleted = :depleted
                WHERE id = :id
            """), {"remaining": str(new_remaining),
                   "depleted": new_remaining <= ZERO,
                   "id": lot["id"]})

            # Create transfer_carryover record
            carryover_basis = (cost_per_unit * consume) if cost_per_unit else None
            co_result = await session.execute(text("""
                INSERT INTO tax.transfer_carryover
                    (asset, quantity, source_wallet, source_lot_id, source_event_id,
                     dest_wallet, dest_event_id,
                     original_acquired_at, carryover_basis_usd, cost_per_unit_usd,
                     transferred_at, tx_hash, transfer_fee, transfer_fee_asset,
                     withdrawal_id, deposit_id, match_confidence, run_id)
                VALUES
                    (:asset, :qty, :sw, :slid, :seid,
                     :dw, :deid,
                     :oaa, :cb, :cpu,
                     :ta, :txh, :tf, :tfa,
                     :wid, :did, :mc, :rid)
                RETURNING id
            """), {
                "asset": asset, "qty": str(consume),
                "sw": source_wallet, "slid": lot["id"],
                "seid": wd["id"], "dw": dest_wallet, "deid": dep["id"],
                "oaa": lot["original_acquired_at"],
                "cb": str(carryover_basis) if carryover_basis else None,
                "cpu": lot["cost_per_unit_usd"],
                "ta": wd["event_at"],
                "txh": wd.get("tx_hash"),
                "tf": str(wd_fee) if wd_fee > ZERO else None,
                "tfa": asset if wd_fee > ZERO else None,
                "wid": wd.get("source_withdrawal_id"),
                "did": dep.get("source_deposit_id"),
                "mc": confidence, "rid": run_id,
            })
            co_row = co_result.fetchone()
            co_id = co_row[0] if co_row else None

            # Create new lot on destination wallet — PRESERVING original_acquired_at
            await session.execute(text("""
                INSERT INTO tax.lots_v4
                    (asset, wallet, original_quantity, remaining,
                     cost_per_unit_usd, total_cost_usd,
                     original_acquired_at, lot_created_at,
                     source_event_id, source_type,
                     parent_lot_id, transfer_carryover_id, run_id)
                VALUES
                    (:asset, :wallet, :qty, :remaining,
                     :cpu, :tc,
                     :oaa, NOW(),
                     :seid, 'transfer_in',
                     :plid, :tcid, :rid)
            """), {
                "asset": asset, "wallet": dest_wallet,
                "qty": str(consume), "remaining": str(consume),
                "cpu": lot["cost_per_unit_usd"],
                "tc": str(carryover_basis) if carryover_basis else None,
                "oaa": lot["original_acquired_at"],
                "seid": dep["id"], "plid": lot["id"],
                "tcid": co_id, "rid": run_id,
            })

            remaining_to_transfer -= consume
            relocated += 1

        if remaining_to_transfer > ZERO:
            exc.log(WARNING, UNMATCHED_TRANSFER,
                    f"Transfer of {asset}: only partially covered by existing lots "
                    f"(shortfall: {remaining_to_transfer})",
                    source_event_id=wd["id"], run_id=run_id)

        return relocated
