"""
Income Classifier v4 — Evidence-Based Classification.

Key change from v3: NO automatic classification by deposit count or
hardcoded asset lists. Everything goes to an exception queue.

Process:
  1. Find UNRESOLVED deposit events not matched as transfers
  2. Exchange-tagged → INCOME with classification_source='exchange_api', review_status='pending'
  3. Not tagged → leave UNRESOLVED, log AMBIGUOUS_DEPOSIT WARNING
  4. Pool rewards (already INCOME from ledger) → create income_events_v4
  5. All income needs manual confirmation before filing-ready
  6. When confirmed → create acquisition lot at FMV

Addresses reviewer Issue 4: heuristic classification replaced with
evidence-based approach + manual review queue.
"""
import logging
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from exceptions import ExceptionManager, WARNING, INFO
from exceptions import AMBIGUOUS_DEPOSIT
from valuation_v4 import ValuationV4

logger = logging.getLogger("tax-collector.income-v4")

D = Decimal
ZERO = D("0")

# Exchange API tags that indicate income
INCOME_TAGS = {
    "staking", "stake_reward", "staking_reward",
    "reward", "mining", "airdrop", "interest",
    "referral", "bonus", "cashback", "rebate",
    "distribution", "dividend", "yield",
}


class IncomeClassifierV4:
    """Evidence-based income classification with manual review queue."""

    def __init__(self, exc_manager: ExceptionManager,
                 valuation: ValuationV4):
        self.exc = exc_manager
        self.valuation = valuation

    async def classify(self, session: AsyncSession, run_id: int) -> dict:
        """
        Classify unresolved deposits and pool rewards as income.
        Returns stats dict.
        """
        stats = {"exchange_tagged": 0, "pool_rewards": 0,
                 "unclassified": 0, "income_events_created": 0}

        # 1. Process unresolved deposits (not matched as transfers)
        dep_result = await session.execute(text("""
            SELECT ne.id, ne.wallet, ne.asset, ne.quantity::text,
                   ne.event_at, ne.source_deposit_id,
                   d.raw_data
            FROM tax.normalized_events ne
            LEFT JOIN tax.deposits d ON d.id = ne.source_deposit_id
            WHERE ne.event_type = 'UNRESOLVED'
              AND ne.source_deposit_id IS NOT NULL
              AND ne.run_id = :run_id
            ORDER BY ne.event_at ASC, ne.id ASC
        """), {"run_id": run_id})
        deposits = [dict(zip(dep_result.keys(), row)) for row in dep_result.fetchall()]

        for dep in deposits:
            income_type = self._check_exchange_tag(dep)

            if income_type:
                # Exchange-tagged income — reclassify but still needs review
                await session.execute(text("""
                    UPDATE tax.normalized_events
                    SET event_type = 'INCOME',
                        classification_rule = :rule
                    WHERE id = :id
                """), {"id": dep["id"],
                       "rule": f"exchange API tagged: {income_type}"})

                # Get FMV
                price, val_id = await self.valuation.get_price(
                    session, dep["asset"], dep["event_at"],
                    run_id=run_id, source_event_id=dep["id"])

                qty = D(dep["quantity"])
                fmv_total = (qty * price) if price else None

                # Create income event with pending review
                await session.execute(text("""
                    INSERT INTO tax.income_events_v4
                        (wallet, asset, quantity, fmv_per_unit_usd, total_fmv_usd,
                         income_type, classification_evidence, classification_source,
                         review_status, dominion_at, valuation_id,
                         source_event_id, source_deposit_id, run_id)
                    VALUES
                        (:wallet, :asset, :qty, :fmv, :total_fmv,
                         :itype, :evidence, 'exchange_api',
                         'pending', :dom_at, :val_id,
                         :seid, :sdid, :rid)
                """), {
                    "wallet": dep["wallet"], "asset": dep["asset"],
                    "qty": dep["quantity"],
                    "fmv": str(price) if price else None,
                    "total_fmv": str(fmv_total) if fmv_total else None,
                    "itype": income_type,
                    "evidence": f"Exchange API deposit type: {income_type}",
                    "dom_at": dep["event_at"],
                    "val_id": val_id,
                    "seid": dep["id"],
                    "sdid": dep["source_deposit_id"],
                    "rid": run_id,
                })

                stats["exchange_tagged"] += 1
                stats["income_events_created"] += 1
            else:
                # Not tagged — leave UNRESOLVED, log warning
                stats["unclassified"] += 1
                self.exc.log(WARNING, AMBIGUOUS_DEPOSIT,
                             f"Deposit on {dep['wallet']}: {dep['quantity']} {dep['asset']} "
                             f"— no exchange tag, needs manual classification",
                             source_deposit_id=dep["source_deposit_id"],
                             source_event_id=dep["id"],
                             tax_year=dep["event_at"].year if dep["event_at"] else None,
                             run_id=run_id)

        # 2. Process pool rewards (already tagged INCOME by ledger)
        pool_result = await session.execute(text("""
            SELECT ne.id, ne.wallet, ne.asset, ne.quantity::text,
                   ne.event_at, ne.source_pool_id, ne.classification_rule
            FROM tax.normalized_events ne
            WHERE ne.event_type = 'INCOME'
              AND ne.source_pool_id IS NOT NULL
              AND ne.run_id = :run_id
            ORDER BY ne.event_at ASC, ne.id ASC
        """), {"run_id": run_id})
        pool_rewards = [dict(zip(pool_result.keys(), row))
                        for row in pool_result.fetchall()]

        for pr in pool_rewards:
            # Get FMV
            price, val_id = await self.valuation.get_price(
                session, pr["asset"], pr["event_at"],
                run_id=run_id, source_event_id=pr["id"])

            qty = D(pr["quantity"])
            fmv_total = (qty * price) if price else None

            await session.execute(text("""
                INSERT INTO tax.income_events_v4
                    (wallet, asset, quantity, fmv_per_unit_usd, total_fmv_usd,
                     income_type, classification_evidence, classification_source,
                     review_status, dominion_at, valuation_id,
                     source_event_id, source_pool_id, run_id)
                VALUES
                    (:wallet, :asset, :qty, :fmv, :total_fmv,
                     'pool_reward', :evidence, 'pool_action',
                     'pending', :dom_at, :val_id,
                     :seid, :spid, :rid)
            """), {
                "wallet": pr["wallet"], "asset": pr["asset"],
                "qty": pr["quantity"],
                "fmv": str(price) if price else None,
                "total_fmv": str(fmv_total) if fmv_total else None,
                "evidence": pr.get("classification_rule", "pool reward"),
                "dom_at": pr["event_at"],
                "val_id": val_id,
                "seid": pr["id"],
                "spid": pr["source_pool_id"],
                "rid": run_id,
            })

            stats["pool_rewards"] += 1
            stats["income_events_created"] += 1

        logger.info(f"Income classification complete: {stats}")
        return stats

    @staticmethod
    def _check_exchange_tag(dep: dict) -> str | None:
        """
        Check if the exchange API provided a transaction type tag.
        Returns the income type string or None.
        """
        raw_data = dep.get("raw_data")
        if not raw_data:
            return None

        # raw_data may be a dict (already parsed) or string
        if isinstance(raw_data, str):
            import json
            try:
                raw_data = json.loads(raw_data)
            except (json.JSONDecodeError, TypeError):
                return None

        if not isinstance(raw_data, dict):
            return None

        # Check common exchange API fields for type info
        for key in ("type", "tx_type", "transaction_type", "depositType",
                    "deposit_type", "category", "sub_type", "subType"):
            val = raw_data.get(key)
            if val and str(val).lower() in INCOME_TAGS:
                return str(val).lower()

        return None

    @staticmethod
    async def create_income_lot(session: AsyncSession,
                                income_event_id: int,
                                run_id: int) -> int | None:
        """
        When an income event is confirmed, create an acquisition lot at FMV.
        Returns the lot ID or None if income event not found/not confirmed.
        """
        # Get the income event
        r = await session.execute(text("""
            SELECT id, wallet, asset, quantity::text, fmv_per_unit_usd::text,
                   total_fmv_usd::text, dominion_at, source_event_id
            FROM tax.income_events_v4
            WHERE id = :id AND review_status = 'confirmed'
        """), {"id": income_event_id})
        row = r.fetchone()
        if not row:
            return None

        ie = dict(zip(r.keys(), row))

        # Create the lot
        lot_result = await session.execute(text("""
            INSERT INTO tax.lots_v4
                (asset, wallet, original_quantity, remaining,
                 cost_per_unit_usd, total_cost_usd,
                 original_acquired_at, lot_created_at,
                 source_event_id, source_type, run_id)
            VALUES
                (:asset, :wallet, :qty, :remaining,
                 :cpu, :tc,
                 :oaa, NOW(),
                 :seid, 'income', :rid)
            RETURNING id
        """), {
            "asset": ie["asset"], "wallet": ie["wallet"],
            "qty": ie["quantity"], "remaining": ie["quantity"],
            "cpu": ie["fmv_per_unit_usd"],
            "tc": ie["total_fmv_usd"],
            "oaa": ie["dominion_at"],
            "seid": ie["source_event_id"],
            "rid": run_id,
        })
        lot_row = lot_result.fetchone()
        lot_id = lot_row[0] if lot_row else None

        # Link the lot back to the income event
        if lot_id:
            await session.execute(text(
                "UPDATE tax.income_events_v4 SET lot_id = :lid WHERE id = :id"),
                {"lid": lot_id, "id": income_event_id})

        return lot_id
