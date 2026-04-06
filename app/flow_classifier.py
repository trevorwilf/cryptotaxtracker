"""
Classify deposits and withdrawals into funding flow categories.

Uses transfer matcher results and income classifier results to determine:
- EXTERNAL_DEPOSIT / EXTERNAL_WITHDRAWAL (true external funding)
- INTERNAL_TRANSFER_IN / INTERNAL_TRANSFER_OUT (self-transfers between exchanges)
- INCOME_RECEIPT (staking/airdrop/reward deposits)
- UNCLASSIFIED (needs manual review)
"""
import logging
from decimal import Decimal

from sqlalchemy import text

logger = logging.getLogger("tax-collector.flow-classifier")

D = Decimal

VALID_FLOW_CLASSES = {
    "EXTERNAL_DEPOSIT", "EXTERNAL_WITHDRAWAL",
    "INTERNAL_TRANSFER_IN", "INTERNAL_TRANSFER_OUT",
    "INCOME_RECEIPT", "UNCLASSIFIED",
}


class FlowClassifier:
    """Classify all deposits and withdrawals into funding flow categories."""

    async def classify_all(self, session, run_id: int = None) -> dict:
        """Classify all deposits and withdrawals.

        Logic:
        1. If deposit matched as TRANSFER_IN in normalized_events -> INTERNAL_TRANSFER_IN
        2. If deposit classified as income in income_events_v4 -> INCOME_RECEIPT
        3. If withdrawal matched as TRANSFER_OUT -> INTERNAL_TRANSFER_OUT
        4. Remaining deposits/withdrawals -> UNCLASSIFIED (requires manual review)
        """
        # Clear previous classifications (run-scoped if run_id provided)
        if run_id:
            await session.execute(text("DELETE FROM tax.classified_flows WHERE run_id = :rid"),
                                  {"rid": run_id})
        else:
            await session.execute(text("DELETE FROM tax.classified_flows"))

        stats = {
            "EXTERNAL_DEPOSIT": 0, "EXTERNAL_WITHDRAWAL": 0,
            "INTERNAL_TRANSFER_IN": 0, "INTERNAL_TRANSFER_OUT": 0,
            "INCOME_RECEIPT": 0, "UNCLASSIFIED": 0,
        }

        # Build run filter for source queries
        run_filter = "AND ne.run_id = :rid" if run_id else ""
        run_params = {"rid": run_id} if run_id else {}

        # Get transfer-matched deposit IDs
        r = await session.execute(text(f"""
            SELECT DISTINCT ne.source_deposit_id FROM tax.normalized_events ne
            WHERE ne.event_type = 'TRANSFER_IN' AND ne.source_deposit_id IS NOT NULL
            {run_filter}
        """), run_params)
        transfer_in_deposit_ids = {row[0] for row in r.fetchall()}

        # Get income-tagged deposit IDs
        r = await session.execute(text("""
            SELECT DISTINCT source_deposit_id FROM tax.income_events_v4
            WHERE source_deposit_id IS NOT NULL
        """))
        income_deposit_ids = {row[0] for row in r.fetchall()}

        # Get transfer-matched withdrawal IDs
        r = await session.execute(text(f"""
            SELECT DISTINCT ne.source_withdrawal_id FROM tax.normalized_events ne
            WHERE ne.event_type = 'TRANSFER_OUT' AND ne.source_withdrawal_id IS NOT NULL
            {run_filter}
        """), run_params)
        transfer_out_withdrawal_ids = {row[0] for row in r.fetchall()}

        # Classify deposits
        r = await session.execute(text("""
            SELECT id, exchange, asset, amount, amount_usd, asset_price_usd, confirmed_at
            FROM tax.deposits ORDER BY confirmed_at
        """))
        for row in r.fetchall():
            dep_id, exchange, asset, amount, amount_usd, price_usd, event_at = row

            if dep_id in transfer_in_deposit_ids:
                flow_class = "INTERNAL_TRANSFER_IN"
                rule = "Matched as TRANSFER_IN in normalized events"
            elif dep_id in income_deposit_ids:
                flow_class = "INCOME_RECEIPT"
                rule = "Classified as income in income_events_v4"
            else:
                flow_class = "UNCLASSIFIED"
                rule = "No transfer match or income classification found — requires manual review"

            qty = D(str(amount or "0"))
            unit_price = D(str(price_usd or "0"))
            total_usd = D(str(amount_usd or "0"))

            await session.execute(text("""
                INSERT INTO tax.classified_flows
                    (source_type, source_id, exchange, asset, quantity,
                     unit_price_usd, total_usd, flow_class, classification_rule,
                     event_at, run_id)
                VALUES ('deposit', :sid, :ex, :asset, :qty, :uprice, :tusd,
                        :fc, :rule, :eat, :rid)
            """), {
                "sid": dep_id, "ex": exchange, "asset": asset,
                "qty": str(qty), "uprice": str(unit_price), "tusd": str(total_usd),
                "fc": flow_class, "rule": rule,
                "eat": event_at, "rid": run_id,
            })
            stats[flow_class] += 1

        # Classify withdrawals
        r = await session.execute(text("""
            SELECT id, exchange, asset, amount, amount_usd, asset_price_usd, confirmed_at
            FROM tax.withdrawals ORDER BY confirmed_at
        """))
        for row in r.fetchall():
            wd_id, exchange, asset, amount, amount_usd, price_usd, event_at = row

            if wd_id in transfer_out_withdrawal_ids:
                flow_class = "INTERNAL_TRANSFER_OUT"
                rule = "Matched as TRANSFER_OUT in normalized events"
            else:
                flow_class = "UNCLASSIFIED"
                rule = "No transfer match found — requires manual review"

            qty = D(str(amount or "0"))
            unit_price = D(str(price_usd or "0"))
            total_usd = D(str(amount_usd or "0"))

            await session.execute(text("""
                INSERT INTO tax.classified_flows
                    (source_type, source_id, exchange, asset, quantity,
                     unit_price_usd, total_usd, flow_class, classification_rule,
                     event_at, run_id)
                VALUES ('withdrawal', :sid, :ex, :asset, :qty, :uprice, :tusd,
                        :fc, :rule, :eat, :rid)
            """), {
                "sid": wd_id, "ex": exchange, "asset": asset,
                "qty": str(qty), "uprice": str(unit_price), "tusd": str(total_usd),
                "fc": flow_class, "rule": rule,
                "eat": event_at, "rid": run_id,
            })
            stats[flow_class] += 1

        total = sum(stats.values())
        logger.info(f"Classified {total} flows: {stats}")
        return {"total_classified": total, "by_class": stats}
