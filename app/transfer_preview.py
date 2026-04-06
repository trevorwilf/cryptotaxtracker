"""
Transfer Match Preview — lightweight matching at import time.

Does NOT modify normalized events, lots, or carryover tables.
Just scans raw deposits/withdrawals for potential matches and returns
them as suggestions for the user to review.
"""
import logging
from datetime import timedelta
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("tax-collector.transfer-preview")

D = Decimal
ZERO = D("0")


class TransferPreview:
    """Preview potential transfer matches without running the full pipeline."""

    def __init__(self, time_window_hours: int = 72,
                 fee_tolerance_pct: float = 10.0):
        """Use wider tolerances than the actual matcher — we want to catch
        possible matches, not just high-confidence ones."""
        self.time_window = timedelta(hours=time_window_hours)
        self.fee_tolerance = D(str(fee_tolerance_pct / 100))

    async def scan_imported_deposits(self, session: AsyncSession,
                                      imported_deposit_ids: list[int],
                                      exchange: str) -> list[dict]:
        """For each newly imported deposit, find potential matching
        withdrawals already in the database (from OTHER exchanges)."""
        if not imported_deposit_ids:
            return []

        matches = []
        for dep_id in imported_deposit_ids:
            dep_result = await session.execute(text("""
                SELECT id, exchange, asset, amount::text, tx_hash, address,
                       confirmed_at, network
                FROM tax.deposits WHERE id = :id
            """), {"id": dep_id})
            dep = dep_result.fetchone()
            if not dep:
                continue
            dep = dict(zip(dep_result.keys(), dep))

            candidates = await self._find_withdrawal_candidates(
                session, dep, exclude_exchange=exchange
            )

            for candidate in candidates:
                confidence = self._score_match(dep, candidate)
                if confidence:
                    matches.append({
                        "type": "deposit_matches_withdrawal",
                        "imported_record": {
                            "id": dep["id"],
                            "exchange": dep["exchange"],
                            "asset": dep["asset"],
                            "amount": dep["amount"],
                            "tx_hash": dep["tx_hash"],
                            "address": dep.get("address"),
                            "confirmed_at": str(dep["confirmed_at"]),
                        },
                        "existing_record": {
                            "id": candidate["id"],
                            "exchange": candidate["exchange"],
                            "asset": candidate["asset"],
                            "amount": candidate["amount"],
                            "fee": candidate.get("fee"),
                            "tx_hash": candidate["tx_hash"],
                            "address": candidate.get("address"),
                            "confirmed_at": str(candidate["confirmed_at"]),
                        },
                        "confidence": confidence["level"],
                        "evidence": confidence["evidence"],
                        "suggestion": (
                            f"This {dep['exchange']} deposit of {dep['amount']} {dep['asset']} "
                            f"may be the receive side of a {candidate['exchange']} withdrawal. "
                            f"If confirmed, this is a non-taxable self-transfer."
                        ),
                    })

        return matches

    async def scan_imported_withdrawals(self, session: AsyncSession,
                                         imported_withdrawal_ids: list[int],
                                         exchange: str) -> list[dict]:
        """For each newly imported withdrawal, find potential matching
        deposits already in the database (from OTHER exchanges)."""
        if not imported_withdrawal_ids:
            return []

        matches = []
        for wd_id in imported_withdrawal_ids:
            wd_result = await session.execute(text("""
                SELECT id, exchange, asset, amount::text, fee::text,
                       tx_hash, address, confirmed_at, network
                FROM tax.withdrawals WHERE id = :id
            """), {"id": wd_id})
            wd = wd_result.fetchone()
            if not wd:
                continue
            wd = dict(zip(wd_result.keys(), wd))

            candidates = await self._find_deposit_candidates(
                session, wd, exclude_exchange=exchange
            )

            for candidate in candidates:
                confidence = self._score_match_wd_to_dep(wd, candidate)
                if confidence:
                    matches.append({
                        "type": "withdrawal_matches_deposit",
                        "imported_record": {
                            "id": wd["id"],
                            "exchange": wd["exchange"],
                            "asset": wd["asset"],
                            "amount": wd["amount"],
                            "fee": wd.get("fee"),
                            "tx_hash": wd["tx_hash"],
                            "address": wd.get("address"),
                            "confirmed_at": str(wd["confirmed_at"]),
                        },
                        "existing_record": {
                            "id": candidate["id"],
                            "exchange": candidate["exchange"],
                            "asset": candidate["asset"],
                            "amount": candidate["amount"],
                            "tx_hash": candidate["tx_hash"],
                            "address": candidate.get("address"),
                            "confirmed_at": str(candidate["confirmed_at"]),
                        },
                        "confidence": confidence["level"],
                        "evidence": confidence["evidence"],
                        "suggestion": (
                            f"This {wd['exchange']} withdrawal of {wd['amount']} {wd['asset']} "
                            f"may match a {candidate['exchange']} deposit. "
                            f"If confirmed, this is a non-taxable self-transfer."
                        ),
                    })

        return matches

    async def scan_address_overlaps(self, session: AsyncSession,
                                     imported_ids: list[int],
                                     data_type: str) -> list[dict]:
        """Find addresses from the import that also appear in records
        from other exchanges — strong signal of self-ownership."""
        table = "deposits" if data_type == "deposits" else "withdrawals"
        if not imported_ids:
            return []

        placeholders = ",".join(str(int(i)) for i in imported_ids)

        # Get addresses from imported records
        result = await session.execute(text(f"""
            SELECT DISTINCT address, asset, exchange
            FROM tax.{table}
            WHERE id IN ({placeholders})
              AND address IS NOT NULL AND address != ''
        """))
        imported_addresses = [dict(zip(result.keys(), r)) for r in result.fetchall()]

        suggestions = []
        for addr_row in imported_addresses:
            address = addr_row["address"]
            source_exchange = addr_row["exchange"]

            # Check if this address appears in deposits on OTHER exchanges
            dep_result = await session.execute(text("""
                SELECT DISTINCT exchange, asset, COUNT(*) as times_seen
                FROM tax.deposits
                WHERE address = :addr AND exchange != :ex
                GROUP BY exchange, asset
            """), {"addr": address, "ex": source_exchange})
            dep_matches = [dict(zip(dep_result.keys(), r)) for r in dep_result.fetchall()]

            # Check if this address appears in withdrawals on OTHER exchanges
            wd_result = await session.execute(text("""
                SELECT DISTINCT exchange, asset, COUNT(*) as times_seen
                FROM tax.withdrawals
                WHERE address = :addr AND exchange != :ex
                GROUP BY exchange, asset
            """), {"addr": address, "ex": source_exchange})
            wd_matches = [dict(zip(wd_result.keys(), r)) for r in wd_result.fetchall()]

            if dep_matches or wd_matches:
                addr_short = address
                if len(address) > 28:
                    addr_short = f"{address[:20]}...{address[-8:]}"
                suggestions.append({
                    "address": address,
                    "found_in_import": {
                        "exchange": source_exchange,
                        "data_type": data_type,
                    },
                    "also_found_in_deposits": dep_matches,
                    "also_found_in_withdrawals": wd_matches,
                    "suggestion": (
                        f"Address {addr_short} appears on multiple exchanges. "
                        f"This is likely a self-owned address. Consider claiming it via "
                        f"POST /v4/wallet/addresses to enable automatic self-transfer detection."
                    ),
                })

            # Check if address is already claimed
            try:
                claimed = await session.execute(text("""
                    SELECT wa.address, wac.claim_type, wac.confidence
                    FROM tax.wallet_addresses wa
                    JOIN tax.wallet_address_claims wac ON wac.address_id = wa.id
                    WHERE wa.address = :addr
                """), {"addr": address})
                claim_rows = claimed.fetchall()
                if claim_rows:
                    for cr in claim_rows:
                        suggestions.append({
                            "address": address,
                            "already_claimed": True,
                            "claim_type": cr[1],
                            "confidence": cr[2],
                            "note": "This address is already claimed — transfer matching will use it automatically.",
                        })
            except Exception:
                # wallet tables may not exist yet
                pass

        return suggestions

    async def _find_withdrawal_candidates(self, session, dep: dict,
                                           exclude_exchange: str) -> list[dict]:
        """Find withdrawals that could be the send-side of this deposit."""
        if not dep.get("confirmed_at"):
            return []
        result = await session.execute(text("""
            SELECT id, exchange, asset, amount::text, fee::text,
                   tx_hash, address, confirmed_at, network
            FROM tax.withdrawals
            WHERE asset = :asset
              AND exchange != :ex
              AND confirmed_at BETWEEN :earliest AND :dep_time
              AND amount > 0
            ORDER BY confirmed_at DESC
        """), {
            "asset": dep["asset"],
            "ex": exclude_exchange,
            "earliest": dep["confirmed_at"] - self.time_window,
            "dep_time": dep["confirmed_at"],
        })
        return [dict(zip(result.keys(), r)) for r in result.fetchall()]

    async def _find_deposit_candidates(self, session, wd: dict,
                                        exclude_exchange: str) -> list[dict]:
        """Find deposits that could be the receive-side of this withdrawal."""
        if not wd.get("confirmed_at"):
            return []
        result = await session.execute(text("""
            SELECT id, exchange, asset, amount::text,
                   tx_hash, address, confirmed_at, network
            FROM tax.deposits
            WHERE asset = :asset
              AND exchange != :ex
              AND confirmed_at BETWEEN :wd_time AND :latest
              AND amount > 0
            ORDER BY confirmed_at ASC
        """), {
            "asset": wd["asset"],
            "ex": exclude_exchange,
            "wd_time": wd["confirmed_at"],
            "latest": wd["confirmed_at"] + self.time_window,
        })
        return [dict(zip(result.keys(), r)) for r in result.fetchall()]

    def _score_match(self, dep: dict, wd: dict) -> dict | None:
        """Score how likely a deposit matches a withdrawal."""
        evidence = []

        # TX hash match — strongest signal
        if (dep.get("tx_hash") and wd.get("tx_hash")
                and self._normalize_tx_hash(dep["tx_hash"]) == self._normalize_tx_hash(wd["tx_hash"])):
            evidence.append("tx_hash_exact_match")
            return {"level": "high", "evidence": evidence}

        # Address match — the deposit address matches the withdrawal destination
        if (dep.get("address") and wd.get("address")
                and dep["address"] == wd["address"]):
            evidence.append("same_address")

        # Amount match (within fee tolerance)
        try:
            dep_amt = D(dep["amount"])
            wd_amt = D(wd["amount"])
            wd_fee = D(wd.get("fee") or "0")
            wd_net = wd_amt - wd_fee

            if wd_net > 0 and abs(dep_amt - wd_net) / wd_net <= self.fee_tolerance:
                evidence.append(f"amount_matches_net_of_fee (dep={dep_amt}, wd_net={wd_net})")
            elif wd_amt > 0 and abs(dep_amt - wd_amt) / wd_amt <= self.fee_tolerance:
                evidence.append(f"amount_matches_gross (dep={dep_amt}, wd={wd_amt})")
            else:
                return None  # amounts too different
        except Exception:
            return None

        if not evidence:
            return None

        level = "high" if "same_address" in str(evidence) else "medium"
        return {"level": level, "evidence": evidence}

    def _score_match_wd_to_dep(self, wd: dict, dep: dict) -> dict | None:
        """Score match from withdrawal perspective (same logic, reversed args)."""
        return self._score_match(dep, wd)

    def _normalize_tx_hash(self, tx_hash: str) -> str:
        """Normalize tx hash for comparison.

        MEXC deposit TxIDs sometimes include ':N' output index suffix.
        Strip it for matching purposes.
        """
        if ":" in tx_hash:
            base, suffix = tx_hash.rsplit(":", 1)
            if suffix.isdigit():
                return base.lower().strip()
        return tx_hash.lower().strip()
