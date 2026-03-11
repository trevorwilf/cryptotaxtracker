"""
Exception System — filing-grade issue tracking.

Every issue that could affect tax accuracy is logged here with severity:
  BLOCKING — prevents filing until resolved
  WARNING  — should be reviewed but doesn't prevent filing
  INFO     — informational, no action required

The filing gate checks: if ANY open BLOCKING exceptions exist for a tax year,
the run is marked as not-filing-ready.

Addresses reviewer Issue 6 (unsupported types not blocked) and the
broader audit-trail requirement from the review's Appendix I.
"""
import logging
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("tax-collector.exceptions")

# ── Severity levels ───────────────────────────────────────────────────────

BLOCKING = "BLOCKING"
WARNING = "WARNING"
INFO = "INFO"

# ── Category constants ────────────────────────────────────────────────────

UNKNOWN_BASIS = "UNKNOWN_BASIS"
MISSING_PRICE = "MISSING_PRICE"
UNMATCHED_TRANSFER = "UNMATCHED_TRANSFER"
UNSUPPORTED_TX = "UNSUPPORTED_TX_TYPE"
AMBIGUOUS_DEPOSIT = "AMBIGUOUS_DEPOSIT"
DUPLICATE_SUSPICION = "DUPLICATE_SUSPICION"
OVERSOLD = "OVERSOLD"
TIMESTAMP_INVALID = "TIMESTAMP_INVALID"
STABLECOIN_DEPEG = "STABLECOIN_DEPEG"
CRYPTO_TO_CRYPTO = "CRYPTO_TO_CRYPTO"
HOLDING_PERIOD_RESET = "HOLDING_PERIOD_RESET"
VALUATION_FALLBACK = "VALUATION_FALLBACK"
INCOMPLETE_HISTORY = "INCOMPLETE_HISTORY"


class ExceptionManager:
    """Manages tax computation exceptions with filing-gate logic."""

    def __init__(self):
        self._buffer: list[dict] = []

    def log(self, severity: str, category: str, message: str,
            detail: str = None, source_trade_id: int = None,
            source_deposit_id: int = None, source_withdrawal_id: int = None,
            source_event_id: int = None, lot_id: int = None,
            dollar_exposure: Decimal = None, tax_year: int = None,
            blocks_filing: bool = None, run_id: int = None):
        """Buffer an exception for batch insert."""
        if blocks_filing is None:
            blocks_filing = (severity == BLOCKING)

        self._buffer.append({
            "severity": severity,
            "category": category,
            "message": message,
            "detail": detail,
            "source_trade_id": source_trade_id,
            "source_deposit_id": source_deposit_id,
            "source_withdrawal_id": source_withdrawal_id,
            "source_event_id": source_event_id,
            "lot_id": lot_id,
            "dollar_exposure": str(dollar_exposure) if dollar_exposure else None,
            "affected_tax_year": tax_year,
            "blocks_filing": blocks_filing,
            "run_id": run_id,
        })

        if severity == BLOCKING:
            logger.warning(f"BLOCKING: [{category}] {message}")
        elif severity == WARNING:
            logger.warning(f"WARNING: [{category}] {message}")
        else:
            logger.info(f"INFO: [{category}] {message}")

    async def flush(self, session: AsyncSession):
        """Write all buffered exceptions to the database."""
        for exc in self._buffer:
            await session.execute(text("""
                INSERT INTO tax.exceptions
                    (severity, category, message, detail,
                     source_trade_id, source_deposit_id, source_withdrawal_id,
                     source_event_id, lot_id, dollar_exposure,
                     affected_tax_year, blocks_filing, run_id)
                VALUES
                    (:severity, :category, :message, :detail,
                     :source_trade_id, :source_deposit_id, :source_withdrawal_id,
                     :source_event_id, :lot_id, :dollar_exposure,
                     :affected_tax_year, :blocks_filing, :run_id)
            """), exc)
        count = len(self._buffer)
        self._buffer.clear()
        return count

    def get_counts(self) -> dict:
        """Get counts of buffered exceptions by severity."""
        counts = {BLOCKING: 0, WARNING: 0, INFO: 0}
        for exc in self._buffer:
            counts[exc["severity"]] = counts.get(exc["severity"], 0) + 1
        return counts

    @property
    def has_blocking(self) -> bool:
        return any(e["severity"] == BLOCKING for e in self._buffer)

    @staticmethod
    async def check_filing_ready(session: AsyncSession, tax_year: int) -> dict:
        """Check if a tax year has any open blocking exceptions."""
        result = await session.execute(text("""
            SELECT severity, COUNT(*), COALESCE(SUM(dollar_exposure), 0)::text
            FROM tax.exceptions
            WHERE (affected_tax_year = :year OR affected_tax_year IS NULL)
              AND resolution_status = 'open'
            GROUP BY severity
            ORDER BY severity
        """), {"year": tax_year})

        summary = {}
        total_blocking = 0
        for row in result.fetchall():
            summary[row[0]] = {"count": row[1], "dollar_exposure": row[2]}
            if row[0] == BLOCKING:
                total_blocking = row[1]

        return {
            "tax_year": tax_year,
            "filing_ready": total_blocking == 0,
            "open_exceptions": summary,
            "blocking_count": total_blocking,
        }

    @staticmethod
    async def get_all(session: AsyncSession, tax_year: int = None,
                      severity: str = None, status: str = "open") -> list[dict]:
        """Get exceptions with optional filters."""
        where = ["1=1"]
        params: dict = {}
        if tax_year:
            where.append("(affected_tax_year = :year OR affected_tax_year IS NULL)")
            params["year"] = tax_year
        if severity:
            where.append("severity = :sev")
            params["sev"] = severity
        if status:
            where.append("resolution_status = :status")
            params["status"] = status

        result = await session.execute(text(f"""
            SELECT id, severity, category, message, detail, dollar_exposure::text,
                   affected_tax_year, blocks_filing, resolution_status, created_at
            FROM tax.exceptions
            WHERE {' AND '.join(where)}
            ORDER BY
                CASE severity WHEN 'BLOCKING' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END,
                created_at DESC
        """), params)
        return [dict(zip(result.keys(), row)) for row in result.fetchall()]

    @staticmethod
    async def resolve(session: AsyncSession, exception_id: int,
                      status: str, notes: str, resolved_by: str = "system"):
        """Resolve or accept-risk an exception."""
        await session.execute(text("""
            UPDATE tax.exceptions SET
                resolution_status = :status,
                resolution_notes = :notes,
                resolved_by = :by,
                resolved_at = NOW()
            WHERE id = :id
        """), {"id": exception_id, "status": status, "notes": notes, "by": resolved_by})

    @staticmethod
    async def clear_for_run(session: AsyncSession, run_id: int = None):
        """Clear exceptions from a specific run (for recomputation)."""
        if run_id:
            await session.execute(text("DELETE FROM tax.exceptions WHERE run_id = :r"), {"r": run_id})
        else:
            await session.execute(text("DELETE FROM tax.exceptions WHERE resolution_status = 'open'"))
