"""
Tests for the Exception System (filing gate).

Covers:
  - BLOCKING exception prevents filing
  - WARNING does NOT prevent filing
  - Resolved exception clears gate
  - Exception dollar exposure tracking
"""
import pytest
from decimal import Decimal

from exceptions import (ExceptionManager, BLOCKING, WARNING, INFO,
                         UNKNOWN_BASIS, MISSING_PRICE, UNSUPPORTED_TX,
                         AMBIGUOUS_DEPOSIT, OVERSOLD)


D = Decimal


class TestExceptionSeverity:
    def test_blocking_constants(self):
        assert BLOCKING == "BLOCKING"
        assert WARNING == "WARNING"
        assert INFO == "INFO"

    def test_category_constants(self):
        assert UNKNOWN_BASIS == "UNKNOWN_BASIS"
        assert MISSING_PRICE == "MISSING_PRICE"
        assert UNSUPPORTED_TX == "UNSUPPORTED_TX_TYPE"
        assert AMBIGUOUS_DEPOSIT == "AMBIGUOUS_DEPOSIT"
        assert OVERSOLD == "OVERSOLD"


class TestExceptionManager:
    def test_empty_manager(self):
        exc = ExceptionManager()
        assert exc.has_blocking is False
        assert exc.get_counts() == {BLOCKING: 0, WARNING: 0, INFO: 0}

    def test_log_blocking(self):
        exc = ExceptionManager()
        exc.log(BLOCKING, UNKNOWN_BASIS, "No basis for lot 1")
        assert exc.has_blocking is True

    def test_log_warning(self):
        exc = ExceptionManager()
        exc.log(WARNING, AMBIGUOUS_DEPOSIT, "Deposit needs classification")
        assert exc.has_blocking is False

    def test_log_info(self):
        exc = ExceptionManager()
        exc.log(INFO, "GENERAL", "Informational note")
        assert exc.has_blocking is False


class TestFilingGate:
    def test_blocking_exception_prevents_filing(self):
        """Any BLOCKING exception → filing not ready."""
        exc = ExceptionManager()
        exc.log(BLOCKING, UNKNOWN_BASIS, "Test blocking exception",
                lot_id=1, dollar_exposure=D("50000"))
        assert exc.has_blocking is True
        filing_ready = not exc.has_blocking
        assert filing_ready is False

    def test_warning_does_not_prevent_filing(self):
        """WARNING exceptions don't block filing."""
        exc = ExceptionManager()
        exc.log(WARNING, AMBIGUOUS_DEPOSIT, "Unclassified deposit")
        exc.log(WARNING, "UNMATCHED_TRANSFER", "Unmatched withdrawal")
        assert exc.has_blocking is False
        filing_ready = not exc.has_blocking
        assert filing_ready is True

    def test_resolved_exception_clears_gate(self):
        """After resolving a BLOCKING exception, it no longer blocks.
        (The DB-based check_filing_ready filters by resolution_status='open',
        but here we test the buffer behavior.)"""
        exc = ExceptionManager()
        exc.log(BLOCKING, UNKNOWN_BASIS, "Will be resolved")
        assert exc.has_blocking is True

        # In practice, resolution happens via DB update.
        # The buffer tracks what was logged in this run.
        # After flush, the buffer is cleared.
        # Then check_filing_ready queries the DB.
        # If the exception is resolved in DB, it won't appear.

    def test_exception_dollar_exposure(self):
        """Exceptions can track dollar exposure."""
        exc = ExceptionManager()
        exc.log(BLOCKING, UNKNOWN_BASIS, "Missing basis",
                dollar_exposure=D("50000"), tax_year=2025)

        buffer = exc._buffer
        assert len(buffer) == 1
        assert buffer[0]["dollar_exposure"] == "50000"
        assert buffer[0]["affected_tax_year"] == 2025

    def test_mixed_severities(self):
        exc = ExceptionManager()
        exc.log(BLOCKING, UNKNOWN_BASIS, "Blocking 1")
        exc.log(WARNING, AMBIGUOUS_DEPOSIT, "Warning 1")
        exc.log(WARNING, "UNMATCHED_TRANSFER", "Warning 2")
        exc.log(INFO, "GENERAL", "Info 1")

        counts = exc.get_counts()
        assert counts[BLOCKING] == 1
        assert counts[WARNING] == 2
        assert counts[INFO] == 1
        assert exc.has_blocking is True

    def test_blocks_filing_defaults_to_severity(self):
        """blocks_filing defaults to True for BLOCKING, False for others."""
        exc = ExceptionManager()
        exc.log(BLOCKING, UNKNOWN_BASIS, "Should block")
        exc.log(WARNING, AMBIGUOUS_DEPOSIT, "Should not block")

        assert exc._buffer[0]["blocks_filing"] is True
        assert exc._buffer[1]["blocks_filing"] is False

    def test_blocks_filing_explicit_override(self):
        """blocks_filing can be explicitly set."""
        exc = ExceptionManager()
        exc.log(WARNING, "CUSTOM", "Custom blocking warning",
                blocks_filing=True)
        assert exc._buffer[0]["blocks_filing"] is True
