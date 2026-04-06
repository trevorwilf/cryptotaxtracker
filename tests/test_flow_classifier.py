"""
Tests for the flow classifier.

Covers:
  - External deposit (no transfer match, no income tag)
  - Matched transfer deposit -> INTERNAL_TRANSFER_IN
  - Income-tagged deposit -> INCOME_RECEIPT
  - External withdrawal
  - Matched transfer withdrawal -> INTERNAL_TRANSFER_OUT
  - Dashboard card labels are correctly updated
"""
import pytest
from flow_classifier import FlowClassifier, VALID_FLOW_CLASSES


class FakeResult:
    def __init__(self, rows=None):
        self._rows = rows or []

    def fetchall(self):
        return self._rows


class FakeClassifierSession:
    """Mock session that simulates DB queries for flow classification."""

    def __init__(self):
        self.transfer_in_deposit_ids = set()
        self.income_deposit_ids = set()
        self.transfer_out_withdrawal_ids = set()
        self.deposits = []
        self.withdrawals = []
        self.inserts = []
        self.deleted = False

    async def execute(self, stmt, params=None):
        sql = str(stmt) if hasattr(stmt, 'text') else str(stmt)

        if "DELETE FROM tax.classified_flows" in sql:
            self.deleted = True
            return FakeResult()

        if "TRANSFER_IN" in sql and "source_deposit_id" in sql and "SELECT" in sql:
            return FakeResult([(did,) for did in self.transfer_in_deposit_ids])

        if "income_events_v4" in sql and "source_deposit_id" in sql:
            return FakeResult([(did,) for did in self.income_deposit_ids])

        if "TRANSFER_OUT" in sql and "source_withdrawal_id" in sql and "SELECT" in sql:
            return FakeResult([(wid,) for wid in self.transfer_out_withdrawal_ids])

        if "FROM tax.deposits" in sql:
            return FakeResult(self.deposits)

        if "FROM tax.withdrawals" in sql:
            return FakeResult(self.withdrawals)

        if "INSERT INTO tax.classified_flows" in sql:
            self.inserts.append(params)
            return FakeResult()

        return FakeResult()


from datetime import datetime, timezone
T = datetime(2025, 3, 15, 12, 0, 0, tzinfo=timezone.utc)


class TestFlowClassification:

    @pytest.mark.asyncio
    async def test_unclassified_deposit(self):
        """Deposit with no transfer match, no income tag -> UNCLASSIFIED."""
        session = FakeClassifierSession()
        session.deposits = [(1, "mexc", "USDT", "10000", "10000", "1.0", T)]
        classifier = FlowClassifier()
        result = await classifier.classify_all(session)
        assert result["by_class"]["UNCLASSIFIED"] == 1
        assert session.inserts[0]["fc"] == "UNCLASSIFIED"

    @pytest.mark.asyncio
    async def test_internal_transfer_in(self):
        """Deposit matched as TRANSFER_IN -> INTERNAL_TRANSFER_IN."""
        session = FakeClassifierSession()
        session.transfer_in_deposit_ids = {42}
        session.deposits = [(42, "nonkyc", "BTC", "0.5", "25000", "50000", T)]
        classifier = FlowClassifier()
        result = await classifier.classify_all(session)
        assert result["by_class"]["INTERNAL_TRANSFER_IN"] == 1

    @pytest.mark.asyncio
    async def test_income_receipt(self):
        """Deposit classified as income -> INCOME_RECEIPT."""
        session = FakeClassifierSession()
        session.income_deposit_ids = {99}
        session.deposits = [(99, "nonkyc", "SAL", "100", "50", "0.5", T)]
        classifier = FlowClassifier()
        result = await classifier.classify_all(session)
        assert result["by_class"]["INCOME_RECEIPT"] == 1

    @pytest.mark.asyncio
    async def test_unclassified_withdrawal(self):
        """Withdrawal with no transfer match -> UNCLASSIFIED."""
        session = FakeClassifierSession()
        session.withdrawals = [(10, "nonkyc", "USDT", "27000", "27000", "1.0", T)]
        classifier = FlowClassifier()
        result = await classifier.classify_all(session)
        assert result["by_class"]["UNCLASSIFIED"] == 1

    @pytest.mark.asyncio
    async def test_internal_transfer_out(self):
        """Withdrawal matched as TRANSFER_OUT -> INTERNAL_TRANSFER_OUT."""
        session = FakeClassifierSession()
        session.transfer_out_withdrawal_ids = {77}
        session.withdrawals = [(77, "mexc", "BTC", "0.5", "25000", "50000", T)]
        classifier = FlowClassifier()
        result = await classifier.classify_all(session)
        assert result["by_class"]["INTERNAL_TRANSFER_OUT"] == 1

    @pytest.mark.asyncio
    async def test_mixed_classification(self):
        """Multiple deposits and withdrawals classified correctly."""
        session = FakeClassifierSession()
        session.transfer_in_deposit_ids = {2}
        session.income_deposit_ids = {3}
        session.transfer_out_withdrawal_ids = {20}
        session.deposits = [
            (1, "mexc", "USDT", "10000", "10000", "1.0", T),  # external
            (2, "nonkyc", "BTC", "0.5", "25000", "50000", T),  # transfer in
            (3, "nonkyc", "SAL", "100", "50", "0.5", T),  # income
        ]
        session.withdrawals = [
            (20, "mexc", "BTC", "0.5", "25000", "50000", T),  # transfer out
            (21, "nonkyc", "USDT", "27000", "27000", "1.0", T),  # external
        ]
        classifier = FlowClassifier()
        result = await classifier.classify_all(session)
        assert result["total_classified"] == 5
        assert result["by_class"]["UNCLASSIFIED"] == 2  # 1 unmatched deposit + 1 unmatched withdrawal
        assert result["by_class"]["INTERNAL_TRANSFER_IN"] == 1
        assert result["by_class"]["INCOME_RECEIPT"] == 1
        assert result["by_class"]["INTERNAL_TRANSFER_OUT"] == 1

    def test_valid_flow_classes(self):
        """All valid flow classes are defined."""
        assert "EXTERNAL_DEPOSIT" in VALID_FLOW_CLASSES
        assert "INTERNAL_TRANSFER_IN" in VALID_FLOW_CLASSES
        assert "INCOME_RECEIPT" in VALID_FLOW_CLASSES
        assert "UNCLASSIFIED" in VALID_FLOW_CLASSES


class TestDashboardLabels:
    """Verify dashboard card labels were updated."""

    def test_deposit_label_relabeled(self):
        html_path = "app/static/index.html"
        with open(html_path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "Raw Deposit Ledger FMV" in content
        assert "Deposits (USD)</div>" not in content

    def test_withdrawal_label_relabeled(self):
        html_path = "app/static/index.html"
        with open(html_path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "Raw Withdrawal Ledger FMV" in content
        assert "Withdrawals (USD)</div>" not in content
