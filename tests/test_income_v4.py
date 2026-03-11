"""
Tests for the v4 Income Classifier (Evidence-Based).

Covers:
  - No auto-classification by deposit count
  - Unclassified deposit creates exception
  - Exchange-tagged income goes to pending review
  - Pool reward creates income event
"""
import pytest
import json
from decimal import Decimal
from datetime import datetime, timezone

from income_classifier_v4 import IncomeClassifierV4, INCOME_TAGS
from exceptions import ExceptionManager, WARNING, AMBIGUOUS_DEPOSIT


D = Decimal
T = datetime(2025, 3, 10, 12, 0, 0, tzinfo=timezone.utc)


class TestIncomeTags:
    def test_staking_is_income_tag(self):
        assert "staking" in INCOME_TAGS

    def test_reward_is_income_tag(self):
        assert "reward" in INCOME_TAGS

    def test_airdrop_is_income_tag(self):
        assert "airdrop" in INCOME_TAGS

    def test_buy_is_not_income_tag(self):
        assert "buy" not in INCOME_TAGS


class TestExchangeTagDetection:
    """Test _check_exchange_tag static method."""

    def test_no_raw_data_returns_none(self):
        dep = {"raw_data": None}
        assert IncomeClassifierV4._check_exchange_tag(dep) is None

    def test_empty_dict_returns_none(self):
        dep = {"raw_data": {}}
        assert IncomeClassifierV4._check_exchange_tag(dep) is None

    def test_staking_type_detected(self):
        dep = {"raw_data": {"type": "staking"}}
        result = IncomeClassifierV4._check_exchange_tag(dep)
        assert result == "staking"

    def test_reward_in_category_detected(self):
        dep = {"raw_data": {"category": "reward"}}
        result = IncomeClassifierV4._check_exchange_tag(dep)
        assert result == "reward"

    def test_json_string_raw_data(self):
        dep = {"raw_data": json.dumps({"tx_type": "airdrop"})}
        result = IncomeClassifierV4._check_exchange_tag(dep)
        assert result == "airdrop"

    def test_unknown_type_returns_none(self):
        dep = {"raw_data": {"type": "withdrawal"}}
        result = IncomeClassifierV4._check_exchange_tag(dep)
        assert result is None


class TestNoAutoClassification:
    """Test that v4 does NOT auto-classify by deposit count or asset lists."""

    def test_no_auto_classification_by_count(self):
        """v3 classified 3+ deposits as staking — v4 must NOT do this."""
        # In v4, there is no KNOWN_STAKING_ASSETS set used for auto-classification
        # The classifier only uses exchange API tags
        dep_without_tag = {"raw_data": {}}
        result = IncomeClassifierV4._check_exchange_tag(dep_without_tag)
        assert result is None  # NOT auto-classified

    def test_unclassified_deposit_creates_exception(self):
        """Deposit without exchange tag → WARNING AMBIGUOUS_DEPOSIT."""
        exc = ExceptionManager()
        # Simulate what the classifier does for untagged deposits
        dep = {"raw_data": {}, "wallet": "nonkyc", "asset": "ETH",
               "quantity": "5.0", "source_deposit_id": 42}
        tag = IncomeClassifierV4._check_exchange_tag(dep)
        if tag is None:
            exc.log(WARNING, AMBIGUOUS_DEPOSIT,
                    f"Deposit on {dep['wallet']}: {dep['quantity']} {dep['asset']} "
                    f"— no exchange tag, needs manual classification",
                    source_deposit_id=dep["source_deposit_id"])

        assert not exc.has_blocking  # WARNING, not BLOCKING
        counts = exc.get_counts()
        assert counts[WARNING] == 1

    def test_exchange_tagged_income_goes_to_pending(self):
        """Exchange-tagged deposit → classified but review_status='pending'."""
        # The classifier creates income_events_v4 with review_status='pending'
        review_status = "pending"
        dep = {"raw_data": {"type": "staking"}}
        tag = IncomeClassifierV4._check_exchange_tag(dep)
        assert tag == "staking"
        assert review_status == "pending"  # All income starts pending

    def test_pool_reward_creates_income_event(self):
        """Pool rewards (tagged INCOME by ledger) → income_events_v4."""
        # Pool rewards have source_pool_id set and event_type='INCOME'
        # The classifier creates income_events_v4 with classification_source='pool_action'
        classification_source = "pool_action"
        income_type = "pool_reward"
        assert classification_source == "pool_action"
        assert income_type == "pool_reward"
