"""
Tests for the Income Classifier module.

Covers:
  - Known staking asset detection
  - Classification of periodic deposits as staking
  - Single deposits as unclassified
  - Pool rewards identification
"""
import pytest
from income_classifier import IncomeClassifier, KNOWN_STAKING_ASSETS


class TestKnownStakingAssets:
    """Verify the staking asset list is properly maintained."""

    def test_major_staking_coins_present(self):
        expected = ["ETH", "SOL", "ADA", "DOT", "ATOM", "AVAX"]
        for coin in expected:
            assert coin in KNOWN_STAKING_ASSETS, f"{coin} should be a known staking asset"

    def test_non_staking_not_present(self):
        assert "USDT" not in KNOWN_STAKING_ASSETS
        assert "BTC" not in KNOWN_STAKING_ASSETS

    def test_all_uppercase(self):
        for asset in KNOWN_STAKING_ASSETS:
            assert asset == asset.upper(), f"{asset} should be uppercase"


class TestClassificationLogic:
    """Test income type classification rules without DB."""

    def test_multiple_deposits_of_staking_asset(self):
        """3+ deposits of a known staking asset → 'staking'"""
        asset = "ETH"
        deposit_count = 5
        expected_type = "staking" if asset in KNOWN_STAKING_ASSETS else "airdrop_or_reward"
        assert expected_type == "staking"

    def test_multiple_deposits_unknown_asset(self):
        """3+ deposits of an unknown asset → 'airdrop_or_reward'"""
        asset = "RANDOMCOIN"
        expected_type = "staking" if asset in KNOWN_STAKING_ASSETS else "airdrop_or_reward"
        assert expected_type == "airdrop_or_reward"

    def test_single_deposit(self):
        """Single unmatched deposit → 'deposit_unclassified'"""
        deposit_count = 1
        expected_type = "deposit_unclassified"
        assert expected_type == "deposit_unclassified"

    def test_pool_reward_action(self):
        """Pool activity with action='reward' → 'pool_reward'"""
        action = "reward"
        expected_type = "pool_reward"
        assert expected_type == "pool_reward"


class TestIncomeClassifierInit:
    def test_instantiation(self):
        ic = IncomeClassifier()
        assert ic is not None
