"""
Tests for the Price Oracle module.

Covers:
  - Stablecoin shortcut ($1.00)
  - Ticker normalization
  - Market symbol parsing (BTC/USDT, BTC_USDT, BTCUSDT)
  - CoinGecko ID mapping
  - USD resolution for trades and transfers
  - Cache hit/miss behavior
"""
import pytest
from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch, MagicMock

from price_oracle import PriceOracle, STABLECOINS, TICKER_TO_COINGECKO

D = Decimal
T = datetime(2025, 3, 10, 12, 0, 0, tzinfo=timezone.utc)


class TestTickerNormalization:
    """Test PriceOracle._normalize_ticker()"""

    def test_basic_ticker(self):
        assert PriceOracle._normalize_ticker("btc") == "BTC"

    def test_whitespace(self):
        assert PriceOracle._normalize_ticker("  eth  ") == "ETH"

    def test_empty_string(self):
        assert PriceOracle._normalize_ticker("") == ""

    def test_already_upper(self):
        assert PriceOracle._normalize_ticker("SOL") == "SOL"

    def test_mixed_case(self):
        assert PriceOracle._normalize_ticker("uSdT") == "USDT"


class TestMarketParsing:
    """Test PriceOracle._parse_market()"""

    def test_slash_separator(self):
        assert PriceOracle._parse_market("BTC/USDT") == ("BTC", "USDT")

    def test_underscore_separator(self):
        assert PriceOracle._parse_market("ETH_USDT") == ("ETH", "USDT")

    def test_dash_separator(self):
        assert PriceOracle._parse_market("SOL-USDC") == ("SOL", "USDC")

    def test_no_separator_usdt(self):
        assert PriceOracle._parse_market("BTCUSDT") == ("BTC", "USDT")

    def test_no_separator_usdc(self):
        assert PriceOracle._parse_market("ETHUSDC") == ("ETH", "USDC")

    def test_no_separator_btc_pair(self):
        assert PriceOracle._parse_market("ETHBTC") == ("ETH", "BTC")

    def test_lowercase_input(self):
        assert PriceOracle._parse_market("btc/usdt") == ("BTC", "USDT")

    def test_whitespace(self):
        assert PriceOracle._parse_market("  BTC / USDT  ") == ("BTC", "USDT")

    def test_unknown_no_separator(self):
        base, quote = PriceOracle._parse_market("ABCXYZ")
        # Should return something reasonable
        assert isinstance(base, str)
        assert isinstance(quote, str)


class TestStablecoins:
    """Test stablecoin shortcut returns $1.00."""

    @pytest.mark.parametrize("coin", ["USDT", "USDC", "DAI", "BUSD", "TUSD", "USD"])
    @pytest.mark.asyncio
    async def test_stablecoin_returns_one(self, coin, mock_session):
        oracle = PriceOracle()
        price = await oracle.get_usd_price(mock_session, coin, T)
        assert price == D("1.0")

    @pytest.mark.asyncio
    async def test_stablecoin_no_db_call(self, mock_session):
        oracle = PriceOracle()
        await oracle.get_usd_price(mock_session, "USDT", T)
        # Should not have hit the DB at all
        assert len(mock_session.executed_sql) == 0


class TestCoinGeckoMapping:
    """Test that common tickers map to CoinGecko IDs."""

    def test_btc_mapping(self):
        assert TICKER_TO_COINGECKO["BTC"] == "bitcoin"

    def test_eth_mapping(self):
        assert TICKER_TO_COINGECKO["ETH"] == "ethereum"

    def test_sol_mapping(self):
        assert TICKER_TO_COINGECKO["SOL"] == "solana"

    def test_major_coins_present(self):
        required = ["BTC", "ETH", "SOL", "ADA", "DOT", "LINK", "AVAX", "MATIC"]
        for coin in required:
            assert coin in TICKER_TO_COINGECKO, f"{coin} missing from CoinGecko mapping"


class TestTradeUsdResolution:
    """Test resolve_trade_usd with mocked prices."""

    @pytest.mark.asyncio
    async def test_usdt_pair_no_external_calls(self, mock_session):
        """A BTC/USDT trade: quote is stablecoin, only base needs price lookup."""
        oracle = PriceOracle()

        # Mock: DB cache miss for BTC, then CoinGecko returns 50000
        mock_session.stage_rows([])  # cache miss
        with patch.object(oracle, '_fetch_coingecko', return_value=D("50000")):
            mock_session.stage_result(MagicMock())  # cache write
            result = await oracle.resolve_trade_usd(
                mock_session, "BTC/USDT", "buy",
                "50000", "0.5", "25000", "25", "USDT", T
            )

        assert result["base_asset"] == "BTC"
        assert result["quote_asset"] == "USDT"
        assert result["quote_price_usd"] == "1.0"

    @pytest.mark.asyncio
    async def test_zero_values_no_crash(self, mock_session):
        """Ensure zero/empty values don't cause division errors."""
        oracle = PriceOracle()
        result = await oracle.resolve_trade_usd(
            mock_session, "BTC/USDT", "buy",
            "0", "0", "0", "0", "", T
        )
        assert result["base_asset"] == "BTC"
        # Should not raise


class TestUSTRemoval:
    def test_ust_not_in_stablecoins(self):
        from price_oracle import STABLECOINS
        assert "UST" not in STABLECOINS

    def test_other_stablecoins_still_present(self):
        from price_oracle import STABLECOINS
        for coin in ["USDT", "USDC", "DAI", "BUSD"]:
            assert coin in STABLECOINS


class TestTransferUsdResolution:
    """Test resolve_transfer_usd."""

    @pytest.mark.asyncio
    async def test_stablecoin_transfer(self, mock_session):
        oracle = PriceOracle()
        result = await oracle.resolve_transfer_usd(
            mock_session, "USDT", "1000", None, T
        )
        assert result["asset_price_usd"] == "1.0"
        assert result["amount_usd"] == "1000.0"

    @pytest.mark.asyncio
    async def test_transfer_with_fee(self, mock_session):
        oracle = PriceOracle()
        result = await oracle.resolve_transfer_usd(
            mock_session, "USDT", "1000", "5", T
        )
        assert result["fee_usd"] == "5.0"
