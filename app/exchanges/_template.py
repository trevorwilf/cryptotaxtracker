"""
Exchange plugin template.

To add a new exchange:
  1. Copy this file to <exchange_name>.py
  2. Implement all 5 fetch methods
  3. Add credentials to .env:
       <NAME>_API_KEY=...
       <NAME>_API_SECRET=...
  4. Add the exchange name to TAX_EXCHANGES env var
  5. Rebuild + restart the container
"""
import json
import logging
from datetime import datetime, timezone

from exchanges import BaseExchange, register

logger = logging.getLogger("tax-collector.template")


# Uncomment @register and rename the class to enable this plugin
# @register
class TemplateExchange(BaseExchange):
    name = "template"  # must match the name in TAX_EXCHANGES

    def _parse_ts(self, val) -> datetime:
        """Parse a timestamp value from the API into a datetime."""
        if val is None:
            return datetime.now(timezone.utc)
        if isinstance(val, (int, float)):
            return datetime.fromtimestamp(val / 1000, tz=timezone.utc)
        if isinstance(val, str):
            try:
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            except ValueError:
                return datetime.now(timezone.utc)
        return datetime.now(timezone.utc)

    async def fetch_trades(self, since: datetime | None = None) -> list[dict]:
        """Pull trade history. Each trade dict must include at minimum:
        exchange, exchange_id, market, side, price, quantity, total, fee,
        fee_asset, executed_at, raw_data. USD fields start as None."""
        return []

    async def fetch_orders(self, since: datetime | None = None) -> list[dict]:
        """Pull order history. Each order dict must include:
        exchange, exchange_id, market, side, order_type, price, quantity,
        executed_qty, status, created_at_ex, updated_at_ex, raw_data.
        USD fields start as None."""
        return []

    async def fetch_deposits(self, since: datetime | None = None) -> list[dict]:
        """Pull deposit history. Each deposit dict must include:
        exchange, exchange_id, asset, amount, network, tx_hash, address,
        status, confirmed_at, raw_data. USD fields start as None."""
        return []

    async def fetch_withdrawals(self, since: datetime | None = None) -> list[dict]:
        """Pull withdrawal history. Each withdrawal dict must include:
        exchange, exchange_id, asset, amount, fee, network, tx_hash,
        address, status, confirmed_at, raw_data. USD fields start as None."""
        return []

    async def fetch_pool_activity(self, since: datetime | None = None) -> list[dict]:
        """Pull liquidity pool activity. Each pool dict must include:
        exchange, exchange_id, pool_name, action, asset_in, amount_in,
        asset_out, amount_out, fee, fee_asset, executed_at, raw_data.
        USD fields start as None."""
        return []
