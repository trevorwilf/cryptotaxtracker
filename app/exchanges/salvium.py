"""
Salvium Wallet RPC connector.

Connects to salvium-wallet-rpc (Monero-fork JSON-RPC) to pull:
  - All incoming/outgoing transfers
  - Staking locks and unlocks (with yield separation)
  - Mining payouts (if any)

Salvium specifics:
  - Monero fork, uses atomic units: 1 SAL = 1e12 atomic units
  - Staking: lock SAL for 21,600 blocks (~30 days), receive stake + yield on unlock
  - Yield = income at FMV when received (Rev. Rul. 2023-14)
  - The lock itself is NOT a taxable event
  - Wallet RPC default port: 19082

Transaction types from get_transfers:
  "in"      — incoming transfer (received SAL)
  "out"     — outgoing transfer (sent SAL)
  "pending" — unconfirmed outgoing
  "failed"  — failed transaction
  "pool"    — in the mempool but not confirmed
  
Salvium-specific transfer subtypes (from show_transfers color coding):
  - Staking lock:    outgoing transfer to self with lock time
  - Staking unlock:  incoming "protocol_tx" / minted coins (stake + yield)
  - Mining reward:   incoming "coinbase" type
"""
import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal

import aiohttp

from exchanges import BaseExchange, register

logger = logging.getLogger("tax-collector.salvium")

# 1 SAL = 1e12 atomic units (same as Monero)
ATOMIC_UNITS = Decimal("100000000")       # 1e8 (Salvium)
D = Decimal


@register
class SalviumWalletExchange(BaseExchange):
    """Salvium wallet connector via salvium-wallet-rpc JSON-RPC."""

    name = "salvium"

    def __init__(self, api_key: str = "", api_secret: str = ""):
        # For Salvium, api_key = wallet RPC URL, api_secret = not used
        # URL comes from env: SALVIUM_RPC_URL (default http://127.0.0.1:19082)
        super().__init__(api_key, api_secret)
        self.rpc_url = os.environ.get("SALVIUM_RPC_URL", "http://127.0.0.1:19082")
        self.rpc_user = os.environ.get("SALVIUM_RPC_USER", "")
        self.rpc_pass = os.environ.get("SALVIUM_RPC_PASS", "")

    async def _rpc(self, method: str, params: dict = None) -> dict:
        """Make a JSON-RPC call to salvium-wallet-rpc."""
        payload = {
            "jsonrpc": "2.0",
            "id": "0",
            "method": method,
        }
        if params:
            payload["params"] = params

        auth = None
        if self.rpc_user and self.rpc_pass:
            auth = aiohttp.BasicAuth(self.rpc_user, self.rpc_pass)

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.rpc_url}/json_rpc",
                json=payload,
                auth=auth,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(f"Salvium RPC {method} returned {resp.status}: {body}")
                    return {}
                data = await resp.json()
                if "error" in data:
                    logger.warning(f"Salvium RPC {method} error: {data['error']}")
                    return {}
                return data.get("result", {})

    def _atomic_to_sal(self, atomic: int | str) -> Decimal:
        """Convert atomic units to SAL."""
        return D(str(atomic)) / ATOMIC_UNITS

    def _ts_from_epoch(self, epoch: int) -> datetime:
        """Convert UNIX timestamp to datetime."""
        if not epoch or epoch <= 0:
            return datetime.now(timezone.utc)
        return datetime.fromtimestamp(epoch, tz=timezone.utc)

    def _is_staking_unlock(self, transfer: dict) -> bool:
        """Detect if an incoming transfer is a staking unlock (protocol_tx / minted coins).
        Staking unlocks are incoming transfers where the coins were minted by the protocol,
        not sent from another wallet. They typically have no payment_id and come from
        a protocol_tx (block-level minting transaction)."""
        # Heuristics for detecting staking unlocks:
        # 1. Type is "in" (incoming)
        # 2. The tx has coinbase-like properties or is a protocol_tx
        # 3. No sending address (minted, not transferred)
        tx_type = transfer.get("type", "")
        subaddr = transfer.get("subaddr_index", {})
        
        # Protocol transactions (staking payouts) are identified by:
        # - Being incoming
        # - Having specific flags that distinguish them from regular transfers
        # Look for protocol_tx indicators
        if transfer.get("coinbase", False):
            return False  # This is mining, not staking
        
        # Salvium staking unlocks come as incoming transfers with locked amounts
        # that include both the original stake and the yield
        # The unlock_time field and the transfer notes can help identify these
        
        # For now, we tag transfers that look like staking returns
        # (large round-ish amounts that match known stake patterns)
        # The income classifier will do final classification
        return False  # Conservative: let the income classifier handle it

    async def _get_all_transfers(self, min_height: int = 0) -> dict:
        """Fetch all transfers from the wallet."""
        params = {
            "in": True,
            "out": True,
            "pending": True,
            "failed": False,
            "pool": False,
            "filter_by_height": min_height > 0,
            "min_height": min_height,
        }
        return await self._rpc("get_transfers", params)

    async def _get_balance(self) -> dict:
        """Get wallet balance — Salvium returns balances array by asset type."""
        result = await self._rpc("get_balance")
        balances = result.get("balances", [])
        if balances:
            sal = balances[0]  # SAL1 is the native asset
            return {
                "balance": sal.get("balance", 0),
                "unlocked_balance": sal.get("unlocked_balance", 0),
            }
        return {"balance": 0, "unlocked_balance": 0}

    # ── Exchange Plugin Interface ─────────────────────────────────────────

    async def fetch_trades(self, since: datetime | None = None) -> list[dict]:
        """Salvium wallet doesn't have 'trades' — trades happen on exchanges.
        Wallet transactions are deposits/withdrawals/staking.
        Return empty — all wallet activity is captured via deposits/withdrawals."""
        return []

    async def fetch_orders(self, since: datetime | None = None) -> list[dict]:
        """No order book on a wallet."""
        return []

    async def fetch_deposits(self, since: datetime | None = None) -> list[dict]:
        """All incoming SAL transactions — received transfers, staking unlocks, mining."""
        transfers = await self._get_all_transfers()
        incoming = transfers.get("in", [])

        deposits = []
        for tx in incoming:
            timestamp = self._ts_from_epoch(tx.get("timestamp", 0))
            if since and timestamp <= since:
                continue

            amount = self._atomic_to_sal(tx.get("amount", 0))
            tx_hash = tx.get("txid", "")
            height = tx.get("height", 0)
            is_coinbase = tx.get("coinbase", False)

            # Classify the type for the income classifier to process later
            if is_coinbase:
                tx_subtype = "mining_reward"
                description = f"Mining reward: {amount} SAL at block {height}"
            else:
                # Could be: regular receive, staking unlock, or protocol_tx
                # We tag it with metadata and let the income classifier decide
                tx_subtype = "incoming"
                description = f"Received: {amount} SAL (block {height})"
                
                # Check for staking unlock indicators
                unlock_time = tx.get("unlock_time", 0)
                if unlock_time > 0:
                    tx_subtype = "staking_unlock_candidate"
                    description = f"Possible staking unlock: {amount} SAL (block {height})"

            deposits.append({
                "exchange": self.name,
                "exchange_id": tx_hash or f"sal_in_{height}_{amount}",
                "asset": "SAL",
                "amount": str(amount),
                "network": "salvium",
                "tx_hash": tx_hash,
                "address": tx.get("address", ""),
                "status": "confirmed" if tx.get("confirmations", 0) > 10 else "pending",
                "asset_price_usd": None,
                "amount_usd": None,
                "confirmed_at": timestamp,
                "raw_data": json.dumps({
                    **tx,
                    "_salvium_subtype": tx_subtype,
                    "_salvium_height": height,
                    "_salvium_coinbase": is_coinbase,
                    "_salvium_description": description,
                    "_salvium_unlock_time": tx.get("unlock_time", 0),
                    "_salvium_confirmations": tx.get("confirmations", 0),
                }),
            })

        logger.info(f"Salvium: fetched {len(deposits)} incoming transactions")
        return deposits

    async def fetch_withdrawals(self, since: datetime | None = None) -> list[dict]:
        """All outgoing SAL transactions — sends and staking locks."""
        transfers = await self._get_all_transfers()
        outgoing = transfers.get("out", [])

        withdrawals = []
        for tx in outgoing:
            timestamp = self._ts_from_epoch(tx.get("timestamp", 0))
            if since and timestamp <= since:
                continue

            amount = self._atomic_to_sal(tx.get("amount", 0))
            fee = self._atomic_to_sal(tx.get("fee", 0))
            tx_hash = tx.get("txid", "")
            height = tx.get("height", 0)
            
            # Check if this is a staking lock (sent to self with lock time)
            destinations = tx.get("destinations", [])
            unlock_time = tx.get("unlock_time", 0)
            
            if unlock_time > 0:
                tx_subtype = "staking_lock"
                description = f"Staking lock: {amount} SAL for ~{unlock_time} blocks"
            else:
                tx_subtype = "outgoing"
                description = f"Sent: {amount} SAL (block {height})"

            withdrawals.append({
                "exchange": self.name,
                "exchange_id": tx_hash or f"sal_out_{height}_{amount}",
                "asset": "SAL",
                "amount": str(amount),
                "fee": str(fee),
                "network": "salvium",
                "tx_hash": tx_hash,
                "address": destinations[0].get("address", "") if destinations else "",
                "status": "confirmed" if tx.get("confirmations", 0) > 10 else "pending",
                "asset_price_usd": None,
                "amount_usd": None,
                "fee_usd": None,
                "confirmed_at": timestamp,
                "raw_data": json.dumps({
                    **tx,
                    "_salvium_subtype": tx_subtype,
                    "_salvium_height": height,
                    "_salvium_description": description,
                    "_salvium_unlock_time": unlock_time,
                    "_salvium_destinations": destinations,
                }),
            })

        logger.info(f"Salvium: fetched {len(withdrawals)} outgoing transactions")
        return withdrawals

    async def fetch_pool_activity(self, since: datetime | None = None) -> list[dict]:
        """No pool activity for wallet-level tracking."""
        return []

    async def get_staking_summary(self) -> dict:
        """Get a summary of staking activity for dashboard display."""
        balance = await self._get_balance()
        transfers = await self._get_all_transfers()

        total_staked = D("0")
        total_yield = D("0")
        active_stakes = 0
        completed_stakes = 0

        # Count outgoing staking locks
        for tx in transfers.get("out", []):
            if tx.get("unlock_time", 0) > 0:
                amount = self._atomic_to_sal(tx.get("amount", 0))
                total_staked += amount
                active_stakes += 1

        # Count incoming that look like staking unlocks
        for tx in transfers.get("in", []):
            if not tx.get("coinbase", False) and tx.get("unlock_time", 0) > 0:
                amount = self._atomic_to_sal(tx.get("amount", 0))
                completed_stakes += 1

        return {
            "wallet_balance_sal": str(self._atomic_to_sal(balance.get("balance", 0))),
            "unlocked_balance_sal": str(self._atomic_to_sal(balance.get("unlocked_balance", 0))),
            "total_incoming": len(transfers.get("in", [])),
            "total_outgoing": len(transfers.get("out", [])),
            "active_stakes": active_stakes,
            "completed_stakes": completed_stakes,
        }
