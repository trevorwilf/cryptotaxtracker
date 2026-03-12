"""
Salvium Wallet RPC connector.

Connects to salvium-wallet-rpc (Monero-fork JSON-RPC) to pull:
  - All incoming/outgoing transfers
  - Staking locks and unlocks (with yield separation)
  - Mining payouts (if any)

Salvium specifics:
  - Monero fork, uses atomic units: 1 SAL = 1e8 atomic units
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

Staking lock detection (from real on-chain data):
  - Staking locks have amount=0, with the staked SAL in the "fee" field
  - Normal fees are < 0.1 SAL (< 10,000,000 atomic). Staking "fees" are >> 1 SAL
  - No destinations and zero payment_id

Salvium-specific transfer subtypes:
  - Staking lock:    outgoing with amount=0, huge fee = staked amount
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

# 1 SAL = 1e8 atomic units (Salvium uses 8 decimal places, NOT Monero's 12)
ATOMIC_UNITS = Decimal("100000000")
D = Decimal

# Normal network fees are < 0.1 SAL. Any "fee" above this threshold in an
# amount=0 outgoing tx is actually a staking lock (staked SAL in fee field).
NORMAL_FEE_THRESHOLD = 100_000_000  # 1 SAL in atomic units


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
                if "error" in data and data["error"]:
                    logger.warning(f"Salvium RPC {method} error: {data['error']}")
                    return {"error": data["error"]}
                return data.get("result", {})

    def _atomic_to_sal(self, atomic: int | str) -> Decimal:
        """Convert atomic units to SAL."""
        return D(str(atomic)) / ATOMIC_UNITS

    def _ts_from_epoch(self, epoch: int) -> datetime:
        """Convert UNIX timestamp to datetime."""
        if not epoch or epoch <= 0:
            return datetime.now(timezone.utc)
        return datetime.fromtimestamp(epoch, tz=timezone.utc)

    async def _get_all_transfers(self, min_height: int = 0) -> dict:
        """Fetch all transfers from ALL accounts in the wallet.

        The wallet may have multiple accounts (e.g. "Primary", "consolidate").
        We use get_accounts to discover them, then query each one.
        """
        accounts_result = await self._rpc("get_accounts")
        accounts = accounts_result.get("subaddress_accounts", [])

        # Fallback: if get_accounts returns nothing, query account 0
        if not accounts:
            accounts = [{"account_index": 0, "label": "Primary"}]

        all_in = []
        all_out = []
        all_pending = []

        for acct in accounts:
            acct_idx = acct.get("account_index", 0)
            acct_label = acct.get("label", "")
            params = {
                "in": True,
                "out": True,
                "pending": True,
                "failed": False,
                "pool": False,
                "account_index": acct_idx,
                "filter_by_height": min_height > 0,
                "min_height": min_height,
            }
            result = await self._rpc("get_transfers", params)

            for tx in result.get("in", []):
                tx["_account_index"] = acct_idx
                tx["_account_label"] = acct_label
                all_in.append(tx)
            for tx in result.get("out", []):
                tx["_account_index"] = acct_idx
                tx["_account_label"] = acct_label
                all_out.append(tx)
            for tx in result.get("pending", []):
                tx["_account_index"] = acct_idx
                tx["_account_label"] = acct_label
                all_pending.append(tx)

        return {"in": all_in, "out": all_out, "pending": all_pending}

    async def _get_balance(self) -> dict:
        """Get total wallet balance across all accounts."""
        result = await self._rpc("get_accounts")
        return {
            "balance": result.get("total_balance", 0),
            "unlocked_balance": result.get("total_unlocked_balance", 0),
            "accounts": result.get("subaddress_accounts", []),
        }

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
                    "_salvium_account_index": tx.get("_account_index", 0),
                    "_salvium_account_label": tx.get("_account_label", ""),
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

            tx_hash = tx.get("txid", "")
            height = tx.get("height", 0)
            destinations = tx.get("destinations", [])

            # Detect staking lock: amount=0, huge "fee" = the staked amount
            fee_atomic = int(tx.get("fee", 0))
            amount_atomic = int(tx.get("amount", 0))
            has_destinations = bool(destinations)

            if amount_atomic == 0 and fee_atomic > NORMAL_FEE_THRESHOLD and not has_destinations:
                # This is a staking lock — the "fee" is actually the staked amount
                tx_subtype = "staking_lock"
                amount = self._atomic_to_sal(fee_atomic)
                fee = D("0")  # The real network fee is absorbed into the staking tx
                description = f"Staking lock: {amount} SAL at block {height}"
            else:
                # Normal outgoing transfer
                amount = self._atomic_to_sal(amount_atomic)
                fee = self._atomic_to_sal(fee_atomic)
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
                    "_salvium_unlock_time": tx.get("unlock_time", 0),
                    "_salvium_destinations": destinations,
                    "_salvium_account_index": tx.get("_account_index", 0),
                    "_salvium_account_label": tx.get("_account_label", ""),
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

        total_bal = self._atomic_to_sal(balance.get("balance", 0))
        unlocked_bal = self._atomic_to_sal(balance.get("unlocked_balance", 0))
        locked_bal = total_bal - unlocked_bal

        total_staked = D("0")
        active_stakes = 0
        completed_stakes = 0

        # Count outgoing staking locks using the corrected detection logic
        for tx in transfers.get("out", []):
            fee_atomic = int(tx.get("fee", 0))
            amount_atomic = int(tx.get("amount", 0))
            has_destinations = bool(tx.get("destinations", []))
            unlock_time = tx.get("unlock_time", 0)

            is_staking_lock = (
                (amount_atomic == 0 and fee_atomic > NORMAL_FEE_THRESHOLD and not has_destinations)
                or unlock_time > 0
            )

            if is_staking_lock:
                if amount_atomic == 0 and fee_atomic > NORMAL_FEE_THRESHOLD:
                    staked = self._atomic_to_sal(fee_atomic)
                else:
                    staked = self._atomic_to_sal(amount_atomic)
                total_staked += staked
                active_stakes += 1

        # Count incoming that look like staking unlocks
        for tx in transfers.get("in", []):
            if not tx.get("coinbase", False) and tx.get("unlock_time", 0) > 0:
                completed_stakes += 1

        return {
            "wallet_balance_sal": str(total_bal),
            "unlocked_balance_sal": str(unlocked_bal),
            "locked_balance_sal": str(locked_bal),
            "total_incoming": len(transfers.get("in", [])),
            "total_outgoing": len(transfers.get("out", [])),
            "total_staked_sal": str(total_staked),
            "active_stakes": active_stakes,
            "completed_stakes": completed_stakes,
        }
