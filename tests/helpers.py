"""
Shared test helpers — factories, mocks, timestamps.

Importable by both conftest.py (for fixtures) and test files directly.
"""
import json
from datetime import datetime, timezone
from decimal import Decimal

D = Decimal

# ── Timestamps ────────────────────────────────────────────────────────────

T_2024_01 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
T_2024_06 = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
T_2025_01 = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
T_2025_03 = datetime(2025, 3, 10, 12, 0, 0, tzinfo=timezone.utc)
T_2025_06 = datetime(2025, 6, 20, 12, 0, 0, tzinfo=timezone.utc)
T_2025_09 = datetime(2025, 9, 5, 12, 0, 0, tzinfo=timezone.utc)


# ── Mock Settings ─────────────────────────────────────────────────────────

class MockSettings:
    def __init__(self):
        self.database_url = "postgresql+asyncpg://test:test@localhost:5432/test_tax"
        self.sync_cron = "0 3 * * *"
        self.export_dir = "/tmp/test_exports"
        self.enabled_exchanges = ["nonkyc", "mexc"]
        self.nonkyc_api_key = "test_nonkyc_key"
        self.nonkyc_api_secret = "test_nonkyc_secret"
        self.mexc_api_key = "test_mexc_key"
        self.mexc_api_secret = "test_mexc_secret"


# ── Mock DB Session ───────────────────────────────────────────────────────

class MockResult:
    def __init__(self, rows=None, columns=None, scalar_val=None):
        self._rows = rows or []
        self._columns = columns or []
        self._scalar = scalar_val

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar

    def keys(self):
        return self._columns


class MockSession:
    def __init__(self):
        self.executed_sql: list[str] = []
        self.executed_params: list[dict] = []
        self._staged_results: list[MockResult] = []
        self._default_result = MockResult()
        self.committed = False

    def stage_result(self, result: MockResult):
        self._staged_results.append(result)

    def stage_scalar(self, value):
        self._staged_results.append(MockResult(scalar_val=value))

    def stage_rows(self, rows, columns=None):
        self._staged_results.append(MockResult(rows=rows, columns=columns))

    async def execute(self, stmt, params=None):
        sql_text = str(stmt) if hasattr(stmt, 'text') else str(stmt)
        self.executed_sql.append(sql_text)
        self.executed_params.append(params or {})
        if self._staged_results:
            return self._staged_results.pop(0)
        return self._default_result

    async def commit(self):
        self.committed = True

    async def rollback(self):
        pass

    def get_sql_containing(self, substring: str) -> list[str]:
        return [s for s in self.executed_sql if substring.lower() in s.lower()]


# ── Data Factories ────────────────────────────────────────────────────────

def make_trade(exchange="nonkyc", market="BTC/USDT", side="buy",
               price="50000", quantity="0.5", total="25000",
               fee="25", fee_asset="USDT", total_usd="25000",
               fee_usd="25", executed_at=None, trade_id=None):
    return {
        "exchange": exchange,
        "exchange_id": trade_id or f"trade_{id(exchange)}_{side}",
        "market": market,
        "base_asset": market.split("/")[0] if "/" in market else None,
        "quote_asset": market.split("/")[1] if "/" in market else None,
        "side": side,
        "price": price,
        "quantity": quantity,
        "total": total,
        "fee": fee,
        "fee_asset": fee_asset,
        "price_usd": price if "USDT" in market else None,
        "quantity_usd": total_usd,
        "total_usd": total_usd,
        "fee_usd": fee_usd,
        "base_price_usd": price,
        "quote_price_usd": "1.0",
        "executed_at": executed_at or T_2025_03,
        "raw_data": json.dumps({"test": True}),
    }


def make_deposit(exchange="nonkyc", asset="BTC", amount="1.0",
                 amount_usd="50000", confirmed_at=None, dep_id=None):
    return {
        "exchange": exchange,
        "exchange_id": dep_id or f"dep_{asset}_{id(exchange)}",
        "asset": asset,
        "amount": amount,
        "network": "BTC",
        "tx_hash": f"0xabc{asset}",
        "address": "addr123",
        "status": "completed",
        "asset_price_usd": str(D(amount_usd) / D(amount)) if D(amount) > 0 else "0",
        "amount_usd": amount_usd,
        "confirmed_at": confirmed_at or T_2025_03,
        "raw_data": json.dumps({"test": True}),
    }


def make_withdrawal(exchange="nonkyc", asset="BTC", amount="1.0",
                    fee="0.0001", amount_usd="50000", confirmed_at=None, wd_id=None):
    return {
        "exchange": exchange,
        "exchange_id": wd_id or f"wd_{asset}_{id(exchange)}",
        "asset": asset,
        "amount": amount,
        "fee": fee,
        "network": "BTC",
        "tx_hash": f"0xdef{asset}",
        "address": "addr456",
        "status": "completed",
        "asset_price_usd": str(D(amount_usd) / D(amount)) if D(amount) > 0 else "0",
        "amount_usd": amount_usd,
        "fee_usd": str(D(fee) * (D(amount_usd) / D(amount))) if D(amount) > 0 else "0",
        "confirmed_at": confirmed_at or T_2025_03,
        "raw_data": json.dumps({"test": True}),
    }
