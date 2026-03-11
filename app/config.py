"""
Configuration — environment-based settings for Tax Collector.
"""
import os


class Settings:
    """Reads configuration from environment variables."""

    def __init__(self):
        self.database_url = os.environ.get(
            "TAX_DATABASE_URL",
            "postgresql+asyncpg://hbot:password@127.0.0.1:5432/hummingbot_api",
        )
        self.sync_cron = os.environ.get("TAX_SYNC_CRON", "0 3 * * *")
        self.export_dir = os.environ.get("TAX_EXPORT_DIR", "/data/exports")

        # Enabled exchanges (comma-separated)
        exchanges_str = os.environ.get("TAX_EXCHANGES", "nonkyc,mexc")
        self.enabled_exchanges = [
            e.strip().lower() for e in exchanges_str.split(",") if e.strip()
        ]

        # Exchange credentials
        self.nonkyc_api_key = os.environ.get("NONKYC_API_KEY", "")
        self.nonkyc_api_secret = os.environ.get("NONKYC_API_SECRET", "")
        self.mexc_api_key = os.environ.get("MEXC_API_KEY", "")
        self.mexc_api_secret = os.environ.get("MEXC_API_SECRET", "")

        # Salvium wallet RPC (not API-key based — uses RPC URL/user/pass)
        self.salvium_rpc_url = os.environ.get("SALVIUM_RPC_URL", "http://127.0.0.1:19082")
        self.salvium_rpc_user = os.environ.get("SALVIUM_RPC_USER", "")
        self.salvium_rpc_pass = os.environ.get("SALVIUM_RPC_PASS", "")
        # Dummy credentials so get_exchange() doesn't skip salvium
        self.salvium_api_key = os.environ.get("SALVIUM_API_KEY", "wallet_rpc")
        self.salvium_api_secret = os.environ.get("SALVIUM_API_SECRET", "unused")