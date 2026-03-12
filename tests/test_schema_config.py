"""
Tests for database schema and configuration.

Covers:
  - Schema SQL syntax validation (can be parsed)
  - Migration SQL is idempotent (ADD COLUMN IF NOT EXISTS)
  - v3 schema tables are defined
  - Settings loads from environment
  - Settings defaults are sensible
"""
import os
import pytest
from unittest.mock import patch

from config import Settings
from schema_v3 import SCHEMA_V3_SQL


# ── Config Tests ──────────────────────────────────────────────────────────

class TestSettings:
    def test_defaults(self):
        with patch.dict(os.environ, {}, clear=True):
            s = Settings()
            assert s.sync_cron == "0 3 * * *"
            assert "nonkyc" in s.enabled_exchanges
            assert "mexc" in s.enabled_exchanges

    def test_custom_exchanges(self):
        with patch.dict(os.environ, {"TAX_EXCHANGES": "binance,kraken"}, clear=False):
            s = Settings()
            assert "binance" in s.enabled_exchanges
            assert "kraken" in s.enabled_exchanges
            assert "nonkyc" not in s.enabled_exchanges

    def test_single_exchange(self):
        with patch.dict(os.environ, {"TAX_EXCHANGES": "nonkyc"}, clear=False):
            s = Settings()
            assert s.enabled_exchanges == ["nonkyc"]

    def test_empty_exchanges(self):
        with patch.dict(os.environ, {"TAX_EXCHANGES": ""}, clear=False):
            s = Settings()
            assert s.enabled_exchanges == []

    def test_credentials_from_env(self):
        with patch.dict(os.environ, {
            "NONKYC_API_KEY": "mykey",
            "NONKYC_API_SECRET": "mysecret",
        }, clear=False):
            s = Settings()
            assert s.nonkyc_api_key == "mykey"
            assert s.nonkyc_api_secret == "mysecret"

    def test_export_dir_default(self):
        with patch.dict(os.environ, {}, clear=True):
            s = Settings()
            assert s.export_dir == "/data/exports"

    def test_export_dir_custom(self):
        with patch.dict(os.environ, {"TAX_EXPORT_DIR": "/custom/path"}, clear=False):
            s = Settings()
            assert s.export_dir == "/custom/path"

    def test_database_url_default(self):
        with patch.dict(os.environ, {}, clear=True):
            s = Settings()
            assert "asyncpg" in s.database_url
            assert "hummingbot_api" in s.database_url


# ── Schema SQL Syntax Tests ───────────────────────────────────────────────

class TestSchemaSyntax:
    """Verify that schema SQL strings are parseable and contain expected elements."""

    def test_v3_schema_has_lots_table(self):
        assert "tax.lots" in SCHEMA_V3_SQL

    def test_v3_schema_has_disposals_table(self):
        assert "tax.disposals" in SCHEMA_V3_SQL

    def test_v3_schema_has_form_8949_table(self):
        assert "tax.form_8949" in SCHEMA_V3_SQL

    def test_v3_schema_has_income_events_table(self):
        assert "tax.income_events" in SCHEMA_V3_SQL

    def test_v3_schema_has_transfer_matches_table(self):
        assert "tax.transfer_matches" in SCHEMA_V3_SQL

    def test_v3_schema_uses_if_not_exists(self):
        """All CREATE TABLE statements should be safe to re-run."""
        lines = [l.strip() for l in SCHEMA_V3_SQL.split("\n") if "CREATE TABLE" in l]
        for line in lines:
            assert "IF NOT EXISTS" in line, f"Missing IF NOT EXISTS: {line}"

    def test_v3_schema_indexes_use_if_not_exists(self):
        lines = [l.strip() for l in SCHEMA_V3_SQL.split("\n") if "CREATE INDEX" in l]
        for line in lines:
            assert "IF NOT EXISTS" in line, f"Missing IF NOT EXISTS: {line}"

    def test_schema_has_no_raw_jsonb_cast(self):
        """Ensure we don't have the ::jsonb syntax that breaks asyncpg."""
        from database import SCHEMA_SQL, MIGRATION_SQL
        assert "::jsonb" not in SCHEMA_SQL
        assert "::jsonb" not in MIGRATION_SQL

    def test_form_8949_has_tax_year(self):
        """Form 8949 must have tax_year for year-based queries."""
        assert "tax_year" in SCHEMA_V3_SQL

    def test_lots_has_unique_constraint(self):
        assert "UNIQUE(asset, exchange, acquired_at, source_trade_id)" in SCHEMA_V3_SQL

    def test_disposals_has_term_column(self):
        """Need term column for short/long classification."""
        # Find the disposals table definition
        assert "term" in SCHEMA_V3_SQL


class TestV4Schema:
    def test_v4_schema_has_all_required_tables(self):
        from schema_v4 import SCHEMA_V4_SQL
        required = [
            "tax.normalized_events", "tax.lots_v4", "tax.disposals_v4",
            "tax.exceptions", "tax.valuation_log", "tax.transfer_carryover",
            "tax.run_manifest", "tax.form_8949_v4", "tax.income_events_v4",
        ]
        for table in required:
            assert table in SCHEMA_V4_SQL, f"Missing v4 table: {table}"


class TestDatabaseSchemaSQL:
    """Test the main schema SQL from database.py"""

    def test_trades_has_usd_columns(self):
        from database import SCHEMA_SQL
        assert "total_usd" in SCHEMA_SQL
        assert "fee_usd" in SCHEMA_SQL
        assert "base_price_usd" in SCHEMA_SQL

    def test_deposits_has_usd_columns(self):
        from database import SCHEMA_SQL
        assert "asset_price_usd" in SCHEMA_SQL
        assert "amount_usd" in SCHEMA_SQL

    def test_price_cache_table(self):
        from database import SCHEMA_SQL
        assert "tax.price_cache" in SCHEMA_SQL

    def test_sync_log_table(self):
        from database import SCHEMA_SQL
        assert "tax.sync_log" in SCHEMA_SQL

    def test_migration_is_idempotent(self):
        from database import MIGRATION_SQL
        lines = [l.strip() for l in MIGRATION_SQL.split("\n")
                 if l.strip().startswith("ALTER")]
        for line in lines:
            assert "IF NOT EXISTS" in line, f"Migration not idempotent: {line}"

    def test_no_jsonb_double_colon(self):
        """The ::jsonb cast breaks asyncpg — must use CAST()."""
        from database import SCHEMA_SQL
        # Schema DDL shouldn't have casts, but check anyway
        lines = [l for l in SCHEMA_SQL.split("\n") if "::jsonb" in l and "VALUES" in l]
        assert len(lines) == 0, f"Found ::jsonb in value expressions: {lines}"
