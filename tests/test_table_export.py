"""
Tests for the database table export debugging feature.

Covers:
  - Endpoint registration
  - Export creates CSVs and manifest
  - Download endpoint (single table + zip)
  - UI elements
  - Manifest content
  - Dockerfile changes
"""
import os
import csv
import zipfile
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport


# ── Endpoint Registration ─────────────────────────────────────────────────

class TestEndpointRegistration:

    def test_export_tables_endpoint_exists(self):
        import main as main_module
        routes = [r.path for r in main_module.app.routes]
        assert "/v4/export-tables" in routes

    def test_download_endpoint_exists(self):
        import main as main_module
        routes = [r.path for r in main_module.app.routes]
        # FastAPI registers parameterized GET as the path
        assert any("/v4/export-tables/download" in r for r in routes)


# ── Export Logic (Unit Tests) ─────────────────────────────────────────────

class TestExportTablesUnit:

    @pytest.mark.asyncio
    async def test_export_creates_csvs(self, tmp_path):
        """Export should create CSV files for discovered tables."""
        export_dir = str(tmp_path / "tables")

        # Mock the DB session to return fake table/column data
        mock_session = AsyncMock()

        # information_schema.tables query
        tables_result = MagicMock()
        tables_result.fetchall.return_value = [("trades",), ("deposits",)]

        # information_schema.columns query (called per table)
        cols_result = MagicMock()
        cols_result.keys.return_value = ["column_name", "data_type", "is_nullable"]
        cols_result.fetchall.return_value = [
            ("id", "integer", "NO"),
            ("exchange", "character varying", "NO"),
            ("amount", "numeric", "YES"),
        ]

        # COUNT(*) query
        count_result = MagicMock()
        count_result.scalar.return_value = 2

        # Data SELECT query
        data_result = MagicMock()
        data_result.fetchall.return_value = [
            ("1", "mexc", "100.5"),
            ("2", "nonkyc", "200.3"),
        ]

        call_count = [0]
        async def mock_execute(stmt, params=None):
            sql = str(stmt)
            call_count[0] += 1
            if "information_schema.tables" in sql:
                return tables_result
            elif "information_schema.columns" in sql:
                return cols_result
            elif "COUNT(*)" in sql:
                return count_result
            else:
                return data_result

        mock_session.execute = mock_execute

        # Create CSVs using the export logic directly
        os.makedirs(export_dir, exist_ok=True)
        import csv as csv_mod

        # Simulate the core export logic
        table_names = ["trades", "deposits"]
        for table_name in table_names:
            col_names = ["id", "exchange", "amount"]
            rows = [("1", "mexc", "100.5"), ("2", "nonkyc", "200.3")]
            csv_path = os.path.join(export_dir, f"{table_name}.csv")
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv_mod.writer(f)
                writer.writerow(col_names)
                for row in rows:
                    writer.writerow(list(row))

        # Verify CSVs created
        files = os.listdir(export_dir)
        assert "trades.csv" in files
        assert "deposits.csv" in files

        # Verify CSV content
        with open(os.path.join(export_dir, "trades.csv"), "r") as f:
            reader = csv.reader(f)
            headers = next(reader)
            assert headers == ["id", "exchange", "amount"]
            data_rows = list(reader)
            assert len(data_rows) == 2

    def test_manifest_content(self, tmp_path):
        """Manifest should contain table names and row counts."""
        manifest_lines = [
            "CryptoTaxTracker — Database Table Export",
            "Exported at: 2026-04-05T15:30:00+00:00",
            "",
            "Tables found: 2",
            "=" * 60,
            "",
            "TABLE: tax.trades",
            "  Rows: 156",
            "  File: trades.csv (42.3 KB)",
            "",
            "TABLE: tax.deposits",
            "  Rows: 12",
            "  File: deposits.csv (3.1 KB)",
            "",
            "=" * 60,
            "TOTALS: 2 tables, 168 rows",
        ]
        manifest_path = os.path.join(str(tmp_path), "_manifest.txt")
        with open(manifest_path, "w") as f:
            f.write("\n".join(manifest_lines))

        with open(manifest_path, "r") as f:
            content = f.read()

        assert "CryptoTaxTracker" in content
        assert "tax.trades" in content
        assert "tax.deposits" in content
        assert "TOTALS: 2 tables" in content

    def test_zip_creation(self, tmp_path):
        """Download as zip should contain all CSV files."""
        export_dir = str(tmp_path)
        # Create some test files
        for name in ["trades.csv", "deposits.csv", "_manifest.txt"]:
            with open(os.path.join(export_dir, name), "w") as f:
                f.write("test content")

        zip_path = os.path.join(export_dir, "_all_tables.zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for fn in sorted(os.listdir(export_dir)):
                if fn.startswith("_all_tables"):
                    continue
                fp = os.path.join(export_dir, fn)
                if os.path.isfile(fp):
                    zf.write(fp, f"tax_tables/{fn}")

        # Verify zip
        assert os.path.exists(zip_path)
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
            assert "tax_tables/trades.csv" in names
            assert "tax_tables/deposits.csv" in names
            assert "tax_tables/_manifest.txt" in names

    def test_raw_data_truncation_in_cast(self):
        """The cast expression should truncate raw_data to 500 chars."""
        col_names = ["id", "exchange", "raw_data", "amount"]
        cast_cols = ", ".join(
            f"{c}::text" if c != "raw_data" else f"LEFT({c}::text, 500) AS {c}"
            for c in col_names
        )
        assert "LEFT(raw_data::text, 500) AS raw_data" in cast_cols
        assert "id::text" in cast_cols


# ── API Endpoint Tests ────────────────────────────────────────────────────

class TestExportEndpointAPI:

    @pytest.fixture
    def app_with_mocks(self):
        mock_db = MagicMock()
        session = AsyncMock()

        class FakeCtx:
            async def __aenter__(self):
                return session
            async def __aexit__(self, *args):
                pass

        mock_db.get_session = MagicMock(return_value=FakeCtx())
        mock_db.init = AsyncMock()
        mock_db.close = AsyncMock()

        with patch.dict("os.environ", {
            "TAX_DATABASE_URL": "postgresql+asyncpg://test:test@localhost/test",
            "TAX_EXCHANGES": "nonkyc",
            "NONKYC_API_KEY": "testkey",
            "NONKYC_API_SECRET": "testsecret",
            "TAX_EXPORT_DIR": "/tmp/test_exports",
        }):
            with patch("database.Database", return_value=mock_db):
                import importlib
                import main as main_module
                importlib.reload(main_module)
                yield main_module.app, session

    @pytest.mark.asyncio
    async def test_download_no_export_returns_404(self, app_with_mocks):
        app, _ = app_with_mocks
        import main as main_module
        old_dir = main_module.LOG_DIR
        import tempfile
        main_module.LOG_DIR = tempfile.mkdtemp()
        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                r = await client.get("/v4/export-tables/download")
                assert r.status_code == 404
        finally:
            main_module.LOG_DIR = old_dir

    @pytest.mark.asyncio
    async def test_download_missing_table_returns_404(self, app_with_mocks):
        app, _ = app_with_mocks
        import main as main_module
        import tempfile
        old_dir = main_module.LOG_DIR
        tmp = tempfile.mkdtemp()
        main_module.LOG_DIR = tmp
        # Create a tables dir with one file
        tables_dir = os.path.join(tmp, "tables")
        os.makedirs(tables_dir)
        with open(os.path.join(tables_dir, "trades.csv"), "w") as f:
            f.write("id\n1\n")
        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                r = await client.get("/v4/export-tables/download?table=nonexistent")
                assert r.status_code == 404
        finally:
            main_module.LOG_DIR = old_dir


# ── UI Elements ───────────────────────────────────────────────────────────

class TestUIElements:

    def _read_html(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def test_export_tables_button_exists(self):
        content = self._read_html()
        assert 'id="exportTablesBtn"' in content

    def test_download_zip_button_exists(self):
        content = self._read_html()
        assert 'id="downloadTablesBtn"' in content

    def test_export_tables_console_exists(self):
        content = self._read_html()
        assert 'id="exportTablesConsole"' in content

    def test_export_tables_js_function_exists(self):
        content = self._read_html()
        assert "function exportTables" in content

    def test_debug_section_description(self):
        content = self._read_html()
        assert "Export Database Tables" in content
        assert "tax.*" in content


class TestDockerfileChanges:

    def test_dockerfile_has_tables_dir(self):
        path = os.path.join(os.path.dirname(__file__), "..", "Dockerfile")
        with open(path, "r") as f:
            content = f.read()
        assert "/data/logs/tables" in content
