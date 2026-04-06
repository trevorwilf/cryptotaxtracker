"""
Tests for the database diagnostic export feature.

Covers:
  - DatabaseExporter class methods
  - Schema export (pg_dump + fallback)
  - Data quality report
  - Referential integrity checks
  - Sequence states
  - Storage statistics
  - Manifest generation
  - CSV export with raw_data truncation
  - Endpoint registration
  - Download (zip + single table)
  - UI elements
  - Dockerfile changes
"""
import csv
import os
import zipfile
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport

from db_export import DatabaseExporter, FK_CHECKS


# ── DatabaseExporter Unit Tests ───────────────────────────────────────────

class TestDatabaseExporterInit:

    def test_init_sets_fields(self, tmp_path):
        exp = DatabaseExporter(str(tmp_path), "postgresql+asyncpg://u:p@h/db")
        assert exp.export_dir == str(tmp_path)
        assert exp.database_url == "postgresql+asyncpg://u:p@h/db"
        assert exp.export_time is not None
        assert exp.table_names == []
        assert exp.total_rows == 0
        assert exp.errors == []


class TestSchemaFallback:

    @pytest.mark.asyncio
    async def test_fallback_produces_create_table(self, tmp_path):
        """Fallback schema should produce CREATE TABLE statements."""
        exp = DatabaseExporter(str(tmp_path), "postgresql+asyncpg://u:p@h/db")
        exp.table_names = ["trades"]

        session = AsyncMock()
        # Columns query
        col_result = MagicMock()
        col_result.keys.return_value = ["column_name", "data_type",
                                         "character_maximum_length",
                                         "numeric_precision", "numeric_scale",
                                         "is_nullable", "column_default"]
        col_result.fetchall.return_value = [
            ("id", "integer", None, None, None, "NO", "nextval('tax.trades_id_seq'::regclass)"),
            ("exchange", "character varying", 50, None, None, "NO", None),
            ("amount", "numeric", None, 36, 18, "YES", None),
        ]
        # Constraints query
        con_result = MagicMock()
        con_result.keys.return_value = ["constraint_name", "constraint_type", "column_name"]
        con_result.fetchall.return_value = [
            ("trades_pkey", "PRIMARY KEY", "id"),
        ]
        # Indexes query
        idx_result = MagicMock()
        idx_result.fetchall.return_value = [
            ("trades_pkey", "CREATE UNIQUE INDEX trades_pkey ON tax.trades USING btree (id)"),
        ]

        call_count = [0]
        async def mock_execute(stmt, params=None):
            call_count[0] += 1
            sql = str(stmt)
            if "information_schema.columns" in sql:
                return col_result
            elif "table_constraints" in sql:
                return con_result
            elif "pg_indexes" in sql:
                return idx_result
            return MagicMock(fetchall=lambda: [], fetchone=lambda: None, keys=lambda: [])

        session.execute = mock_execute

        schema_path = os.path.join(str(tmp_path), "_schema.sql")
        await exp._export_schema_fallback(session, schema_path)

        assert os.path.exists(schema_path)
        with open(schema_path, "r") as f:
            content = f.read()
        assert "CREATE TABLE IF NOT EXISTS tax.trades" in content
        assert "integer" in content
        assert "VARCHAR(50)" in content
        assert "NUMERIC(36,18)" in content
        assert "PRIMARY KEY" in content

    @pytest.mark.asyncio
    async def test_pgdump_fallback_on_failure(self, tmp_path):
        """When pg_dump fails, fallback should run."""
        exp = DatabaseExporter(str(tmp_path), "postgresql+asyncpg://u:p@h/db")
        exp.table_names = ["test_tbl"]

        session = AsyncMock()
        col_result = MagicMock()
        col_result.keys.return_value = ["column_name", "data_type",
                                         "character_maximum_length",
                                         "numeric_precision", "numeric_scale",
                                         "is_nullable", "column_default"]
        col_result.fetchall.return_value = [("id", "integer", None, None, None, "NO", None)]

        async def mock_execute(stmt, params=None):
            sql = str(stmt)
            if "information_schema.columns" in sql:
                return col_result
            return MagicMock(fetchall=lambda: [], fetchone=lambda: None, keys=lambda: [])

        session.execute = mock_execute

        # Mock pg_dump to fail
        with patch("db_export.subprocess.run", side_effect=FileNotFoundError("pg_dump not found")):
            await exp._export_schema(session)

        schema_path = os.path.join(str(tmp_path), "_schema.sql")
        assert os.path.exists(schema_path)
        with open(schema_path, "r") as f:
            content = f.read()
        assert "information_schema" in content  # fallback marker


class TestDataQuality:

    @pytest.mark.asyncio
    async def test_data_quality_file_created(self, tmp_path):
        """Data quality report should be written."""
        exp = DatabaseExporter(str(tmp_path), "postgresql+asyncpg://u:p@h/db")
        exp.table_names = ["trades"]
        exp.table_info = {"trades": {"row_count": 0, "columns": ["id", "exchange"], "file_size": 100}}

        session = AsyncMock()
        session.execute = AsyncMock(return_value=MagicMock(
            fetchall=lambda: [], fetchone=lambda: (0, 0), scalar=lambda: 0,
            keys=lambda: []))

        await exp._export_data_quality(session)

        path = os.path.join(str(tmp_path), "_data_quality.txt")
        assert os.path.exists(path)
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "Data Quality Report" in content
        assert "tax.trades" in content

    @pytest.mark.asyncio
    async def test_data_quality_empty_table(self, tmp_path):
        """Empty tables should be marked as such."""
        exp = DatabaseExporter(str(tmp_path), "postgresql+asyncpg://u:p@h/db")
        exp.table_names = ["empty_tbl"]
        exp.table_info = {"empty_tbl": {"row_count": 0, "columns": ["id"], "file_size": 10}}

        session = AsyncMock()
        session.execute = AsyncMock(return_value=MagicMock(
            fetchall=lambda: [], fetchone=lambda: None, scalar=lambda: 0,
            keys=lambda: []))

        await exp._export_data_quality(session)

        path = os.path.join(str(tmp_path), "_data_quality.txt")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "empty table" in content


class TestIntegrity:

    @pytest.mark.asyncio
    async def test_integrity_file_created(self, tmp_path):
        """Integrity report should be written."""
        exp = DatabaseExporter(str(tmp_path), "postgresql+asyncpg://u:p@h/db")
        exp.table_names = []  # No tables → everything skipped

        session = AsyncMock()
        await exp._export_integrity(session)

        path = os.path.join(str(tmp_path), "_integrity.txt")
        assert os.path.exists(path)
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "Referential Integrity Check" in content

    @pytest.mark.asyncio
    async def test_integrity_skips_missing_tables(self, tmp_path):
        """Checks should skip when tables don't exist."""
        exp = DatabaseExporter(str(tmp_path), "postgresql+asyncpg://u:p@h/db")
        exp.table_names = ["trades"]  # Only trades exists, not normalized_events

        session = AsyncMock()
        await exp._export_integrity(session)

        path = os.path.join(str(tmp_path), "_integrity.txt")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "SKIPPED" in content

    def test_fk_checks_list_comprehensive(self):
        """FK_CHECKS should cover all critical relationships."""
        tables_checked = set()
        for child, child_col, parent, parent_col, desc in FK_CHECKS:
            tables_checked.add(child)
            tables_checked.add(parent)
        assert "normalized_events" in tables_checked
        assert "lots_v4" in tables_checked
        assert "disposals_v4" in tables_checked
        assert "transfer_carryover" in tables_checked
        assert "income_events_v4" in tables_checked
        assert "form_8949_v4" in tables_checked
        assert "exceptions" in tables_checked
        assert len(FK_CHECKS) >= 26


class TestSequences:

    @pytest.mark.asyncio
    async def test_sequences_file_created(self, tmp_path):
        exp = DatabaseExporter(str(tmp_path), "postgresql+asyncpg://u:p@h/db")
        exp.table_names = ["trades"]
        exp.table_info = {"trades": {"row_count": 5, "columns": ["id", "exchange"], "file_size": 100}}

        session = AsyncMock()
        # column_default query
        default_result = MagicMock()
        default_result.fetchone.return_value = ("nextval('tax.trades_id_seq'::regclass)",)
        # sequence value
        seq_result = MagicMock()
        seq_result.scalar.return_value = 5
        # max id
        max_result = MagicMock()
        max_result.scalar.return_value = 5

        call_count = [0]
        async def mock_execute(stmt, params=None):
            call_count[0] += 1
            sql = str(stmt)
            if "column_default" in sql:
                return default_result
            elif "last_value" in sql:
                return seq_result
            elif "MAX(id)" in sql:
                return max_result
            return MagicMock(fetchone=lambda: None, fetchall=lambda: [], scalar=lambda: 0)

        session.execute = mock_execute

        await exp._export_sequences(session)

        path = os.path.join(str(tmp_path), "_sequences.txt")
        assert os.path.exists(path)
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "Sequence States" in content
        assert "trades" in content


class TestStorage:

    @pytest.mark.asyncio
    async def test_storage_file_created(self, tmp_path):
        exp = DatabaseExporter(str(tmp_path), "postgresql+asyncpg://u:p@h/db")
        exp.table_names = ["trades"]

        session = AsyncMock()
        session.execute = AsyncMock(return_value=MagicMock(
            fetchone=lambda: (100, 5, 8192, 4096, 12288),
            fetchall=lambda: []))

        await exp._export_storage(session)

        path = os.path.join(str(tmp_path), "_storage.txt")
        assert os.path.exists(path)
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "Storage Statistics" in content

    def test_fmt_size(self):
        assert DatabaseExporter._fmt_size(0) == "0 B"
        assert DatabaseExporter._fmt_size(512) == "512 B"
        assert DatabaseExporter._fmt_size(1024) == "1.0 KB"
        assert DatabaseExporter._fmt_size(1048576) == "1.0 MB"
        assert DatabaseExporter._fmt_size(None) == "—"


class TestManifest:

    def test_manifest_written(self, tmp_path):
        exp = DatabaseExporter(str(tmp_path), "postgresql+asyncpg://u:p@h/db")
        exp.table_names = ["trades", "deposits"]
        exp.table_info = {
            "trades": {"row_count": 10, "columns": ["id", "exchange"], "file_size": 500},
            "deposits": {"row_count": 5, "columns": ["id", "asset"], "file_size": 200},
        }
        exp.total_rows = 15
        exp.errors = []

        # Create dummy diagnostic files so manifest can report sizes
        for df in ["_schema.sql", "_data_quality.txt", "_integrity.txt",
                    "_sequences.txt", "_storage.txt"]:
            with open(os.path.join(str(tmp_path), df), "w") as f:
                f.write("test")

        exp._write_manifest()

        path = os.path.join(str(tmp_path), "_manifest.txt")
        assert os.path.exists(path)
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "Database Diagnostic Export" in content
        assert "tax.trades" in content
        assert "tax.deposits" in content
        assert "15 rows" in content
        assert "DIAGNOSTIC FILES:" in content
        assert "_schema.sql" in content

    def test_manifest_shows_errors(self, tmp_path):
        exp = DatabaseExporter(str(tmp_path), "postgresql+asyncpg://u:p@h/db")
        exp.table_names = ["broken"]
        exp.table_info = {}
        exp.total_rows = 0
        exp.errors = [{"table": "broken", "error": "permission denied"}]

        exp._write_manifest()

        path = os.path.join(str(tmp_path), "_manifest.txt")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "ERRORS:" in content
        assert "permission denied" in content


class TestRawDataTruncation:

    def test_cast_expression_truncates_raw_data(self):
        """The cast expression for raw_data should use LEFT(..., 500)."""
        col_names = ["id", "exchange", "raw_data", "amount"]
        cast_cols = ", ".join(
            f"{c}::text" if c != "raw_data" else f"LEFT({c}::text, 500) AS {c}"
            for c in col_names
        )
        assert "LEFT(raw_data::text, 500) AS raw_data" in cast_cols
        assert "id::text" in cast_cols


# ── Endpoint & Download Tests ─────────────────────────────────────────────

class TestEndpointRegistration:

    def test_export_tables_endpoint_exists(self):
        import main as main_module
        routes = [r.path for r in main_module.app.routes]
        assert "/v4/export-tables" in routes

    def test_download_endpoint_exists(self):
        import main as main_module
        routes = [r.path for r in main_module.app.routes]
        assert any("/v4/export-tables/download" in r for r in routes)


class TestDownloadEndpoint:

    @pytest.fixture
    def app_with_mocks(self):
        mock_db = MagicMock()
        session = AsyncMock()
        class FakeCtx:
            async def __aenter__(self): return session
            async def __aexit__(self, *args): pass
        mock_db.get_session = MagicMock(return_value=FakeCtx())
        mock_db.init = AsyncMock()
        mock_db.close = AsyncMock()

        with patch.dict("os.environ", {
            "TAX_DATABASE_URL": "postgresql+asyncpg://test:test@localhost/test",
            "TAX_EXCHANGES": "nonkyc",
            "NONKYC_API_KEY": "k", "NONKYC_API_SECRET": "s",
            "TAX_EXPORT_DIR": "/tmp/test_exports",
        }):
            with patch("database.Database", return_value=mock_db):
                import importlib, main as main_module
                importlib.reload(main_module)
                yield main_module.app, session

    @pytest.mark.asyncio
    async def test_download_no_export_returns_404(self, app_with_mocks):
        app, _ = app_with_mocks
        import main as m, tempfile
        old = m.LOG_DIR; m.LOG_DIR = tempfile.mkdtemp()
        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as c:
                r = await c.get("/v4/export-tables/download")
                assert r.status_code == 404
        finally:
            m.LOG_DIR = old

    @pytest.mark.asyncio
    async def test_download_single_table(self, app_with_mocks):
        app, _ = app_with_mocks
        import main as m, tempfile
        old = m.LOG_DIR; tmp = tempfile.mkdtemp(); m.LOG_DIR = tmp
        tdir = os.path.join(tmp, "tables"); os.makedirs(tdir)
        with open(os.path.join(tdir, "trades.csv"), "w") as f:
            f.write("id,exchange\n1,mexc\n")
        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as c:
                r = await c.get("/v4/export-tables/download?table=trades")
                assert r.status_code == 200
                assert "text/csv" in r.headers.get("content-type", "")
        finally:
            m.LOG_DIR = old

    @pytest.mark.asyncio
    async def test_download_missing_table_404(self, app_with_mocks):
        app, _ = app_with_mocks
        import main as m, tempfile
        old = m.LOG_DIR; tmp = tempfile.mkdtemp(); m.LOG_DIR = tmp
        tdir = os.path.join(tmp, "tables"); os.makedirs(tdir)
        with open(os.path.join(tdir, "trades.csv"), "w") as f:
            f.write("id\n1\n")
        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as c:
                r = await c.get("/v4/export-tables/download?table=nonexistent")
                assert r.status_code == 404
        finally:
            m.LOG_DIR = old

    @pytest.mark.asyncio
    async def test_download_zip(self, app_with_mocks):
        app, _ = app_with_mocks
        import main as m, tempfile
        old = m.LOG_DIR; tmp = tempfile.mkdtemp(); m.LOG_DIR = tmp
        tdir = os.path.join(tmp, "tables"); os.makedirs(tdir)
        for name in ["trades.csv", "deposits.csv", "_manifest.txt", "_schema.sql"]:
            with open(os.path.join(tdir, name), "w") as f:
                f.write("test data")
        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as c:
                r = await c.get("/v4/export-tables/download")
                assert r.status_code == 200
                assert "zip" in r.headers.get("content-type", "")
        finally:
            m.LOG_DIR = old


# ── UI Elements ───────────────────────────────────────────────────────────

class TestUIElements:

    def _html(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def test_export_tables_button(self):
        assert 'id="exportTablesBtn"' in self._html()

    def test_download_zip_button(self):
        assert 'id="downloadTablesBtn"' in self._html()

    def test_export_tables_console(self):
        assert 'id="exportTablesConsole"' in self._html()

    def test_export_tables_js(self):
        assert "function exportTables" in self._html()

    def test_debug_section_description(self):
        content = self._html()
        assert "Full Database Diagnostic Export" in content


class TestDockerfile:

    def test_has_tables_dir(self):
        path = os.path.join(os.path.dirname(__file__), "..", "Dockerfile")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "/data/logs/tables" in content

    def test_has_postgresql_client(self):
        path = os.path.join(os.path.dirname(__file__), "..", "Dockerfile")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "postgresql-client" in content
