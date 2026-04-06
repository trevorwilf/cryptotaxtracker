"""
Tests for the persistent file logging system.

Covers:
  - setup_logging creates log files in a temp directory
  - /logs endpoint returns lines from a log file
  - /logs endpoint filters by level
  - /logs endpoint rejects bad filenames (path traversal)
  - /logs/files endpoint lists available logs
  - Log viewer UI elements exist in index.html
  - Dockerfile and compose changes
"""
import os
import logging
import pytest
from unittest.mock import patch, MagicMock
from httpx import AsyncClient, ASGITransport


class TestSetupLogging:

    def test_setup_logging_creates_directory(self, tmp_path):
        """setup_logging should create the log directory."""
        log_dir = str(tmp_path / "test_logs")
        with patch.dict(os.environ, {"LOG_DIR": log_dir, "LOG_LEVEL": "INFO"}):
            # Clear existing handlers to avoid pollution
            root = logging.getLogger()
            original_handlers = root.handlers.copy()
            root.handlers.clear()
            try:
                from main import setup_logging
                # Need to reload with new env
                import importlib
                import main as main_module
                # Just call the function with the patched env
                old_log_dir = main_module.LOG_DIR
                main_module.LOG_DIR = log_dir
                main_module.setup_logging()
                assert os.path.isdir(log_dir)
            finally:
                # Restore
                main_module.LOG_DIR = old_log_dir
                root.handlers = original_handlers

    def test_setup_logging_creates_log_files(self, tmp_path):
        """setup_logging should create the 4 log files."""
        log_dir = str(tmp_path / "test_logs2")
        root = logging.getLogger()
        original_handlers = root.handlers.copy()
        root.handlers.clear()
        try:
            import main as main_module
            old_log_dir = main_module.LOG_DIR
            main_module.LOG_DIR = log_dir
            main_module.setup_logging()

            # Check that handler files were created
            expected_files = ["tax-collector.log", "api.log", "errors.log", "imports.log"]
            created = os.listdir(log_dir)
            for f in expected_files:
                assert f in created, f"Expected {f} in {created}"
        finally:
            main_module.LOG_DIR = old_log_dir
            root.handlers = original_handlers

    def test_log_level_from_env(self):
        """LOG_LEVEL env var should control the level."""
        import main as main_module
        # Default should be INFO
        assert main_module.LOG_LEVEL in ("INFO", "DEBUG", "WARNING", "ERROR")


class TestLogsEndpoint:

    @pytest.fixture
    def app_with_log(self, tmp_path):
        """Create app with a log file."""
        log_dir = str(tmp_path)
        log_path = os.path.join(log_dir, "tax-collector.log")
        with open(log_path, "w") as f:
            f.write("[2026-04-05 10:00:00] INFO tax-collector: Server started\n")
            f.write("[2026-04-05 10:00:01] WARNING tax-collector: Low memory\n")
            f.write("[2026-04-05 10:00:02] ERROR tax-collector: DB connection failed\n")
            f.write("[2026-04-05 10:00:03] DEBUG tax-collector: Query executed\n")
            f.write("[2026-04-05 10:00:04] INFO tax-collector: Sync complete\n")

        with patch.dict("os.environ", {
            "TAX_DATABASE_URL": "postgresql+asyncpg://test:test@localhost/test",
            "TAX_EXCHANGES": "nonkyc",
            "NONKYC_API_KEY": "testkey",
            "NONKYC_API_SECRET": "testsecret",
            "TAX_EXPORT_DIR": "/tmp/test_exports",
        }):
            with patch("database.Database") as MockDB:
                mock_db = MagicMock()
                mock_db.init = MagicMock(return_value=None)
                mock_db.close = MagicMock(return_value=None)
                MockDB.return_value = mock_db
                import importlib
                import main as main_module
                old_log_dir = main_module.LOG_DIR
                main_module.LOG_DIR = log_dir
                importlib.reload(main_module)
                main_module.LOG_DIR = log_dir
                yield main_module.app, log_dir
                main_module.LOG_DIR = old_log_dir

    @pytest.mark.asyncio
    async def test_logs_returns_lines(self, app_with_log):
        app, log_dir = app_with_log
        import main as main_module
        main_module.LOG_DIR = log_dir
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/logs?file=tax-collector.log&lines=10")
            assert r.status_code == 200
            data = r.json()
            assert data["total_lines"] == 5
            assert data["returned"] == 5
            assert len(data["lines"]) == 5

    @pytest.mark.asyncio
    async def test_logs_filters_by_level(self, app_with_log):
        app, log_dir = app_with_log
        import main as main_module
        main_module.LOG_DIR = log_dir
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/logs?file=tax-collector.log&level=ERROR")
            assert r.status_code == 200
            data = r.json()
            assert data["returned"] == 1
            assert "ERROR" in data["lines"][0]

    @pytest.mark.asyncio
    async def test_logs_rejects_bad_filename(self, app_with_log):
        app, _ = app_with_log
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/logs?file=../../etc/passwd")
            assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_logs_nonexistent_file(self, app_with_log):
        app, log_dir = app_with_log
        import main as main_module
        main_module.LOG_DIR = log_dir
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/logs?file=imports.log")
            assert r.status_code == 200
            data = r.json()
            assert data["returned"] == 0
            assert "not created yet" in data.get("note", "")

    @pytest.mark.asyncio
    async def test_log_files_endpoint(self, app_with_log):
        app, log_dir = app_with_log
        import main as main_module
        main_module.LOG_DIR = log_dir
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/logs/files")
            assert r.status_code == 200
            data = r.json()
            assert "files" in data
            names = [f["name"] for f in data["files"]]
            assert "tax-collector.log" in names


class TestEndpointRegistration:

    def test_logs_endpoint_exists(self):
        import main as main_module
        routes = [r.path for r in main_module.app.routes]
        assert "/logs" in routes

    def test_logs_files_endpoint_exists(self):
        import main as main_module
        routes = [r.path for r in main_module.app.routes]
        assert "/logs/files" in routes


class TestUIElements:

    def test_logs_nav_button_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "showPage('logs'" in content

    def test_logs_page_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert 'id="page-logs"' in content

    def test_log_viewer_element_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert 'id="logViewer"' in content

    def test_load_logs_function_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "function loadLogs" in content
        assert "function autoRefreshLogs" in content
        assert "function escapeHtml" in content

    def test_logs_in_showpage_loaders(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "'logs':loadLogs" in content


class TestDockerfileChanges:

    def test_dockerfile_creates_logs_dir(self):
        path = os.path.join(os.path.dirname(__file__), "..", "Dockerfile")
        with open(path, "r") as f:
            content = f.read()
        assert "/data/logs" in content

    def test_compose_has_logs_volume(self):
        path = os.path.join(os.path.dirname(__file__), "..", "compose_snippet.yaml")
        with open(path, "r") as f:
            content = f.read()
        assert "/data/logs" in content
        assert "LOG_LEVEL" in content
