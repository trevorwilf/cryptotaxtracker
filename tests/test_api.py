"""
Tests for FastAPI API endpoints.

Uses httpx AsyncClient with mocked dependencies.
Covers all endpoint groups: health, sync, export, tax computation.
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from httpx import AsyncClient, ASGITransport


@pytest.fixture
def mock_db():
    """Mock the Database object and its session context manager."""
    db = MagicMock()
    session = AsyncMock()
    session.execute = AsyncMock(return_value=MagicMock(
        fetchall=lambda: [], fetchone=lambda: None, scalar=lambda: 0, keys=lambda: []
    ))
    session.commit = AsyncMock()

    class FakeCtx:
        async def __aenter__(self):
            return session
        async def __aexit__(self, *args):
            pass

    db.get_session = MagicMock(return_value=FakeCtx())
    db.init = AsyncMock()
    db.close = AsyncMock()
    db.get_summary = AsyncMock(return_value={
        "total_trades": 0, "total_deposits": 0, "total_withdrawals": 0,
        "exchanges": [], "assets": [], "date_range": {"first": None, "last": None},
    })
    return db, session


@pytest.fixture
def app_with_mocks(mock_db):
    """Create the FastAPI app with mocked database."""
    db_obj, session = mock_db

    with patch.dict("os.environ", {
        "TAX_DATABASE_URL": "postgresql+asyncpg://test:test@localhost/test",
        "TAX_EXCHANGES": "nonkyc",
        "NONKYC_API_KEY": "testkey",
        "NONKYC_API_SECRET": "testsecret",
        "TAX_EXPORT_DIR": "/tmp/test_exports",
    }):
        # Patch the Database class before importing main
        with patch("database.Database", return_value=db_obj):
            import importlib
            import main as main_module
            importlib.reload(main_module)
            yield main_module.app, session


# ── Health ────────────────────────────────────────────────────────────────

class TestHealthEndpoint:
    @pytest.mark.asyncio
    async def test_health_returns_200(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/health")
            assert r.status_code == 200
            data = r.json()
            assert data["status"] == "ok"
            assert "exchanges" in data


# ── Sync Status ───────────────────────────────────────────────────────────

class TestSyncEndpoints:
    @pytest.mark.asyncio
    async def test_sync_status_returns_200(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/sync/status")
            assert r.status_code == 200
            assert isinstance(r.json(), dict)

    @pytest.mark.asyncio
    async def test_sync_unknown_exchange_404(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/sync/fakeexchange")
            assert r.status_code == 404


# ── Backfill ──────────────────────────────────────────────────────────────

class TestBackfillEndpoints:
    @pytest.mark.asyncio
    async def test_backfill_status_returns_200(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/backfill-usd/status")
            assert r.status_code == 200


# ── Tax Computation ───────────────────────────────────────────────────────

class TestTaxEndpoints:
    @pytest.mark.asyncio
    async def test_compute_status_returns_200(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/tax/compute/status")
            assert r.status_code == 200

    @pytest.mark.asyncio
    async def test_form_8949_requires_year(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/tax/form-8949")
            assert r.status_code == 422  # missing required year param

    @pytest.mark.asyncio
    async def test_schedule_d_requires_year(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/tax/schedule-d")
            assert r.status_code == 422

    @pytest.mark.asyncio
    async def test_fee_summary_no_year(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/tax/fee-summary")
            assert r.status_code == 200

    @pytest.mark.asyncio
    async def test_lots_endpoint(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/tax/lots")
            assert r.status_code == 200


# ── Price Stats ───────────────────────────────────────────────────────────

class TestPriceEndpoints:
    @pytest.mark.asyncio
    async def test_price_stats(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/prices/stats")
            assert r.status_code == 200


# ── Summary ───────────────────────────────────────────────────────────────

class TestSummaryEndpoint:
    @pytest.mark.asyncio
    async def test_summary_no_year(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/summary")
            assert r.status_code == 200


# ── V4 Accountant Handoff Endpoints ─────────────────────────────────────

class TestV4PnlByExchange:
    @pytest.mark.asyncio
    async def test_pnl_by_exchange_returns_list(self, app_with_mocks):
        app, session = app_with_mocks
        session.execute.return_value = MagicMock(
            fetchall=lambda: [], keys=lambda: [])
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/v4/pnl-by-exchange?year=2025")
            assert r.status_code == 200
            assert isinstance(r.json(), list)

    @pytest.mark.asyncio
    async def test_pnl_by_exchange_requires_year(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/v4/pnl-by-exchange")
            assert r.status_code == 422

    @pytest.mark.asyncio
    async def test_pnl_empty_year_returns_empty(self, app_with_mocks):
        app, session = app_with_mocks
        session.execute.return_value = MagicMock(
            fetchall=lambda: [], keys=lambda: [])
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/v4/pnl-by-exchange?year=2020")
            assert r.status_code == 200
            assert r.json() == []


class TestV4DataCoverage:
    @pytest.mark.asyncio
    async def test_data_coverage_returns_200(self, app_with_mocks):
        app, session = app_with_mocks
        session.execute.return_value = MagicMock(
            fetchall=lambda: [], keys=lambda: [])
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/v4/data-coverage")
            assert r.status_code == 200


class TestV4FundingByExchange:
    @pytest.mark.asyncio
    async def test_funding_returns_200(self, app_with_mocks):
        app, session = app_with_mocks
        session.execute.return_value = MagicMock(
            fetchall=lambda: [], keys=lambda: [])
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/v4/funding-by-exchange")
            assert r.status_code == 200


class TestV4CsvImports:
    @pytest.mark.asyncio
    async def test_csv_imports_list_returns_200(self, app_with_mocks):
        app, session = app_with_mocks
        session.execute.return_value = MagicMock(
            fetchall=lambda: [], keys=lambda: [])
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/v4/csv-imports")
            assert r.status_code == 200
            data = r.json()
            assert "count" in data
            assert "imports" in data
