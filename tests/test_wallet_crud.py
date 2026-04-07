"""
Comprehensive tests for Wallet CRUD endpoints.

Covers entities, accounts, addresses: create, read, update, delete.
Validates type constraints, 404 handling, cascade behavior, and UI elements.
"""
import os
import inspect
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def app_with_mocks():
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
    mock_db.get_summary = AsyncMock(return_value={
        "total_trades": 0, "total_deposits": 0, "total_withdrawals": 0,
        "exchanges": [], "assets": [], "date_range": {"first": None, "last": None},
    })

    with patch.dict("os.environ", {
        "TAX_DATABASE_URL": "postgresql+asyncpg://test:test@localhost/test",
        "TAX_EXCHANGES": "nonkyc",
        "NONKYC_API_KEY": "k", "NONKYC_API_SECRET": "s",
        "TAX_EXPORT_DIR": "/tmp/test",
    }):
        with patch("database.Database", return_value=mock_db):
            import importlib
            import main as main_module
            importlib.reload(main_module)
            yield main_module.app, session


# ── Entity CRUD ───────────────────────────────────────────────────────────

class TestWalletEntityCRUD:

    @pytest.mark.asyncio
    async def test_create_entity_success(self, app_with_mocks):
        app, session = app_with_mocks
        result = MagicMock()
        result.fetchone.return_value = (1,)
        session.execute.return_value = result
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.post("/v4/wallet/entities?entity_type=taxpayer&label=My+Wallets")
            assert r.status_code == 200
            assert r.json()["id"] == 1

    @pytest.mark.asyncio
    async def test_create_entity_invalid_type(self, app_with_mocks):
        app, session = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.post("/v4/wallet/entities?entity_type=garbage&label=Test")
            assert r.status_code == 400
            assert "entity_type" in r.json()["detail"]

    @pytest.mark.asyncio
    async def test_get_entity_not_found(self, app_with_mocks):
        app, session = app_with_mocks
        result = MagicMock()
        result.fetchall.return_value = []
        result.keys.return_value = []
        session.execute.return_value = result
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.get("/v4/wallet/entities/999")
            assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_update_entity_invalid_type(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.patch("/v4/wallet/entities/1?entity_type=invalid")
            assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_update_entity_no_fields(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.patch("/v4/wallet/entities/1")
            assert r.status_code == 400
            assert "No fields" in r.json()["detail"]

    @pytest.mark.asyncio
    async def test_update_entity_empty_label(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.patch("/v4/wallet/entities/1?label=%20")
            assert r.status_code == 400
            assert "empty" in r.json()["detail"]

    @pytest.mark.asyncio
    async def test_update_entity_not_found(self, app_with_mocks):
        app, session = app_with_mocks
        result = MagicMock()
        result.fetchone.return_value = None
        session.execute.return_value = result
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.patch("/v4/wallet/entities/999?label=NewLabel")
            assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_entity_not_found(self, app_with_mocks):
        app, session = app_with_mocks
        result = MagicMock()
        result.fetchone.return_value = None
        session.execute.return_value = result
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.delete("/v4/wallet/entities/999")
            assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_entity_success(self, app_with_mocks):
        app, session = app_with_mocks
        result = MagicMock()
        result.fetchone.return_value = (1,)
        session.execute.return_value = result
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.delete("/v4/wallet/entities/1")
            assert r.status_code == 200
            assert r.json()["deleted"] == 1


class TestWalletAccountCRUD:

    @pytest.mark.asyncio
    async def test_create_account_success(self, app_with_mocks):
        app, session = app_with_mocks
        result = MagicMock()
        result.fetchone.return_value = (10,)
        session.execute.return_value = result
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.post("/v4/wallet/accounts?entity_id=1&account_type=exchange&label=MEXC")
            assert r.status_code == 200
            assert r.json()["id"] == 10

    @pytest.mark.asyncio
    async def test_create_account_invalid_type(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.post("/v4/wallet/accounts?entity_id=1&account_type=bogus&label=Test")
            assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_update_account_not_found(self, app_with_mocks):
        app, session = app_with_mocks
        result = MagicMock()
        result.fetchone.return_value = None
        session.execute.return_value = result
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.patch("/v4/wallet/accounts/999?label=NewLabel")
            assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_account_not_found(self, app_with_mocks):
        app, session = app_with_mocks
        result = MagicMock()
        result.fetchone.return_value = None
        session.execute.return_value = result
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.delete("/v4/wallet/accounts/999")
            assert r.status_code == 404


class TestWalletAddressCRUD:

    @pytest.mark.asyncio
    async def test_create_address_success(self, app_with_mocks):
        app, session = app_with_mocks
        result = MagicMock()
        result.fetchone.return_value = (20,)
        session.execute.return_value = result
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.post("/v4/wallet/addresses?account_id=10&address=bc1q_test&chain=bitcoin")
            assert r.status_code == 200
            assert r.json()["id"] == 20

    @pytest.mark.asyncio
    async def test_update_address_no_fields(self, app_with_mocks):
        app, _ = app_with_mocks
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.patch("/v4/wallet/addresses/1")
            assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_update_address_not_found(self, app_with_mocks):
        app, session = app_with_mocks
        result = MagicMock()
        result.fetchone.return_value = None
        session.execute.return_value = result
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.patch("/v4/wallet/addresses/999?chain=bitcoin")
            assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_address_not_found(self, app_with_mocks):
        app, session = app_with_mocks
        result = MagicMock()
        result.fetchone.return_value = None
        session.execute.return_value = result
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.delete("/v4/wallet/addresses/999")
            assert r.status_code == 404


# ── Endpoint Registration ─────────────────────────────────────────────────

class TestWalletEndpointRegistration:

    def _routes(self):
        import main as m
        return [(r.path, getattr(r, 'methods', set())) for r in m.app.routes]

    def test_get_single_entity_exists(self):
        routes = self._routes()
        assert any("/v4/wallet/entities/{entity_id}" in p and "GET" in m for p, m in routes)

    def test_patch_entity_exists(self):
        routes = self._routes()
        assert any("/v4/wallet/entities/{entity_id}" in p and "PATCH" in m for p, m in routes)

    def test_patch_account_exists(self):
        routes = self._routes()
        assert any("/v4/wallet/accounts/{account_id}" in p and "PATCH" in m for p, m in routes)

    def test_patch_address_exists(self):
        routes = self._routes()
        assert any("/v4/wallet/addresses/{address_id}" in p and "PATCH" in m for p, m in routes)


# ── Validation Constants ──────────────────────────────────────────────────

class TestValidationConstants:

    def test_valid_entity_types(self):
        import main as m
        assert "taxpayer" in m.VALID_ENTITY_TYPES
        assert "spouse" in m.VALID_ENTITY_TYPES
        assert "business" in m.VALID_ENTITY_TYPES
        assert "third_party" in m.VALID_ENTITY_TYPES

    def test_valid_account_types(self):
        import main as m
        assert "exchange" in m.VALID_ACCOUNT_TYPES
        assert "hardware_wallet" in m.VALID_ACCOUNT_TYPES
        assert "software_wallet" in m.VALID_ACCOUNT_TYPES
        assert "custodial" in m.VALID_ACCOUNT_TYPES
        assert "other" in m.VALID_ACCOUNT_TYPES


# ── Schema Cascade Fix ────────────────────────────────────────────────────

class TestSchemaCascade:

    def test_cascade_fix_in_schema(self):
        from schema_v4 import SCHEMA_V4_SQL
        assert "ON DELETE CASCADE" in SCHEMA_V4_SQL
        assert "wallet_accounts_entity_id_fkey" in SCHEMA_V4_SQL
        assert "wallet_addresses_account_id_fkey" in SCHEMA_V4_SQL
        assert "wallet_address_claims_address_id_fkey" in SCHEMA_V4_SQL
        assert "wallet_claim_evidence_claim_id_fkey" in SCHEMA_V4_SQL


# ── UI Elements ───────────────────────────────────────────────────────────

class TestWalletUI:

    def _html(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def test_inline_forms_exist(self):
        h = self._html()
        assert "wallet-form" in h

    def test_edit_entity_function(self):
        assert "function editEntity" in self._html()

    def test_delete_entity_function(self):
        assert "function deleteEntity" in self._html()

    def test_edit_account_function(self):
        assert "function editAccount" in self._html()

    def test_delete_account_function(self):
        assert "function deleteAccount" in self._html()

    def test_edit_address_function(self):
        assert "function editAddress" in self._html()

    def test_delete_address_function(self):
        assert "function deleteAddress" in self._html()

    def test_no_prompt_in_entity_create(self):
        """showAddWalletForm should NOT use prompt()."""
        h = self._html()
        # The old prompt-based code should be gone
        assert "prompt('Entity label" not in h

    def test_entity_type_dropdown(self):
        h = self._html()
        assert "newEntityType" in h
        assert "taxpayer" in h

    def test_account_type_dropdown(self):
        h = self._html()
        assert "newAcctType" in h
        assert "hardware_wallet" in h


# ═══ Part 2 Tests ═════════════════════════════════════════════════════════

class TestListEntitiesEnhanced:

    def test_list_query_includes_account_notes(self):
        """The list entities SQL should select a.notes AS account_notes."""
        import main as m
        import inspect
        src = inspect.getsource(m.wallet_list_entities)
        assert "account_notes" in src

    def test_list_query_includes_is_active(self):
        """The list entities SQL should select wa.is_active."""
        import main as m
        import inspect
        src = inspect.getsource(m.wallet_list_entities)
        assert "is_active" in src

    def test_list_grouping_includes_notes(self):
        """The grouping code should include notes in account objects."""
        import main as m
        import inspect
        src = inspect.getsource(m.wallet_list_entities)
        assert '"notes"' in src and "account_notes" in src


class TestAutoDiscoverEnhanced:

    def test_auto_discover_returns_new_structure(self):
        """The response should have summary, addresses, entities keys."""
        import main as m
        import inspect
        src = inspect.getsource(m.wallet_auto_discover)
        assert '"summary"' in src
        assert '"addresses"' in src
        assert '"entities"' in src

    def test_auto_discover_includes_tx_count(self):
        """Each address group should include total_tx_count."""
        import main as m
        import inspect
        src = inspect.getsource(m.wallet_auto_discover)
        assert "total_tx_count" in src

    def test_auto_discover_includes_status(self):
        """Each address should have status: claimed or unclaimed."""
        import main as m
        import inspect
        src = inspect.getsource(m.wallet_auto_discover)
        assert '"claimed"' in src
        assert '"unclaimed"' in src

    def test_auto_discover_queries_both_tables(self):
        """The SQL should query both deposits and withdrawals."""
        import main as m
        import inspect
        src = inspect.getsource(m.wallet_auto_discover)
        assert "tax.deposits" in src
        assert "tax.withdrawals" in src


class TestUIDiscoverFilter:

    def _html(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def test_discover_filter_buttons_exist(self):
        h = self._html()
        assert "discoverFilter" in h
        assert "filterDiscovered" in h

    def test_render_discovered_function(self):
        assert "function renderDiscoveredAddresses" in self._html()

    def test_toggle_add_to_wallet_function(self):
        assert "function toggleAddToWallet" in self._html()

    def test_save_discovered_function(self):
        assert "function saveDiscoveredAddress" in self._html()

    def test_discover_summary_badge(self):
        assert 'id="discoverSummary"' in self._html()


class TestUIInlineEditNoPrompt:

    def _html(self):
        path = os.path.join(os.path.dirname(__file__), "..", "app", "static", "index.html")
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def test_edit_account_has_notes_field(self):
        """editAccount form should include Notes textarea."""
        h = self._html()
        assert "editAcctNotes" in h

    def test_edit_account_has_exchange_conditional(self):
        """editAccount form should conditionally show exchange name."""
        h = self._html()
        assert "editAcctExDiv" in h

    def test_edit_address_no_prompt(self):
        """editAddress should NOT use prompt()."""
        h = self._html()
        # The old prompt-based editAddress code should be gone
        assert "prompt('Chain:')" not in h

    def test_edit_address_has_inline_form(self):
        """editAddress should create an inline form."""
        h = self._html()
        assert "editAddrChain" in h
        assert "editAddrLabel" in h

    def test_suggest_chain_function(self):
        assert "function suggestChain" in self._html()
