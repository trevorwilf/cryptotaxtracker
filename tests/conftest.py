"""
Shared test fixtures for the Tax Collector test suite.
Factory functions and mocks live in helpers.py (importable by tests directly).
"""
import os
import sys

# Ensure tests/ directory is on sys.path so helpers.py is importable
sys.path.insert(0, os.path.dirname(__file__))

import pytest
import pytest_asyncio
from helpers import MockSettings, MockSession


@pytest.fixture
def settings():
    return MockSettings()


@pytest.fixture
def mock_session():
    return MockSession()


@pytest.fixture
def fixtures_dir():
    """Path to tests/fixtures/ directory."""
    return os.path.join(os.path.dirname(__file__), "fixtures")


@pytest_asyncio.fixture
async def real_db():
    """Real database for integration tests. Requires test Postgres running."""
    test_url = os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql+asyncpg://test:test@localhost:5433/test_tax"
    )
    from database import Database
    db = Database(test_url)
    await db.init()
    yield db
    async with db.engine.begin() as conn:
        from sqlalchemy import text
        await conn.execute(text("DROP SCHEMA IF EXISTS tax CASCADE"))
    await db.close()


@pytest_asyncio.fixture
async def real_session(real_db):
    """Real DB session for integration tests."""
    async with real_db.get_session() as session:
        yield session
        await session.rollback()
