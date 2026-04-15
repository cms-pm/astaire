"""Shared test fixtures for Astaire."""

import pytest

from src.db import get_connection, init_db


@pytest.fixture
def db_conn():
    """In-memory SQLite database with schema initialized."""
    conn = get_connection(":memory:")
    init_db(conn)
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def _isolate_agent_sessions(monkeypatch):
    """Prevent agent-sessions plugin from scanning real ~/. directories during tests."""
    monkeypatch.setattr(
        "src.collections.agent_sessions._discover_claude_sessions",
        lambda root=None: [],
    )
    monkeypatch.setattr(
        "src.collections.agent_sessions._discover_codex_sessions",
        lambda root=None: [],
    )
    monkeypatch.setattr(
        "src.collections.agent_sessions._discover_gemini_sessions",
        lambda root=None: [],
    )
