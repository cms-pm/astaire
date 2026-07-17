"""Shared test fixtures for Astaire."""

import pytest

from src.db import get_connection, init_db


@pytest.fixture
def db_conn():
    """In-memory SQLite database with core registry + claims module initialized.

    The existing test suite predates the Proposal A module split and assumes
    entity/claim/relationship/contradiction tables are always present, so
    this fixture opts in explicitly (`with_claims=True`). New tests that
    need to exercise the opt-in boundary itself should use
    `db_conn_core_only` instead.
    """
    conn = get_connection(":memory:")
    init_db(conn, with_claims=True)
    yield conn
    conn.close()


@pytest.fixture
def db_conn_core_only():
    """In-memory SQLite database with only the core registry installed.

    No entity/claim/relationship/contradiction tables — use this fixture to
    test the optional claims module's opt-in boundary (Proposal A).
    """
    conn = get_connection(":memory:")
    init_db(conn)
    yield conn
    conn.close()
