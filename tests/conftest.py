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
