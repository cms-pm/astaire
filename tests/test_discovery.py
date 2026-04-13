"""Tests for src/collections/discovery — convention-based collection plugin loading."""

import pytest

from src.collections.discovery import (
    discover_collection_modules,
    register_all_collections,
    scan_all_collections,
)
from src.db import get_connection, init_db


@pytest.fixture
def db_conn():
    conn = get_connection(":memory:")
    init_db(conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_project(tmp_path):
    """Minimal project structure matching discovered collection scan rules."""
    root = tmp_path / "project"
    root.mkdir()
    pq = root / "docs" / "planning" / "pool_questions"
    pq.mkdir(parents=True)
    (pq / "phase-1-test.md").write_text("# Pool Question\n")
    (root / "governance.yaml").write_text("profile: strict-baseline\n")
    return root


class TestDiscoverModules:
    def test_discovers_ai_dev_governance(self):
        modules = discover_collection_modules()
        names = [m.COLLECTION_NAME for m in modules]
        assert "ai-dev-governance" in names

    def test_excludes_discovery_module(self):
        modules = discover_collection_modules()
        modnames = [m.__name__ for m in modules]
        assert not any("discovery" in n for n in modnames)

    def test_all_modules_have_required_attrs(self):
        for mod in discover_collection_modules():
            assert hasattr(mod, "COLLECTION_NAME")
            assert hasattr(mod, "register_collection")
            assert hasattr(mod, "scan_and_register")
            assert callable(mod.register_collection)
            assert callable(mod.scan_and_register)


class TestRegisterAll:
    def test_registers_all_collections(self, db_conn):
        names = register_all_collections(db_conn)
        assert "ai-dev-governance" in names

    def test_idempotent(self, db_conn):
        names1 = register_all_collections(db_conn)
        names2 = register_all_collections(db_conn)
        assert names1 == names2


class TestScanAll:
    def test_scans_all_collections(self, db_conn, sample_project):
        register_all_collections(db_conn)
        results = scan_all_collections(db_conn, sample_project)
        assert len(results) > 0

    def test_scan_idempotent(self, db_conn, sample_project):
        register_all_collections(db_conn)
        scan_all_collections(db_conn, sample_project)
        second = scan_all_collections(db_conn, sample_project)
        assert len(second) == 0


class TestBackwardCompat:
    def test_old_function_name_still_works(self):
        from src.collections.ai_dev_governance import register_ai_dev_governance, register_collection
        assert register_ai_dev_governance is register_collection
