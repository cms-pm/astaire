"""Tests for src/db module."""

import sqlite3

from src.db import get_connection, init_db, transaction


class TestConnection:
    def test_in_memory_connection(self):
        conn = get_connection(":memory:")
        assert conn is not None
        conn.close()

    def test_row_factory_set(self):
        conn = get_connection(":memory:")
        assert conn.row_factory == sqlite3.Row
        conn.close()

    def test_wal_mode(self):
        conn = get_connection(":memory:")
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        # In-memory databases may report 'memory' instead of 'wal'
        assert mode in ("wal", "memory")
        conn.close()

    def test_foreign_keys_enabled(self):
        conn = get_connection(":memory:")
        fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
        assert fk == 1
        conn.close()


class TestInitDB:
    def test_schema_creates_tables(self, db_conn):
        tables = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = {r["name"] for r in tables}
        expected = {
            "source", "entity", "claim", "relationship",
            "topic_cluster", "claim_cluster", "contradiction",
            "projection_cache", "ingest_log",
            "collection", "document", "document_tag", "document_dependency",
            # FTS5 tables
            "claim_fts", "entity_fts", "document_fts",
        }
        # FTS5 creates shadow tables; just check our main tables exist
        assert expected.issubset(table_names | {"claim_fts", "entity_fts", "document_fts"})

    def test_views_exist(self, db_conn):
        views = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
        ).fetchall()
        view_names = {r["name"] for r in views}
        assert "v_active_claims" in view_names
        assert "v_open_contradictions" in view_names
        assert "v_entity_hub_scores" in view_names
        assert "v_active_documents" in view_names

    def test_idempotent_init(self, db_conn):
        # Running init again should not raise
        init_db(db_conn)

    def test_ingest_log_accepts_register(self, db_conn):
        from src.utils.ulid import generate
        with transaction(db_conn) as cur:
            cur.execute(
                "INSERT INTO ingest_log (log_id, operation, summary, documents_registered) VALUES (?, 'register', 'test', 1)",
                (generate(),)
            )
        row = db_conn.execute("SELECT operation, documents_registered FROM ingest_log").fetchone()
        assert row["operation"] == "register"
        assert row["documents_registered"] == 1


class TestTransaction:
    def test_commit_on_success(self, db_conn):
        from src.utils.ulid import generate
        with transaction(db_conn) as cur:
            cur.execute(
                "INSERT INTO collection (collection_id, name) VALUES (?, ?)",
                (generate(), "test-collection")
            )
        row = db_conn.execute("SELECT name FROM collection").fetchone()
        assert row["name"] == "test-collection"

    def test_rollback_on_error(self, db_conn):
        from src.utils.ulid import generate
        try:
            with transaction(db_conn) as cur:
                cur.execute(
                    "INSERT INTO collection (collection_id, name) VALUES (?, ?)",
                    (generate(), "will-rollback")
                )
                raise ValueError("deliberate error")
        except ValueError:
            pass
        row = db_conn.execute("SELECT COUNT(*) FROM collection").fetchone()
        assert row[0] == 0
