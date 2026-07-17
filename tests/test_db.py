"""Tests for src/db module."""

import sqlite3

from src.db import (
    claims_module_present,
    get_connection,
    init_db,
    install_claims_module,
    migrate_document_fts_body_column,
    migrate_source_type_taxonomy,
    transaction,
)


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


class TestSourceTypeTaxonomyMigration:
    """Issue #15 — source_type CHECK constraint extension with governance types."""

    _OLD_DDL = """
        CREATE TABLE source (
            source_id     TEXT PRIMARY KEY,
            title         TEXT NOT NULL,
            source_type   TEXT NOT NULL CHECK (source_type IN (
                'article','paper','transcript','note','code','synthesis'
            )),
            content_hash  TEXT NOT NULL,
            file_path     TEXT,
            media_type    TEXT DEFAULT 'text/markdown',
            token_count   INTEGER DEFAULT 0,
            ingested_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            metadata_json TEXT DEFAULT '{}'
        )
    """

    def _legacy_db(self):
        conn = get_connection(":memory:")
        conn.executescript(self._OLD_DDL)
        return conn

    def test_legacy_db_rejects_governance_type_before_migration(self):
        conn = self._legacy_db()
        from src.utils.ulid import generate
        try:
            conn.execute(
                "INSERT INTO source (source_id, title, source_type, content_hash) "
                "VALUES (?, 'plan', 'chunk-plan', 'h')",
                (generate(),),
            )
            raised = False
        except sqlite3.IntegrityError:
            raised = True
        assert raised
        conn.close()

    def test_migration_rebuilds_and_preserves_rows(self):
        conn = self._legacy_db()
        from src.utils.ulid import generate

        sid = generate()
        conn.execute(
            "INSERT INTO source (source_id, title, source_type, content_hash) "
            "VALUES (?, 'note-1', 'note', 'h1')",
            (sid,),
        )
        conn.commit()

        ran = migrate_source_type_taxonomy(conn)
        assert ran is True

        row = conn.execute(
            "SELECT title, source_type FROM source WHERE source_id = ?", (sid,)
        ).fetchone()
        assert row["title"] == "note-1"
        assert row["source_type"] == "note"

        conn.execute(
            "INSERT INTO source (source_id, title, source_type, content_hash) "
            "VALUES (?, 'plan', 'chunk-plan', 'h2')",
            (generate(),),
        )
        conn.commit()
        conn.close()

    def test_migration_preserves_rows_with_foreign_key_dependents(self):
        conn = self._legacy_db()
        from src.utils.ulid import generate

        conn.execute("""
            CREATE TABLE ingest_log (
                log_id    TEXT PRIMARY KEY,
                source_id TEXT REFERENCES source(source_id)
            )
        """)
        sid = generate()
        log_id = generate()
        conn.execute(
            "INSERT INTO source (source_id, title, source_type, content_hash) "
            "VALUES (?, 'note-1', 'note', 'h1')",
            (sid,),
        )
        conn.execute(
            "INSERT INTO ingest_log (log_id, source_id) VALUES (?, ?)",
            (log_id, sid),
        )
        conn.commit()

        assert migrate_source_type_taxonomy(conn) is True

        row = conn.execute(
            """
            SELECT s.title
            FROM ingest_log il
            JOIN source s ON s.source_id = il.source_id
            WHERE il.log_id = ?
            """,
            (log_id,),
        ).fetchone()
        assert row["title"] == "note-1"
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        conn.close()

    def test_migration_is_idempotent(self):
        conn = self._legacy_db()
        assert migrate_source_type_taxonomy(conn) is True
        assert migrate_source_type_taxonomy(conn) is False
        conn.close()

    def test_migration_noop_on_fresh_init_db(self, db_conn):
        assert migrate_source_type_taxonomy(db_conn) is False

    def test_init_db_upgrades_legacy_schema(self):
        conn = self._legacy_db()
        init_db(conn)
        from src.utils.ulid import generate
        for stype in ("chunk-plan", "validation", "gherkin", "architecture", "adr", "memo", "contract-test"):
            conn.execute(
                "INSERT INTO source (source_id, title, source_type, content_hash) "
                "VALUES (?, ?, ?, ?)",
                (generate(), f"t-{stype}", stype, "h"),
            )
        conn.commit()
        conn.close()


class TestDocumentFtsBodyColumnMigration:
    """Proposal B item 1's schema half: document_fts gains an opt-in `body` column."""

    _OLD_DOCUMENT_FTS_DDL = """
        CREATE VIRTUAL TABLE document_fts USING fts5(
            title, external_id, doc_type, tokenize = 'porter unicode61'
        );
        CREATE TRIGGER trg_document_fts_insert AFTER INSERT ON document
        BEGIN
            INSERT INTO document_fts(rowid, title, external_id, doc_type)
            VALUES (NEW.rowid, NEW.title, COALESCE(NEW.external_id,''), NEW.doc_type);
        END;
        CREATE TRIGGER trg_document_fts_update AFTER UPDATE OF title, external_id, doc_type ON document
        BEGIN
            DELETE FROM document_fts WHERE rowid = OLD.rowid;
            INSERT INTO document_fts(rowid, title, external_id, doc_type)
            VALUES (NEW.rowid, NEW.title, COALESCE(NEW.external_id,''), NEW.doc_type);
        END;
        CREATE TRIGGER trg_document_fts_delete AFTER DELETE ON document
        BEGIN
            DELETE FROM document_fts WHERE rowid = OLD.rowid;
        END;
    """

    def _legacy_db(self):
        # Bring up the full current schema, then revert document_fts + its
        # triggers back to the pre-migration (3-column) shape — every other
        # table (document, collection, ...) stays current, since only
        # document_fts's own shape is what the migration detects/rebuilds.
        conn = get_connection(":memory:")
        init_db(conn)
        conn.execute("DROP TRIGGER trg_document_fts_insert")
        conn.execute("DROP TRIGGER trg_document_fts_update")
        conn.execute("DROP TRIGGER trg_document_fts_delete")
        conn.execute("DROP TABLE document_fts")
        conn.executescript(self._OLD_DOCUMENT_FTS_DDL)
        conn.commit()
        return conn

    def test_legacy_db_has_no_body_column(self):
        conn = self._legacy_db()
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='document_fts'"
        ).fetchone()
        assert "body" not in row["sql"]
        conn.close()

    def test_migration_adds_body_column_and_preserves_existing_rows(self, tmp_path):
        from src.registry import create_collection, register_document, search_documents

        conn = self._legacy_db()
        create_collection(conn, "legacy-col", config={})
        f = tmp_path / "f.md"
        f.write_text("legacy content")
        register_document(conn, "legacy-col", f, "spec", "Legacy Doc", external_id="LEG-1")

        ran = migrate_document_fts_body_column(conn)
        assert ran is True

        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='document_fts'"
        ).fetchone()
        assert "body" in row["sql"]

        # title/external_id/doc_type indexing survived the rebuild.
        hits = search_documents(conn, "Legacy")
        assert [h["title"] for h in hits] == ["Legacy Doc"]
        conn.close()

    def test_migration_is_idempotent(self, tmp_path):
        conn = self._legacy_db()
        assert migrate_document_fts_body_column(conn) is True
        assert migrate_document_fts_body_column(conn) is False
        conn.close()

    def test_migration_noop_on_fresh_init_db(self, db_conn):
        assert migrate_document_fts_body_column(db_conn) is False

    def test_body_column_usable_immediately_after_migration(self, tmp_path):
        from src.registry import create_collection, register_document, search_documents

        conn = self._legacy_db()
        create_collection(conn, "legacy-col", config={})
        f = tmp_path / "f.md"
        f.write_text("legacy content")
        doc_id = register_document(conn, "legacy-col", f, "spec", "Legacy Doc")
        migrate_document_fts_body_column(conn)

        # body is empty for the pre-existing row (not backfilled from disk at
        # migration time — see the function's own docstring), but the column
        # is immediately writable for any *new* content-indexing write path.
        conn.execute(
            "UPDATE document_fts SET body = ? WHERE rowid = "
            "(SELECT rowid FROM document WHERE document_id = ?)",
            ("backfilled content", doc_id),
        )
        conn.commit()
        assert [h["title"] for h in search_documents(conn, "backfilled")] == ["Legacy Doc"]
        conn.close()


class TestClaimsModule:
    """Proposal A — the claim/entity/relationship/contradiction subsystem is
    an optional module, installed only by explicit opt-in."""

    _CLAIMS_TABLES = {
        "entity", "claim", "relationship", "topic_cluster",
        "claim_cluster", "contradiction",
    }

    def test_core_only_db_has_no_claims_tables(self, db_conn_core_only):
        tables = {
            r["name"] for r in db_conn_core_only.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert self._CLAIMS_TABLES.isdisjoint(tables)
        assert "document" in tables
        assert "source" in tables

    def test_claims_module_present_false_on_core_only_db(self, db_conn_core_only):
        assert claims_module_present(db_conn_core_only) is False

    def test_claims_module_present_true_after_install(self, db_conn_core_only):
        install_claims_module(db_conn_core_only)
        assert claims_module_present(db_conn_core_only) is True

    def test_claims_module_present_true_with_init_db_with_claims(self, db_conn):
        # db_conn fixture calls init_db(conn, with_claims=True)
        assert claims_module_present(db_conn) is True

    def test_install_claims_module_creates_tables(self, db_conn_core_only):
        install_claims_module(db_conn_core_only)
        tables = {
            r["name"] for r in db_conn_core_only.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert self._CLAIMS_TABLES.issubset(tables)

    def test_install_claims_module_is_idempotent(self, db_conn_core_only):
        from src.utils.ulid import generate

        install_claims_module(db_conn_core_only)
        with transaction(db_conn_core_only) as cur:
            cur.execute(
                "INSERT INTO entity (entity_id, canonical_name, entity_type) "
                "VALUES (?, 'Widget', 'concept')",
                (generate(),),
            )
        # Re-running must not raise and must not touch existing rows.
        install_claims_module(db_conn_core_only)
        row = db_conn_core_only.execute(
            "SELECT COUNT(*) AS n FROM entity"
        ).fetchone()
        assert row["n"] == 1

    def test_init_db_with_claims_true_installs_module(self, db_conn_core_only):
        assert claims_module_present(db_conn_core_only) is False
        init_db(db_conn_core_only, with_claims=True)
        assert claims_module_present(db_conn_core_only) is True

    def test_backward_compat_existing_claims_data_untouched_by_core_reinit(self):
        """Regression: an old-style DB (core + claims schema applied together,
        the pre-Proposal-A shape) must keep working unchanged when the new
        core-only init_db() is re-run against it."""
        from src.utils.ulid import generate

        conn = get_connection(":memory:")
        init_db(conn, with_claims=True)

        entity_id = generate()
        with transaction(conn) as cur:
            cur.execute(
                "INSERT INTO entity (entity_id, canonical_name, entity_type) "
                "VALUES (?, 'Old Entity', 'concept')",
                (entity_id,),
            )

        # Re-running the (now core-only-by-default) init_db() must not drop
        # or otherwise disturb the pre-existing claims-module data.
        init_db(conn)

        assert claims_module_present(conn) is True
        row = conn.execute(
            "SELECT canonical_name FROM entity WHERE entity_id = ?", (entity_id,)
        ).fetchone()
        assert row["canonical_name"] == "Old Entity"
        conn.close()


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
