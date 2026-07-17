"""Database connection management, schema initialization, and transaction handling."""

import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "db" / "memory_palace.db"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "docs" / "schema" / "memory_palace_schema.sql"


def get_connection(db_path: str | Path | None = None) -> sqlite3.Connection:
    """Open a SQLite connection with WAL mode, foreign keys, and busy_timeout.

    Pass ":memory:" for an in-memory database (used in tests).
    """
    path = str(db_path) if db_path else str(DB_PATH)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_db(conn: sqlite3.Connection, schema_path: str | Path | None = None) -> None:
    """Initialize the database schema. Idempotent — uses IF NOT EXISTS throughout."""
    path = Path(schema_path) if schema_path else SCHEMA_PATH
    ddl = path.read_text()
    conn.executescript(ddl)
    migrate_source_type_taxonomy(conn)
    migrate_document_fts_body_column(conn)
    logger.info("Database schema initialized from %s", path)


# Governance-artifact source_type values added in issue #15. An existing DB
# created before this migration carries the older research-only CHECK
# constraint and must be rebuilt before governance ingest can succeed.
_GOVERNANCE_SOURCE_TYPES = ("chunk-plan", "validation", "gherkin", "architecture", "adr", "memo", "contract-test")


def migrate_source_type_taxonomy(conn: sqlite3.Connection) -> bool:
    """Extend source.source_type CHECK constraint with governance-artifact types.

    Idempotent. Returns True if a rebuild ran, False if the schema was already
    current. SQLite cannot ALTER a CHECK constraint, so when the old constraint
    is detected we rebuild source via create-new + copy + drop + rename inside
    a single transaction.
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='source'"
    ).fetchone()
    if row is None:
        return False
    existing_sql = row[0] or ""
    if "'chunk-plan'" in existing_sql:
        return False

    foreign_keys_enabled = bool(conn.execute("PRAGMA foreign_keys").fetchone()[0])
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        conn.execute("BEGIN")
        conn.execute("""
            CREATE TABLE source__new (
                source_id     TEXT PRIMARY KEY,
                title         TEXT NOT NULL,
                source_type   TEXT NOT NULL CHECK (source_type IN (
                    'article','paper','transcript','note','code','synthesis',
                    'chunk-plan','validation','gherkin','architecture','adr','memo','contract-test'
                )),
                content_hash  TEXT NOT NULL,
                file_path     TEXT,
                media_type    TEXT DEFAULT 'text/markdown',
                token_count   INTEGER DEFAULT 0,
                ingested_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                metadata_json TEXT DEFAULT '{}'
            )
        """)
        conn.execute("""
            INSERT INTO source__new
                (source_id, title, source_type, content_hash, file_path,
                 media_type, token_count, ingested_at, metadata_json)
            SELECT source_id, title, source_type, content_hash, file_path,
                   media_type, token_count, ingested_at, metadata_json
            FROM source
        """)
        conn.execute("DROP TABLE source")
        conn.execute("ALTER TABLE source__new RENAME TO source")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.execute(f"PRAGMA foreign_keys = {'ON' if foreign_keys_enabled else 'OFF'}")
    if foreign_keys_enabled:
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise sqlite3.IntegrityError(
                f"foreign key violations after source migration: {violations}"
            )
    logger.info("Migrated source.source_type CHECK constraint with governance types")
    return True


def migrate_document_fts_body_column(conn: sqlite3.Connection) -> bool:
    """Add the opt-in `body` content-index column to `document_fts` (idempotent).

    FTS5 virtual tables cannot be ALTERed. Detects the old (3-column) shape
    via `sqlite_master`'s stored `CREATE VIRTUAL TABLE` text — mirroring
    `migrate_source_type_taxonomy`'s detect-then-rebuild pattern — and if
    found: drops `document_fts` (a purely derived cache, safe to rebuild)
    and its three triggers, recreates all four from the current schema, then
    repopulates `title`/`external_id`/`doc_type` for every existing document
    row. `body` is intentionally left empty for pre-existing rows — content
    indexing is opt-in and size-gated at write time (see
    `src/registry.py::register_document`), and re-reading every registered
    file from disk at migration time risks hitting files that have since
    moved or been deleted; a document's body is (re-)indexed the next time
    it is registered or synced.
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='document_fts'"
    ).fetchone()
    if row is None:
        return False  # not yet created; init_db's own DDL creates the current shape
    existing_sql = row[0] or ""
    if "body" in existing_sql:
        return False

    conn.execute("BEGIN")
    try:
        conn.execute("DROP TRIGGER IF EXISTS trg_document_fts_insert")
        conn.execute("DROP TRIGGER IF EXISTS trg_document_fts_update")
        conn.execute("DROP TRIGGER IF EXISTS trg_document_fts_delete")
        conn.execute("DROP TABLE document_fts")
        conn.execute(
            "CREATE VIRTUAL TABLE document_fts USING fts5("
            "title, external_id, doc_type, body, tokenize = 'porter unicode61')"
        )
        conn.execute("""
            CREATE TRIGGER trg_document_fts_insert AFTER INSERT ON document
            BEGIN
                INSERT INTO document_fts(rowid, title, external_id, doc_type)
                VALUES (NEW.rowid, NEW.title, COALESCE(NEW.external_id,''), NEW.doc_type);
            END
        """)
        conn.execute("""
            CREATE TRIGGER trg_document_fts_update AFTER UPDATE OF title, external_id, doc_type ON document
            BEGIN
                UPDATE document_fts
                SET title = NEW.title, external_id = COALESCE(NEW.external_id,''), doc_type = NEW.doc_type
                WHERE rowid = NEW.rowid;
            END
        """)
        conn.execute("""
            CREATE TRIGGER trg_document_fts_delete AFTER DELETE ON document
            BEGIN
                DELETE FROM document_fts WHERE rowid = OLD.rowid;
            END
        """)
        conn.execute(
            "INSERT INTO document_fts(rowid, title, external_id, doc_type) "
            "SELECT rowid, title, COALESCE(external_id,''), doc_type FROM document"
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    logger.info("Migrated document_fts to add the opt-in `body` content-index column")
    return True


@contextmanager
def managed_connection(db_path: str | Path | None = None) -> Generator[sqlite3.Connection, None, None]:
    """Context manager for a database connection. Closes on exit."""
    path = str(db_path) if db_path else str(DB_PATH)
    db_dir = Path(path).parent
    if path != ":memory:" and not db_dir.exists():
        raise SystemExit(f"Error: directory does not exist: {db_dir}")
    conn = get_connection(path)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Generator[sqlite3.Cursor, None, None]:
    """Context manager for a database transaction. Commits on success, rolls back on error."""
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
