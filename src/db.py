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

    conn.execute("BEGIN")
    try:
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
    logger.info("Migrated source.source_type CHECK constraint with governance types")
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
