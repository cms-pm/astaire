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
    logger.info("Database schema initialized from %s", path)


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
