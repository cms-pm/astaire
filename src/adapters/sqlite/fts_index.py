"""SQLite FTS5 adapter for the `FTSIndex` Protocol.

Wraps the existing `claim_fts` and `document_fts` virtual tables. The
FTS5 query sanitiser lives here because the syntax constraints belong
to the adapter, not the domain.
"""

from __future__ import annotations

import re
import sqlite3

from src.domain.claims.models import FTSHit


_FTS5_SAFE = re.compile(r"[^\w\s]")


def sanitize_fts_query(query: str) -> str:
    """Strip FTS5-significant punctuation so callers can pass raw
    identifiers (e.g. `SCN-3.2`) without triggering parse errors.

    Mirrors `astaire/src/registry.py:_sanitize_fts_query` so the new
    adapter and the legacy registry produce identical match sets.
    """

    return _FTS5_SAFE.sub(" ", query)


class SQLiteFTSIndex:
    """FTSIndex implemented against the live FTS5 virtual tables.

    Defaults to the claim index; pass `table='document_fts'` for the
    document registry index.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        table: str = "claim_fts",
    ) -> None:
        if table not in {"claim_fts", "document_fts", "entity_fts"}:
            raise ValueError(f"unsupported FTS table: {table!r}")
        self._conn = conn
        self._table = table

    def search(self, query: str, limit: int = 50) -> list[FTSHit]:
        safe = sanitize_fts_query(query)
        if not safe.strip():
            return []
        rows = self._conn.execute(
            f"SELECT rowid, rank FROM {self._table} "
            f"WHERE {self._table} MATCH ? "
            "ORDER BY rank LIMIT ?",
            (safe, limit),
        ).fetchall()
        return [FTSHit(rowid=r["rowid"], rank=float(r["rank"])) for r in rows]
