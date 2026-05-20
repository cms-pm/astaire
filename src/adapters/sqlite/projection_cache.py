"""SQLite-backed `ProjectionCache` Protocol implementation.

Wraps the existing `projection_cache` table writes that
`astaire/src/project.py:_upsert_cache` performs inline. The legacy
function continues to work; new domain code can route through this
adapter.
"""

from __future__ import annotations

import sqlite3

from src.db import transaction
from src.utils import hashing, tokens, ulid

from src.domain.claims.models import ProjectionTier


class SQLiteProjectionCache:
    """ProjectionCache implemented against the live SQLite schema."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def read(self, tier: ProjectionTier, scope_key: str) -> str | None:
        row = self._conn.execute(
            "SELECT content_md FROM projection_cache "
            "WHERE tier = ? AND scope_key = ?",
            (tier, scope_key),
        ).fetchone()
        return row["content_md"] if row else None

    def write(
        self,
        tier: ProjectionTier,
        scope_key: str,
        content: str,
        encoding: str = "cl100k_base",
    ) -> None:
        content_hash = hashing.hash_content(content)
        token_count = tokens.count_tokens(content, encoding)
        cache_id = ulid.generate()
        with transaction(self._conn) as cur:
            cur.execute(
                "INSERT INTO projection_cache "
                "  (cache_id, tier, scope_key, content_md, token_count, "
                "   content_hash, encoding) "
                "VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(tier, scope_key) DO UPDATE SET "
                "    content_md = excluded.content_md, "
                "    token_count = excluded.token_count, "
                "    content_hash = excluded.content_hash, "
                "    encoding = excluded.encoding, "
                "    generated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')",
                (
                    cache_id,
                    tier,
                    scope_key,
                    content,
                    token_count,
                    content_hash,
                    encoding,
                ),
            )

    def invalidate(self, scope_key: str) -> None:
        with transaction(self._conn) as cur:
            cur.execute(
                "DELETE FROM projection_cache WHERE scope_key = ?",
                (scope_key,),
            )
