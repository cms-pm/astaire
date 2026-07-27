"""SQLite adapter for the claims/projection domain ports.

Concrete `ClaimRepository`, `EntityRepository`, `FTSIndex`, and
`ProjectionCache` implementations backed by the existing
`astaire/docs/schema/memory_palace_schema.sql` tables and views.
"""

from src.adapters.sqlite.claim_repo import (
    SQLiteClaimRepository,
    SQLiteEntityRepository,
)
from src.adapters.sqlite.fts_index import SQLiteFTSIndex
from src.adapters.sqlite.projection_cache import SQLiteProjectionCache

__all__ = [
    "SQLiteClaimRepository",
    "SQLiteEntityRepository",
    "SQLiteFTSIndex",
    "SQLiteProjectionCache",
]
