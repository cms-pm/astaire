"""Port Protocols — the SQLite and FTS seams.

Per `core/MODULARITY_GOVERNANCE.md` (ADG) §Ports and Adapters, these
Protocols name the operations the projection engine performs against
the storage layer. Adapter implementations live under
`astaire/src/adapters/sqlite/`. The architecture-fitness rule forbids
this file from importing `sqlite3`, `src.db`, or any FTS-coupled
symbol; everything below is pure-domain.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from src.domain.claims.models import (
    Claim,
    ClaimId,
    ClusterId,
    Entity,
    EntityHubRow,
    EntityId,
    FTSHit,
    ProjectionMetrics,
    ProjectionTier,
)


@runtime_checkable
class ClaimRepository(Protocol):
    """Read-side of the claim store.

    The projection engine consumes only this Protocol; it never touches
    `sqlite3.Connection` directly under the SCN-9.4 pilot path.
    """

    def count_active_claims(self) -> int: ...

    def get_claim(self, claim_id: ClaimId) -> Claim | None: ...

    def list_active_claims_for_entity(
        self, entity_id: EntityId
    ) -> list[Claim]: ...

    def list_active_claims_for_cluster(
        self, cluster_id: ClusterId
    ) -> list[Claim]: ...


@runtime_checkable
class EntityRepository(Protocol):
    """Read-side of the entity registry.

    Surfaces the hub-score view that drives the L0 summary.
    """

    def count_entities(self) -> int: ...

    def get_entity(self, entity_id: EntityId) -> Entity | None: ...

    def list_hub_rows(self, limit: int = 30) -> list[EntityHubRow]: ...


@runtime_checkable
class FTSIndex(Protocol):
    """The full-text-search seam.

    Domain code consumes only `search(query)`; adapters own the SQL
    sanitiser and the `MATCH` issuance.
    """

    def search(self, query: str, limit: int = 50) -> list[FTSHit]: ...


@runtime_checkable
class ProjectionCache(Protocol):
    """Read/write seam for the `projection_cache` table.

    The legacy projection engine continues to call this seam via the
    SQLite adapter; new domain code can substitute an in-memory fake.
    """

    def read(self, tier: ProjectionTier, scope_key: str) -> str | None: ...

    def write(
        self,
        tier: ProjectionTier,
        scope_key: str,
        content: str,
        encoding: str = "cl100k_base",
    ) -> None: ...

    def invalidate(self, scope_key: str) -> None: ...


# ── Read-side aggregators (pure functions) ─────────────────────


def build_metrics(
    *,
    source_count: int,
    entity_count: int,
    claim_count: int,
    relationship_count: int,
    document_count: int,
    collection_count: int,
    open_contradictions: int,
) -> ProjectionMetrics:
    """Pure constructor for `ProjectionMetrics`.

    Callers (the SQLite adapter today, port fakes in tests) supply the
    individual counts; this function lives in the domain because the
    invariant under test (every field present, no I/O) is a domain
    concern, not an adapter concern.
    """

    return ProjectionMetrics(
        source_count=source_count,
        entity_count=entity_count,
        claim_count=claim_count,
        relationship_count=relationship_count,
        document_count=document_count,
        collection_count=collection_count,
        open_contradictions=open_contradictions,
    )


def render_entity_registry_lines(rows: list[EntityHubRow]) -> list[str]:
    """Pure rendering of the L0 entity-registry section.

    The format mirrors `astaire/src/project.py:build_l0_content` so the
    adapter can adopt the function incrementally without changing the
    output byte-for-byte.
    """

    return [
        f"- **{row.canonical_name}** ({row.entity_type}): "
        f"{row.claim_count} claims, hub score {row.hub_score}"
        for row in rows
    ]
