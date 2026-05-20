"""SQLite-backed `ClaimRepository` and `EntityRepository`.

Wraps the existing `claim`, `entity`, and `v_entity_hub_scores` SQL
surface that the legacy `astaire/src/project.py` calls inline. The
projection engine MAY consume the Protocol via these adapters (SCN-9.4
pilot path) or continue to issue inline SQL (legacy path); both must
return byte-identical L0 content.
"""

from __future__ import annotations

import sqlite3

from src.domain.claims.models import (
    Claim,
    ClaimId,
    ClaimType,
    ClusterId,
    Entity,
    EntityHubRow,
    EntityId,
    EntityType,
    EpistemicTag,
    SourceId,
)


class SQLiteClaimRepository:
    """ClaimRepository implemented against the live SQLite schema."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def count_active_claims(self) -> int:
        return self._conn.execute(
            "SELECT COUNT(*) FROM claim "
            "WHERE superseded_by IS NULL "
            "  AND epistemic_tag != 'retracted'"
        ).fetchone()[0]

    def get_claim(self, claim_id: ClaimId) -> Claim | None:
        row = self._conn.execute(
            "SELECT claim_id, entity_id, predicate, value, claim_type, "
            "       confidence, epistemic_tag, source_id, superseded_by "
            "FROM claim WHERE claim_id = ?",
            (claim_id,),
        ).fetchone()
        return _row_to_claim(row) if row else None

    def list_active_claims_for_entity(self, entity_id: EntityId) -> list[Claim]:
        rows = self._conn.execute(
            "SELECT claim_id, entity_id, predicate, value, claim_type, "
            "       confidence, epistemic_tag, source_id, superseded_by "
            "FROM claim "
            "WHERE entity_id = ? "
            "  AND superseded_by IS NULL "
            "  AND epistemic_tag != 'retracted' "
            "ORDER BY confidence DESC, updated_at DESC",
            (entity_id,),
        ).fetchall()
        return [_row_to_claim(r) for r in rows]

    def list_active_claims_for_cluster(self, cluster_id: ClusterId) -> list[Claim]:
        rows = self._conn.execute(
            "SELECT c.claim_id, c.entity_id, c.predicate, c.value, "
            "       c.claim_type, c.confidence, c.epistemic_tag, "
            "       c.source_id, c.superseded_by "
            "FROM claim c "
            "JOIN claim_cluster cc ON cc.claim_id = c.claim_id "
            "WHERE cc.cluster_id = ? "
            "  AND c.superseded_by IS NULL "
            "  AND c.epistemic_tag != 'retracted' "
            "ORDER BY c.confidence DESC, c.updated_at DESC",
            (cluster_id,),
        ).fetchall()
        return [_row_to_claim(r) for r in rows]


class SQLiteEntityRepository:
    """EntityRepository implemented against the live SQLite schema."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def count_entities(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM entity").fetchone()[0]

    def get_entity(self, entity_id: EntityId) -> Entity | None:
        row = self._conn.execute(
            "SELECT entity_id, canonical_name, entity_type, description, "
            "       aliases_json "
            "FROM entity WHERE entity_id = ?",
            (entity_id,),
        ).fetchone()
        if row is None:
            return None
        import json as _json
        aliases = tuple(_json.loads(row["aliases_json"] or "[]"))
        return Entity(
            entity_id=EntityId(row["entity_id"]),
            canonical_name=row["canonical_name"],
            entity_type=row["entity_type"],
            description=row["description"],
            aliases=aliases,
        )

    def list_hub_rows(self, limit: int = 30) -> list[EntityHubRow]:
        rows = self._conn.execute(
            "SELECT canonical_name, entity_type, claim_count, hub_score "
            "FROM v_entity_hub_scores LIMIT ?",
            (limit,),
        ).fetchall()
        return [
            EntityHubRow(
                canonical_name=r["canonical_name"],
                entity_type=r["entity_type"],
                claim_count=r["claim_count"],
                hub_score=r["hub_score"],
            )
            for r in rows
        ]


def _row_to_claim(row: sqlite3.Row) -> Claim:
    superseded = row["superseded_by"]
    return Claim(
        claim_id=ClaimId(row["claim_id"]),
        entity_id=EntityId(row["entity_id"]),
        predicate=row["predicate"],
        value=row["value"],
        claim_type=_cast_claim_type(row["claim_type"]),
        confidence=float(row["confidence"]),
        epistemic_tag=_cast_epistemic_tag(row["epistemic_tag"]),
        source_id=SourceId(row["source_id"]),
        superseded_by=ClaimId(superseded) if superseded else None,
    )


def _cast_claim_type(value: str) -> ClaimType:
    # The schema CHECK constraint already restricts the universe; the
    # cast here documents the invariant for type checkers.
    return value  # type: ignore[return-value]


def _cast_epistemic_tag(value: str) -> EpistemicTag:
    return value  # type: ignore[return-value]
