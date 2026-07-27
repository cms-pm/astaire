"""Claims/projection domain models — frozen, I/O-free.

Per `core/MODULARITY_GOVERNANCE.md` (ADG) and the SCN-9.4 pilot, these
dataclasses carry only the fields the projection engine consumes; they
do not own behaviour that talks to SQLite, the filesystem, or the
tokenizer.

Glossary references for each public name land in
`docs/glossary/memory-palace.md` (ADG repo).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, NewType

# Branded IDs (NewType keeps them mypy-distinguishable without runtime
# overhead). Matches the schema columns of the same name.
ClaimId = NewType("ClaimId", str)
EntityId = NewType("EntityId", str)
SourceId = NewType("SourceId", str)
ClusterId = NewType("ClusterId", str)

EntityType = Literal["person", "org", "system", "concept", "place", "event"]
ClaimType = Literal["fact", "opinion", "metric", "status", "definition"]
EpistemicTag = Literal["confirmed", "provisional", "contested", "retracted"]
ProjectionTier = Literal["L0", "L1", "L2"]


@dataclass(frozen=True, slots=True)
class Entity:
    """A de-duplicated subject of one or more claims.

    Glossary: `docs/glossary/memory-palace.md` §Entity.
    """

    entity_id: EntityId
    canonical_name: str
    entity_type: EntityType
    description: str | None = None
    aliases: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class Claim:
    """A single structured assertion `(entity, predicate, value)`.

    Glossary: `docs/glossary/memory-palace.md` §Claim.
    """

    claim_id: ClaimId
    entity_id: EntityId
    predicate: str
    value: str
    claim_type: ClaimType
    confidence: float
    epistemic_tag: EpistemicTag
    source_id: SourceId
    superseded_by: ClaimId | None = None


@dataclass(frozen=True, slots=True)
class Contradiction:
    """A first-class record of two claims that disagree.

    Glossary: `docs/glossary/memory-palace.md` §Contradiction.
    """

    contradiction_id: str
    claim_a_id: ClaimId
    claim_b_id: ClaimId
    description: str | None
    resolution_status: Literal["open", "superseded", "accepted-both"]


@dataclass(frozen=True, slots=True)
class EntityHubRow:
    """A row of the `v_entity_hub_scores` view; the L0 generator's
    primary input.
    """

    canonical_name: str
    entity_type: EntityType
    claim_count: int
    hub_score: int


@dataclass(frozen=True, slots=True)
class ProjectionMetrics:
    """The numeric envelope of an L0 summary.

    Pure data; the markdown rendering is the projection engine's job.
    """

    source_count: int
    entity_count: int
    claim_count: int
    relationship_count: int
    document_count: int
    collection_count: int
    open_contradictions: int


@dataclass(frozen=True, slots=True)
class FTSHit:
    """A single full-text-search result.

    Glossary: `docs/glossary/memory-palace.md` §FTS.
    """

    rowid: int
    rank: float
    snippet: str | None = None
