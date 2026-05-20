"""Claims/projection domain — I/O-free.

Re-exports the frozen-dataclass models and Port Protocols consumed by
adapter modules under `astaire/src/adapters/`.
"""

from src.domain.claims.models import (
    Claim,
    ClaimId,
    ClaimType,
    Contradiction,
    Entity,
    EntityId,
    EntityType,
    EpistemicTag,
    EntityHubRow,
    FTSHit,
    ProjectionMetrics,
    ProjectionTier,
)
from src.domain.claims.ports import (
    ClaimRepository,
    EntityRepository,
    FTSIndex,
    ProjectionCache,
)

__all__ = [
    "Claim",
    "ClaimId",
    "ClaimRepository",
    "ClaimType",
    "Contradiction",
    "Entity",
    "EntityHubRow",
    "EntityId",
    "EntityRepository",
    "EntityType",
    "EpistemicTag",
    "FTSHit",
    "FTSIndex",
    "ProjectionCache",
    "ProjectionMetrics",
    "ProjectionTier",
]
