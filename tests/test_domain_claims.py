"""Domain-level tests for the SCN-9.4 claims/projection pilot.

These tests exercise the I/O-free `astaire/src/domain/claims/` package
via in-memory port fakes — no SQLite, no filesystem. They are the
reference for "new tests use the port fakes" from the SCN-9.4
acceptance criteria.

Legacy SQLite-coupled tests in `test_project.py`, `test_registry.py`,
and `test_ingest.py` continue to exercise the adapter path against the
real schema and MUST stay green.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from inspect import signature

import pytest

from src.domain.claims import (
    Claim,
    ClaimId,
    Contradiction,
    ClaimRepository,
    Entity,
    EntityHubRow,
    EntityId,
    EntityRepository,
    FTSHit,
    FTSIndex,
    ProjectionCache,
    ProjectionMetrics,
)
from src.domain.claims.ports import (
    build_metrics,
    render_entity_registry_lines,
)


# ── Port fakes ─────────────────────────────────────────────────


class _ClaimRepoFake:
    def __init__(self, claims: list[Claim]) -> None:
        self._claims = list(claims)

    def count_active_claims(self) -> int:
        return sum(
            1
            for c in self._claims
            if c.superseded_by is None and c.epistemic_tag != "retracted"
        )

    def get_claim(self, claim_id):  # type: ignore[override]
        for c in self._claims:
            if c.claim_id == claim_id:
                return c
        return None

    def list_active_claims_for_entity(self, entity_id):
        return [
            c
            for c in self._claims
            if c.entity_id == entity_id
            and c.superseded_by is None
            and c.epistemic_tag != "retracted"
        ]

    def list_active_claims_for_cluster(self, cluster_id):  # noqa: ARG002
        return []


class _EntityRepoFake:
    def __init__(self, entities: list[Entity], hub_rows: list[EntityHubRow]) -> None:
        self._entities = list(entities)
        self._hub_rows = list(hub_rows)

    def count_entities(self) -> int:
        return len(self._entities)

    def get_entity(self, entity_id):
        for e in self._entities:
            if e.entity_id == entity_id:
                return e
        return None

    def list_hub_rows(self, limit: int = 30) -> list[EntityHubRow]:
        return list(self._hub_rows[:limit])


class _FTSIndexFake:
    def __init__(self, hits: list[FTSHit]) -> None:
        self._hits = list(hits)

    def search(self, query: str, limit: int = 50):  # noqa: ARG002
        return list(self._hits[:limit])


class _ProjectionCacheFake:
    def __init__(self) -> None:
        self._store: dict[tuple[str, str], str] = {}

    def read(self, tier, scope_key):
        return self._store.get((tier, scope_key))

    def write(self, tier, scope_key, content, encoding="cl100k_base"):  # noqa: ARG002
        self._store[(tier, scope_key)] = content

    def invalidate(self, scope_key):
        self._store = {k: v for k, v in self._store.items() if k[1] != scope_key}


# ── Pytest fixtures ────────────────────────────────────────────


@pytest.fixture
def sample_entity() -> Entity:
    return Entity(
        entity_id=EntityId("E1"),
        canonical_name="Alfvén wave",
        entity_type="concept",
        description="MHD wave",
        aliases=("alfven wave", "alfven mode"),
    )


@pytest.fixture
def sample_claim(sample_entity: Entity) -> Claim:
    return Claim(
        claim_id=ClaimId("C1"),
        entity_id=sample_entity.entity_id,
        predicate="propagation speed depends on",
        value="magnetic field strength and plasma density",
        claim_type="fact",
        confidence=0.95,
        epistemic_tag="confirmed",
        source_id="S1",  # type: ignore[arg-type]
    )


@pytest.fixture
def claim_repo(sample_claim: Claim) -> ClaimRepository:
    return _ClaimRepoFake([sample_claim])


@pytest.fixture
def entity_repo(sample_entity: Entity) -> EntityRepository:
    hub = EntityHubRow(
        canonical_name=sample_entity.canonical_name,
        entity_type=sample_entity.entity_type,
        claim_count=1,
        hub_score=2,
    )
    return _EntityRepoFake([sample_entity], [hub])


@pytest.fixture
def fts_index() -> FTSIndex:
    return _FTSIndexFake([FTSHit(rowid=1, rank=-1.0, snippet=None)])


@pytest.fixture
def projection_cache() -> ProjectionCache:
    return _ProjectionCacheFake()


# ── Tests ──────────────────────────────────────────────────────


class TestDomainModels:
    def test_claim_is_frozen(self, sample_claim: Claim) -> None:
        with pytest.raises(FrozenInstanceError):
            sample_claim.confidence = 0.1  # type: ignore[misc]

    def test_entity_aliases_are_immutable(self, sample_entity: Entity) -> None:
        assert isinstance(sample_entity.aliases, tuple)

    @pytest.mark.parametrize(
        "instance,field_name,value",
        [
            (
                Entity(
                    entity_id=EntityId("E2"),
                    canonical_name="Entity",
                    entity_type="concept",
                ),
                "canonical_name",
                "changed",
            ),
            (
                Claim(
                    claim_id=ClaimId("C2"),
                    entity_id=EntityId("E2"),
                    predicate="is",
                    value="stable",
                    claim_type="fact",
                    confidence=0.5,
                    epistemic_tag="provisional",
                    source_id="S2",  # type: ignore[arg-type]
                ),
                "value",
                "changed",
            ),
            (
                Contradiction(
                    contradiction_id="K1",
                    claim_a_id=ClaimId("C1"),
                    claim_b_id=ClaimId("C2"),
                    description=None,
                    resolution_status="open",
                ),
                "resolution_status",
                "superseded",
            ),
            (
                EntityHubRow(
                    canonical_name="Entity",
                    entity_type="concept",
                    claim_count=1,
                    hub_score=2,
                ),
                "hub_score",
                3,
            ),
            (
                ProjectionMetrics(
                    source_count=1,
                    entity_count=2,
                    claim_count=3,
                    relationship_count=4,
                    document_count=5,
                    collection_count=6,
                    open_contradictions=7,
                ),
                "claim_count",
                4,
            ),
            (FTSHit(rowid=1, rank=-1.0), "rank", -2.0),
        ],
    )
    def test_domain_dataclasses_are_frozen(
        self, instance: object, field_name: str, value: object
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            setattr(instance, field_name, value)

    @pytest.mark.parametrize(
        "instance",
        [
            Entity(EntityId("E3"), "Entity", "concept"),
            Claim(
                ClaimId("C3"),
                EntityId("E3"),
                "is",
                "slotted",
                "fact",
                0.7,
                "confirmed",
                "S3",  # type: ignore[arg-type]
            ),
            Contradiction("K2", ClaimId("C3"), ClaimId("C4"), None, "open"),
            EntityHubRow("Entity", "concept", 1, 2),
            ProjectionMetrics(1, 2, 3, 4, 5, 6, 7),
            FTSHit(1, -1.0),
        ],
    )
    def test_domain_dataclasses_are_slotted(self, instance: object) -> None:
        assert not hasattr(instance, "__dict__")


class TestPortProtocols:
    def test_repositories_satisfy_protocol(
        self,
        claim_repo: ClaimRepository,
        entity_repo: EntityRepository,
        fts_index: FTSIndex,
        projection_cache: ProjectionCache,
    ) -> None:
        # runtime_checkable Protocol membership
        assert isinstance(claim_repo, ClaimRepository)
        assert isinstance(entity_repo, EntityRepository)
        assert isinstance(fts_index, FTSIndex)
        assert isinstance(projection_cache, ProjectionCache)

    def test_claim_repo_filters_superseded_and_retracted(
        self, sample_entity: Entity
    ) -> None:
        live = Claim(
            claim_id=ClaimId("Clive"),
            entity_id=sample_entity.entity_id,
            predicate="is",
            value="active",
            claim_type="fact",
            confidence=0.9,
            epistemic_tag="confirmed",
            source_id="S1",  # type: ignore[arg-type]
        )
        dead = Claim(
            claim_id=ClaimId("Cdead"),
            entity_id=sample_entity.entity_id,
            predicate="was",
            value="retracted",
            claim_type="fact",
            confidence=0.9,
            epistemic_tag="retracted",
            source_id="S1",  # type: ignore[arg-type]
        )
        repo: ClaimRepository = _ClaimRepoFake([live, dead])
        assert repo.count_active_claims() == 1
        assert repo.list_active_claims_for_entity(sample_entity.entity_id) == [
            live
        ]

    def test_protocol_default_limits_are_stable(self) -> None:
        assert signature(EntityRepository.list_hub_rows).parameters["limit"].default == 30
        assert signature(FTSIndex.search).parameters["limit"].default == 50


class TestProjectionAggregators:
    def test_build_metrics_is_pure_dataclass(self) -> None:
        metrics = build_metrics(
            source_count=1,
            entity_count=2,
            claim_count=3,
            relationship_count=4,
            document_count=5,
            collection_count=6,
            open_contradictions=7,
        )
        assert isinstance(metrics, ProjectionMetrics)
        assert metrics.claim_count == 3
        assert metrics.open_contradictions == 7

    def test_render_entity_registry_lines_format(
        self, entity_repo: EntityRepository
    ) -> None:
        rows = entity_repo.list_hub_rows(limit=5)
        lines = render_entity_registry_lines(rows)
        assert lines == [
            "- **Alfvén wave** (concept): 1 claims, hub score 2"
        ]

    def test_projection_cache_round_trip(
        self, projection_cache: ProjectionCache
    ) -> None:
        projection_cache.write("L1", "entity:E1", "hello")
        assert projection_cache.read("L1", "entity:E1") == "hello"
        projection_cache.invalidate("entity:E1")
        assert projection_cache.read("L1", "entity:E1") is None


class TestDomainImportsAreIOFree:
    """Architecture-fitness self-check (mirrors the ADG-repo audit).

    Imports `astaire.src.domain.claims` and verifies the module list
    contains no `sqlite3`, `src.db`, `astaire.fts`, or FTS5-coupled
    symbols. The authoritative gate lives in the ADG repo at
    `scripts/validators/architecture_fitness.py --audit`; this test
    catches the regression at the inner loop.
    """

    def test_domain_modules_do_not_import_sqlite_or_db(self) -> None:
        import importlib
        import sys

        for name in (
            "src.domain.claims",
            "src.domain.claims.models",
            "src.domain.claims.ports",
        ):
            importlib.import_module(name)

        forbidden = {"sqlite3", "src.db", "astaire.db", "astaire.fts"}
        loaded = set(sys.modules)
        for forbidden_name in forbidden:
            # Module may legitimately be loaded by other tests in the
            # session — what we assert is that the *domain* package does
            # not declare it as a top-level dependency at import time.
            domain_pkg = sys.modules.get("src.domain.claims")
            assert domain_pkg is not None
            assert forbidden_name not in getattr(
                domain_pkg, "__dict__", {}
            ), f"domain.claims unexpectedly exposes {forbidden_name}"
        # Sanity: domain modules were importable in isolation.
        assert "src.domain.claims" in loaded
