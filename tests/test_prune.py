"""Tests for src/prune module — Phase 5 chunk 5.3."""

import pytest

from src.db import get_connection, init_db, transaction
from src.project import generate_l0, read_cache, read_l0
from src.prune import prune_expired_claims
from src.utils import hashing, ulid


# ── Helpers ──────────────────────────────────────────────────────

def _create_source(conn, title="Test Source"):
    source_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO source (source_id, title, source_type, content_hash, file_path, token_count)
               VALUES (?, ?, 'note', ?, '/tmp/fake.md', 10)""",
            (source_id, title, hashing.hash_content(source_id)),
        )
    return source_id


def _create_entity(conn, name):
    entity_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            "INSERT INTO entity (entity_id, canonical_name, entity_type) VALUES (?, ?, 'concept')",
            (entity_id, name),
        )
    return entity_id


def _create_claim(conn, entity_id, source_id, predicate="prop", value="val", expires_at=None):
    claim_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO claim
               (claim_id, entity_id, predicate, value, claim_type, confidence,
                epistemic_tag, source_id, expires_at)
               VALUES (?, ?, ?, ?, 'fact', 0.8, 'provisional', ?, ?)""",
            (claim_id, entity_id, predicate, value, source_id, expires_at),
        )
    return claim_id


def _create_cluster_with_claims(conn, entity_id, source_id, label, n_total, n_expired):
    """Create a cluster with n_total claims, n_expired of which are expired."""
    cluster_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            "INSERT INTO topic_cluster (cluster_id, label, claim_count) VALUES (?, ?, ?)",
            (cluster_id, label, n_total),
        )
    claim_ids = []
    for i in range(n_total):
        expires = "2020-01-01T00:00:00Z" if i < n_expired else None
        cid = _create_claim(conn, entity_id, source_id, f"p{i}", f"v{i}", expires_at=expires)
        claim_ids.append(cid)
        with transaction(conn) as cur:
            cur.execute(
                "INSERT INTO claim_cluster (claim_id, cluster_id) VALUES (?, ?)",
                (cid, cluster_id),
            )
    return cluster_id, claim_ids


# ── SCN-5.3-01: Expired claims deleted ──────────────────────────

class TestExpiredClaimDeletion:
    def test_deletes_expired_claims(self, db_conn):
        src = _create_source(db_conn)
        eid = _create_entity(db_conn, "Widget")
        _create_claim(db_conn, eid, src, "color", "red", expires_at="2020-01-01T00:00:00Z")

        result = prune_expired_claims(db_conn)
        assert result["claims_pruned"] == 1

        remaining = db_conn.execute("SELECT COUNT(*) FROM claim").fetchone()[0]
        assert remaining == 0

    def test_deletes_multiple_expired(self, db_conn):
        src = _create_source(db_conn)
        eid = _create_entity(db_conn, "Widget")
        _create_claim(db_conn, eid, src, "a", "1", expires_at="2020-01-01T00:00:00Z")
        _create_claim(db_conn, eid, src, "b", "2", expires_at="2020-06-01T00:00:00Z")

        result = prune_expired_claims(db_conn)
        assert result["claims_pruned"] == 2


# ── SCN-5.3-02: Non-expired claims preserved ────────────────────

class TestPreservation:
    def test_preserves_future_expiry(self, db_conn):
        src = _create_source(db_conn)
        eid = _create_entity(db_conn, "Widget")
        _create_claim(db_conn, eid, src, "color", "blue", expires_at="2099-01-01T00:00:00Z")

        result = prune_expired_claims(db_conn)
        assert result["claims_pruned"] == 0

        remaining = db_conn.execute("SELECT COUNT(*) FROM claim").fetchone()[0]
        assert remaining == 1

    def test_preserves_null_expiry(self, db_conn):
        src = _create_source(db_conn)
        eid = _create_entity(db_conn, "Widget")
        _create_claim(db_conn, eid, src, "color", "green")  # no expires_at

        result = prune_expired_claims(db_conn)
        assert result["claims_pruned"] == 0


# ── SCN-5.3-03: Orphan claim_cluster cleanup ────────────────────

class TestClusterCleanup:
    def test_removes_claim_cluster_rows(self, db_conn):
        src = _create_source(db_conn)
        eid = _create_entity(db_conn, "Widget")
        cluster_id, _ = _create_cluster_with_claims(db_conn, eid, src, "Test", 3, 2)

        result = prune_expired_claims(db_conn)
        assert result["claims_pruned"] == 2
        assert result["clusters_cleaned"] == 2

        remaining = db_conn.execute(
            "SELECT COUNT(*) FROM claim_cluster WHERE cluster_id = ?", (cluster_id,)
        ).fetchone()[0]
        assert remaining == 1


# ── SCN-5.3-04: Cluster count update ────────────────────────────

class TestClusterCountUpdate:
    def test_decrements_claim_count(self, db_conn):
        src = _create_source(db_conn)
        eid = _create_entity(db_conn, "Widget")
        cluster_id, _ = _create_cluster_with_claims(db_conn, eid, src, "Test", 5, 2)

        prune_expired_claims(db_conn)

        row = db_conn.execute(
            "SELECT claim_count FROM topic_cluster WHERE cluster_id = ?", (cluster_id,)
        ).fetchone()
        assert row["claim_count"] == 3


# ── SCN-5.3-05: Cache refresh ───────────────────────────────────

class TestCacheRefresh:
    def test_regenerates_l0_after_prune(self, db_conn):
        src = _create_source(db_conn)
        eid = _create_entity(db_conn, "Widget")
        _create_claim(db_conn, eid, src, "a", "1", expires_at="2020-01-01T00:00:00Z")
        generate_l0(db_conn)

        result = prune_expired_claims(db_conn)
        assert result["l0_regenerated"] is True

        l0 = read_l0(db_conn)
        assert l0 is not None

    def test_invalidates_entity_l1_cache(self, db_conn):
        from src.project import generate_l1_entity
        src = _create_source(db_conn)
        eid = _create_entity(db_conn, "Widget")
        _create_claim(db_conn, eid, src, "a", "1", expires_at="2020-01-01T00:00:00Z")
        _create_claim(db_conn, eid, src, "b", "2")  # permanent claim
        generate_l1_entity(db_conn, eid)
        assert read_cache(db_conn, "L1", f"entity:{eid}") is not None

        prune_expired_claims(db_conn)
        # L1 should be invalidated
        assert read_cache(db_conn, "L1", f"entity:{eid}") is None


# ── SCN-5.3-06: Prune log ───────────────────────────────────────

class TestPruneLog:
    def test_writes_ingest_log(self, db_conn):
        src = _create_source(db_conn)
        eid = _create_entity(db_conn, "Widget")
        _create_claim(db_conn, eid, src, "a", "1", expires_at="2020-01-01T00:00:00Z")

        prune_expired_claims(db_conn)

        row = db_conn.execute(
            "SELECT * FROM ingest_log WHERE operation = 'prune' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        assert row is not None
        assert "Pruned 1" in row["summary"]


# ── SCN-5.3-07: No-op prune ─────────────────────────────────────

class TestNoOpPrune:
    def test_no_expired_claims(self, db_conn):
        src = _create_source(db_conn)
        eid = _create_entity(db_conn, "Widget")
        _create_claim(db_conn, eid, src, "a", "1")  # no expiry

        result = prune_expired_claims(db_conn)
        assert result["claims_pruned"] == 0
        assert result["l0_regenerated"] is False

        # No ingest_log entry
        row = db_conn.execute(
            "SELECT * FROM ingest_log WHERE operation = 'prune'"
        ).fetchone()
        assert row is None

    def test_empty_database(self, db_conn):
        result = prune_expired_claims(db_conn)
        assert result["claims_pruned"] == 0
        assert result["clusters_cleaned"] == 0
