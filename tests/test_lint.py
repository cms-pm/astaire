"""Tests for src/lint module — Phase 5 chunk 5.1."""

import pytest

from src.db import get_connection, init_db, transaction
from src.lint import (
    check_document_drift,
    check_hub_score_anomalies,
    check_l0_performance,
    check_l0_staleness,
    check_missing_documents,
    check_open_contradictions,
    check_orphan_claims,
    check_orphan_entities,
    check_stale_claims,
    check_unbounded_clusters,
    run_all_checks,
)
from src.project import generate_l0, generate_l1_entity, read_cache
from src.registry import create_collection, register_document
from src.utils import ulid


# ── Helpers ──────────────────────────────────────────────────────

def _create_source(conn, title="Test Source"):
    """Create a minimal source row. Returns source_id."""
    from src.utils import hashing
    source_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO source (source_id, title, source_type, content_hash, file_path, token_count)
               VALUES (?, ?, 'note', ?, '/tmp/fake.md', 10)""",
            (source_id, title, hashing.hash_content(source_id)),
        )
    return source_id


def _create_entity(conn, name, entity_type="concept"):
    """Create an entity. Returns entity_id."""
    entity_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            "INSERT INTO entity (entity_id, canonical_name, entity_type) VALUES (?, ?, ?)",
            (entity_id, name, entity_type),
        )
    return entity_id


def _create_claim(conn, entity_id, source_id, predicate="has_property", value="test",
                  epistemic_tag="provisional", expires_at=None, updated_at=None):
    """Create a claim. Returns claim_id."""
    claim_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO claim
               (claim_id, entity_id, predicate, value, claim_type, confidence,
                epistemic_tag, source_id, expires_at)
               VALUES (?, ?, ?, ?, 'fact', 0.8, ?, ?, ?)""",
            (claim_id, entity_id, predicate, value, epistemic_tag, source_id, expires_at),
        )
        if updated_at:
            cur.execute(
                "UPDATE claim SET updated_at = ? WHERE claim_id = ?",
                (updated_at, claim_id),
            )
    return claim_id


def _create_relationship(conn, from_id, to_id, rel_type="related_to"):
    """Create a relationship. Returns rel_id."""
    rel_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO relationship (rel_id, from_entity_id, to_entity_id, rel_type)
               VALUES (?, ?, ?, ?)""",
            (rel_id, from_id, to_id, rel_type),
        )
    return rel_id


def _create_contradiction(conn, claim_a_id, claim_b_id, status="open"):
    """Create a contradiction row. Returns contradiction_id."""
    contra_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO contradiction
               (contradiction_id, claim_a_id, claim_b_id, description, resolution_status)
               VALUES (?, ?, ?, 'Test contradiction', ?)""",
            (contra_id, claim_a_id, claim_b_id, status),
        )
    return contra_id


# ── SCN-5.1-01: Orphan entities ─────────────────────────────────

class TestOrphanEntities:
    def test_detects_entity_with_no_claims(self, db_conn):
        _create_entity(db_conn, "Orphan Node")
        issues = check_orphan_entities(db_conn)
        assert len(issues) == 1
        assert issues[0]["severity"] == "warning"
        assert "Orphan Node" in issues[0]["message"]

    def test_no_false_positive_for_entity_with_claims(self, db_conn):
        src_id = _create_source(db_conn)
        eid = _create_entity(db_conn, "Active Entity")
        _create_claim(db_conn, eid, src_id)
        issues = check_orphan_entities(db_conn)
        assert len(issues) == 0


# ── SCN-5.1-02: Orphan claims ───────────────────────────────────

class TestOrphanClaims:
    def test_detects_claim_with_missing_entity(self, db_conn):
        src_id = _create_source(db_conn)
        # Insert a claim with a bogus entity_id (bypass FK since SQLite FKs are per-conn)
        claim_id = ulid.generate()
        fake_entity = ulid.generate()
        with transaction(db_conn) as cur:
            # Temporarily disable FK enforcement for this insert
            cur.execute("PRAGMA foreign_keys = OFF")
            cur.execute(
                """INSERT INTO claim
                   (claim_id, entity_id, predicate, value, claim_type, confidence,
                    epistemic_tag, source_id)
                   VALUES (?, ?, 'pred', 'val', 'fact', 0.5, 'provisional', ?)""",
                (claim_id, fake_entity, src_id),
            )
            cur.execute("PRAGMA foreign_keys = ON")
        issues = check_orphan_claims(db_conn)
        assert len(issues) == 1
        assert issues[0]["severity"] == "error"

    def test_no_false_positive(self, db_conn):
        src_id = _create_source(db_conn)
        eid = _create_entity(db_conn, "Real Entity")
        _create_claim(db_conn, eid, src_id)
        issues = check_orphan_claims(db_conn)
        assert len(issues) == 0


# ── SCN-5.1-03: Open contradictions ─────────────────────────────

class TestOpenContradictions:
    def test_lists_open_contradictions(self, db_conn):
        src_id = _create_source(db_conn)
        eid = _create_entity(db_conn, "Contested Thing")
        c1 = _create_claim(db_conn, eid, src_id, value="A")
        c2 = _create_claim(db_conn, eid, src_id, value="B")
        _create_contradiction(db_conn, c1, c2, "open")
        issues = check_open_contradictions(db_conn)
        assert len(issues) == 1
        assert issues[0]["severity"] == "warning"

    def test_excludes_resolved(self, db_conn):
        src_id = _create_source(db_conn)
        eid = _create_entity(db_conn, "Resolved Thing")
        c1 = _create_claim(db_conn, eid, src_id, value="A")
        c2 = _create_claim(db_conn, eid, src_id, value="B")
        _create_contradiction(db_conn, c1, c2, "resolved")
        issues = check_open_contradictions(db_conn)
        assert len(issues) == 0


# ── SCN-5.1-04: Stale claims ────────────────────────────────────

class TestStaleClaims:
    def test_flags_old_provisional_claims(self, db_conn):
        src_id = _create_source(db_conn)
        eid = _create_entity(db_conn, "Stale Entity")
        _create_claim(db_conn, eid, src_id, epistemic_tag="provisional",
                      updated_at="2025-01-01T00:00:00Z")
        issues = check_stale_claims(db_conn, days=90)
        assert len(issues) == 1
        assert issues[0]["severity"] == "warning"

    def test_ignores_confirmed_claims(self, db_conn):
        src_id = _create_source(db_conn)
        eid = _create_entity(db_conn, "Confirmed Entity")
        _create_claim(db_conn, eid, src_id, epistemic_tag="confirmed",
                      updated_at="2025-01-01T00:00:00Z")
        issues = check_stale_claims(db_conn, days=90)
        assert len(issues) == 0

    def test_ignores_recent_provisional(self, db_conn):
        src_id = _create_source(db_conn)
        eid = _create_entity(db_conn, "Fresh Entity")
        _create_claim(db_conn, eid, src_id, epistemic_tag="provisional")
        issues = check_stale_claims(db_conn, days=90)
        assert len(issues) == 0


# ── SCN-5.1-05: Hub score anomalies ─────────────────────────────

class TestHubScoreAnomalies:
    def _build_hub_entity(self, db_conn):
        """Create an entity with high hub score (5+ claims/relationships)."""
        src_id = _create_source(db_conn)
        eid = _create_entity(db_conn, "Hub Entity")
        for i in range(5):
            _create_claim(db_conn, eid, src_id, predicate=f"prop_{i}", value=f"v{i}")
        other = _create_entity(db_conn, "Other")
        _create_relationship(db_conn, eid, other)
        return eid

    def test_flags_hub_without_l1(self, db_conn):
        self._build_hub_entity(db_conn)
        issues = check_hub_score_anomalies(db_conn)
        assert len(issues) >= 1
        assert "Hub Entity" in issues[0]["message"]

    def test_fix_generates_l1(self, db_conn):
        eid = self._build_hub_entity(db_conn)
        issues = check_hub_score_anomalies(db_conn, fix=True)
        assert len(issues) >= 1
        assert issues[0].get("fixed") is True
        cached = read_cache(db_conn, "L1", f"entity:{eid}")
        assert cached is not None

    def test_no_flag_when_l1_exists(self, db_conn):
        eid = self._build_hub_entity(db_conn)
        generate_l1_entity(db_conn, eid)
        issues = check_hub_score_anomalies(db_conn)
        # Filter to just the hub entity issue
        hub_issues = [i for i in issues if "Hub Entity" in i.get("message", "")]
        assert len(hub_issues) == 0


# ── SCN-5.1-06: L0 staleness ────────────────────────────────────

class TestL0Staleness:
    def test_detects_fresh_l0(self, db_conn):
        generate_l0(db_conn)
        issues = check_l0_staleness(db_conn)
        assert len(issues) == 0

    def test_detects_stale_l0(self, db_conn):
        generate_l0(db_conn)
        # Create actual data change after L0 was generated (new source)
        from src.utils import ulid as _ulid
        with transaction(db_conn) as cur:
            cur.execute(
                "INSERT INTO source (source_id, title, source_type, content_hash, token_count) "
                "VALUES (?, 'Stale Test', 'note', 'xyz999', 50)",
                (_ulid.generate(),),
            )
        issues = check_l0_staleness(db_conn)
        assert len(issues) == 1
        assert issues[0]["severity"] == "error"

    def test_fix_regenerates(self, db_conn):
        generate_l0(db_conn)
        # Create actual data change
        from src.utils import ulid as _ulid
        with transaction(db_conn) as cur:
            cur.execute(
                "INSERT INTO source (source_id, title, source_type, content_hash, token_count) "
                "VALUES (?, 'Fix Test', 'note', 'xyz888', 50)",
                (_ulid.generate(),),
            )
        issues = check_l0_staleness(db_conn, fix=True)
        assert len(issues) == 1
        assert issues[0].get("fixed") is True


# ── SCN-5.1-07: Unbounded clusters ──────────────────────────────

class TestUnboundedClusters:
    def test_flags_oversized_cluster(self, db_conn):
        cid = ulid.generate()
        with transaction(db_conn) as cur:
            cur.execute(
                "INSERT INTO topic_cluster (cluster_id, label, claim_count) VALUES (?, ?, ?)",
                (cid, "Big Cluster", 250),
            )
        issues = check_unbounded_clusters(db_conn, threshold=200)
        assert len(issues) == 1
        assert "Big Cluster" in issues[0]["message"]

    def test_no_flag_under_threshold(self, db_conn):
        cid = ulid.generate()
        with transaction(db_conn) as cur:
            cur.execute(
                "INSERT INTO topic_cluster (cluster_id, label, claim_count) VALUES (?, ?, ?)",
                (cid, "Small Cluster", 50),
            )
        issues = check_unbounded_clusters(db_conn, threshold=200)
        assert len(issues) == 0


# ── SCN-5.1-08: Document drift ──────────────────────────────────

class TestDocumentDrift:
    def test_detects_modified_file(self, db_conn, tmp_path):
        f = tmp_path / "drifted.md"
        f.write_text("Original content")
        create_collection(db_conn, "test-drift")
        register_document(db_conn, "test-drift", f, "spec", "Drifted Doc")
        # Modify file after registration
        f.write_text("Modified content — now different")
        issues = check_document_drift(db_conn)
        assert len(issues) == 1
        assert "Drifted Doc" in issues[0]["message"]

    def test_no_drift_when_unchanged(self, db_conn, tmp_path):
        f = tmp_path / "stable.md"
        f.write_text("Stable content")
        create_collection(db_conn, "test-stable")
        register_document(db_conn, "test-stable", f, "spec", "Stable Doc")
        issues = check_document_drift(db_conn)
        assert len(issues) == 0


# ── SCN-5.1-09: Missing documents ───────────────────────────────

class TestMissingDocuments:
    def test_detects_deleted_file(self, db_conn, tmp_path):
        f = tmp_path / "will-delete.md"
        f.write_text("Temporary content")
        create_collection(db_conn, "test-missing")
        register_document(db_conn, "test-missing", f, "spec", "Will Delete")
        f.unlink()  # Delete the file
        issues = check_missing_documents(db_conn)
        assert len(issues) == 1
        assert "Will Delete" in issues[0]["message"]
        assert issues[0]["severity"] == "error"

    def test_no_false_positive(self, db_conn, tmp_path):
        f = tmp_path / "exists.md"
        f.write_text("I exist")
        create_collection(db_conn, "test-exists")
        register_document(db_conn, "test-exists", f, "spec", "Exists Doc")
        issues = check_missing_documents(db_conn)
        assert len(issues) == 0


# ── SCN-5.1-10: L0 performance ──────────────────────────────────

class TestL0Performance:
    def test_reports_generation_time(self, db_conn):
        issues = check_l0_performance(db_conn, threshold_ms=5000)
        assert len(issues) == 1
        assert "elapsed_ms" in issues[0]
        assert issues[0]["severity"] == "info"

    def test_warns_on_slow_generation(self, db_conn):
        # Use an impossibly low threshold to trigger warning
        issues = check_l0_performance(db_conn, threshold_ms=0.001)
        assert issues[0]["severity"] == "warning"


# ── SCN-5.1-11, SCN-5.1-12: run_all_checks ─────────────────────

class TestRunAllChecks:
    def test_aggregates_all_checks(self, db_conn):
        # Create some issues to find
        _create_entity(db_conn, "Orphan")
        generate_l0(db_conn)

        results = run_all_checks(db_conn)
        assert "orphan_entities" in results
        assert "orphan_claims" in results
        assert "open_contradictions" in results
        assert "stale_claims" in results
        assert "hub_score_anomalies" in results
        assert "l0_staleness" in results
        assert "unbounded_clusters" in results
        assert "document_drift" in results
        assert "missing_documents" in results
        assert "l0_performance" in results
        assert "total_warnings" in results
        assert "total_errors" in results
        assert results["total_warnings"] >= 1  # orphan entity

    def test_writes_ingest_log(self, db_conn):
        generate_l0(db_conn)
        run_all_checks(db_conn)
        row = db_conn.execute(
            "SELECT * FROM ingest_log WHERE operation = 'lint' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        assert row is not None
        assert "Lint:" in row["summary"]

    def test_fix_mode(self, db_conn):
        generate_l0(db_conn)
        # Create actual data change to make L0 stale
        from src.utils import ulid as _ulid
        with transaction(db_conn) as cur:
            cur.execute(
                "INSERT INTO source (source_id, title, source_type, content_hash, token_count) "
                "VALUES (?, 'Fix Mode Test', 'note', 'fixhash', 50)",
                (_ulid.generate(),),
            )
        results = run_all_checks(db_conn, fix=True)
        # L0 should have been fixed
        l0_issues = results["l0_staleness"]
        assert any(i.get("fixed") for i in l0_issues)
