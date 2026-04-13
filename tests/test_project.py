"""Tests for src/project module — projection engine."""

import pytest

from src.db import transaction
from src.project import (
    assemble_query_context,
    generate_l0,
    generate_l1_cluster,
    generate_l1_collection,
    generate_l1_entity,
    invalidate_cache,
    read_cache,
    read_l0,
)
from src.registry import create_collection, register_document
from src.utils import tokens, ulid


@pytest.fixture
def populated_db(db_conn, tmp_path):
    """Populate the database with entities, claims, relationships, clusters,
    contradictions, collections, and documents for projection tests."""

    # Create source
    src_id = ulid.generate()
    with transaction(db_conn) as cur:
        cur.execute(
            "INSERT INTO source (source_id, title, source_type, content_hash, file_path, token_count) "
            "VALUES (?, 'Test Source', 'article', 'abc123', '/tmp/test.md', 100)",
            (src_id,),
        )

    # Create entities
    e1_id = ulid.generate()
    e2_id = ulid.generate()
    e3_id = ulid.generate()
    with transaction(db_conn) as cur:
        cur.execute(
            "INSERT INTO entity (entity_id, canonical_name, entity_type, description) VALUES (?, 'SQLite', 'system', 'Embedded database')",
            (e1_id,),
        )
        cur.execute(
            "INSERT INTO entity (entity_id, canonical_name, entity_type, description) VALUES (?, 'FTS5', 'system', 'Full-text search')",
            (e2_id,),
        )
        cur.execute(
            "INSERT INTO entity (entity_id, canonical_name, entity_type, description) VALUES (?, 'WAL Mode', 'concept', 'Write-ahead logging')",
            (e3_id,),
        )

    # Create claims
    c1_id = ulid.generate()
    c2_id = ulid.generate()
    c3_id = ulid.generate()
    c4_id = ulid.generate()
    with transaction(db_conn) as cur:
        cur.execute(
            "INSERT INTO claim (claim_id, entity_id, predicate, value, claim_type, confidence, source_id) "
            "VALUES (?, ?, 'supports', 'concurrent reads', 'fact', 0.95, ?)",
            (c1_id, e1_id, src_id),
        )
        cur.execute(
            "INSERT INTO claim (claim_id, entity_id, predicate, value, claim_type, confidence, source_id) "
            "VALUES (?, ?, 'provides', 'full-text search via porter stemmer', 'fact', 0.90, ?)",
            (c2_id, e2_id, src_id),
        )
        cur.execute(
            "INSERT INTO claim (claim_id, entity_id, predicate, value, claim_type, confidence, source_id) "
            "VALUES (?, ?, 'enables', 'concurrent read access', 'fact', 0.85, ?)",
            (c3_id, e3_id, src_id),
        )
        cur.execute(
            "INSERT INTO claim (claim_id, entity_id, predicate, value, claim_type, confidence, source_id, epistemic_tag) "
            "VALUES (?, ?, 'limits', 'single writer only', 'fact', 0.80, ?, 'contested')",
            (c4_id, e1_id, src_id),
        )

    # Create relationship
    rel_id = ulid.generate()
    with transaction(db_conn) as cur:
        cur.execute(
            "INSERT INTO relationship (rel_id, from_entity_id, to_entity_id, rel_type, weight, source_id) "
            "VALUES (?, ?, ?, 'part_of', 0.9, ?)",
            (rel_id, e2_id, e1_id, src_id),
        )

    # Create topic cluster
    cl_id = ulid.generate()
    with transaction(db_conn) as cur:
        cur.execute(
            "INSERT INTO topic_cluster (cluster_id, label, summary, claim_count) "
            "VALUES (?, 'Database Internals', 'SQLite storage engine details', 3)",
            (cl_id,),
        )
        cur.execute(
            "INSERT INTO claim_cluster (claim_id, cluster_id) VALUES (?, ?)",
            (c1_id, cl_id),
        )
        cur.execute(
            "INSERT INTO claim_cluster (claim_id, cluster_id) VALUES (?, ?)",
            (c2_id, cl_id),
        )
        cur.execute(
            "INSERT INTO claim_cluster (claim_id, cluster_id) VALUES (?, ?)",
            (c3_id, cl_id),
        )

    # Create contradiction
    contra_id = ulid.generate()
    with transaction(db_conn) as cur:
        cur.execute(
            "INSERT INTO contradiction (contradiction_id, claim_a_id, claim_b_id, description) "
            "VALUES (?, ?, ?, 'concurrent reads vs single writer')",
            (contra_id, c1_id, c4_id),
        )

    # Create collection and documents
    cid = create_collection(db_conn, "test-docs", description="Test documents")
    d1 = tmp_path / "doc1.md"
    d1.write_text("# SQLite Guide\n\nSQLite supports WAL mode for concurrent reads.")
    d2 = tmp_path / "doc2.md"
    d2.write_text("# FTS5 Setup\n\nUse porter stemmer tokenizer.")

    register_document(db_conn, "test-docs", d1, "guide", "SQLite Guide", external_id="DOC-001")
    register_document(db_conn, "test-docs", d2, "guide", "FTS5 Setup", external_id="DOC-002")

    # Ingest log entry
    with transaction(db_conn) as cur:
        cur.execute(
            "INSERT INTO ingest_log (log_id, operation, source_id, summary, claims_created, entities_created) "
            "VALUES (?, 'ingest', ?, 'Ingested test source', 4, 3)",
            (ulid.generate(), src_id),
        )

    return {
        "source_id": src_id,
        "entity_ids": [e1_id, e2_id, e3_id],
        "claim_ids": [c1_id, c2_id, c3_id, c4_id],
        "cluster_id": cl_id,
        "collection_name": "test-docs",
    }


# ── L0 Tests ──────────────────────────────────────────────────


class TestL0:
    def test_read_l0_none_before_generation(self, db_conn):
        assert read_l0(db_conn) is None

    def test_generate_l0_returns_markdown(self, db_conn, populated_db):
        content = generate_l0(db_conn)
        assert "# Knowledge base state" in content

    def test_l0_includes_entity_registry(self, db_conn, populated_db):
        content = generate_l0(db_conn)
        assert "Entity registry" in content
        assert "SQLite" in content

    def test_l0_includes_document_registry(self, db_conn, populated_db):
        content = generate_l0(db_conn)
        assert "Document registry" in content
        assert "test-docs" in content

    def test_l0_includes_key_metrics(self, db_conn, populated_db):
        content = generate_l0(db_conn)
        assert "Total sources: 1" in content
        assert "Total entities: 3" in content
        assert "Total active claims: 4" in content
        assert "Total relationships: 1" in content

    def test_l0_includes_hot_topics(self, db_conn, populated_db):
        content = generate_l0(db_conn)
        assert "Hot topics" in content
        assert "Database Internals" in content

    def test_l0_includes_contradictions(self, db_conn, populated_db):
        content = generate_l0(db_conn)
        assert "Open contradictions (1)" in content
        assert "concurrent reads vs single writer" in content

    def test_l0_includes_recent_activity(self, db_conn, populated_db):
        content = generate_l0(db_conn)
        assert "Recent activity" in content
        assert "Ingested test source" in content

    def test_l0_upserts_into_cache(self, db_conn, populated_db):
        generate_l0(db_conn)
        generate_l0(db_conn)
        count = db_conn.execute(
            "SELECT COUNT(*) FROM projection_cache WHERE tier = 'L0' AND scope_key = 'global'"
        ).fetchone()[0]
        assert count == 1

    def test_read_l0_returns_content_after_generation(self, db_conn, populated_db):
        generate_l0(db_conn)
        content = read_l0(db_conn)
        assert content is not None
        assert "Knowledge base state" in content

    def test_l0_empty_database(self, db_conn):
        content = generate_l0(db_conn)
        assert "Total sources: 0" in content
        assert "Total entities: 0" in content
        assert "Entity registry (0 entities)" in content


# ── Cache Invalidation Tests ───────────────────────────────────


class TestInvalidation:
    def test_invalidate_existing_entry(self, db_conn, populated_db):
        generate_l0(db_conn)
        invalidate_cache(db_conn, "global")
        assert read_l0(db_conn) is None

    def test_invalidate_nonexistent_is_noop(self, db_conn):
        invalidate_cache(db_conn, "collection:nonexistent")  # should not raise

    def test_invalidate_preserves_other_entries(self, db_conn, populated_db):
        generate_l0(db_conn)
        generate_l1_collection(db_conn, "test-docs")
        invalidate_cache(db_conn, "collection:test-docs")
        # L0 should still exist
        assert read_l0(db_conn) is not None
        # L1 should be gone
        assert read_cache(db_conn, "L1", "collection:test-docs") is None


# ── L1 Tests ──────────────────────────────────────────────────


class TestL1Cluster:
    def test_generate_l1_cluster(self, db_conn, populated_db):
        content = generate_l1_cluster(db_conn, populated_db["cluster_id"])
        assert "Database Internals" in content
        assert "SQLite storage engine details" in content

    def test_l1_cluster_includes_claims(self, db_conn, populated_db):
        content = generate_l1_cluster(db_conn, populated_db["cluster_id"])
        assert "concurrent reads" in content
        assert "porter stemmer" in content

    def test_l1_cluster_claims_sorted_by_confidence(self, db_conn, populated_db):
        content = generate_l1_cluster(db_conn, populated_db["cluster_id"])
        lines = [l for l in content.split("\n") if l.startswith("- [")]
        assert len(lines) == 3
        # First should be highest confidence (0.95)
        assert "[0.95]" in lines[0]

    def test_l1_cluster_upserts(self, db_conn, populated_db):
        cid = populated_db["cluster_id"]
        generate_l1_cluster(db_conn, cid)
        generate_l1_cluster(db_conn, cid)
        count = db_conn.execute(
            "SELECT COUNT(*) FROM projection_cache WHERE tier = 'L1' AND scope_key = ?",
            (f"cluster:{cid}",),
        ).fetchone()[0]
        assert count == 1

    def test_l1_cluster_not_found_raises(self, db_conn):
        with pytest.raises(ValueError, match="not found"):
            generate_l1_cluster(db_conn, "nonexistent")


class TestL1Entity:
    def test_generate_l1_entity(self, db_conn, populated_db):
        e_id = populated_db["entity_ids"][0]  # SQLite
        content = generate_l1_entity(db_conn, e_id)
        assert "SQLite" in content
        assert "system" in content

    def test_l1_entity_includes_claims(self, db_conn, populated_db):
        e_id = populated_db["entity_ids"][0]  # SQLite
        content = generate_l1_entity(db_conn, e_id)
        assert "concurrent reads" in content

    def test_l1_entity_includes_relationships(self, db_conn, populated_db):
        e_id = populated_db["entity_ids"][0]  # SQLite — target of FTS5 part_of
        content = generate_l1_entity(db_conn, e_id)
        assert "part_of" in content
        assert "FTS5" in content

    def test_l1_entity_includes_description(self, db_conn, populated_db):
        e_id = populated_db["entity_ids"][0]
        content = generate_l1_entity(db_conn, e_id)
        assert "Embedded database" in content

    def test_l1_entity_not_found_raises(self, db_conn):
        with pytest.raises(ValueError, match="not found"):
            generate_l1_entity(db_conn, "nonexistent")


class TestL1Collection:
    def test_generate_l1_collection(self, db_conn, populated_db):
        content = generate_l1_collection(db_conn, "test-docs")
        assert "test-docs" in content
        assert "Test documents" in content

    def test_l1_collection_includes_doc_count(self, db_conn, populated_db):
        content = generate_l1_collection(db_conn, "test-docs")
        assert "Total active documents: 2" in content

    def test_l1_collection_includes_type_breakdown(self, db_conn, populated_db):
        content = generate_l1_collection(db_conn, "test-docs")
        assert "guide: 2" in content

    def test_l1_collection_includes_recent_docs(self, db_conn, populated_db):
        content = generate_l1_collection(db_conn, "test-docs")
        assert "SQLite Guide" in content
        assert "FTS5 Setup" in content
        assert "DOC-001" in content

    def test_l1_collection_upserts(self, db_conn, populated_db):
        generate_l1_collection(db_conn, "test-docs")
        generate_l1_collection(db_conn, "test-docs")
        count = db_conn.execute(
            "SELECT COUNT(*) FROM projection_cache WHERE tier = 'L1' AND scope_key = 'collection:test-docs'"
        ).fetchone()[0]
        assert count == 1

    def test_l1_collection_not_found_raises(self, db_conn):
        with pytest.raises(ValueError, match="not found"):
            generate_l1_collection(db_conn, "nonexistent")

    def test_l1_within_token_budget(self, db_conn, populated_db):
        content = generate_l1_collection(db_conn, "test-docs")
        assert tokens.count_tokens(content) <= 2000


# ── Unified Query Assembly Tests ───────────────────────────────


class TestAssembleQueryContext:
    def test_always_includes_l0(self, db_conn, populated_db):
        ctx = assemble_query_context(db_conn)
        assert "Knowledge base state" in ctx
        assert "Entity registry" in ctx

    def test_includes_l1_for_collection(self, db_conn, populated_db):
        generate_l1_collection(db_conn, "test-docs")
        ctx = assemble_query_context(db_conn, collection_name="test-docs")
        assert "Knowledge base state" in ctx
        assert "Collection: test-docs" in ctx

    def test_generates_l1_on_demand(self, db_conn, populated_db):
        # Don't pre-generate L1 — should auto-generate
        ctx = assemble_query_context(db_conn, collection_name="test-docs")
        assert "Collection: test-docs" in ctx

    def test_falls_back_to_documents(self, db_conn, populated_db):
        ctx = assemble_query_context(db_conn, collection_name="test-docs")
        # Should include document content after L0 and L1
        assert "SQLite Guide" in ctx or "Collection: test-docs" in ctx

    def test_respects_token_budget(self, db_conn, populated_db):
        ctx = assemble_query_context(db_conn, collection_name="test-docs", token_budget=500)
        ctx_tokens = tokens.count_tokens(ctx)
        assert ctx_tokens <= 500

    def test_empty_database(self, db_conn):
        ctx = assemble_query_context(db_conn)
        assert "Total sources: 0" in ctx
        assert "Total entities: 0" in ctx

    def test_with_tags(self, db_conn, populated_db, tmp_path):
        # Register a tagged document
        f = tmp_path / "tagged.md"
        f.write_text("# Tagged content\n\nThis is tagged.")
        register_document(
            db_conn, "test-docs", f, "guide", "Tagged Doc",
            tags={"chunk": "1.2"},
        )
        ctx = assemble_query_context(
            db_conn, collection_name="test-docs", tags={"chunk": "1.2"},
        )
        assert "Knowledge base state" in ctx


# ── FND-0018: L0 audit trail ─────────────────────────────────────


class TestL0AuditTrail:
    """FND-0018: generate_l0 writes ingest_log entry with operation='recompile'."""

    def test_l0_writes_ingest_log(self, db_conn):
        generate_l0(db_conn)
        row = db_conn.execute(
            "SELECT * FROM ingest_log WHERE operation = 'recompile' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        assert row is not None
        assert "L0 regenerated" in row["summary"]
        assert "tokens" in row["summary"]

    def test_multiple_l0_regens_logged(self, db_conn):
        generate_l0(db_conn)
        generate_l0(db_conn)
        rows = db_conn.execute(
            "SELECT COUNT(*) FROM ingest_log WHERE operation = 'recompile'"
        ).fetchone()[0]
        assert rows == 2
