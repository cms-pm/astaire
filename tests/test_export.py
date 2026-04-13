"""Tests for src/export module — Phase 5 chunk 5.2."""

import pytest

from src.db import get_connection, init_db, transaction
from src.export import (
    export_collection_index,
    export_contradictions,
    export_entity_page,
    export_index,
    export_timeline,
    export_wiki,
)
from src.registry import create_collection, register_document
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


def _create_entity(conn, name, entity_type="concept"):
    entity_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            "INSERT INTO entity (entity_id, canonical_name, entity_type) VALUES (?, ?, ?)",
            (entity_id, name, entity_type),
        )
    return entity_id


def _create_claim(conn, entity_id, source_id, predicate="has_property", value="test_value"):
    claim_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO claim
               (claim_id, entity_id, predicate, value, claim_type, confidence,
                epistemic_tag, source_id)
               VALUES (?, ?, ?, ?, 'fact', 0.9, 'confirmed', ?)""",
            (claim_id, entity_id, predicate, value, source_id),
        )
    return claim_id


def _create_relationship(conn, from_id, to_id, rel_type="related_to"):
    rel_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            "INSERT INTO relationship (rel_id, from_entity_id, to_entity_id, rel_type) VALUES (?, ?, ?, ?)",
            (rel_id, from_id, to_id, rel_type),
        )
    return rel_id


def _create_contradiction(conn, claim_a_id, claim_b_id):
    contra_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO contradiction
               (contradiction_id, claim_a_id, claim_b_id, description, resolution_status)
               VALUES (?, ?, ?, 'Conflicting values', 'open')""",
            (contra_id, claim_a_id, claim_b_id),
        )
    return contra_id


def _write_ingest_log(conn, operation="ingest", summary="Test ingest"):
    log_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO ingest_log
               (log_id, operation, summary,
                claims_created, claims_updated, claims_superseded,
                entities_created, relationships_created, contradictions_found,
                documents_registered, documents_updated)
               VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0)""",
            (log_id, operation, summary),
        )
    return log_id


# ── SCN-5.2-01: Entity pages ────────────────────────────────────

class TestEntityPage:
    def test_entity_with_claims_and_relationships(self, db_conn):
        src = _create_source(db_conn)
        e1 = _create_entity(db_conn, "Alfven Wave", "concept")
        e2 = _create_entity(db_conn, "Plasma Physics", "concept")
        _create_claim(db_conn, e1, src, "propagation_speed", "depends on B-field strength")
        _create_claim(db_conn, e1, src, "propagation_speed", "v_A = B/sqrt(mu*rho)")
        _create_claim(db_conn, e1, src, "discovered_by", "Hannes Alfven")
        _create_relationship(db_conn, e1, e2, "part_of")

        page = export_entity_page(db_conn, e1)
        assert "---" in page  # YAML frontmatter
        assert "entity_type: concept" in page
        assert "hub_score:" in page
        assert "# Alfven Wave" in page
        assert "### propagation_speed" in page
        assert "### discovered_by" in page
        assert "[[Plasma Physics]]" in page

    def test_entity_with_no_data(self, db_conn):
        eid = _create_entity(db_conn, "Empty Entity")
        page = export_entity_page(db_conn, eid)
        assert "# Empty Entity" in page
        assert "No claims or relationships" in page


# ── SCN-5.2-02: Collection index ────────────────────────────────

class TestCollectionIndex:
    def test_collection_with_documents(self, db_conn, tmp_path):
        create_collection(db_conn, "test-col")
        for i in range(5):
            f = tmp_path / f"doc_{i}.md"
            f.write_text(f"Content {i}")
            dtype = "spec" if i < 3 else "plan"
            register_document(db_conn, "test-col", f, dtype, f"Doc {i}")

        page = export_collection_index(db_conn, "test-col")
        assert "# test-col" in page
        assert "## Type Breakdown" in page
        assert "**spec**: 3" in page
        assert "**plan**: 2" in page
        assert "## Documents (5)" in page
        assert "Doc 0" in page

    def test_empty_collection(self, db_conn):
        create_collection(db_conn, "empty-col")
        page = export_collection_index(db_conn, "empty-col")
        assert "Documents (0)" in page
        assert "No documents registered" in page


# ── SCN-5.2-03: Contradictions page ─────────────────────────────

class TestContradictions:
    def test_lists_open_contradictions(self, db_conn):
        src = _create_source(db_conn)
        eid = _create_entity(db_conn, "Widget")
        c1 = _create_claim(db_conn, eid, src, "color", "red")
        c2 = _create_claim(db_conn, eid, src, "color", "blue")
        _create_contradiction(db_conn, c1, c2)

        page = export_contradictions(db_conn)
        assert "1 open contradiction" in page
        assert "Widget" in page
        assert "red" in page
        assert "blue" in page

    def test_no_contradictions(self, db_conn):
        page = export_contradictions(db_conn)
        assert "No open contradictions" in page


# ── SCN-5.2-04: Timeline ────────────────────────────────────────

class TestTimeline:
    def test_renders_ingest_log(self, db_conn):
        _write_ingest_log(db_conn, "ingest", "Ingested source A")
        _write_ingest_log(db_conn, "register", "Registered 3 docs")
        _write_ingest_log(db_conn, "lint", "Lint: 0 warnings")

        page = export_timeline(db_conn)
        assert "# Timeline" in page
        assert "ingest" in page
        assert "register" in page
        assert "lint" in page

    def test_empty_timeline(self, db_conn):
        page = export_timeline(db_conn)
        assert "No activity recorded" in page


# ── SCN-5.2-05: Master index ────────────────────────────────────

class TestMasterIndex:
    def test_catalogs_entities_and_collections(self, db_conn):
        _create_entity(db_conn, "Alpha")
        _create_entity(db_conn, "Beta")
        _create_entity(db_conn, "Gamma")
        create_collection(db_conn, "docs")
        create_collection(db_conn, "specs")

        page = export_index(db_conn)
        assert "# Knowledge Base Index" in page
        assert "Entities (3)" in page
        assert "Alpha" in page
        assert "Collections (2)" in page
        assert "docs" in page
        assert "specs" in page
        assert "[[contradictions]]" in page
        assert "[[timeline]]" in page

    def test_empty_index(self, db_conn):
        page = export_index(db_conn)
        assert "Entities (0)" in page
        assert "No entities registered" in page


# ── SCN-5.2-06: Full rebuild ────────────────────────────────────

class TestFullRebuild:
    def test_deletes_stale_files(self, db_conn, tmp_path):
        wiki_dir = tmp_path / "wiki"
        wiki_dir.mkdir()
        stale = wiki_dir / "stale_file.md"
        stale.write_text("old content")

        result = export_wiki(db_conn, output_dir=wiki_dir)
        assert not stale.exists()
        assert (wiki_dir / "index.md").exists()
        assert result["total_pages"] >= 3  # at least the 3 static pages

    def test_creates_all_page_types(self, db_conn, tmp_path):
        src = _create_source(db_conn)
        eid = _create_entity(db_conn, "Test Entity")
        _create_claim(db_conn, eid, src)
        create_collection(db_conn, "test-wiki")

        wiki_dir = tmp_path / "wiki"
        result = export_wiki(db_conn, output_dir=wiki_dir)

        assert (wiki_dir / "index.md").exists()
        assert (wiki_dir / "contradictions.md").exists()
        assert (wiki_dir / "timeline.md").exists()
        assert (wiki_dir / "entities" / "test-entity.md").exists()
        assert (wiki_dir / "collections" / "test-wiki" / "index.md").exists()
        assert result["entities"] == 1
        assert result["collections"] == 1


# ── SCN-5.2-07: Empty database ──────────────────────────────────

class TestEmptyDatabase:
    def test_export_with_no_data(self, db_conn, tmp_path):
        wiki_dir = tmp_path / "wiki"
        result = export_wiki(db_conn, output_dir=wiki_dir)

        assert (wiki_dir / "index.md").exists()
        assert (wiki_dir / "contradictions.md").exists()
        assert (wiki_dir / "timeline.md").exists()
        assert result["entities"] == 0
        assert result["collections"] == 0

        # Pages should have zero-state messaging
        index = (wiki_dir / "index.md").read_text()
        assert "No entities registered" in index
        contras = (wiki_dir / "contradictions.md").read_text()
        assert "No open contradictions" in contras
