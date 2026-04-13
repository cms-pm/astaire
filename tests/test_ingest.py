"""Tests for src/ingest module — Phase 4 (chunks 4.1 through 4.7)."""

import pytest

from src.db import get_connection, init_db, transaction
from src.ingest import ingest_document, ingest_source, scan_directory
from src.project import (
    assemble_query_context,
    generate_l1_collection,
    generate_l1_entity,
    read_cache,
    read_l0,
)
from src.registry import create_collection, get_document
from src.utils import ulid


@pytest.fixture
def collection(db_conn):
    """Create a test collection with permissive config."""
    create_collection(
        db_conn,
        "test",
        description="Test collection",
        config={"doc_types": ["spec", "plan", "gherkin"], "statuses": ["draft", "active"]},
    )
    return "test"


@pytest.fixture
def sample_file(tmp_path):
    """Create a sample document file."""
    f = tmp_path / "sample.md"
    f.write_text("# Sample\n\nThis is a test document about SQLite and FTS5.")
    return f


@pytest.fixture
def sample_files(tmp_path):
    """Create multiple sample files for scan tests."""
    (tmp_path / "spec-one.md").write_text("# Spec One\n\nFirst specification.")
    (tmp_path / "spec-two.md").write_text("# Spec Two\n\nSecond specification.")
    (tmp_path / "plan-alpha.md").write_text("# Plan Alpha\n\nA plan document.")
    (tmp_path / "readme.txt").write_text("Ignore this file.")
    (tmp_path / "test.feature").write_text("Feature: Test\n  Scenario: Basic")
    return tmp_path


# ══════════════════════════════════════════════════════════════
# Chunk 4.1: ingest_document + scan_directory
# ══════════════════════════════════════════════════════════════


class TestIngestDocument:
    """SCN-4.1-01 through SCN-4.1-03."""

    def test_ingest_registers_document(self, db_conn, collection, sample_file):
        """SCN-4.1-01: ingest_document registers document."""
        result = ingest_document(
            db_conn, "test", sample_file, "spec", "My Doc"
        )
        assert result["duplicate"] is False
        doc = get_document(db_conn, result["document_id"])
        assert doc is not None
        assert doc["title"] == "My Doc"
        assert doc["doc_type"] == "spec"

    def test_ingest_writes_ingest_log(self, db_conn, collection, sample_file):
        """SCN-4.1-01: ingest_log entry with operation='register'."""
        ingest_document(db_conn, "test", sample_file, "spec", "Logged Doc")
        log = db_conn.execute(
            "SELECT * FROM ingest_log WHERE operation = 'register' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        assert log is not None
        assert log["documents_registered"] == 1
        assert "Logged Doc" in log["summary"]

    def test_ingest_regenerates_l0(self, db_conn, collection, sample_file):
        """SCN-4.1-01: L0 cache is regenerated."""
        ingest_document(db_conn, "test", sample_file, "spec", "L0 Doc")
        l0 = read_l0(db_conn)
        assert l0 is not None
        assert "1" in l0  # at least 1 document

    def test_ingest_with_source_linkage(self, db_conn, collection, sample_file):
        """SCN-4.1-02: extract_claims=True creates source row and links it."""
        result = ingest_document(
            db_conn, "test", sample_file, "spec", "Source-Linked",
            extract_claims=True,
        )
        assert "source_id" in result
        doc = get_document(db_conn, result["document_id"])
        assert doc["source_id"] == result["source_id"]

        # Verify source row exists
        source = db_conn.execute(
            "SELECT * FROM source WHERE source_id = ?", (result["source_id"],)
        ).fetchone()
        assert source is not None
        assert source["title"] == "Source-Linked"

    def test_ingest_dedup_by_content_hash(self, db_conn, collection, sample_file):
        """SCN-4.1-03: same content returns existing doc, no duplicate."""
        r1 = ingest_document(db_conn, "test", sample_file, "spec", "First")
        r2 = ingest_document(db_conn, "test", sample_file, "spec", "First Again")
        assert r2["duplicate"] is True
        assert r2["document_id"] == r1["document_id"]

        # Only one document row
        count = db_conn.execute(
            "SELECT COUNT(*) FROM document"
        ).fetchone()[0]
        assert count == 1

    def test_ingest_with_tags(self, db_conn, collection, sample_file):
        """Ingest with tags are preserved."""
        result = ingest_document(
            db_conn, "test", sample_file, "spec", "Tagged",
            tags={"stage": "implementation"},
        )
        doc = get_document(db_conn, result["document_id"])
        assert "stage" in doc["tags"]
        assert "implementation" in doc["tags"]["stage"]

    def test_ingest_nonexistent_file_raises(self, db_conn, collection, tmp_path):
        with pytest.raises(FileNotFoundError):
            ingest_document(db_conn, "test", tmp_path / "nope.md", "spec", "X")

    def test_ingest_nonexistent_collection_raises(self, db_conn, sample_file):
        with pytest.raises(ValueError, match="does not exist"):
            ingest_document(db_conn, "nonexistent", sample_file, "spec", "X")


class TestScanDirectory:
    """SCN-4.1-04 through SCN-4.1-06."""

    def test_scan_matches_type_rules(self, db_conn, collection, sample_files):
        """SCN-4.1-04: matches files against type_rules, registers each."""
        result = scan_directory(
            db_conn, "test", sample_files,
            type_rules=[("*.md", "spec"), ("*.feature", "gherkin")],
        )
        assert result["registered"] == 4  # 3 .md + 1 .feature
        assert result["skipped"] == 1  # readme.txt

    def test_scan_skips_unmatched(self, db_conn, collection, sample_files):
        """SCN-4.1-04: files not matching any rule are skipped."""
        result = scan_directory(
            db_conn, "test", sample_files,
            type_rules=[("*.feature", "gherkin")],
        )
        assert result["registered"] == 1
        assert result["skipped"] == 4  # 3 .md + 1 .txt

    def test_scan_idempotent(self, db_conn, collection, sample_files):
        """SCN-4.1-05: second run registers zero new documents."""
        r1 = scan_directory(
            db_conn, "test", sample_files,
            type_rules=[("*.md", "spec")],
        )
        r2 = scan_directory(
            db_conn, "test", sample_files,
            type_rules=[("*.md", "spec")],
        )
        assert r1["registered"] == 3
        assert r2["registered"] == 0

    def test_scan_returns_summary(self, db_conn, collection, sample_files):
        """SCN-4.1-06: returns count of registered/skipped."""
        result = scan_directory(
            db_conn, "test", sample_files,
            type_rules=[("*.md", "spec")],
        )
        assert "registered" in result
        assert "skipped" in result
        assert "documents" in result
        assert len(result["documents"]) == result["registered"]

    def test_scan_writes_ingest_log(self, db_conn, collection, sample_files):
        scan_directory(
            db_conn, "test", sample_files,
            type_rules=[("*.md", "spec")],
        )
        log = db_conn.execute(
            "SELECT * FROM ingest_log WHERE operation = 'register' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        assert log is not None
        assert log["documents_registered"] == 3

    def test_scan_regenerates_l0_once(self, db_conn, collection, sample_files):
        """L0 is regenerated once after bulk scan, not per document."""
        scan_directory(
            db_conn, "test", sample_files,
            type_rules=[("*.md", "spec")],
        )
        l0 = read_l0(db_conn)
        assert l0 is not None

    def test_scan_not_a_directory_raises(self, db_conn, collection, sample_files):
        with pytest.raises(NotADirectoryError):
            scan_directory(
                db_conn, "test", sample_files / "spec-one.md",
                type_rules=[("*.md", "spec")],
            )

    def test_scan_with_tags(self, db_conn, collection, sample_files):
        result = scan_directory(
            db_conn, "test", sample_files,
            type_rules=[("*.md", "spec")],
            tags={"phase": "1"},
        )
        # Verify tags on registered documents
        for doc_info in result["documents"]:
            doc = get_document(db_conn, doc_info["document_id"])
            assert "phase" in doc["tags"]
            assert "1" in doc["tags"]["phase"]


# ══════════════════════════════════════════════════════════════
# Chunk 4.2: ingest_source
# ══════════════════════════════════════════════════════════════


class TestIngestSource:
    """SCN-4.2-01 through SCN-4.2-07."""

    def test_creates_source_row(self, db_conn, sample_file):
        """SCN-4.2-01: creates source with hash, path, token_count."""
        result = ingest_source(db_conn, sample_file, "article", "Test Source")
        assert result["duplicate"] is False
        source = db_conn.execute(
            "SELECT * FROM source WHERE source_id = ?", (result["source_id"],)
        ).fetchone()
        assert source is not None
        assert source["title"] == "Test Source"
        assert source["source_type"] == "article"
        assert source["content_hash"] is not None
        assert source["token_count"] > 0
        assert str(sample_file) in source["file_path"]

    def test_dedup_by_content_hash(self, db_conn, sample_file):
        """SCN-4.2-02: returns existing source_id for duplicate."""
        r1 = ingest_source(db_conn, sample_file, "article", "First")
        r2 = ingest_source(db_conn, sample_file, "article", "Second")
        assert r2["duplicate"] is True
        assert r2["source_id"] == r1["source_id"]

        count = db_conn.execute("SELECT COUNT(*) FROM source").fetchone()[0]
        assert count == 1

    def test_processes_claims_creates_entities(self, db_conn, sample_file):
        """SCN-4.2-03: creates entities and claims from pre-extracted data."""
        claims = [
            {
                "entity": "SQLite",
                "predicate": "supports",
                "value": "full-text search via FTS5",
                "claim_type": "fact",
                "confidence": 0.9,
            },
            {
                "entity": "FTS5",
                "predicate": "is part of",
                "value": "SQLite extension system",
                "claim_type": "fact",
                "confidence": 0.85,
            },
        ]
        result = ingest_source(db_conn, sample_file, "article", "Test", claims=claims)
        assert result["entities_created"] == 2
        assert result["claims_created"] == 2

        # Verify entities exist
        sqlite_entity = db_conn.execute(
            "SELECT * FROM entity WHERE canonical_name = 'SQLite'"
        ).fetchone()
        assert sqlite_entity is not None

        fts5_entity = db_conn.execute(
            "SELECT * FROM entity WHERE canonical_name = 'FTS5'"
        ).fetchone()
        assert fts5_entity is not None

        # Verify claims exist
        claim_count = db_conn.execute("SELECT COUNT(*) FROM claim").fetchone()[0]
        assert claim_count == 2

    def test_reuses_existing_entities(self, db_conn, sample_file, tmp_path):
        """SCN-4.2-03: finds existing entities by canonical_name."""
        f1 = tmp_path / "source1.md"
        f1.write_text("Source one content")
        f2 = tmp_path / "source2.md"
        f2.write_text("Source two content")

        claims1 = [{"entity": "SQLite", "predicate": "is", "value": "a database", "claim_type": "fact", "confidence": 0.9}]
        claims2 = [{"entity": "SQLite", "predicate": "version", "value": "3.45", "claim_type": "metric", "confidence": 0.8}]

        r1 = ingest_source(db_conn, f1, "article", "S1", claims=claims1)
        r2 = ingest_source(db_conn, f2, "article", "S2", claims=claims2)

        assert r1["entities_created"] == 1
        assert r2["entities_created"] == 0  # reused

        entity_count = db_conn.execute("SELECT COUNT(*) FROM entity").fetchone()[0]
        assert entity_count == 1

    def test_detects_contradiction(self, db_conn, tmp_path):
        """SCN-4.2-04: same entity+predicate, different value creates contradiction."""
        f1 = tmp_path / "source1.md"
        f1.write_text("SQLite max size is 281 TB")
        f2 = tmp_path / "source2.md"
        f2.write_text("SQLite max size is 140 TB")

        claims1 = [{"entity": "SQLite", "predicate": "max_db_size", "value": "281 TB", "claim_type": "metric", "confidence": 0.9}]
        claims2 = [{"entity": "SQLite", "predicate": "max_db_size", "value": "140 TB", "claim_type": "metric", "confidence": 0.8}]

        ingest_source(db_conn, f1, "article", "S1", claims=claims1)
        r2 = ingest_source(db_conn, f2, "article", "S2", claims=claims2)

        assert r2["contradictions_found"] == 1

        # Both claims remain active (contested, not retracted)
        contested = db_conn.execute(
            "SELECT COUNT(*) FROM claim WHERE epistemic_tag = 'contested'"
        ).fetchone()[0]
        assert contested == 2

        # Contradiction row exists
        contra = db_conn.execute(
            "SELECT * FROM contradiction WHERE resolution_status = 'open'"
        ).fetchone()
        assert contra is not None
        assert "281 TB" in contra["description"]
        assert "140 TB" in contra["description"]

    def test_no_contradiction_on_same_value(self, db_conn, tmp_path):
        """Same entity+predicate with same value should NOT create contradiction."""
        f1 = tmp_path / "source1.md"
        f1.write_text("SQLite supports WAL mode")
        f2 = tmp_path / "source2.md"
        f2.write_text("SQLite also supports WAL mode")

        claims = [{"entity": "SQLite", "predicate": "supports", "value": "WAL mode", "claim_type": "fact", "confidence": 0.9}]

        ingest_source(db_conn, f1, "article", "S1", claims=claims)
        r2 = ingest_source(db_conn, f2, "article", "S2", claims=claims)

        assert r2["contradictions_found"] == 0

    def test_regenerates_l0(self, db_conn, sample_file):
        """SCN-4.2-05: L0 regenerated after ingest."""
        ingest_source(db_conn, sample_file, "article", "Test")
        l0 = read_l0(db_conn)
        assert l0 is not None
        assert "1" in l0  # at least 1 source

    def test_writes_ingest_log(self, db_conn, tmp_path):
        """SCN-4.2-06: ingest_log has accurate counts."""
        f = tmp_path / "counted.md"
        f.write_text("Content for counting")
        claims = [
            {"entity": "A", "predicate": "is", "value": "alpha", "claim_type": "fact", "confidence": 0.9},
            {"entity": "B", "predicate": "is", "value": "beta", "claim_type": "fact", "confidence": 0.8},
            {"entity": "A", "predicate": "has", "value": "property", "claim_type": "fact", "confidence": 0.7},
        ]
        ingest_source(db_conn, f, "article", "Counted", claims=claims)

        log = db_conn.execute(
            "SELECT * FROM ingest_log WHERE operation = 'ingest' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        assert log["claims_created"] == 3
        assert log["entities_created"] == 2  # A and B

    def test_ingest_with_no_claims(self, db_conn, sample_file):
        """SCN-4.2-07: no claims still creates source and logs."""
        result = ingest_source(db_conn, sample_file, "article", "No Claims")
        assert result["duplicate"] is False
        assert result["claims_created"] == 0
        assert result["entities_created"] == 0

        source = db_conn.execute(
            "SELECT * FROM source WHERE source_id = ?", (result["source_id"],)
        ).fetchone()
        assert source is not None

        log = db_conn.execute(
            "SELECT * FROM ingest_log WHERE operation = 'ingest'"
        ).fetchone()
        assert log is not None
        assert log["claims_created"] == 0

    def test_ingest_nonexistent_file_raises(self, db_conn, tmp_path):
        with pytest.raises(FileNotFoundError):
            ingest_source(db_conn, tmp_path / "missing.md", "article", "X")

    def test_claim_defaults(self, db_conn, sample_file):
        """Claims use default claim_type and confidence when not specified."""
        claims = [{"entity": "Test", "predicate": "is", "value": "a test"}]
        result = ingest_source(db_conn, sample_file, "article", "Defaults", claims=claims)

        claim = db_conn.execute("SELECT * FROM claim").fetchone()
        assert claim["claim_type"] == "fact"
        assert claim["confidence"] == 0.5


# ══════════════════════════════════════════════════════════════
# Chunk 4.3: Contradiction transaction atomicity (FND-0011)
# ══════════════════════════════════════════════════════════════


class TestContradictionAtomicity:
    """SCN-4.3-01: contradiction INSERT and claim UPDATE in single transaction."""

    def test_contradiction_and_tags_consistent(self, db_conn, tmp_path):
        """Both contradiction row and contested tags exist together."""
        f1 = tmp_path / "s1.md"
        f1.write_text("Source one")
        f2 = tmp_path / "s2.md"
        f2.write_text("Source two")

        claims1 = [{"entity": "X", "predicate": "size", "value": "10", "claim_type": "metric", "confidence": 0.9}]
        claims2 = [{"entity": "X", "predicate": "size", "value": "20", "claim_type": "metric", "confidence": 0.8}]

        ingest_source(db_conn, f1, "article", "S1", claims=claims1)
        ingest_source(db_conn, f2, "article", "S2", claims=claims2)

        # Contradiction exists
        contra_count = db_conn.execute("SELECT COUNT(*) FROM contradiction").fetchone()[0]
        assert contra_count == 1

        # Both claims are contested
        contested = db_conn.execute(
            "SELECT COUNT(*) FROM claim WHERE epistemic_tag = 'contested'"
        ).fetchone()[0]
        assert contested == 2

    def test_multiple_contradictions_each_atomic(self, db_conn, tmp_path):
        """Multiple contradictions from one ingest are each handled."""
        f1 = tmp_path / "s1.md"
        f1.write_text("Source one")
        f2 = tmp_path / "s2.md"
        f2.write_text("Source two")

        claims1 = [
            {"entity": "Y", "predicate": "size", "value": "10", "claim_type": "metric", "confidence": 0.9},
            {"entity": "Y", "predicate": "color", "value": "red", "claim_type": "fact", "confidence": 0.9},
        ]
        claims2 = [
            {"entity": "Y", "predicate": "size", "value": "20", "claim_type": "metric", "confidence": 0.8},
            {"entity": "Y", "predicate": "color", "value": "blue", "claim_type": "fact", "confidence": 0.8},
        ]

        ingest_source(db_conn, f1, "article", "S1", claims=claims1)
        r2 = ingest_source(db_conn, f2, "article", "S2", claims=claims2)

        assert r2["contradictions_found"] == 2
        contra_count = db_conn.execute("SELECT COUNT(*) FROM contradiction").fetchone()[0]
        assert contra_count == 2
        contested = db_conn.execute(
            "SELECT COUNT(*) FROM claim WHERE epistemic_tag = 'contested'"
        ).fetchone()[0]
        assert contested == 4


# ══════════════════════════════════════════════════════════════
# Chunk 4.4: L1 cache invalidation (FND-0014)
# ══════════════════════════════════════════════════════════════


class TestL1CacheInvalidation:
    """SCN-4.4-01 through SCN-4.4-03."""

    def test_ingest_document_invalidates_collection_l1(self, db_conn, collection, tmp_path):
        """SCN-4.4-01: collection L1 cache is invalidated after document ingest."""
        f1 = tmp_path / "d1.md"
        f1.write_text("# Doc 1\n\nFirst doc.")
        ingest_document(db_conn, "test", f1, "spec", "Doc 1")

        # Pre-generate L1 cache
        generate_l1_collection(db_conn, "test")
        cached = read_cache(db_conn, "L1", "collection:test")
        assert cached is not None
        assert "1" in cached  # 1 document

        # Ingest second document — should invalidate the L1
        f2 = tmp_path / "d2.md"
        f2.write_text("# Doc 2\n\nSecond doc.")
        ingest_document(db_conn, "test", f2, "spec", "Doc 2")

        cached_after = read_cache(db_conn, "L1", "collection:test")
        assert cached_after is None  # invalidated

    def test_ingest_source_invalidates_entity_l1(self, db_conn, tmp_path):
        """SCN-4.4-02: entity L1 caches are invalidated after source ingest."""
        f1 = tmp_path / "s1.md"
        f1.write_text("Source one content")
        claims1 = [{"entity": "SQLite", "predicate": "is", "value": "a database", "claim_type": "fact", "confidence": 0.9}]
        r1 = ingest_source(db_conn, f1, "article", "S1", claims=claims1)

        # Get entity_id and generate L1 cache
        entity = db_conn.execute("SELECT entity_id FROM entity WHERE canonical_name = 'SQLite'").fetchone()
        generate_l1_entity(db_conn, entity["entity_id"])
        cached = read_cache(db_conn, "L1", f"entity:{entity['entity_id']}")
        assert cached is not None

        # Ingest second source with claim on same entity — should invalidate L1
        f2 = tmp_path / "s2.md"
        f2.write_text("Source two content")
        claims2 = [{"entity": "SQLite", "predicate": "version", "value": "3.46", "claim_type": "metric", "confidence": 0.8}]
        ingest_source(db_conn, f2, "article", "S2", claims=claims2)

        cached_after = read_cache(db_conn, "L1", f"entity:{entity['entity_id']}")
        assert cached_after is None  # invalidated

    def test_e2e_ingest_then_query_fresh(self, db_conn, collection, tmp_path):
        """SCN-4.4-03: assemble_query_context returns fresh data after ingest."""
        f1 = tmp_path / "d1.md"
        f1.write_text("# Doc 1\n\nFirst document content.")
        ingest_document(db_conn, "test", f1, "spec", "Doc 1")

        # Generate L1 (shows 1 doc)
        generate_l1_collection(db_conn, "test")

        # Ingest second document
        f2 = tmp_path / "d2.md"
        f2.write_text("# Doc 2\n\nSecond document content.")
        ingest_document(db_conn, "test", f2, "spec", "Doc 2")

        # Query should reflect 2 documents (L1 was invalidated, will be regenerated)
        ctx = assemble_query_context(db_conn, collection_name="test")
        assert "Doc 1" in ctx
        assert "Doc 2" in ctx


# ══════════════════════════════════════════════════════════════
# Chunk 4.5: Atomic entity find-or-create (FND-0012)
# ══════════════════════════════════════════════════════════════


class TestEntityAtomicUpsert:
    """SCN-4.5-01 through SCN-4.5-04."""

    def test_creates_new_entity(self, db_conn, sample_file):
        """SCN-4.5-01: INSERT OR IGNORE creates new entity."""
        claims = [{"entity": "NewConcept", "predicate": "is", "value": "novel", "claim_type": "fact", "confidence": 0.9}]
        ingest_source(db_conn, sample_file, "article", "Test", claims=claims)

        entity = db_conn.execute(
            "SELECT * FROM entity WHERE canonical_name = 'NewConcept'"
        ).fetchone()
        assert entity is not None
        assert entity["entity_type"] == "concept"

    def test_reuses_existing_entity(self, db_conn, tmp_path):
        """SCN-4.5-01: INSERT OR IGNORE finds existing entity."""
        f1 = tmp_path / "s1.md"
        f1.write_text("Source one")
        f2 = tmp_path / "s2.md"
        f2.write_text("Source two")

        claims = [{"entity": "SQLite", "predicate": "is", "value": "great", "claim_type": "opinion", "confidence": 0.7}]
        ingest_source(db_conn, f1, "article", "S1", claims=claims)
        ingest_source(db_conn, f2, "article", "S2", claims=claims)

        entity_count = db_conn.execute("SELECT COUNT(*) FROM entity").fetchone()[0]
        assert entity_count == 1

    def test_entity_type_from_claim_data(self, db_conn, sample_file):
        """SCN-4.5-02: entity_type is accepted from claim data."""
        claims = [{"entity": "Linus Torvalds", "predicate": "created", "value": "Linux",
                    "claim_type": "fact", "confidence": 0.99, "entity_type": "person"}]
        ingest_source(db_conn, sample_file, "article", "Test", claims=claims)

        entity = db_conn.execute(
            "SELECT * FROM entity WHERE canonical_name = 'Linus Torvalds'"
        ).fetchone()
        assert entity["entity_type"] == "person"

    def test_entity_type_defaults_to_concept(self, db_conn, sample_file):
        """SCN-4.5-02: default entity_type is 'concept'."""
        claims = [{"entity": "WAL", "predicate": "means", "value": "write-ahead logging", "claim_type": "definition", "confidence": 0.95}]
        ingest_source(db_conn, sample_file, "article", "Test", claims=claims)

        entity = db_conn.execute(
            "SELECT * FROM entity WHERE canonical_name = 'WAL'"
        ).fetchone()
        assert entity["entity_type"] == "concept"


# ══════════════════════════════════════════════════════════════
# Chunk 4.7: Scan directory error handling (FND-0015)
# ══════════════════════════════════════════════════════════════


class TestScanErrorHandling:
    """SCN-4.7-01 through SCN-4.7-04."""

    def test_binary_file_skipped(self, db_conn, collection, tmp_path):
        """SCN-4.7-01: binary file is skipped, scan continues."""
        (tmp_path / "good.md").write_text("# Good\n\nValid content.")
        # Write a binary file that will fail UTF-8 decode
        (tmp_path / "bad.md").write_bytes(b"\x80\x81\x82\xff" * 100)

        result = scan_directory(
            db_conn, "test", tmp_path,
            type_rules=[("*.md", "spec")],
        )
        assert result["registered"] == 1
        assert len(result["errors"]) == 1
        assert "bad.md" in result["errors"][0]["file_path"]

    def test_scan_continues_after_error(self, db_conn, collection, tmp_path):
        """SCN-4.7-02: valid files before and after bad file are registered."""
        (tmp_path / "a_first.md").write_text("# First\n\nContent A.")
        (tmp_path / "b_bad.md").write_bytes(b"\xff\xfe" * 200)
        (tmp_path / "c_third.md").write_text("# Third\n\nContent C.")

        result = scan_directory(
            db_conn, "test", tmp_path,
            type_rules=[("*.md", "spec")],
        )
        assert result["registered"] == 2
        assert len(result["errors"]) == 1

    def test_errors_list_in_return(self, db_conn, collection, tmp_path):
        """SCN-4.7-03: return dict includes errors with file_path and error."""
        (tmp_path / "binary.md").write_bytes(b"\x80\x81\x82\xff" * 50)

        result = scan_directory(
            db_conn, "test", tmp_path,
            type_rules=[("*.md", "spec")],
        )
        assert "errors" in result
        assert len(result["errors"]) == 1
        err = result["errors"][0]
        assert "file_path" in err
        assert "error" in err

    def test_ingest_log_and_l0_for_successful_files(self, db_conn, collection, tmp_path):
        """SCN-4.7-04: ingest log and L0 regen happen for successful registrations."""
        (tmp_path / "good1.md").write_text("# Good 1\n\nValid.")
        (tmp_path / "good2.md").write_text("# Good 2\n\nAlso valid.")
        (tmp_path / "bad.md").write_bytes(b"\xff" * 100)

        result = scan_directory(
            db_conn, "test", tmp_path,
            type_rules=[("*.md", "spec")],
        )
        assert result["registered"] == 2

        log = db_conn.execute(
            "SELECT * FROM ingest_log WHERE operation = 'register' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        assert log is not None
        assert log["documents_registered"] == 2

        l0 = read_l0(db_conn)
        assert l0 is not None

    def test_all_errors_no_ingest_log(self, db_conn, collection, tmp_path):
        """If all files error, no ingest_log or L0 regen (same as zero registered)."""
        (tmp_path / "bad.md").write_bytes(b"\xff\xfe\xfd" * 100)

        result = scan_directory(
            db_conn, "test", tmp_path,
            type_rules=[("*.md", "spec")],
        )
        assert result["registered"] == 0
        assert len(result["errors"]) == 1

    def test_empty_errors_on_clean_scan(self, db_conn, collection, sample_files):
        """No errors on a clean scan."""
        result = scan_directory(
            db_conn, "test", sample_files,
            type_rules=[("*.md", "spec")],
        )
        assert result["errors"] == []


# ══════════════════════════════════════════════════════════════
# FND-0016: TOCTOU fix — single-read in ingest_source
# ══════════════════════════════════════════════════════════════


class TestSingleRead:
    """FND-0016: Verify ingest_source uses single-read (hash matches content)."""

    def test_hash_matches_content(self, db_conn, tmp_path):
        """After ingest, the stored content_hash should match the file content."""
        from src.utils import hashing
        f = tmp_path / "single_read.md"
        f.write_text("# Single Read Test\n\nContent for TOCTOU verification.")

        result = ingest_source(db_conn, f, "note", "Single Read Test")
        assert not result["duplicate"]

        # Verify the hash in DB matches what we get from hashing the file content
        row = db_conn.execute(
            "SELECT content_hash FROM source WHERE source_id = ?",
            (result["source_id"],),
        ).fetchone()
        expected_hash = hashing.hash_content(f.read_text())
        assert row["content_hash"] == expected_hash


# ══════════════════════════════════════════════════════════════
# FND-0017: L0 failure resilience
# ══════════════════════════════════════════════════════════════


class TestL0FailureResilience:
    """FND-0017: Ingest succeeds even when L0 regeneration fails."""

    def test_ingest_source_survives_l0_failure(self, db_conn, tmp_path, monkeypatch):
        f = tmp_path / "resilient.md"
        f.write_text("# Resilient\n\nThis should survive L0 failure.")

        # Make generate_l0 raise
        def _exploding_l0(*a, **kw):
            raise RuntimeError("L0 generation failed")

        monkeypatch.setattr("src.ingest.generate_l0", _exploding_l0)

        result = ingest_source(db_conn, f, "note", "Resilient Test")
        assert not result["duplicate"]
        assert result["source_id"]

        # Verify source was actually stored
        row = db_conn.execute(
            "SELECT title FROM source WHERE source_id = ?",
            (result["source_id"],),
        ).fetchone()
        assert row["title"] == "Resilient Test"

    def test_ingest_document_survives_l0_failure(self, db_conn, collection, tmp_path, monkeypatch):
        f = tmp_path / "resilient_doc.md"
        f.write_text("# Resilient Doc\n\nThis should survive too.")

        def _exploding_l0(*a, **kw):
            raise RuntimeError("L0 generation failed")

        monkeypatch.setattr("src.ingest.generate_l0", _exploding_l0)

        result = ingest_document(db_conn, "test", f, "spec", "Resilient Doc")
        assert not result["duplicate"]
        assert result["document_id"]
