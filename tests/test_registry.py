"""Tests for src/registry module."""

import time

import pytest

from src.db import get_connection, init_db, transaction
from src.registry import (
    assemble_context,
    assemble_tagged_context,
    create_collection,
    get_by_external_id,
    get_collection,
    get_document,
    query_documents,
    register_dependency,
    register_document,
    search_documents,
    sync_collection,
    sync_document,
    validate_document,
)
from src.utils import hashing, tokens


@pytest.fixture
def sample_docs(tmp_path):
    """Create sample document files for testing."""
    d1 = tmp_path / "doc1.md"
    d1.write_text("# Document One\n\nThis is the first document about testing.")

    d2 = tmp_path / "doc2.md"
    d2.write_text("# Document Two\n\nThis is the second document about deployment.")

    d3 = tmp_path / "doc3.md"
    d3.write_text("# Document Three\n\nA short doc.")

    return {"doc1": d1, "doc2": d2, "doc3": d3}


@pytest.fixture
def collection(db_conn):
    """Create a test collection."""
    cid = create_collection(
        db_conn,
        "test-collection",
        description="A test collection",
        config={"doc_types": ["spec", "plan"], "statuses": ["draft", "active"]},
    )
    return cid


class TestCollectionManagement:
    def test_create_collection(self, db_conn):
        cid = create_collection(db_conn, "my-collection", description="Test")
        assert len(cid) == 26  # ULID

    def test_get_collection(self, db_conn):
        create_collection(db_conn, "lookup-test", config={"doc_types": ["spec"]})
        col = get_collection(db_conn, "lookup-test")
        assert col is not None
        assert col["name"] == "lookup-test"
        assert col["config"]["doc_types"] == ["spec"]

    def test_get_collection_not_found(self, db_conn):
        assert get_collection(db_conn, "nonexistent") is None

    def test_duplicate_name_raises(self, db_conn):
        create_collection(db_conn, "unique-name")
        with pytest.raises(Exception):
            create_collection(db_conn, "unique-name")


class TestDocumentRegistration:
    def test_register_document(self, db_conn, collection, sample_docs):
        doc_id = register_document(
            db_conn, "test-collection", sample_docs["doc1"], "spec", "Doc One"
        )
        assert len(doc_id) == 26

    def test_register_with_tags(self, db_conn, collection, sample_docs):
        doc_id = register_document(
            db_conn,
            "test-collection",
            sample_docs["doc1"],
            "spec",
            "Tagged Doc",
            tags={"stage": "implementation", "chunk": "1.2"},
        )
        doc = get_document(db_conn, doc_id)
        assert "stage" in doc["tags"]
        assert "implementation" in doc["tags"]["stage"]
        assert "1.2" in doc["tags"]["chunk"]

    def test_register_with_multi_value_tags(self, db_conn, collection, sample_docs):
        doc_id = register_document(
            db_conn,
            "test-collection",
            sample_docs["doc1"],
            "spec",
            "Multi-tag Doc",
            tags={"lens": ["security", "performance"]},
        )
        doc = get_document(db_conn, doc_id)
        assert set(doc["tags"]["lens"]) == {"security", "performance"}

    def test_register_with_external_id(self, db_conn, collection, sample_docs):
        doc_id = register_document(
            db_conn,
            "test-collection",
            sample_docs["doc1"],
            "spec",
            "Ext ID Doc",
            external_id="SCN-1.1-01",
        )
        doc = get_document(db_conn, doc_id)
        assert doc["external_id"] == "SCN-1.1-01"

    def test_register_stores_hash_and_tokens(self, db_conn, collection, sample_docs):
        doc_id = register_document(
            db_conn, "test-collection", sample_docs["doc1"], "spec", "Hash Doc"
        )
        doc = get_document(db_conn, doc_id)
        expected_hash = hashing.hash_file(sample_docs["doc1"])
        assert doc["content_hash"] == expected_hash
        assert doc["token_count"] > 0

    def test_register_nonexistent_collection_raises(self, db_conn, sample_docs):
        with pytest.raises(ValueError, match="does not exist"):
            register_document(db_conn, "no-such-collection", sample_docs["doc1"], "spec", "X")

    def test_register_nonexistent_file_raises(self, db_conn, collection, tmp_path):
        with pytest.raises(FileNotFoundError):
            register_document(
                db_conn, "test-collection", tmp_path / "missing.md", "spec", "X"
            )

    def test_register_with_metadata(self, db_conn, collection, sample_docs):
        doc_id = register_document(
            db_conn,
            "test-collection",
            sample_docs["doc1"],
            "spec",
            "Meta Doc",
            metadata={"risk_tier": "low", "author": "test"},
        )
        doc = get_document(db_conn, doc_id)
        assert doc["metadata"]["risk_tier"] == "low"


class TestDocumentDependency:
    def test_register_dependency(self, db_conn, collection, sample_docs):
        d1 = register_document(db_conn, "test-collection", sample_docs["doc1"], "spec", "From")
        d2 = register_document(db_conn, "test-collection", sample_docs["doc2"], "plan", "To")
        dep_id = register_dependency(db_conn, d1, d2, "produces")
        assert len(dep_id) == 26

    def test_dependency_unique_constraint(self, db_conn, collection, sample_docs):
        d1 = register_document(db_conn, "test-collection", sample_docs["doc1"], "spec", "From")
        d2 = register_document(db_conn, "test-collection", sample_docs["doc2"], "plan", "To")
        register_dependency(db_conn, d1, d2, "produces")
        with pytest.raises(Exception):
            register_dependency(db_conn, d1, d2, "produces")


class TestQuery:
    def test_get_document(self, db_conn, collection, sample_docs):
        doc_id = register_document(
            db_conn, "test-collection", sample_docs["doc1"], "spec", "Get Me"
        )
        doc = get_document(db_conn, doc_id)
        assert doc["title"] == "Get Me"
        assert doc["doc_type"] == "spec"

    def test_get_document_not_found(self, db_conn):
        assert get_document(db_conn, "nonexistent") is None

    def test_get_by_external_id(self, db_conn, collection, sample_docs):
        register_document(
            db_conn,
            "test-collection",
            sample_docs["doc1"],
            "spec",
            "Ext Lookup",
            external_id="FND-0001",
        )
        doc = get_by_external_id(db_conn, "test-collection", "FND-0001")
        assert doc is not None
        assert doc["title"] == "Ext Lookup"

    def test_get_by_external_id_not_found(self, db_conn, collection):
        assert get_by_external_id(db_conn, "test-collection", "NOPE") is None

    def test_query_by_collection(self, db_conn, collection, sample_docs):
        register_document(db_conn, "test-collection", sample_docs["doc1"], "spec", "D1")
        register_document(db_conn, "test-collection", sample_docs["doc2"], "plan", "D2")
        results = query_documents(db_conn, collection_name="test-collection")
        assert len(results) == 2

    def test_query_by_doc_type(self, db_conn, collection, sample_docs):
        register_document(db_conn, "test-collection", sample_docs["doc1"], "spec", "S1")
        register_document(db_conn, "test-collection", sample_docs["doc2"], "plan", "P1")
        results = query_documents(db_conn, doc_type="spec")
        assert len(results) == 1
        assert results[0]["doc_type"] == "spec"

    def test_query_by_tags(self, db_conn, collection, sample_docs):
        register_document(
            db_conn, "test-collection", sample_docs["doc1"], "spec", "Tagged",
            tags={"stage": "implementation", "chunk": "1.2"},
        )
        register_document(
            db_conn, "test-collection", sample_docs["doc2"], "spec", "Other",
            tags={"stage": "plan"},
        )
        results = query_documents(db_conn, tags={"stage": "implementation"})
        assert len(results) == 1
        assert results[0]["title"] == "Tagged"

    def test_query_by_status(self, db_conn, collection, sample_docs):
        register_document(
            db_conn, "test-collection", sample_docs["doc1"], "spec", "Active",
            status="active",
        )
        register_document(
            db_conn, "test-collection", sample_docs["doc2"], "spec", "Draft",
        )
        results = query_documents(db_conn, status="active")
        assert len(results) == 1
        assert results[0]["title"] == "Active"

    def test_query_combined_filters(self, db_conn, collection, sample_docs):
        register_document(
            db_conn, "test-collection", sample_docs["doc1"], "spec", "Match",
            tags={"stage": "implementation"}, status="active",
        )
        register_document(
            db_conn, "test-collection", sample_docs["doc2"], "plan", "NoMatch",
            tags={"stage": "implementation"}, status="active",
        )
        results = query_documents(
            db_conn, collection_name="test-collection", doc_type="spec",
            tags={"stage": "implementation"}, status="active",
        )
        assert len(results) == 1
        assert results[0]["title"] == "Match"

    def test_query_no_results(self, db_conn, collection):
        results = query_documents(db_conn, doc_type="nonexistent")
        assert results == []


class TestSync:
    def test_sync_no_change(self, db_conn, collection, sample_docs):
        doc_id = register_document(
            db_conn, "test-collection", sample_docs["doc1"], "spec", "Stable"
        )
        result = sync_document(db_conn, doc_id)
        assert result["changed"] is False
        assert result["missing"] is False

    def test_sync_detects_change(self, db_conn, collection, sample_docs):
        doc_id = register_document(
            db_conn, "test-collection", sample_docs["doc1"], "spec", "Will Change"
        )
        # Modify the file
        sample_docs["doc1"].write_text("# Modified content\n\nCompletely different.")
        result = sync_document(db_conn, doc_id)
        assert result["changed"] is True
        assert result["old_hash"] != result["new_hash"]
        assert result["missing"] is False

    def test_sync_detects_missing(self, db_conn, collection, sample_docs):
        doc_id = register_document(
            db_conn, "test-collection", sample_docs["doc1"], "spec", "Will Vanish"
        )
        sample_docs["doc1"].unlink()
        result = sync_document(db_conn, doc_id)
        assert result["changed"] is True
        assert result["missing"] is True
        # Check status was changed to archived
        doc = get_document(db_conn, doc_id)
        assert doc["status"] == "archived"

    def test_sync_collection(self, db_conn, collection, sample_docs):
        register_document(db_conn, "test-collection", sample_docs["doc1"], "spec", "D1")
        register_document(db_conn, "test-collection", sample_docs["doc2"], "spec", "D2")
        # Modify one file
        sample_docs["doc1"].write_text("# Changed")
        changes = sync_collection(db_conn, "test-collection")
        assert len(changes) == 1

    def test_sync_nonexistent_document_raises(self, db_conn):
        with pytest.raises(ValueError, match="not found"):
            sync_document(db_conn, "nonexistent")

    def test_sync_nonexistent_collection_raises(self, db_conn):
        with pytest.raises(ValueError, match="does not exist"):
            sync_collection(db_conn, "no-such")


class TestContextAssembly:
    def test_assemble_context_basic(self, db_conn, collection, sample_docs):
        register_document(db_conn, "test-collection", sample_docs["doc1"], "spec", "D1")
        register_document(db_conn, "test-collection", sample_docs["doc2"], "spec", "D2")
        ctx = assemble_context(db_conn, collection_name="test-collection")
        assert "D1" in ctx
        assert "D2" in ctx

    def test_assemble_respects_budget(self, db_conn, collection, tmp_path):
        # Create a large file
        big = tmp_path / "big.md"
        big.write_text("word " * 5000)
        register_document(db_conn, "test-collection", big, "spec", "Big Doc")

        ctx = assemble_context(
            db_conn, collection_name="test-collection", token_budget=100
        )
        ctx_tokens = tokens.count_tokens(ctx)
        assert ctx_tokens <= 100

    def test_assemble_tagged_context(self, db_conn, collection, sample_docs):
        register_document(
            db_conn, "test-collection", sample_docs["doc1"], "spec", "Chunk Doc",
            tags={"chunk": "1.2"},
        )
        register_document(
            db_conn, "test-collection", sample_docs["doc2"], "spec", "Other Doc",
            tags={"chunk": "2.1"},
        )
        ctx = assemble_tagged_context(db_conn, "chunk", "1.2")
        assert "Chunk Doc" in ctx
        assert "Other Doc" not in ctx

    def test_assemble_empty_result(self, db_conn, collection):
        ctx = assemble_context(db_conn, doc_type="nonexistent")
        assert ctx == ""

    def test_assemble_includes_metadata(self, db_conn, collection, sample_docs):
        register_document(
            db_conn, "test-collection", sample_docs["doc1"], "spec", "Info Doc",
            external_id="EXT-001", status="active",
        )
        ctx = assemble_context(db_conn, collection_name="test-collection")
        assert "EXT-001" in ctx
        assert "spec" in ctx
        assert "active" in ctx


class TestValidation:
    @pytest.fixture
    def constrained_collection(self, db_conn):
        """Create a collection with doc_type and status constraints."""
        create_collection(
            db_conn, "constrained",
            config={"doc_types": ["spec", "plan"], "statuses": ["draft", "active"]},
        )

    def test_validate_allowed_type(self):
        config = {"doc_types": ["spec", "plan"]}
        validate_document(config, doc_type="spec")  # should not raise

    def test_validate_disallowed_type(self):
        config = {"doc_types": ["spec", "plan"]}
        with pytest.raises(ValueError, match="not allowed"):
            validate_document(config, doc_type="gherkin")

    def test_validate_allowed_status(self):
        config = {"statuses": ["draft", "active"]}
        validate_document(config, status="draft")  # should not raise

    def test_validate_disallowed_status(self):
        config = {"statuses": ["draft", "active"]}
        with pytest.raises(ValueError, match="not allowed"):
            validate_document(config, status="archived")

    def test_validate_no_constraints_skips(self):
        config = {}
        validate_document(config, doc_type="anything", status="anything")  # should not raise

    def test_register_rejects_invalid_type(self, db_conn, constrained_collection, sample_docs):
        with pytest.raises(ValueError, match="not allowed"):
            register_document(
                db_conn, "constrained", sample_docs["doc1"], "gherkin", "Bad Type"
            )

    def test_register_rejects_invalid_status(self, db_conn, constrained_collection, sample_docs):
        with pytest.raises(ValueError, match="not allowed"):
            register_document(
                db_conn, "constrained", sample_docs["doc1"], "spec", "Bad Status",
                status="archived",
            )


class TestNormalization:
    def test_doc_type_lowercased(self, db_conn, collection, sample_docs):
        doc_id = register_document(
            db_conn, "test-collection", sample_docs["doc1"], "SPEC", "Upper Type"
        )
        doc = get_document(db_conn, doc_id)
        assert doc["doc_type"] == "spec"

    def test_status_lowercased(self, db_conn, collection, sample_docs):
        doc_id = register_document(
            db_conn, "test-collection", sample_docs["doc1"], "spec", "Upper Status",
            status="ACTIVE",
        )
        doc = get_document(db_conn, doc_id)
        assert doc["status"] == "active"

    def test_tag_keys_lowercased(self, db_conn, collection, sample_docs):
        doc_id = register_document(
            db_conn, "test-collection", sample_docs["doc1"], "spec", "Upper Tags",
            tags={"Stage": "Implementation", "CHUNK": "1.2"},
        )
        doc = get_document(db_conn, doc_id)
        assert "stage" in doc["tags"]
        assert "chunk" in doc["tags"]

    def test_tag_values_lowercased(self, db_conn, collection, sample_docs):
        doc_id = register_document(
            db_conn, "test-collection", sample_docs["doc1"], "spec", "Upper Tag Vals",
            tags={"stage": "Implementation"},
        )
        doc = get_document(db_conn, doc_id)
        assert "implementation" in doc["tags"]["stage"]

    def test_validation_uses_normalized_type(self, db_conn, sample_docs):
        """Config says 'spec' but we pass 'SPEC' — should pass after normalization."""
        create_collection(
            db_conn, "norm-test",
            config={"doc_types": ["spec"], "statuses": ["draft"]},
        )
        doc_id = register_document(
            db_conn, "norm-test", sample_docs["doc1"], "SPEC", "Norm Test"
        )
        doc = get_document(db_conn, doc_id)
        assert doc["doc_type"] == "spec"


# ── Performance benchmarks (ACT-008, ACT-009) ─────────────────


class TestPerformanceBenchmarks:
    """SCN-1.4-01, SCN-1.4-02: Performance benchmarks for tag queries and assembly."""

    def test_tag_query_performance(self, db_conn):
        """SCN-1.4-01 / ACT-008: Tag query on 10K documents with multi-tag filter under 10ms."""
        from src.utils import ulid as ulid_mod

        # Create collection directly
        col_id = ulid_mod.generate()
        with transaction(db_conn) as cur:
            cur.execute(
                "INSERT INTO collection (collection_id, name) VALUES (?, ?)",
                (col_id, "bench"),
            )

        # Seed 10K documents with 5 tag dimensions via raw SQL
        tag_keys = ["stage", "chunk", "phase", "risk_tier", "owner"]
        tag_values_per_key = {
            "stage": ["planning", "implementation", "validation", "review", "release"],
            "chunk": [f"{p}.{c}" for p in range(1, 6) for c in range(1, 5)],
            "phase": ["1", "2", "3", "4", "5"],
            "risk_tier": ["low", "medium", "high"],
            "owner": ["alice", "bob", "carol", "dave"],
        }

        with transaction(db_conn) as cur:
            for i in range(10_000):
                doc_id = ulid_mod.generate()
                cur.execute(
                    """INSERT INTO document
                       (document_id, collection_id, doc_type, title, status,
                        file_path, content_hash, token_count, metadata_json)
                       VALUES (?, ?, 'spec', ?, 'draft', ?, ?, 100, '{}')""",
                    (doc_id, col_id, f"Doc {i}", f"/tmp/bench/{i}.md",
                     f"hash_{i:06d}"),
                )
                # Assign tags with independent cycling so multi-tag filters intersect
                for ki, key in enumerate(tag_keys):
                    vals = tag_values_per_key[key]
                    val = vals[(i >> ki) % len(vals)]
                    cur.execute(
                        "INSERT INTO document_tag (document_id, tag_key, tag_value) VALUES (?, ?, ?)",
                        (doc_id, key, val),
                    )

        # Warm up SQLite query planner
        db_conn.execute(
            """SELECT d.document_id FROM document d
               JOIN document_tag t0 ON t0.document_id = d.document_id
               JOIN document_tag t1 ON t1.document_id = d.document_id
               WHERE t0.tag_key = 'stage' AND t0.tag_value = 'implementation'
                 AND t1.tag_key = 'phase' AND t1.tag_value = '3'""",
        ).fetchall()

        # Timed run: multi-tag JOIN query (FND-0001 concern is the SQL JOIN perf)
        start = time.perf_counter()
        rows = db_conn.execute(
            """SELECT d.* FROM document d
               JOIN document_tag t0 ON t0.document_id = d.document_id
               JOIN document_tag t1 ON t1.document_id = d.document_id
               WHERE t0.tag_key = ? AND t0.tag_value = ?
                 AND t1.tag_key = ? AND t1.tag_value = ?
               ORDER BY d.created_at DESC""",
            ("stage", "implementation", "phase", "3"),
        ).fetchall()
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert len(rows) > 0, "Query should return results"
        assert elapsed_ms < 10, f"Tag query took {elapsed_ms:.1f}ms, expected <10ms"

        # Also verify query_documents() returns correct results (functional check)
        results = query_documents(db_conn, tags={"stage": "implementation", "phase": "3"})
        assert len(results) == len(rows)

    def test_assembly_latency(self, db_conn, tmp_path):
        """SCN-1.4-02 / ACT-009: 20-file context assembly under 100ms."""
        create_collection(db_conn, "bench-assembly")

        # Create 20 small files and register them
        for i in range(20):
            f = tmp_path / f"doc_{i:02d}.md"
            f.write_text(f"# Document {i}\n\nContent for benchmark document {i}.\n" * 10)
            register_document(
                db_conn, "bench-assembly", f, "spec", f"Bench Doc {i}",
                tags={"bench": "true"},
            )

        # Warm up
        assemble_context(db_conn, collection_name="bench-assembly", token_budget=50_000)

        # Timed run
        start = time.perf_counter()
        ctx = assemble_context(db_conn, collection_name="bench-assembly", token_budget=50_000)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert len(ctx) > 0, "Assembly should produce content"
        assert elapsed_ms < 100, f"Assembly took {elapsed_ms:.1f}ms, expected <100ms"
