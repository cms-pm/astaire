"""Tests for src/collections/governance_authoring module."""

import pytest

from src.collections.governance_authoring import (
    COLLECTION_CONFIG,
    COLLECTION_NAME,
    register_collection,
    scan_and_register,
)
from src.registry import get_collection, query_documents


@pytest.fixture
def authoring_tree(tmp_path):
    """Minimal governance source tree mirroring ai-dev-governance layout."""
    (tmp_path / "core").mkdir()
    (tmp_path / "core" / "PLANNING_METHODOLOGY.md").write_text("# Planning\n")
    (tmp_path / "core" / "EVIDENCE_CONTRACT.md").write_text("# Evidence\n")

    (tmp_path / "adapters" / "providers").mkdir(parents=True)
    (tmp_path / "adapters" / "providers" / "CLAUDE_CONTEXT_ADAPTER.md").write_text("# Claude\n")
    (tmp_path / "adapters" / "tooling").mkdir(parents=True)
    (tmp_path / "adapters" / "tooling" / "RTK_CONTEXT_ADAPTER.md").write_text("# RTK\n")

    (tmp_path / "contracts").mkdir()
    (tmp_path / "contracts" / "governance-manifest.schema.json").write_text("{}\n")

    (tmp_path / "templates").mkdir()
    (tmp_path / "templates" / "ASTAIRE_CLI_SNIPPET.md").write_text("# Snippet\n")

    (tmp_path / "runbooks").mkdir()
    (tmp_path / "runbooks" / "RELEASE_PROCESS.md").write_text("# Release\n")

    (tmp_path / "CHANGELOG.md").write_text("# Changelog\n")
    return tmp_path


class TestRegisterCollection:
    def test_creates_collection(self, db_conn):
        cid = register_collection(db_conn)
        assert len(cid) == 26
        col = get_collection(db_conn, COLLECTION_NAME)
        assert col is not None
        assert col["name"] == COLLECTION_NAME

    def test_idempotent(self, db_conn):
        assert register_collection(db_conn) == register_collection(db_conn)

    def test_config_doc_types(self, db_conn):
        register_collection(db_conn)
        col = get_collection(db_conn, COLLECTION_NAME)
        for dt in ("core-policy", "adapter-spec", "contract-schema", "template",
                   "runbook", "compatibility-entry", "changelog-entry"):
            assert dt in col["config"]["doc_types"]


class TestScanAndRegister:
    def test_registers_at_least_one_per_doc_type(self, db_conn, authoring_tree):
        register_collection(db_conn)
        results = scan_and_register(db_conn, authoring_tree)
        types_found = {r["doc_type"] for r in results}
        assert "core-policy" in types_found
        assert "adapter-spec" in types_found
        assert "contract-schema" in types_found
        assert "template" in types_found
        assert "runbook" in types_found
        assert "changelog-entry" in types_found

    def test_query_core_policy(self, db_conn, authoring_tree):
        register_collection(db_conn)
        scan_and_register(db_conn, authoring_tree)
        docs = query_documents(db_conn, collection_name=COLLECTION_NAME, doc_type="core-policy")
        assert len(docs) >= 2
        titles = {d["title"] for d in docs}
        assert any("Planning" in t for t in titles)

    def test_adapter_spec_provider_tag(self, db_conn, authoring_tree):
        register_collection(db_conn)
        results = scan_and_register(db_conn, authoring_tree)
        adapter_docs = [r for r in results if r["doc_type"] == "adapter-spec"]
        assert len(adapter_docs) >= 2

    def test_idempotent_scan(self, db_conn, authoring_tree):
        register_collection(db_conn)
        first = scan_and_register(db_conn, authoring_tree)
        second = scan_and_register(db_conn, authoring_tree)
        assert len(second) == 0  # nothing new on second run

    def test_skips_hidden_and_binary(self, db_conn, authoring_tree):
        (authoring_tree / "core" / ".hidden.md").write_text("hidden\n")
        (authoring_tree / "core" / "policy.pyc").write_bytes(b"\x00")
        register_collection(db_conn)
        results = scan_and_register(db_conn, authoring_tree)
        paths = {r["file_path"] for r in results}
        assert not any(".hidden" in p for p in paths)
        assert not any(".pyc" in p for p in paths)

    def test_missing_dir_skipped_gracefully(self, db_conn, tmp_path):
        # A root with none of the expected dirs should register 0 docs, not crash
        register_collection(db_conn)
        results = scan_and_register(db_conn, tmp_path)
        assert results == []
