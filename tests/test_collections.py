"""Tests for src/collections/ai_dev_governance module."""

import json

import pytest

from src.collections.ai_dev_governance import (
    COLLECTION_CONFIG,
    COLLECTION_NAME,
    register_ai_dev_governance,
    scan_and_register,
)
from src.registry import get_collection, get_document, query_documents


class TestRegisterCollection:
    def test_creates_collection(self, db_conn):
        cid = register_ai_dev_governance(db_conn)
        assert len(cid) == 26
        col = get_collection(db_conn, COLLECTION_NAME)
        assert col is not None
        assert col["name"] == "ai-dev-governance"
        assert col["config"]["doc_types"] == COLLECTION_CONFIG["doc_types"]

    def test_idempotent(self, db_conn):
        cid1 = register_ai_dev_governance(db_conn)
        cid2 = register_ai_dev_governance(db_conn)
        assert cid1 == cid2

    def test_config_includes_lifecycle_stages(self, db_conn):
        register_ai_dev_governance(db_conn)
        col = get_collection(db_conn, COLLECTION_NAME)
        assert "ingest" in col["config"]["lifecycle_stages"]
        assert "release" in col["config"]["lifecycle_stages"]


class TestScanAndRegister:
    @pytest.fixture
    def gov_tree(self, tmp_path):
        """Create a minimal governance artifact tree for scanning."""
        # Pool question
        pq_dir = tmp_path / "docs" / "planning" / "pool_questions"
        pq_dir.mkdir(parents=True)
        (pq_dir / "phase-1-foundation.md").write_text("# Pool Questions Phase 1\n")

        # Scenarios
        scn_dir = tmp_path / "docs" / "planning" / "scenarios"
        scn_dir.mkdir(parents=True)
        (scn_dir / "SCN-1.1-01-ulid.feature").write_text("Feature: ULID\n")
        (scn_dir / "SCN-1.3-02-registration.feature").write_text("Feature: Registration\n")

        # Chunk plans
        chunk_dir = tmp_path / "docs" / "planning" / "chunks"
        chunk_dir.mkdir(parents=True)
        (chunk_dir / "chunk-1.1-utils.md").write_text("# Chunk 1.1\n")
        (chunk_dir / "chunk-1.3-registry.md").write_text("# Chunk 1.3\n")
        nested_chunk_dir = chunk_dir / "phase-7"
        nested_chunk_dir.mkdir()
        (nested_chunk_dir / "chunk-7.1.11b-two-lane-provisioning-contract.md").write_text("# Nested Chunk\n")

        # Sign-offs and traceability
        plan_dir = tmp_path / "docs" / "planning"
        (plan_dir / "signoffs.md").write_text("# Sign-offs\n")
        (plan_dir / "traceability.md").write_text("# Traceability\n")

        # Risk log
        (plan_dir / "phase-1-risks.md").write_text("# Phase 1 Risks\n")

        # Board artifacts
        board_dir = plan_dir / "board"
        board_dir.mkdir()
        (board_dir / "board-selection-dossier-2026-04-11.md").write_text("# Board Selection\n")
        (board_dir / "committee-review-packet-2026-04-11.md").write_text("# Packet\n")
        (board_dir / "committee-virtual-meeting-architecture-review-2026-04-11.md").write_text("# Meeting\n")
        members_dir = board_dir / "members"
        members_dir.mkdir()
        (members_dir / "BM-001.md").write_text("# Martin Kleppmann\n")
        (members_dir / "BM-007.md").write_text("# Andrej Karpathy\n")

        governance_board_dir = tmp_path / "docs" / "governance" / "board"
        governance_board_dir.mkdir(parents=True)
        (governance_board_dir / "2026-04-17-phase7-secure-transport-and-phase-posture-meeting-minutes.md").write_text("# Minutes\n")
        (governance_board_dir / "2026-04-17-phase7-secure-transport-and-phase-posture-review-packet.md").write_text("# Packet\n")
        (governance_board_dir / "2026-04-19-phase7-cockpitvm-startup-host-loop-followup-note.md").write_text("# Note\n")
        (governance_board_dir / "2026-04-23-phase7-1-11b-iam-ca-posture-and-provisioning-input-memo.md").write_text("# Memo\n")

        # Implementation plan
        impl_dir = tmp_path / "docs" / "plan"
        impl_dir.mkdir(parents=True)
        (impl_dir / "implementation-plan.md").write_text("# Implementation Plan\n")

        # Governance manifest
        (tmp_path / "governance.yaml").write_text("profile: strict-baseline\n")

        return tmp_path

    def test_scan_registers_all_artifacts(self, db_conn, gov_tree):
        register_ai_dev_governance(db_conn)
        results = scan_and_register(db_conn, gov_tree)
        assert len(results) >= 12  # at least the files we created

    def test_scan_assigns_correct_types(self, db_conn, gov_tree):
        register_ai_dev_governance(db_conn)
        scan_and_register(db_conn, gov_tree)

        gherkins = query_documents(db_conn, collection_name=COLLECTION_NAME, doc_type="gherkin")
        assert len(gherkins) == 2

        chunks = query_documents(db_conn, collection_name=COLLECTION_NAME, doc_type="chunk-plan")
        assert len(chunks) == 3

        manifests = query_documents(db_conn, collection_name=COLLECTION_NAME, doc_type="governance-manifest")
        assert len(manifests) == 1

    def test_scan_extracts_external_ids(self, db_conn, gov_tree):
        register_ai_dev_governance(db_conn)
        scan_and_register(db_conn, gov_tree)

        gherkins = query_documents(db_conn, collection_name=COLLECTION_NAME, doc_type="gherkin")
        ext_ids = {d["external_id"] for d in gherkins}
        assert "SCN-1.1-01" in ext_ids
        assert "SCN-1.3-02" in ext_ids

    def test_scan_extracts_chunk_tags(self, db_conn, gov_tree):
        register_ai_dev_governance(db_conn)
        scan_and_register(db_conn, gov_tree)

        chunks = query_documents(db_conn, collection_name=COLLECTION_NAME, doc_type="chunk-plan")
        chunk_tags = set()
        for doc in chunks:
            if "chunk" in doc["tags"]:
                chunk_tags.update(doc["tags"]["chunk"])
        assert "1.1" in chunk_tags
        assert "1.3" in chunk_tags
        assert "7.1" in chunk_tags

    def test_scan_extracts_phase_tags(self, db_conn, gov_tree):
        register_ai_dev_governance(db_conn)
        scan_and_register(db_conn, gov_tree)

        risks = query_documents(db_conn, collection_name=COLLECTION_NAME, doc_type="risk-log")
        assert len(risks) == 1
        assert "1" in risks[0]["tags"].get("phase", [])

    def test_scan_is_idempotent(self, db_conn, gov_tree):
        register_ai_dev_governance(db_conn)
        r1 = scan_and_register(db_conn, gov_tree)
        r2 = scan_and_register(db_conn, gov_tree)
        assert len(r1) >= 12
        assert len(r2) == 0  # no new registrations

    def test_scan_sets_active_status(self, db_conn, gov_tree):
        register_ai_dev_governance(db_conn)
        results = scan_and_register(db_conn, gov_tree)
        for r in results:
            doc = get_document(db_conn, r["document_id"])
            assert doc["status"] == "active"

    def test_scan_without_collection_raises(self, db_conn, gov_tree):
        with pytest.raises(ValueError, match="does not exist"):
            scan_and_register(db_conn, gov_tree)

    def test_board_member_external_ids(self, db_conn, gov_tree):
        register_ai_dev_governance(db_conn)
        scan_and_register(db_conn, gov_tree)

        profiles = query_documents(db_conn, collection_name=COLLECTION_NAME, doc_type="board-member-profile")
        ext_ids = {d["external_id"] for d in profiles}
        assert "BM-001" in ext_ids
        assert "BM-007" in ext_ids

    def test_scan_registers_nested_chunk_plan(self, db_conn, gov_tree):
        register_ai_dev_governance(db_conn)
        scan_and_register(db_conn, gov_tree)

        docs = query_documents(db_conn, collection_name=COLLECTION_NAME, doc_type="chunk-plan")
        titles = {d["title"] for d in docs}
        assert "Chunk 7.1.11b Two Lane Provisioning Contract" in titles

    def test_scan_registers_governance_board_artifacts(self, db_conn, gov_tree):
        register_ai_dev_governance(db_conn)
        scan_and_register(db_conn, gov_tree)

        packets = query_documents(db_conn, collection_name=COLLECTION_NAME, doc_type="board-packet")
        meetings = query_documents(db_conn, collection_name=COLLECTION_NAME, doc_type="meeting-record")
        decisions = query_documents(db_conn, collection_name=COLLECTION_NAME, doc_type="board-decision")
        handoffs = query_documents(db_conn, collection_name=COLLECTION_NAME, doc_type="implementation-handoff")

        packet_titles = {d["title"] for d in packets}
        meeting_titles = {d["title"] for d in meetings}
        decision_titles = {d["title"] for d in decisions}
        handoff_titles = {d["title"] for d in handoffs}

        assert "2026 04 17 Phase7 Secure Transport And Phase Posture Review Packet" in packet_titles
        assert "2026 04 17 Phase7 Secure Transport And Phase Posture Meeting Minutes" in meeting_titles
        assert "2026 04 19 Phase7 Cockpitvm Startup Host Loop Followup Note" in decision_titles
        assert "2026 04 23 Phase7 1 11b Iam Ca Posture And Provisioning Input Memo" in handoff_titles
