"""Tests for src/collections/scenario_predicate (issue #17)."""

import hashlib
import json

from src.collections.scenario_predicate import (
    COLLECTION_NAME,
    _predicate_hash,
    register_collection,
    scan_and_register,
)
from src.registry import get_collection, query_documents


def _write_predicate(tmp_path, chunk: str, scenarios: dict, routing=None):
    pred_dir = tmp_path / "docs" / "validation" / f"phase-{chunk[0]}" / "predicates"
    pred_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "chunk": chunk,
        "feature": f"feat-{chunk}",
        "owner_chunk_plan": f"docs/planning/chunks/phase-{chunk[0]}/chunk-{chunk}.md",
        "schema_version": "0.1.0-pilot",
        "scenarios": scenarios,
    }
    if routing is not None:
        payload["routing_hint"] = routing
    path = pred_dir / f"scn-{chunk}.json"
    path.write_text(json.dumps(payload))
    return path


class TestRegisterCollection:
    def test_creates_collection(self, db_conn):
        cid = register_collection(db_conn)
        assert len(cid) == 26
        col = get_collection(db_conn, COLLECTION_NAME)
        assert col is not None
        assert col["name"] == COLLECTION_NAME

    def test_idempotent(self, db_conn):
        cid1 = register_collection(db_conn)
        cid2 = register_collection(db_conn)
        assert cid1 == cid2


class TestPredicateHash:
    def test_canonical_and_stable(self):
        a = {"predicate_kind": "queue_depth_at_least", "minimum_depth": 16}
        b = {"minimum_depth": 16, "predicate_kind": "queue_depth_at_least"}
        assert _predicate_hash(a) == _predicate_hash(b)

    def test_changes_with_content(self):
        a = {"predicate_kind": "queue_depth_at_least", "minimum_depth": 16}
        b = {"predicate_kind": "queue_depth_at_least", "minimum_depth": 32}
        assert _predicate_hash(a) != _predicate_hash(b)

    def test_known_hash(self):
        block = {"predicate_kind": "noop"}
        canonical = '{"predicate_kind":"noop"}'
        expected = hashlib.sha256(canonical.encode()).hexdigest()
        assert _predicate_hash(block) == expected


class TestScanAndRegister:
    def test_registers_one_row_per_scenario(self, db_conn, tmp_path):
        _write_predicate(
            tmp_path,
            "8.0t",
            {
                "SCN-8.0t-01": {
                    "intent": "depth bumped",
                    "predicate_kind": "queue_depth_at_least",
                    "surface": "src/esp32_bridge_main.c",
                },
                "SCN-8.0t-03": {
                    "intent": "STATUS additive",
                    "predicate_kind": "status_field_additive",
                    "surface": "behavioral",
                },
            },
            routing={"haiku_eligible": ["SCN-8.0t-03"]},
        )
        register_collection(db_conn)
        result = scan_and_register(db_conn, tmp_path)
        assert len(result) == 2

        docs = query_documents(db_conn, collection_name=COLLECTION_NAME)
        ext_ids = {d["external_id"] for d in docs}
        assert ext_ids == {"SCN-8.0t-01", "SCN-8.0t-03"}

    def test_idempotent_skip_existing(self, db_conn, tmp_path):
        _write_predicate(
            tmp_path,
            "8.0t",
            {"SCN-8.0t-01": {"intent": "x", "predicate_kind": "k"}},
        )
        register_collection(db_conn)
        first = scan_and_register(db_conn, tmp_path)
        second = scan_and_register(db_conn, tmp_path)
        assert len(first) == 1
        assert len(second) == 0

    def test_haiku_eligible_tag(self, db_conn, tmp_path):
        _write_predicate(
            tmp_path,
            "8.0t",
            {
                "SCN-A": {"intent": "a", "predicate_kind": "k"},
                "SCN-B": {"intent": "b", "predicate_kind": "k"},
            },
            routing={"haiku_eligible": ["SCN-B"]},
        )
        register_collection(db_conn)
        scan_and_register(db_conn, tmp_path)

        rows = db_conn.execute(
            "SELECT d.external_id, dt.tag_value "
            "FROM document d JOIN document_tag dt USING(document_id) "
            "WHERE dt.tag_key='haiku_eligible' ORDER BY d.external_id"
        ).fetchall()
        result = {r["external_id"]: r["tag_value"] for r in rows}
        assert result == {"SCN-A": "false", "SCN-B": "true"}

    def test_predicate_hash_in_metadata(self, db_conn, tmp_path):
        block = {"intent": "x", "predicate_kind": "queue_depth_at_least"}
        _write_predicate(tmp_path, "8.0t", {"SCN-X": block})
        register_collection(db_conn)
        scan_and_register(db_conn, tmp_path)

        row = db_conn.execute(
            "SELECT metadata_json FROM document WHERE external_id='SCN-X'"
        ).fetchone()
        meta = json.loads(row["metadata_json"])
        assert meta["predicate_hash"] == _predicate_hash(block)
        assert meta["scenario_id"] == "SCN-X"

    def test_skips_malformed_json(self, db_conn, tmp_path):
        pred_dir = tmp_path / "docs" / "validation" / "phase-9" / "predicates"
        pred_dir.mkdir(parents=True)
        (pred_dir / "scn-bad.json").write_text("{not json")
        register_collection(db_conn)
        result = scan_and_register(db_conn, tmp_path)
        assert result == []
