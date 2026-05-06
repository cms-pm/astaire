"""Tests for src/collections/scenario_ledger (issue #16)."""

import json

from src.collections.scenario_ledger import (
    COLLECTION_NAME,
    LEDGER_PATH,
    register_collection,
    scan_and_register,
)
from src.registry import get_collection, query_documents


def _write_ledger(tmp_path, records):
    path = tmp_path / LEDGER_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n")
    return path


class TestRegisterCollection:
    def test_creates_collection(self, db_conn):
        cid = register_collection(db_conn)
        assert len(cid) == 26
        col = get_collection(db_conn, COLLECTION_NAME)
        assert col is not None
        assert col["name"] == COLLECTION_NAME
        assert "passing" in col["config"]["statuses"]

    def test_idempotent(self, db_conn):
        a = register_collection(db_conn)
        b = register_collection(db_conn)
        assert a == b


class TestScanAndRegister:
    def test_no_ledger_file_returns_empty(self, db_conn, tmp_path):
        register_collection(db_conn)
        assert scan_and_register(db_conn, tmp_path) == []

    def test_latest_record_wins(self, db_conn, tmp_path):
        _write_ledger(
            tmp_path,
            [
                {"scenario_id": "SCN-A", "status": "failing",
                 "timestamp": "2026-05-01T00:00:00Z", "predicate_hash": "h1"},
                {"scenario_id": "SCN-A", "status": "passing",
                 "timestamp": "2026-05-04T00:00:00Z", "predicate_hash": "h2"},
                {"scenario_id": "SCN-B", "status": "expected-fail",
                 "timestamp": "2026-05-02T00:00:00Z", "predicate_hash": "h3"},
            ],
        )
        register_collection(db_conn)
        result = scan_and_register(db_conn, tmp_path)
        assert len(result) == 2
        by_id = {r["scenario_id"]: r for r in result}
        assert by_id["SCN-A"]["status"] == "passing"
        assert by_id["SCN-A"]["predicate_hash"] == "h2"
        assert by_id["SCN-B"]["status"] == "expected-fail"
        for row in result:
            assert "doc_type" in row and row["doc_type"] == "ledger-entry"
            assert "title" in row and row["title"]
            assert "file_path" in row and row["file_path"]

    def test_status_enum_normalized(self, db_conn, tmp_path):
        _write_ledger(
            tmp_path,
            [
                {"scenario_id": "SCN-X", "status": "PASSING",
                 "timestamp": "2026-05-01T00:00:00Z"},
            ],
        )
        register_collection(db_conn)
        scan_and_register(db_conn, tmp_path)
        row = db_conn.execute(
            "SELECT status FROM document WHERE external_id='SCN-X'"
        ).fetchone()
        assert row["status"] == "passing"

    def test_unknown_status_falls_back_to_not_started(self, db_conn, tmp_path):
        _write_ledger(
            tmp_path,
            [{"scenario_id": "SCN-Y", "status": "weird",
              "timestamp": "2026-05-01T00:00:00Z"}],
        )
        register_collection(db_conn)
        scan_and_register(db_conn, tmp_path)
        row = db_conn.execute(
            "SELECT status FROM document WHERE external_id='SCN-Y'"
        ).fetchone()
        assert row["status"] == "not-started"

    def test_predicate_hash_indexed_as_tag(self, db_conn, tmp_path):
        _write_ledger(
            tmp_path,
            [{"scenario_id": "SCN-Z", "status": "passing",
              "timestamp": "2026-05-04T00:00:00Z",
              "predicate_hash": "abc123", "chunk": "8.0t"}],
        )
        register_collection(db_conn)
        scan_and_register(db_conn, tmp_path)
        rows = db_conn.execute(
            "SELECT tag_key, tag_value FROM document_tag dt "
            "JOIN document d USING(document_id) "
            "WHERE d.external_id='SCN-Z'"
        ).fetchall()
        tags = {r["tag_key"]: r["tag_value"] for r in rows}
        assert tags.get("predicate_hash") == "abc123"
        assert tags.get("chunk") == "8.0t"
        assert tags.get("status") == "passing"

    def test_idempotent_skip_existing(self, db_conn, tmp_path):
        _write_ledger(
            tmp_path,
            [{"scenario_id": "SCN-A", "status": "passing",
              "timestamp": "2026-05-04T00:00:00Z"}],
        )
        register_collection(db_conn)
        first = scan_and_register(db_conn, tmp_path)
        second = scan_and_register(db_conn, tmp_path)
        assert len(first) == 1
        assert second == []

    def test_skips_malformed_lines(self, db_conn, tmp_path):
        path = tmp_path / LEDGER_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            '{"scenario_id":"SCN-A","status":"passing","timestamp":"2026-05-04T00:00:00Z"}\n'
            "{not json}\n"
            '{"status":"passing"}\n'
        )
        register_collection(db_conn)
        result = scan_and_register(db_conn, tmp_path)
        assert len(result) == 1
        assert result[0]["scenario_id"] == "SCN-A"

    def test_query_by_status_tag(self, db_conn, tmp_path):
        _write_ledger(
            tmp_path,
            [
                {"scenario_id": "SCN-1", "status": "passing",
                 "timestamp": "2026-05-04T00:00:00Z"},
                {"scenario_id": "SCN-2", "status": "failing",
                 "timestamp": "2026-05-04T00:00:00Z"},
            ],
        )
        register_collection(db_conn)
        scan_and_register(db_conn, tmp_path)
        passing = query_documents(
            db_conn, collection_name=COLLECTION_NAME, tags={"status": "passing"}
        )
        assert len(passing) == 1
        assert passing[0]["external_id"] == "SCN-1"
