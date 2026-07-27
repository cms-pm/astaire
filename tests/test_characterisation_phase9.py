"""SCN-9.5 characterisation tests for non-pilot Astaire modules.

These tests pin legacy ingest, FTS, and CLI behavior before the Phase 10
full-repo hexagonal refactor. They deliberately assert current output
shape rather than new behavior.
"""

from __future__ import annotations

from argparse import Namespace

import pytest

from src.adapters.sqlite.fts_index import SQLiteFTSIndex, sanitize_fts_query
from src.cli import cmd_doctor
from src.db import get_connection, init_db
from src.ingest import scan_directory
from src.registry import create_collection


@pytest.mark.characterisation
def test_ingest_scan_directory_sorts_and_titles_registered_documents(
    db_conn, tmp_path, monkeypatch
) -> None:
    create_collection(
        db_conn,
        "characterisation",
        description="Characterisation collection",
        config={"doc_types": ["spec"], "statuses": ["draft"]},
    )
    (tmp_path / "zeta-note.md").write_text("# Zeta\n")
    (tmp_path / "alpha_note.md").write_text("# Alpha\n")
    (tmp_path / "ignored.txt").write_text("ignored\n")
    monkeypatch.setattr("src.ingest.generate_l0", lambda *args, **kwargs: None)

    result = scan_directory(
        db_conn,
        "characterisation",
        tmp_path,
        type_rules=[("*.md", "spec")],
    )

    assert result["registered"] == 2
    assert result["skipped"] == 1
    assert [doc["title"] for doc in result["documents"]] == [
        "Alpha Note",
        "Zeta Note",
    ]
    assert [doc["doc_type"] for doc in result["documents"]] == ["spec", "spec"]


@pytest.mark.characterisation
def test_fts_adapter_sanitizes_punctuation_and_rejects_unknown_table(db_conn) -> None:
    assert sanitize_fts_query("SCN-3.2: claims/projection?") == (
        "SCN 3 2  claims projection "
    )
    assert SQLiteFTSIndex(db_conn, table="claim_fts").search("!!!") == []

    with pytest.raises(ValueError, match="unsupported FTS table"):
        SQLiteFTSIndex(db_conn, table="source")


@pytest.mark.characterisation
def test_cli_doctor_reports_schema_and_tokenizer_state(tmp_path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "doctor.db"
    conn = get_connection(db_path)
    init_db(conn)
    conn.close()
    monkeypatch.setattr(
        "src.utils.tokens.check_tokenizer_health",
        lambda encoding="cl100k_base": {
            "ok": True,
            "message": "Tokenizer available",
            "approx_tokens_enabled": False,
        },
    )

    cmd_doctor(Namespace(db=str(db_path)))

    out = capsys.readouterr().out.splitlines()
    assert out == [
        f"[PASS] Database directory exists: {tmp_path}",
        "[PASS] Database schema initialized",
        "[INFO] Claims module: not installed (run 'astaire init --with-claims' to enable)",
        "[PASS] Tokenizer available",
        "[INFO] Approximate token fallback: disabled",
    ]
