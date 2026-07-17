"""Tests for src/cli.py — CLI subcommands and end-to-end integration.

Chunk 6.1: Every subcommand is tested via direct function call against
in-memory databases and temporary filesystem fixtures.
"""

import json
import sqlite3
from argparse import Namespace
from pathlib import Path

import pytest

from src.cli import (
    cmd_context,
    cmd_doctor,
    cmd_export,
    cmd_graphify_import,
    cmd_init,
    cmd_ingest,
    cmd_lint,
    cmd_prune,
    cmd_query,
    cmd_reindex_content,
    cmd_scan,
    cmd_status,
    cmd_startup,
    cmd_sync,
    build_parser,
)
from src.db import get_connection, init_db, transaction
from src.registry import create_collection, register_document
from src.utils import ulid


# ── Fixtures ─────────────────────────────────────────────────────


@pytest.fixture
def tmp_db(tmp_path):
    """Create a temporary database file with core registry + claims module
    initialized (with_claims=True) — most existing CLI tests predate the
    Proposal A module split and exercise features (export, prune, ingest,
    graphify import) that assume claim-side tables are present."""
    db_path = str(tmp_path / "test.db")
    conn = get_connection(db_path)
    init_db(conn, with_claims=True)
    conn.close()
    return db_path


@pytest.fixture
def tmp_db_core_only(tmp_path):
    """Create a temporary database file with only the core registry
    installed (no claims module) — used to exercise the B1/B2/B3
    core-only guard regressions at the CLI-command level."""
    db_path = str(tmp_path / "test_core_only.db")
    conn = get_connection(db_path)
    init_db(conn)
    conn.close()
    return db_path


@pytest.fixture
def sample_project(tmp_path):
    """Create a temporary directory mimicking a project structure
    with sample artifacts matching discovered collection scan rules."""
    root = tmp_path / "project"
    root.mkdir()

    # Pool questions
    pq_dir = root / "docs" / "planning" / "pool_questions"
    pq_dir.mkdir(parents=True)
    (pq_dir / "phase-1-foundation.md").write_text(
        "# Pool Questions: Phase 1\n\n### PQ-1.1: How do ULIDs work?\n"
    )

    # Gherkin scenarios
    scn_dir = root / "docs" / "planning" / "scenarios"
    scn_dir.mkdir(parents=True)
    (scn_dir / "SCN-1.1-01-ulid.feature").write_text(
        "Feature: ULID\n  @SCN-1.1-01\n  Scenario: Generate ULID\n"
    )
    (scn_dir / "SCN-1.2-01-connection.feature").write_text(
        "Feature: DB Connection\n  @SCN-1.2-01\n  Scenario: WAL mode\n"
    )

    # Chunk plans
    chunk_dir = root / "docs" / "planning" / "chunks"
    chunk_dir.mkdir(parents=True)
    (chunk_dir / "chunk-1.1-utils.md").write_text(
        "# Chunk 1.1: Utils\n\nImplement utility modules.\n"
    )
    (chunk_dir / "chunk-1.2-database.md").write_text(
        "# Chunk 1.2: Database\n\nImplement the registry database layer.\n"
    )

    # Signoff and traceability
    planning = root / "docs" / "planning"
    (planning / "signoffs.md").write_text(
        "# Sign-offs\n\n## Phase 1\n| Chunk | Status |\n|---|---|\n| 1.1 | complete |\n"
    )
    (planning / "traceability.md").write_text(
        "# Traceability Matrix\n\n| ID | Status |\n|---|---|\n| SCN-1.1-01 | pass |\n"
    )

    # Risk log
    (planning / "phase-1-risks.md").write_text(
        "# Phase 1 Risk Log\n\n- Low: utility functions are well-understood\n"
    )

    # Board artifacts
    board_dir = root / "docs" / "planning" / "board"
    board_dir.mkdir()
    (board_dir / "board-selection-dossier-2026-04-11.md").write_text(
        "# Board Selection\n\nPanel selected.\n"
    )
    members_dir = board_dir / "members"
    members_dir.mkdir()
    (members_dir / "BM-001.md").write_text("# BM-001\n\nExpert profile.\n")

    # Implementation plan
    plan_dir = root / "docs" / "plan"
    plan_dir.mkdir(parents=True)
    (plan_dir / "implementation-plan.md").write_text(
        "# Implementation Plan\n\nPhased delivery.\n"
    )

    # Governance manifest
    (root / "governance.yaml").write_text(
        "profile: strict-baseline\nproviders:\n  - claude\n"
    )

    return root


def _args(**kwargs):
    """Build a Namespace with defaults."""
    defaults = {
        "db": None,
        "verbose": False,
        "no_sync": False,
        "tag_prefix": None,
        "with_claims": False,
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


# ── Init ─────────────────────────────────────────────────────────


class TestInit:
    """SCN-6.1-01, SCN-6.1-02"""

    def test_init_creates_schema(self, tmp_path):
        """Proposal A: a bare `init` creates only the core registry — the
        claims module (entity/claim) is opt-in only."""
        db_path = str(tmp_path / "new.db")
        cmd_init(_args(db=db_path))
        conn = get_connection(db_path)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert "entity" not in tables
        assert "claim" not in tables
        assert "document" in tables
        assert "collection" in tables
        conn.close()

    def test_init_with_claims_creates_claims_module(self, tmp_path):
        db_path = str(tmp_path / "new_claims.db")
        cmd_init(_args(db=db_path, with_claims=True))
        conn = get_connection(db_path)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert "entity" in tables
        assert "claim" in tables
        assert "relationship" in tables
        assert "contradiction" in tables
        assert "document" in tables
        conn.close()

    def test_init_idempotent(self, tmp_path):
        db_path = str(tmp_path / "idem.db")
        cmd_init(_args(db=db_path))
        cmd_init(_args(db=db_path))  # no error

    def test_init_with_claims_idempotent(self, tmp_path):
        db_path = str(tmp_path / "idem_claims.db")
        cmd_init(_args(db=db_path, with_claims=True))
        cmd_init(_args(db=db_path, with_claims=True))  # no error


# ── Startup ──────────────────────────────────────────────────────


class TestStartup:
    """SCN-6.1-03, SCN-6.1-04"""

    def test_startup_full_checklist(self, tmp_path, sample_project, capsys):
        db_path = str(tmp_path / "startup.db")
        cmd_startup(_args(db=db_path, root=str(sample_project)))
        out = capsys.readouterr().out
        assert "Knowledge base state" in out

        # Verify documents were registered
        conn = get_connection(db_path)
        count = conn.execute("SELECT COUNT(*) FROM document").fetchone()[0]
        assert count > 0

        # Verify L0 cache exists
        l0 = conn.execute(
            "SELECT content_md FROM projection_cache WHERE tier='L0'"
        ).fetchone()
        assert l0 is not None
        conn.close()

    def test_startup_idempotent(self, tmp_path, sample_project, capsys):
        db_path = str(tmp_path / "startup2.db")
        cmd_startup(_args(db=db_path, root=str(sample_project)))
        first = capsys.readouterr().out

        cmd_startup(_args(db=db_path, root=str(sample_project)))
        second = capsys.readouterr().out

        # Second run should not report new documents
        assert "new document(s) registered" not in second


class TestStatus:
    def test_status_regenerates_stale_l0(self, tmp_path, sample_project, capsys):
        db_path = str(tmp_path / "status.db")
        cmd_startup(_args(db=db_path, root=str(sample_project)))
        capsys.readouterr()

        conn = get_connection(db_path)
        conn.execute(
            """UPDATE projection_cache
               SET content_md = ?, content_hash = ?
               WHERE tier = 'L0' AND scope_key = 'global'""",
            ("# stale\n", "stale-hash"),
        )
        conn.commit()
        conn.close()
        cmd_status(_args(db=db_path))
        out = capsys.readouterr().out
        assert "Knowledge base state" in out

        conn = get_connection(db_path)
        fresh_hash = conn.execute(
            "SELECT content_hash FROM projection_cache WHERE tier = 'L0' AND scope_key = 'global'"
        ).fetchone()["content_hash"]
        conn.close()
        assert fresh_hash != "stale-hash"


class TestDoctor:
    def test_doctor_reports_healthy_runtime(self, tmp_db, monkeypatch, capsys):
        monkeypatch.setattr(
            "src.utils.tokens.check_tokenizer_health",
            lambda encoding="cl100k_base": {
                "ok": True,
                "encoding": encoding,
                "message": f"Tokenizer encoding '{encoding}' is ready.",
                "approx_tokens_enabled": True,
            },
        )
        cmd_doctor(_args(db=tmp_db))
        out = capsys.readouterr().out
        assert "[PASS] Database schema initialized" in out
        assert "[PASS] Tokenizer encoding 'cl100k_base' is ready." in out

    def test_doctor_reports_missing_schema_as_warning(self, tmp_path, monkeypatch, capsys):
        db_path = str(tmp_path / "empty.db")
        Path(db_path).touch()
        monkeypatch.setattr(
            "src.utils.tokens.check_tokenizer_health",
            lambda encoding="cl100k_base": {
                "ok": True,
                "encoding": encoding,
                "message": f"Tokenizer encoding '{encoding}' is ready.",
                "approx_tokens_enabled": True,
            },
        )
        cmd_doctor(_args(db=db_path))
        out = capsys.readouterr().out
        assert "[WARN] Database schema incomplete or not initialized" in out

    def test_doctor_fails_when_database_directory_missing(self, tmp_path, monkeypatch, capsys):
        db_path = str(tmp_path / "missing" / "doctor.db")
        monkeypatch.setattr(
            "src.utils.tokens.check_tokenizer_health",
            lambda encoding="cl100k_base": {
                "ok": True,
                "encoding": encoding,
                "message": f"Tokenizer encoding '{encoding}' is ready.",
                "approx_tokens_enabled": True,
            },
        )
        with pytest.raises(SystemExit):
            cmd_doctor(_args(db=db_path))
        out = capsys.readouterr().out
        assert "[FAIL] Database directory missing" in out

    def test_doctor_warns_when_tokenizer_falls_back(self, tmp_db, monkeypatch, capsys):
        monkeypatch.setattr(
            "src.utils.tokens.check_tokenizer_health",
            lambda encoding="cl100k_base": {
                "ok": False,
                "encoding": encoding,
                "message": "Tokenizer encoding 'cl100k_base' is unavailable.",
                "approx_tokens_enabled": True,
            },
        )
        cmd_doctor(_args(db=tmp_db))
        out = capsys.readouterr().out
        assert "[WARN] Tokenizer encoding 'cl100k_base' is unavailable." in out
        assert "Approximate token fallback is enabled" in out

    def test_doctor_does_not_require_claim_tables(self, tmp_path, monkeypatch, capsys):
        """Proposal A: doctor's required_tables no longer includes the
        claims module — a core-only DB must report healthy, not [FAIL]."""
        db_path = str(tmp_path / "core_only.db")
        conn = get_connection(db_path)
        init_db(conn)
        conn.close()
        monkeypatch.setattr(
            "src.utils.tokens.check_tokenizer_health",
            lambda encoding="cl100k_base": {
                "ok": True,
                "encoding": encoding,
                "message": f"Tokenizer encoding '{encoding}' is ready.",
                "approx_tokens_enabled": True,
            },
        )
        cmd_doctor(_args(db=db_path))
        out = capsys.readouterr().out
        assert "[PASS] Database schema initialized" in out
        assert "[FAIL]" not in out
        assert "[INFO] Claims module: not installed" in out

    def test_doctor_reports_claims_module_installed(self, tmp_db, monkeypatch, capsys):
        monkeypatch.setattr(
            "src.utils.tokens.check_tokenizer_health",
            lambda encoding="cl100k_base": {
                "ok": True,
                "encoding": encoding,
                "message": f"Tokenizer encoding '{encoding}' is ready.",
                "approx_tokens_enabled": True,
            },
        )
        cmd_doctor(_args(db=tmp_db))
        out = capsys.readouterr().out
        assert "[INFO] Claims module: installed" in out


# ── Scan ─────────────────────────────────────────────────────────


class TestScan:
    """SCN-6.1-05, SCN-6.1-06"""

    def test_scan_registers_artifacts(self, tmp_path, sample_project, capsys):
        db_path = str(tmp_path / "scan.db")
        cmd_init(_args(db=db_path))
        cmd_scan(_args(db=db_path, root=str(sample_project), collection=None))
        out = capsys.readouterr().out
        assert "Registered" in out
        assert "0 new" not in out

        # Check specific types
        conn = get_connection(db_path)
        types = [r[0] for r in conn.execute(
            "SELECT DISTINCT doc_type FROM document"
        ).fetchall()]
        assert "gherkin" in types
        assert "chunk-plan" in types
        assert "pool-question" in types
        assert "signoff" in types
        conn.close()

    def test_scan_idempotent(self, tmp_path, sample_project, capsys):
        db_path = str(tmp_path / "scan2.db")
        cmd_init(_args(db=db_path))
        cmd_scan(_args(db=db_path, root=str(sample_project), collection=None))
        capsys.readouterr()

        cmd_scan(_args(db=db_path, root=str(sample_project), collection=None))
        out = capsys.readouterr().out
        assert "Registered 0 new document(s)" in out

    def test_scan_refreshes_modified_documents(self, tmp_path, sample_project, capsys):
        """Issue #14: scan must refresh existing documents whose content has
        changed on disk, not only register new files."""
        db_path = str(tmp_path / "scan_refresh.db")
        cmd_init(_args(db=db_path))
        cmd_scan(_args(db=db_path, root=str(sample_project), collection=None))
        capsys.readouterr()

        conn = get_connection(db_path)
        row = conn.execute("SELECT file_path, content_hash FROM document LIMIT 1").fetchone()
        target = Path(row["file_path"])
        old_hash = row["content_hash"]
        conn.close()
        assert target.exists()

        original_text = target.read_text(encoding="utf-8")
        target.write_text(original_text + "\n\n## Appended for issue #14\n", encoding="utf-8")

        cmd_scan(_args(db=db_path, root=str(sample_project), collection=None))
        out = capsys.readouterr().out
        assert "Refreshed" in out

        conn = get_connection(db_path)
        new_hash = conn.execute(
            "SELECT content_hash FROM document WHERE file_path = ?", (str(target),)
        ).fetchone()["content_hash"]
        conn.close()
        assert new_hash != old_hash, "scan should refresh content_hash on modified files"

    def test_scan_no_sync_skips_refresh(self, tmp_path, sample_project, capsys):
        """--no-sync preserves the prior scan-only behavior for callers that
        want to register without paying for a full-collection rehash."""
        db_path = str(tmp_path / "scan_no_sync.db")
        cmd_init(_args(db=db_path))
        cmd_scan(_args(db=db_path, root=str(sample_project), collection=None))
        capsys.readouterr()

        conn = get_connection(db_path)
        row = conn.execute("SELECT file_path FROM document LIMIT 1").fetchone()
        target = Path(row["file_path"])
        conn.close()
        target.write_text(target.read_text() + "\nedit\n", encoding="utf-8")

        cmd_scan(_args(db=db_path, root=str(sample_project), collection=None, no_sync=True))
        out = capsys.readouterr().out
        assert "Refreshed" not in out


# ── Query ────────────────────────────────────────────────────────


class TestQuery:
    """SCN-6.1-07 through SCN-6.1-11, SCN-6.1-23"""

    @pytest.fixture
    def query_db(self, tmp_path, sample_project):
        db_path = str(tmp_path / "query.db")
        cmd_init(_args(db=db_path))
        cmd_scan(_args(db=db_path, root=str(sample_project), collection=None))
        # Discover which collection was registered
        conn = get_connection(db_path)
        col_name = conn.execute("SELECT name FROM collection LIMIT 1").fetchone()["name"]
        conn.close()
        return db_path, col_name

    def test_query_by_collection(self, query_db, capsys):
        db_path, col_name = query_db
        cmd_query(_args(
            db=db_path, collection=col_name,
            type=None, status=None, tag=None, fts=None, json=False,
        ))
        out = capsys.readouterr().out
        assert "document(s) found" in out
        assert "0 document(s)" not in out

    def test_query_by_type(self, query_db, capsys):
        db_path, _ = query_db
        cmd_query(_args(
            db=db_path, collection=None,
            type="gherkin", status=None, tag=None, fts=None, json=False,
        ))
        out = capsys.readouterr().out
        assert "gherkin" in out

    def test_query_by_tag(self, query_db, capsys):
        db_path, _ = query_db
        cmd_query(_args(
            db=db_path, collection=None,
            type=None, status=None, tag=["chunk=1.1"], fts=None, json=False,
        ))
        out = capsys.readouterr().out
        assert "document(s) found" in out
        # Should include SCN-1.1-01 and chunk-1.1
        assert "1.1" in out

    def test_query_fts(self, query_db, capsys):
        db_path, _ = query_db
        cmd_query(_args(
            db=db_path, collection=None,
            type=None, status=None, tag=None, fts="registry", json=False,
        ))
        out = capsys.readouterr().out
        assert "document(s) found" in out

    def test_query_json_output(self, query_db, capsys):
        db_path, col_name = query_db
        cmd_query(_args(
            db=db_path, collection=col_name,
            type=None, status=None, tag=None, fts=None, json=True,
        ))
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert isinstance(parsed, list)
        assert len(parsed) > 0

    def test_query_no_results(self, query_db, capsys):
        db_path, _ = query_db
        cmd_query(_args(
            db=db_path, collection="nonexistent",
            type=None, status=None, tag=None, fts=None, json=False,
        ))
        out = capsys.readouterr().out
        assert "0 document(s) found" in out

    def test_query_fts_no_results_shows_diagnostics(self, query_db, capsys):
        db_path, _ = query_db
        cmd_query(_args(
            db=db_path, collection=None,
            type=None, status=None, tag=None, fts="nonexistent-term-xyz", json=False,
        ))
        out = capsys.readouterr().out
        assert "0 document(s) found" in out
        assert "searched fields:" in out
        assert "doc_types in scope:" in out


class TestQueryTagPrefix:
    """Proposal B item 3: --tag-prefix CLI flag for hierarchical tag queries."""

    def test_tag_prefix_matches_hierarchical_values(self, tmp_db, capsys):
        conn = get_connection(tmp_db)
        create_collection(conn, "chunked", config={})
        conn.close()

        f1 = Path(tmp_db).parent / "coarse.md"
        f1.write_text("Coarse chunk doc")
        f2 = Path(tmp_db).parent / "fine.md"
        f2.write_text("Fine chunk doc")

        conn = get_connection(tmp_db)
        register_document(conn, "chunked", f1, "spec", "Coarse", tags={"chunk": "7.1"})
        register_document(conn, "chunked", f2, "spec", "Fine", tags={"chunk": "7.1.16"})
        conn.close()

        cmd_query(_args(
            db=tmp_db, collection="chunked",
            type=None, status=None, tag=None, tag_prefix=["chunk=7.1"], fts=None, json=False,
        ))
        out = capsys.readouterr().out
        assert "2 document(s) found" in out


class TestQueryLog:
    """Proposal B item 5: query operations write an ingest_log row."""

    @pytest.fixture
    def query_db(self, tmp_path, sample_project):
        db_path = str(tmp_path / "query.db")
        cmd_init(_args(db=db_path))
        cmd_scan(_args(db=db_path, root=str(sample_project), collection=None))
        conn = get_connection(db_path)
        col_name = conn.execute("SELECT name FROM collection LIMIT 1").fetchone()["name"]
        conn.close()
        return db_path, col_name

    def test_query_writes_ingest_log_row_with_filters_and_hit_count(self, query_db, capsys):
        db_path, col_name = query_db
        cmd_query(_args(
            db=db_path, collection=col_name,
            type=None, status=None, tag=None, fts=None, json=False,
        ))
        out = capsys.readouterr().out
        hit_count = int(out.split(" document(s) found")[0].strip())

        conn = get_connection(db_path)
        rows = conn.execute("SELECT * FROM ingest_log WHERE operation = 'query'").fetchall()
        conn.close()
        assert len(rows) == 1
        assert f"{hit_count} hit(s)" in rows[0]["summary"]
        assert f"collection={col_name}" in rows[0]["summary"]

    def test_query_log_summary_reflects_fts_query(self, query_db, capsys):
        db_path, _ = query_db
        cmd_query(_args(
            db=db_path, collection=None,
            type=None, status=None, tag=None, fts="registry", json=False,
        ))
        capsys.readouterr()

        conn = get_connection(db_path)
        row = conn.execute(
            "SELECT * FROM ingest_log WHERE operation = 'query' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        conn.close()
        assert "registry" in row["summary"]
        assert "hit(s)" in row["summary"]


# ── Context ──────────────────────────────────────────────────────


class TestContext:
    """SCN-6.1-12"""

    def test_context_includes_l0_and_docs(self, tmp_path, sample_project, capsys):
        db_path = str(tmp_path / "ctx.db")
        cmd_startup(_args(db=db_path, root=str(sample_project)))
        capsys.readouterr()

        # Discover collection name dynamically
        conn = get_connection(db_path)
        col_name = conn.execute("SELECT name FROM collection LIMIT 1").fetchone()["name"]
        conn.close()

        cmd_context(_args(
            db=db_path, collection=col_name,
            tag=None, budget=8000,
        ))
        out = capsys.readouterr().out
        assert "Knowledge base state" in out  # L0


# ── Lint ─────────────────────────────────────────────────────────


class TestLint:
    """SCN-6.1-13, SCN-6.1-14"""

    def test_lint_reports_issues(self, tmp_path, sample_project, capsys):
        db_path = str(tmp_path / "lint.db")
        cmd_startup(_args(db=db_path, root=str(sample_project)))
        capsys.readouterr()

        cmd_lint(_args(db=db_path, fix=False))
        out = capsys.readouterr().out
        assert "Total:" in out

    def test_lint_fix(self, tmp_path, sample_project, capsys):
        db_path = str(tmp_path / "lint_fix.db")
        cmd_startup(_args(db=db_path, root=str(sample_project)))
        capsys.readouterr()

        cmd_lint(_args(db=db_path, fix=True))
        out = capsys.readouterr().out
        assert "Total:" in out


# ── Export ───────────────────────────────────────────────────────


class TestExport:
    """SCN-6.1-15"""

    def test_export_generates_wiki(self, tmp_path, sample_project, capsys):
        db_path = str(tmp_path / "export.db")
        # Export renders entity hub scores / contradictions, which requires
        # the optional claims module (Proposal A) — install it explicitly
        # before startup's core-only init_db() runs (idempotent, additive).
        cmd_init(_args(db=db_path, with_claims=True))
        cmd_startup(_args(db=db_path, root=str(sample_project)))
        capsys.readouterr()

        wiki_dir = str(tmp_path / "wiki_out")
        cmd_export(_args(db=db_path, output=wiki_dir))
        out = capsys.readouterr().out
        assert "Wiki exported" in out

        wiki = Path(wiki_dir)
        assert (wiki / "index.md").exists()
        assert (wiki / "contradictions.md").exists()
        assert (wiki / "timeline.md").exists()
        assert (wiki / "collections").is_dir()

    def test_export_on_core_only_db_fails_cleanly_without_deleting(
        self, tmp_db_core_only, tmp_path, capsys
    ):
        """Regression (B1): a core-only DB must not have its existing wiki
        output destroyed before export_wiki() discovers the claims module
        is absent."""
        wiki_dir = tmp_path / "wiki_out"
        wiki_dir.mkdir()
        existing = wiki_dir / "keepme.md"
        existing.write_text("do not delete me")

        with pytest.raises(RuntimeError, match="Claims module not installed"):
            cmd_export(_args(db=tmp_db_core_only, output=str(wiki_dir)))

        assert existing.exists()
        assert existing.read_text() == "do not delete me"


# ── Prune ────────────────────────────────────────────────────────


class TestPrune:
    """SCN-6.1-16, SCN-6.1-17"""

    def test_prune_nothing_to_prune(self, tmp_db, capsys):
        cmd_prune(_args(db=tmp_db))
        out = capsys.readouterr().out
        assert "Nothing to prune" in out

    def test_prune_removes_expired(self, tmp_db, capsys):
        conn = get_connection(tmp_db)

        # Create entity, source, and expired claim
        entity_id = ulid.generate()
        source_id = ulid.generate()
        claim_id = ulid.generate()
        with transaction(conn) as cur:
            cur.execute(
                "INSERT INTO entity (entity_id, canonical_name, entity_type) VALUES (?, ?, ?)",
                (entity_id, "Test Entity", "concept"),
            )
            cur.execute(
                "INSERT INTO source (source_id, title, source_type, content_hash, token_count) "
                "VALUES (?, ?, ?, ?, ?)",
                (source_id, "Test Source", "note", "abc123", 100),
            )
            cur.execute(
                "INSERT INTO claim (claim_id, entity_id, predicate, value, claim_type, "
                "confidence, source_id, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (claim_id, entity_id, "test", "value", "fact", 0.5, source_id,
                 "2020-01-01T00:00:00Z"),
            )
        conn.close()

        cmd_prune(_args(db=tmp_db))
        out = capsys.readouterr().out
        assert "Pruned 1 claim(s)" in out

        # Verify claim is gone
        conn = get_connection(tmp_db)
        count = conn.execute(
            "SELECT COUNT(*) FROM claim WHERE claim_id = ?", (claim_id,)
        ).fetchone()[0]
        assert count == 0
        conn.close()

    def test_prune_removes_stale_query_log(self, tmp_db, capsys):
        conn = get_connection(tmp_db)
        log_id = ulid.generate()
        with transaction(conn) as cur:
            cur.execute(
                "INSERT INTO ingest_log (log_id, operation, summary, created_at) "
                "VALUES (?, 'query', ?, ?)",
                (log_id, "test query", "2000-01-01T00:00:00Z"),
            )
        conn.close()

        cmd_prune(_args(db=tmp_db))
        out = capsys.readouterr().out
        assert "Pruned 1 stale query log entry" in out

        conn = get_connection(tmp_db)
        count = conn.execute(
            "SELECT COUNT(*) FROM ingest_log WHERE log_id = ?", (log_id,)
        ).fetchone()[0]
        assert count == 0
        conn.close()

    def test_prune_on_core_only_db_still_prunes_query_log(
        self, tmp_db_core_only, capsys
    ):
        """Regression (B3): cmd_prune() calls prune_expired_claims() before
        prune_query_log() — on a core-only DB the former must no-op cleanly
        rather than raise, so the latter (a core-registry feature) still
        runs."""
        conn = get_connection(tmp_db_core_only)
        log_id = ulid.generate()
        with transaction(conn) as cur:
            cur.execute(
                "INSERT INTO ingest_log (log_id, operation, summary, created_at) "
                "VALUES (?, 'query', ?, ?)",
                (log_id, "test query", "2000-01-01T00:00:00Z"),
            )
        conn.close()

        cmd_prune(_args(db=tmp_db_core_only))
        out = capsys.readouterr().out
        assert "Pruned 1 stale query log entry" in out

        conn = get_connection(tmp_db_core_only)
        count = conn.execute(
            "SELECT COUNT(*) FROM ingest_log WHERE log_id = ?", (log_id,)
        ).fetchone()[0]
        assert count == 0
        conn.close()

    def test_prune_on_core_only_db_with_nothing_to_prune(self, tmp_db_core_only, capsys):
        cmd_prune(_args(db=tmp_db_core_only))
        out = capsys.readouterr().out
        assert "Nothing to prune" in out


# ── Sync ─────────────────────────────────────────────────────────


class TestSync:
    """SCN-6.1-18, SCN-6.1-19, SCN-6.1-20"""

    @pytest.fixture
    def sync_db(self, tmp_path, sample_project):
        db_path = str(tmp_path / "sync.db")
        cmd_init(_args(db=db_path))
        cmd_scan(_args(db=db_path, root=str(sample_project), collection=None))
        return db_path, sample_project

    def test_sync_no_changes(self, sync_db, capsys):
        db_path, _ = sync_db
        cmd_sync(_args(db=db_path, collection=None))
        out = capsys.readouterr().out
        assert "All documents up to date" in out

        conn = get_connection(db_path)
        l0 = conn.execute(
            "SELECT content_md FROM projection_cache WHERE tier = 'L0' AND scope_key = 'global'"
        ).fetchone()
        conn.close()
        assert l0 is not None

    def test_sync_detects_modified(self, sync_db, capsys):
        db_path, project = sync_db
        # Modify a registered file
        (project / "governance.yaml").write_text("profile: modified\n")
        cmd_sync(_args(db=db_path, collection=None))
        out = capsys.readouterr().out
        assert "MODIFIED" in out

    def test_sync_detects_missing(self, sync_db, capsys):
        db_path, project = sync_db
        # Delete a registered file
        (project / "governance.yaml").unlink()
        cmd_sync(_args(db=db_path, collection=None))
        out = capsys.readouterr().out
        assert "MISSING" in out


class TestReindexContent:
    """Backfill path for a collection that opts into fts_content_index after
    its documents are already registered — found via live testing against a
    real populated database."""

    def test_reindex_backfills_and_reports_count(self, tmp_path, sample_project, capsys):
        db_path = str(tmp_path / "reindex2.db")
        cmd_init(_args(db=db_path))
        cmd_scan(_args(db=db_path, root=str(sample_project), collection=None))

        conn = get_connection(db_path)
        col_name = conn.execute("SELECT name FROM collection LIMIT 1").fetchone()["name"]
        conn.execute(
            "UPDATE collection SET config_json = json_set(config_json, '$.fts_content_index', json('true')) "
            "WHERE name = ?",
            (col_name,),
        )
        conn.commit()
        conn.close()

        cmd_reindex_content(_args(db=db_path, collection=col_name))
        out = capsys.readouterr().out
        assert "Indexed" in out
        assert "document(s)" in out

    def test_reindex_reports_zero_when_no_collection_opted_in(self, tmp_path, sample_project, capsys):
        db_path = str(tmp_path / "reindex3.db")
        cmd_init(_args(db=db_path))
        cmd_scan(_args(db=db_path, root=str(sample_project), collection=None))

        cmd_reindex_content(_args(db=db_path, collection=None))
        out = capsys.readouterr().out
        assert "Indexed 0 document(s)" in out
        assert "not opted in" in out


# ── Ingest ───────────────────────────────────────────────────────


class TestIngest:
    """SCN-6.1-21, SCN-6.1-22, SCN-6.1-24"""

    def test_ingest_with_claims(self, tmp_db, tmp_path, capsys):
        source_file = tmp_path / "article.md"
        source_file.write_text("# Test Article\n\nSome content about plasma physics.\n")

        claims_file = tmp_path / "claims.json"
        claims_file.write_text(json.dumps([
            {
                "entity": "Plasma",
                "predicate": "state of matter",
                "value": "fourth state of matter, ionized gas",
                "claim_type": "fact",
                "confidence": 0.9,
            },
            {
                "entity": "Plasma",
                "predicate": "temperature",
                "value": "typically above 10,000K",
                "claim_type": "fact",
                "confidence": 0.7,
            },
        ]))

        cmd_ingest(_args(
            db=tmp_db, file=str(source_file), title="Test Article",
            source_type="article", claims=str(claims_file),
        ))
        out = capsys.readouterr().out
        assert "Ingested:" in out
        assert "Entities created: 1" in out
        assert "Claims created: 2" in out

    def test_ingest_duplicate(self, tmp_db, tmp_path, capsys):
        source_file = tmp_path / "dup.md"
        source_file.write_text("Duplicate content.\n")

        cmd_ingest(_args(
            db=tmp_db, file=str(source_file), title="First",
            source_type="note", claims=None,
        ))
        capsys.readouterr()

        cmd_ingest(_args(
            db=tmp_db, file=str(source_file), title="Second",
            source_type="note", claims=None,
        ))
        out = capsys.readouterr().out
        assert "Duplicate source" in out

    def test_ingest_missing_file(self, tmp_db):
        with pytest.raises(FileNotFoundError):
            cmd_ingest(_args(
                db=tmp_db, file="/nonexistent/file.md", title="Missing",
                source_type="note", claims=None,
            ))


# ── Parser ───────────────────────────────────────────────────────


class TestParser:
    """Verify argparse wiring."""

    def test_all_subcommands_registered(self):
        parser = build_parser()
        # Parse each subcommand to verify it's registered
        for cmd in ["init", "startup", "status", "scan", "query", "context",
                     "lint", "export", "prune", "sync", "graphify-import"]:
            args = parser.parse_args([cmd])
            assert args.command == cmd

    def test_ingest_requires_title(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["ingest", "file.md"])  # missing --title

    def test_scan_accepts_collection_flag(self):
        parser = build_parser()
        args = parser.parse_args(["scan", "-c", "my-collection"])
        assert args.collection == "my-collection"

    def test_graphify_import_accepts_budget_flag(self):
        parser = build_parser()
        args = parser.parse_args(["graphify-import", "--l0-budget", "123"])
        assert args.command == "graphify-import"
        assert args.l0_budget == 123


class TestGraphifyImport:
    def test_graphify_import_duplicate_cli_run_has_complete_summary(self, tmp_path, capsys):
        db_path = str(tmp_path / "graphify.db")
        project = tmp_path / "project"
        graph_dir = project / "graphify-out"
        graph_dir.mkdir(parents=True)
        # Graphify import writes entities/relationships/claims, which
        # requires the optional claims module (Proposal A).
        cmd_init(_args(db=db_path, with_claims=True))
        (project / "governance.yaml").write_text(
            "profile: strict-baseline\n"
            "graphify:\n"
            "  promotionThreshold: absolute:2\n"
            "  promotionFloor: 1\n"
            "  promotionCeiling: 10\n"
        )
        (graph_dir / "graph.json").write_text(json.dumps({
            "source_repo": "repo-a",
            "graph_version": "v1",
            "graph_schema_version": "gs1",
            "nodes": [
                {"id": "svc", "label": "Service", "node_type": "service"},
                {"id": "contract", "label": "Contract", "node_type": "contract"},
            ],
            "links": [
                {"source": "svc", "target": "contract", "relation": "depends_on", "confidence": "EXTRACTED"},
            ],
        }))

        cmd_graphify_import(_args(db=db_path, root=str(project), graph=None, threshold=None, floor=None, ceiling=None,
                                  pinned_node=None, inferred_edge_threshold=None, annotate_approval_status=False,
                                  contract_registry=None, auto_tune=False, l0_budget=2000))
        capsys.readouterr()

        cmd_graphify_import(_args(db=db_path, root=str(project), graph=None, threshold=None, floor=None, ceiling=None,
                                  pinned_node=None, inferred_edge_threshold=None, annotate_approval_status=False,
                                  contract_registry=None, auto_tune=False, l0_budget=2000))
        out = capsys.readouterr().out
        assert "Graphify import: v1 (no-op)" in out
        assert "Selected nodes: 2" in out
        assert "Effective threshold: absolute:2" in out
        assert "Relationships created: 0" in out
        assert "Claims created: 0" in out

    def test_graphify_import_uses_manifest_contract_registry(self, tmp_path, monkeypatch, capsys):
        db_path = str(tmp_path / "graphify-registry.db")
        project = tmp_path / "project"
        graph_dir = project / "graphify-out"
        contracts_dir = project / "docs" / "governance"
        graph_dir.mkdir(parents=True)
        contracts_dir.mkdir(parents=True)
        cmd_init(_args(db=db_path, with_claims=True))
        registry = contracts_dir / "contracts.json"
        registry.write_text(json.dumps([{"id": "contract", "approval_status": "approved"}]))
        (project / "governance.yaml").write_text(
            "profile: strict-baseline\n"
            "contracts:\n"
            "  registryPath: docs/governance/contracts.json\n"
            "graphify:\n"
            "  annotateApprovalStatus: true\n"
        )
        (graph_dir / "graph.json").write_text(json.dumps({
            "source_repo": "repo-a",
            "graph_version": "v1",
            "graph_schema_version": "gs1",
            "nodes": [{"id": "contract", "label": "Contract", "node_type": "contract"}],
            "links": [],
        }))

        captured = {}

        def fake_import(conn, **kwargs):
            captured.update(kwargs)
            return {
                "duplicate": False,
                "source_id": "src-1",
                "graph_version": "v1",
                "graph_schema_version": "gs1",
                "source_repo": "repo-a",
                "cache_key": "graphify:test",
                "selected_nodes": 1,
                "effective_threshold": "p90",
                "auto_tuned": False,
                "entities_created": 1,
                "entities_existing": 0,
                "relationships_created": 0,
                "relationships_updated": 0,
                "relationships_removed": 0,
                "claims_created": 1,
                "claims_updated": 0,
                "claims_superseded": 0,
                "skipped_inferred": 0,
                "skipped_ambiguous": 0,
            }

        import src.ingest_graphify as ingest_graphify

        monkeypatch.setattr(ingest_graphify, "import_graphify", fake_import)
        cmd_graphify_import(_args(
            db=db_path,
            root=str(project),
            graph=None,
            threshold=None,
            floor=None,
            ceiling=None,
            pinned_node=None,
            inferred_edge_threshold=None,
            annotate_approval_status=False,
            contract_registry=None,
            auto_tune=False,
            l0_budget=2000,
        ))
        capsys.readouterr()
        assert captured["contract_registry_path"] == str(registry)

    def test_graphify_import_on_core_only_db_leaves_no_orphan_source(
        self, tmp_db_core_only, tmp_path, capsys
    ):
        """Regression (B2): a core-only DB must not end up with a
        committed `source` row when the claims-side write fails."""
        project = tmp_path / "project"
        graph_dir = project / "graphify-out"
        graph_dir.mkdir(parents=True)
        (project / "governance.yaml").write_text(
            "profile: strict-baseline\n"
            "graphify:\n"
            "  promotionThreshold: absolute:2\n"
            "  promotionFloor: 1\n"
            "  promotionCeiling: 10\n"
        )
        (graph_dir / "graph.json").write_text(json.dumps({
            "source_repo": "repo-a",
            "graph_version": "v1",
            "graph_schema_version": "gs1",
            "nodes": [
                {"id": "svc", "label": "Service", "node_type": "service"},
                {"id": "contract", "label": "Contract", "node_type": "contract"},
            ],
            "links": [
                {"source": "svc", "target": "contract", "relation": "depends_on", "confidence": "EXTRACTED"},
            ],
        }))

        with pytest.raises(RuntimeError, match="Claims module not installed"):
            cmd_graphify_import(_args(
                db=tmp_db_core_only, root=str(project), graph=None, threshold=None,
                floor=None, ceiling=None, pinned_node=None, inferred_edge_threshold=None,
                annotate_approval_status=False, contract_registry=None, auto_tune=False,
                l0_budget=2000,
            ))

        conn = get_connection(tmp_db_core_only)
        count = conn.execute("SELECT COUNT(*) FROM source").fetchone()[0]
        conn.close()
        assert count == 0


# ── Negative paths (Phase 5 hardening) ──────────────────────────


class TestNegativePaths:
    """CLI negative path coverage per Nygard's board feedback."""

    def test_db_nonexistent_directory(self):
        with pytest.raises(SystemExit, match="directory does not exist"):
            cmd_init(_args(db="/nonexistent/dir/test.db"))

    def test_command_on_uninitialized_db(self, tmp_path, capsys):
        db_path = str(tmp_path / "empty.db")
        # Create DB file but don't init schema
        from src.db import get_connection
        conn = get_connection(db_path)
        conn.close()
        from src.cli import main
        import sys
        old_argv = sys.argv
        sys.argv = ["astaire", "--db", db_path, "status"]
        try:
            with pytest.raises(SystemExit):
                main()
        finally:
            sys.argv = old_argv
        err = capsys.readouterr().err
        assert "not initialized" in err

    def test_ingest_missing_claims_file(self, tmp_db, tmp_path):
        source_file = tmp_path / "src.md"
        source_file.write_text("content")
        with pytest.raises(SystemExit, match="claims file not found"):
            cmd_ingest(_args(
                db=tmp_db, file=str(source_file), title="Test",
                source_type="note", claims="/nonexistent/claims.json",
            ))

    def test_scan_unknown_collection(self, tmp_db, tmp_path, capsys):
        with pytest.raises(SystemExit):
            cmd_scan(_args(db=tmp_db, root=str(tmp_path), collection="nonexistent"))


# ── Sync output enrichment (ACT-022) ────────────────────────────


class TestSyncOutput:
    """Verify sync output includes title and file_path."""

    def test_sync_shows_title_on_modified(self, tmp_path, sample_project, capsys):
        db_path = str(tmp_path / "sync_out.db")
        cmd_init(_args(db=db_path))
        cmd_scan(_args(db=db_path, root=str(sample_project), collection=None))
        capsys.readouterr()

        (sample_project / "governance.yaml").write_text("modified\n")
        cmd_sync(_args(db=db_path, collection=None))
        out = capsys.readouterr().out
        assert "MODIFIED" in out
        assert "governance" in out.lower()  # title or path should appear

    def test_sync_shows_title_on_missing(self, tmp_path, sample_project, capsys):
        db_path = str(tmp_path / "sync_miss.db")
        cmd_init(_args(db=db_path))
        cmd_scan(_args(db=db_path, root=str(sample_project), collection=None))
        capsys.readouterr()

        (sample_project / "governance.yaml").unlink()
        cmd_sync(_args(db=db_path, collection=None))
        out = capsys.readouterr().out
        assert "MISSING" in out
        assert "governance" in out.lower()


# ── Connection cleanup (ACT-023) ─────────────────────────────────


class TestConnectionCleanup:
    """Verify connections are closed even on exception."""

    def test_managed_connection_closes_on_error(self, tmp_path):
        from src.db import managed_connection
        db_path = str(tmp_path / "cleanup.db")
        try:
            with managed_connection(db_path) as conn:
                init_db(conn)
                raise RuntimeError("simulated error")
        except RuntimeError:
            pass
        # Connection should be closed — opening a new one should work fine
        conn2 = get_connection(db_path)
        tables = [r[0] for r in conn2.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert "document" in tables
        conn2.close()


class TestCLIParser:
    def test_parser_shows_help_for_missing_command(self):
        parser = build_parser()
        assert parser.parse_args([]).command is None

    def test_parser_accepts_doctor(self):
        parser = build_parser()
        args = parser.parse_args(["doctor"])
        assert args.command == "doctor"
