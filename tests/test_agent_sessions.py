"""Tests for the agent-sessions collection plugin."""

from pathlib import Path

import pytest

from src.collections.agent_sessions import (
    SessionMeta,
    _parse_claude_session,
    _parse_codex_session,
    _parse_gemini_session,
    register_collection,
    scan_and_register,
    _discover_claude_sessions,
    _discover_codex_sessions,
    _discover_gemini_sessions,
)

FIXTURES = Path(__file__).parent / "fixtures" / "agent_sessions"


# ---------------------------------------------------------------------------
# Claude parsing
# ---------------------------------------------------------------------------


class TestClaudeParsing:
    def test_extracts_title(self):
        meta = _parse_claude_session(FIXTURES / "claude_small.jsonl")
        assert meta.title == "Refactoring auth module"

    def test_extracts_prompt(self):
        meta = _parse_claude_session(FIXTURES / "claude_small.jsonl")
        assert meta.first_prompt == "Refactor the auth module to use JWT tokens"

    def test_extracts_model(self):
        meta = _parse_claude_session(FIXTURES / "claude_small.jsonl")
        assert meta.model == "claude-sonnet-4-5-20250514"

    def test_extracts_cwd(self):
        meta = _parse_claude_session(FIXTURES / "claude_small.jsonl")
        assert meta.cwd == "/tmp/myproject"

    def test_extracts_session_id(self):
        meta = _parse_claude_session(FIXTURES / "claude_small.jsonl")
        assert meta.session_id == "ses_test"

    def test_extracts_timestamp(self):
        meta = _parse_claude_session(FIXTURES / "claude_small.jsonl")
        assert meta.timestamp == "2026-04-10T10:00:00Z"

    def test_not_housekeeping(self):
        meta = _parse_claude_session(FIXTURES / "claude_small.jsonl")
        assert meta.is_housekeeping is False


# ---------------------------------------------------------------------------
# Codex parsing
# ---------------------------------------------------------------------------


class TestCodexParsing:
    def test_extracts_cwd(self):
        meta = _parse_codex_session(FIXTURES / "codex_small.jsonl")
        assert meta.cwd == "/tmp/webapp"

    def test_extracts_prompt(self):
        meta = _parse_codex_session(FIXTURES / "codex_small.jsonl")
        assert meta.first_prompt == "Fix the login page CSS"

    def test_extracts_session_id(self):
        meta = _parse_codex_session(FIXTURES / "codex_small.jsonl")
        assert meta.session_id == "019d0000-test"

    def test_extracts_timestamp(self):
        meta = _parse_codex_session(FIXTURES / "codex_small.jsonl")
        assert meta.timestamp is not None
        assert "2026-04-10" in meta.timestamp

    def test_not_housekeeping(self):
        meta = _parse_codex_session(FIXTURES / "codex_small.jsonl")
        assert meta.is_housekeeping is False


# ---------------------------------------------------------------------------
# Gemini parsing
# ---------------------------------------------------------------------------


class TestGeminiParsing:
    def test_extracts_prompt(self):
        meta = _parse_gemini_session(FIXTURES / "gemini_small.json")
        assert meta.first_prompt == "Optimize the database queries"

    def test_extracts_model(self):
        meta = _parse_gemini_session(FIXTURES / "gemini_small.json")
        assert meta.model == "gemini-3-flash"

    def test_extracts_session_id(self):
        meta = _parse_gemini_session(FIXTURES / "gemini_small.json")
        assert meta.session_id == "session-test"

    def test_extracts_timestamp(self):
        meta = _parse_gemini_session(FIXTURES / "gemini_small.json")
        assert meta.timestamp == "2026-04-10T10:00:00Z"

    def test_not_housekeeping(self):
        meta = _parse_gemini_session(FIXTURES / "gemini_small.json")
        assert meta.is_housekeeping is False


# ---------------------------------------------------------------------------
# Housekeeping detection
# ---------------------------------------------------------------------------


class TestHousekeeping:
    def test_claude_housekeeping_detected(self):
        meta = _parse_claude_session(FIXTURES / "housekeeping_claude.jsonl")
        assert meta.is_housekeeping is True

    def test_claude_housekeeping_no_prompt(self):
        meta = _parse_claude_session(FIXTURES / "housekeeping_claude.jsonl")
        assert meta.first_prompt is None


# ---------------------------------------------------------------------------
# Discovery (against fixture dirs)
# ---------------------------------------------------------------------------


class TestDiscovery:
    def test_discover_claude_finds_jsonl(self, tmp_path):
        proj = tmp_path / "projects" / "-Users-test-proj"
        proj.mkdir(parents=True)
        (proj / "abc123.jsonl").write_text("{}")
        result = _discover_claude_sessions(tmp_path / "projects")
        assert len(result) == 1
        assert result[0].name == "abc123.jsonl"

    def test_discover_codex_finds_rollout(self, tmp_path):
        day = tmp_path / "2026" / "04" / "10"
        day.mkdir(parents=True)
        (day / "rollout-2026-04-10T10-00-00-abc.jsonl").write_text("{}")
        (day / "other.jsonl").write_text("{}")  # should not match
        result = _discover_codex_sessions(tmp_path)
        assert len(result) == 1
        assert "rollout-" in result[0].name

    def test_discover_gemini_finds_sessions(self, tmp_path):
        proj = tmp_path / "abc123hash" / "chats"
        proj.mkdir(parents=True)
        (proj / "session-2026-01-01.json").write_text("{}")
        result = _discover_gemini_sessions(tmp_path)
        assert len(result) == 1
        assert result[0].name == "session-2026-01-01.json"


# ---------------------------------------------------------------------------
# Scan and register (integration)
# ---------------------------------------------------------------------------


def _fixture_discovery(claude=None, codex=None, gemini=None):
    """Helper to set up monkeypatched discovery returning fixture files."""
    def apply(monkeypatch):
        monkeypatch.setattr(
            "src.collections.agent_sessions._discover_claude_sessions",
            lambda root=None: claude or [],
        )
        monkeypatch.setattr(
            "src.collections.agent_sessions._discover_codex_sessions",
            lambda root=None: codex or [],
        )
        monkeypatch.setattr(
            "src.collections.agent_sessions._discover_gemini_sessions",
            lambda root=None: gemini or [],
        )
    return apply


class TestScanAndRegister:
    @pytest.fixture()
    def db_with_collection(self, db_conn):
        """Register the collection and set up a fixture-based scan."""
        register_collection(db_conn)
        return db_conn

    def test_registers_documents(self, db_with_collection, monkeypatch):
        """Scan fixture files and verify documents are registered."""
        _fixture_discovery(
            claude=[FIXTURES / "claude_small.jsonl"],
            codex=[FIXTURES / "codex_small.jsonl"],
            gemini=[FIXTURES / "gemini_small.json"],
        )(monkeypatch)

        result = scan_and_register(db_with_collection, ".")
        assert len(result) == 3

        doc_types = {r["doc_type"] for r in result}
        assert doc_types == {"claude-session", "codex-session", "gemini-session"}

    def test_scan_idempotent(self, db_with_collection, monkeypatch):
        """Scanning twice should not create duplicates."""
        _fixture_discovery(claude=[FIXTURES / "claude_small.jsonl"])(monkeypatch)

        first = scan_and_register(db_with_collection, ".")
        second = scan_and_register(db_with_collection, ".")
        assert len(first) == 1
        assert len(second) == 0

    def test_housekeeping_skipped(self, db_with_collection, monkeypatch):
        """Housekeeping sessions should not be registered."""
        _fixture_discovery(claude=[FIXTURES / "housekeeping_claude.jsonl"])(monkeypatch)

        result = scan_and_register(db_with_collection, ".")
        assert len(result) == 0

    def test_tags_set_correctly(self, db_with_collection, monkeypatch):
        """Verify provider/date/project/model tags are set on registered documents."""
        _fixture_discovery(claude=[FIXTURES / "claude_small.jsonl"])(monkeypatch)

        result = scan_and_register(db_with_collection, ".")
        assert len(result) == 1
        doc_id = result[0]["document_id"]

        tags = db_with_collection.execute(
            "SELECT tag_key, tag_value FROM document_tag WHERE document_id = ?",
            (doc_id,),
        ).fetchall()
        tag_dict = {t["tag_key"]: t["tag_value"] for t in tags}

        assert tag_dict["provider"] == "claude"
        assert tag_dict["date"] == "2026-04-10"
        assert tag_dict["project"] == "myproject"
        assert tag_dict["model"] == "claude-sonnet-4-5-20250514"
        assert tag_dict["cwd"] == "/tmp/myproject"
