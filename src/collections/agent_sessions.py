"""agent-sessions collection — indexes local AI coding sessions across providers.

Discovers and registers session files from Claude Code, Codex CLI, and Gemini CLI.
Each session is registered as a document with tags for provider, date, project, and model.
Housekeeping sessions (no meaningful prompts) are skipped.
"""

import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from src.registry import create_collection, get_collection, register_document
from src.utils import hashing, tokens

logger = logging.getLogger(__name__)

COLLECTION_NAME = "agent-sessions"

COLLECTION_CONFIG = {
    "doc_types": [
        "claude-session",
        "codex-session",
        "gemini-session",
    ],
    "statuses": [
        "active",
        "archived",
    ],
    "tag_keys": [
        "provider",
        "date",
        "project",
        "model",
        "cwd",
    ],
}

# Max JSONL lines to read for metadata extraction (avoid reading multi-MB files)
_CLAUDE_HEAD_LINES = 50
_CODEX_HEAD_LINES = 60

# Patterns that indicate a user message is just a meta/system command, not a real prompt
_META_PATTERNS = re.compile(
    r"<command-name>|<local-command-caveat>|^/clear$|^/model$|^/help$"
    r"|^<permissions instructions>|^<system-reminder>"
    r"|^# AGENTS\.md|^<environment_context>|^<local-command-stdout>"
    r"|^You are |^sandbox_mode",
    re.IGNORECASE,
)


@dataclass
class SessionMeta:
    """Lightweight metadata extracted from a session file."""

    provider: str
    session_id: str
    title: str | None
    first_prompt: str | None
    timestamp: str | None
    model: str | None
    cwd: str | None
    file_path: str
    is_housekeeping: bool


# ---------------------------------------------------------------------------
# Collection registration
# ---------------------------------------------------------------------------


def register_collection(conn: sqlite3.Connection) -> str:
    """Create or update the agent-sessions collection. Returns collection_id."""
    existing = get_collection(conn, COLLECTION_NAME)
    if existing:
        conn.execute(
            "UPDATE collection SET config_json = ? WHERE collection_id = ?",
            (json.dumps(COLLECTION_CONFIG), existing["collection_id"]),
        )
        conn.commit()
        return existing["collection_id"]
    return create_collection(
        conn,
        COLLECTION_NAME,
        "AI coding sessions from Claude Code, Codex CLI, and Gemini CLI",
        COLLECTION_CONFIG,
    )


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def _discover_claude_sessions(root: Path | None = None) -> list[Path]:
    """Find Claude Code session files under ~/.claude/projects/."""
    base = root or (Path.home() / ".claude" / "projects")
    if not base.exists():
        return []
    return sorted(base.glob("*/*.jsonl"))


def _discover_codex_sessions(root: Path | None = None) -> list[Path]:
    """Find Codex CLI session files under ~/.codex/sessions/."""
    if root is None:
        codex_home = os.environ.get("CODEX_HOME", "")
        base = Path(codex_home) / "sessions" if codex_home else Path.home() / ".codex" / "sessions"
    else:
        base = root
    if not base.exists():
        return []
    return sorted(base.rglob("rollout-*.jsonl"))


def _discover_gemini_sessions(root: Path | None = None) -> list[Path]:
    """Find Gemini CLI session files under ~/.gemini/tmp/."""
    base = root or (Path.home() / ".gemini" / "tmp")
    if not base.exists():
        return []
    found: list[Path] = []
    for project_dir in sorted(base.iterdir()):
        if not project_dir.is_dir():
            continue
        # Prefer chats/ subdirectory
        chats_dir = project_dir / "chats"
        if chats_dir.is_dir():
            found.extend(sorted(chats_dir.glob("session-*.json")))
        # Fallback: session files directly in project dir
        found.extend(sorted(project_dir.glob("session-*.json")))
    return found


# ---------------------------------------------------------------------------
# Parsing (lightweight, head-only)
# ---------------------------------------------------------------------------


def _read_jsonl_head(path: Path, max_lines: int) -> list[dict]:
    """Read up to max_lines JSON objects from a JSONL file."""
    events = []
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for i, line in enumerate(f):
                if i >= max_lines:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except OSError:
        pass
    return events


def _is_meta_content(text: str) -> bool:
    """Check if user message text is a meta/system command rather than a real prompt."""
    return bool(_META_PATTERNS.search(text))


def _parse_claude_session(path: Path) -> SessionMeta:
    """Extract metadata from a Claude Code session JSONL file."""
    events = _read_jsonl_head(path, _CLAUDE_HEAD_LINES)

    session_id = None
    title = None
    has_custom_title = False
    first_prompt = None
    timestamp = None
    model = None
    cwd = None
    has_meaningful_content = False

    for obj in events:
        evt_type = obj.get("type", "")

        # Session ID
        if session_id is None:
            session_id = obj.get("sessionId")

        # CWD
        if cwd is None:
            cwd_val = obj.get("cwd") or obj.get("project")
            if isinstance(cwd_val, str) and cwd_val.startswith("/"):
                cwd = cwd_val

        # Timestamp
        if timestamp is None and "timestamp" in obj:
            timestamp = obj["timestamp"]

        # Title from summary
        if evt_type == "summary" and not has_custom_title:
            summary = obj.get("summary")
            if isinstance(summary, str) and summary:
                title = summary

        # Custom title overrides summary
        if evt_type == "custom-title":
            ct = obj.get("customTitle")
            if isinstance(ct, str) and ct:
                title = ct
                has_custom_title = True

        # User messages
        if evt_type == "user":
            message = obj.get("message", {})
            content = message.get("content", "") if isinstance(message, dict) else ""
            is_meta = obj.get("isMeta", False)

            if isinstance(content, str) and content and not is_meta and not _is_meta_content(content):
                has_meaningful_content = True
                if first_prompt is None:
                    first_prompt = content

        # Model from assistant messages
        if evt_type == "assistant" and model is None:
            message = obj.get("message", {})
            if isinstance(message, dict):
                msg_model = message.get("model")
                if isinstance(msg_model, str) and msg_model:
                    model = msg_model

    return SessionMeta(
        provider="claude",
        session_id=session_id or path.stem,
        title=title,
        first_prompt=first_prompt,
        timestamp=timestamp,
        model=model,
        cwd=cwd,
        file_path=str(path),
        is_housekeeping=not has_meaningful_content,
    )


def _parse_codex_session(path: Path) -> SessionMeta:
    """Extract metadata from a Codex CLI session JSONL file."""
    events = _read_jsonl_head(path, _CODEX_HEAD_LINES)

    session_id = None
    first_prompt = None
    timestamp = None
    model = None
    cwd = None
    has_meaningful_content = False
    # Collect all candidate prompts; take the last non-meta one (preamble comes first)
    candidate_prompts: list[str] = []

    for obj in events:
        evt_type = obj.get("type", "")
        payload = obj.get("payload") if isinstance(obj.get("payload"), dict) else {}

        # session_meta carries most metadata
        if evt_type == "session_meta":
            if session_id is None:
                session_id = payload.get("id")
            if cwd is None:
                cwd_val = payload.get("cwd")
                if isinstance(cwd_val, str) and cwd_val:
                    cwd = cwd_val
            if timestamp is None:
                timestamp = payload.get("timestamp") or obj.get("timestamp")

        # turn_context carries model
        if evt_type == "turn_context" and model is None:
            turn_model = payload.get("model")
            if isinstance(turn_model, str) and turn_model:
                model = turn_model

        # response_item with role=user or role=developer contains prompts
        # Codex puts system preamble as early developer messages, then the real prompt last
        if evt_type == "response_item":
            role = payload.get("role", "")
            if role in ("user", "developer"):
                content_parts = payload.get("content", [])
                if isinstance(content_parts, list):
                    for part in content_parts:
                        if isinstance(part, dict):
                            text = part.get("text", "")
                            if isinstance(text, str) and text and not _is_meta_content(text):
                                candidate_prompts.append(text)
                            break

        if timestamp is None and "timestamp" in obj:
            timestamp = obj["timestamp"]

    # The real user prompt is typically the last developer message before the assistant responds
    if candidate_prompts:
        first_prompt = candidate_prompts[-1]
        has_meaningful_content = True

    # Extract session UUID from filename: rollout-YYYY-MM-DDThh-mm-ss-UUID.jsonl
    if session_id is None:
        m = re.search(r"rollout-[\dT-]+-(.+)\.jsonl$", path.name)
        session_id = m.group(1) if m else path.stem

    return SessionMeta(
        provider="codex",
        session_id=session_id,
        title=None,  # Codex doesn't have summary events
        first_prompt=first_prompt,
        timestamp=timestamp,
        model=model,
        cwd=cwd,
        file_path=str(path),
        is_housekeeping=not has_meaningful_content,
    )


def _parse_gemini_session(path: Path) -> SessionMeta:
    """Extract metadata from a Gemini CLI session JSON file."""
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
        data = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return SessionMeta(
            provider="gemini",
            session_id=path.stem,
            title=None,
            first_prompt=None,
            timestamp=None,
            model=None,
            cwd=None,
            file_path=str(path),
            is_housekeeping=True,
        )

    # Handle both object-with-messages and raw array formats
    if isinstance(data, list):
        messages = data
        session_id = path.stem
        timestamp = None
    else:
        messages = data.get("messages") or data.get("history") or []
        session_id = data.get("sessionId") or path.stem
        timestamp = data.get("startTime")

    first_prompt = None
    model = None
    has_meaningful_content = False

    for msg in messages:
        if not isinstance(msg, dict):
            continue
        msg_type = (msg.get("type") or msg.get("role") or "").lower()
        content = msg.get("content") or msg.get("text") or msg.get("displayContent") or ""

        if msg_type in ("user", "human"):
            if isinstance(content, str) and content and not _is_meta_content(content):
                has_meaningful_content = True
                if first_prompt is None:
                    first_prompt = content

        if msg_type in ("gemini", "model", "assistant") and model is None:
            msg_model = msg.get("model")
            if isinstance(msg_model, str) and msg_model:
                model = msg_model

        if timestamp is None:
            ts = msg.get("timestamp")
            if isinstance(ts, str):
                timestamp = ts

    return SessionMeta(
        provider="gemini",
        session_id=session_id,
        title=None,
        first_prompt=first_prompt,
        timestamp=timestamp,
        model=model,
        cwd=None,  # Gemini uses projectHash (opaque); cwd needs resolver
        file_path=str(path),
        is_housekeeping=not has_meaningful_content,
    )


# ---------------------------------------------------------------------------
# Scan and register
# ---------------------------------------------------------------------------

# Provider names — discovery and parse functions are looked up dynamically
# so monkeypatching works in tests.
_PROVIDER_NAMES = ("claude", "codex", "gemini")


def scan_and_register(
    conn: sqlite3.Connection,
    root_dir: str | Path,
) -> list[dict]:
    """Discover and register AI coding sessions from all providers.

    root_dir is unused — session files are at fixed provider-specific paths.
    Override roots can be passed via _discover_*_sessions() for testing.

    Returns list of newly registered document dicts.
    """
    # Import this module to look up functions dynamically (enables monkeypatch)
    import src.collections.agent_sessions as _self

    col = get_collection(conn, COLLECTION_NAME)
    if col is None:
        raise ValueError(f"Collection {COLLECTION_NAME!r} does not exist. Call register_collection() first.")

    # Get existing file paths and external IDs to skip duplicates
    existing_paths = set()
    existing_external_ids = set()
    rows = conn.execute(
        "SELECT file_path, external_id FROM document WHERE collection_id = ?",
        (col["collection_id"],),
    ).fetchall()
    for row in rows:
        existing_paths.add(row["file_path"])
        if row["external_id"]:
            existing_external_ids.add(row["external_id"])

    registered = []

    for provider_name in _PROVIDER_NAMES:
        discover_fn = getattr(_self, f"_discover_{provider_name}_sessions")
        parse_fn = getattr(_self, f"_parse_{provider_name}_session")
        try:
            files = discover_fn()
        except Exception:
            logger.warning("Failed to discover %s sessions", provider_name, exc_info=True)
            continue

        for filepath in files:
            path_str = str(filepath)
            if path_str in existing_paths:
                continue

            try:
                meta = parse_fn(filepath)
            except Exception:
                logger.debug("Failed to parse %s session: %s", provider_name, filepath, exc_info=True)
                continue

            # Skip if external_id already registered (e.g. same session across paths)
            if meta.session_id in existing_external_ids:
                continue

            if meta.is_housekeeping:
                continue

            # Build title from summary or first prompt
            title = meta.title
            if not title and meta.first_prompt:
                title = meta.first_prompt[:120].split("\n")[0]
            if not title:
                title = f"{provider_name} session {meta.session_id[:12]}"

            # Build tags
            tags: dict[str, str] = {"provider": provider_name}
            if meta.timestamp:
                # Extract date portion from ISO timestamp
                date_str = meta.timestamp[:10] if len(meta.timestamp) >= 10 else meta.timestamp
                tags["date"] = date_str
            if meta.cwd:
                tags["cwd"] = meta.cwd
                # Derive project name from cwd basename
                project = Path(meta.cwd).name
                if project:
                    tags["project"] = project
            if meta.model:
                tags["model"] = meta.model

            # Use path+mtime hash since session files grow over time
            try:
                mtime = filepath.stat().st_mtime
            except OSError:
                mtime = 0
            content_hash = hashing.hash_content(f"{path_str}:{mtime}")

            # Estimate tokens from extracted text only (not full file)
            indexable_text = f"{title or ''}\n{meta.first_prompt or ''}"
            token_count = tokens.count_tokens(indexable_text, "cl100k_base")

            doc_id = register_document(
                conn,
                COLLECTION_NAME,
                filepath,
                f"{provider_name}-session",
                title,
                tags=tags,
                external_id=meta.session_id,
                status="active",
                content_hash=content_hash,
                token_count=token_count,
            )
            existing_paths.add(path_str)
            existing_external_ids.add(meta.session_id)
            registered.append({
                "document_id": doc_id,
                "file_path": path_str,
                "doc_type": f"{provider_name}-session",
                "title": title,
            })

    logger.info("Scanned and registered %d new session(s) in %s", len(registered), COLLECTION_NAME)
    return registered
