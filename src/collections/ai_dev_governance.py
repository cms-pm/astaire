"""ai-dev-governance collection — application-layer helper for SDLC artifacts.

This is a thin helper that exercises the generic registry core.
It defines the collection config and provides a scan-and-register helper
that maps file patterns to document types and tags.
"""

import json
import logging
import re
import sqlite3
from pathlib import Path

from src.db import transaction
from src.registry import create_collection, get_collection, register_document

logger = logging.getLogger(__name__)

COLLECTION_NAME = "ai-dev-governance"

COLLECTION_CONFIG = {
    "doc_types": [
        "pool-question",
        "signoff",
        "risk-log",
        "chunk-plan",
        "gherkin",
        "traceability",
        "board-packet",
        "meeting-record",
        "board-finding",
        "board-decision",
        "board-selection",
        "board-member-profile",
        "implementation-handoff",
        "implementation-plan",
        "validation-evidence",
        "exception-registry",
        "governance-manifest",
        "test-card",
    ],
    "lifecycle_stages": [
        "ingest",
        "plan",
        "artifact-generation",
        "implementation",
        "validation",
        "board-review",
        "gate",
        "release",
    ],
    "tag_keys": [
        "stage_produced",
        "consumed_by",
        "chunk",
        "phase",
        "risk_tier",
        "bundle_type",
        "id",
        "paradigm",
    ],
    "statuses": [
        "draft",
        "ready",
        "active",
        "superseded",
        "archived",
    ],
}

# Maps glob-like path patterns to (doc_type, base_tags) tuples.
# Patterns are matched against the file path relative to the project root.
# Additional tags (phase, chunk) are extracted from filenames where possible.
SCAN_RULES: list[tuple[str, str, dict[str, str]]] = [
    ("docs/planning/pool_questions/", "pool-question", {"stage_produced": "plan"}),
    ("docs/planning/scenarios/", "gherkin", {"stage_produced": "artifact-generation"}),
    ("docs/planning/chunks/", "chunk-plan", {"stage_produced": "plan"}),
    ("docs/planning/signoffs.md", "signoff", {"stage_produced": "plan"}),
    ("docs/planning/traceability.md", "traceability", {"stage_produced": "artifact-generation"}),
    ("docs/planning/phase-", "risk-log", {"stage_produced": "plan"}),
    ("docs/planning/board/board-selection-", "board-selection", {"stage_produced": "plan"}),
    ("docs/planning/board/committee-review-packet-", "board-packet", {"stage_produced": "board-review"}),
    ("docs/planning/board/board-composition-approval-", "board-decision", {"stage_produced": "board-review"}),
    ("docs/planning/board/committee-opportunity-register-", "board-packet", {"stage_produced": "board-review"}),
    ("docs/planning/board/committee-virtual-meeting-", "meeting-record", {"stage_produced": "board-review"}),
    ("docs/planning/board/members/", "board-member-profile", {"stage_produced": "plan"}),
    ("docs/planning/implementation-plan.md", "implementation-plan", {"stage_produced": "plan"}),
    ("docs/governance/exceptions.yaml", "exception-registry", {}),
    ("governance.yaml", "governance-manifest", {"stage_produced": "ingest"}),
    ("docs/releases/astaire/", "validation-evidence", {"stage_produced": "release", "bundle_type": "astaire"}),
    ("docs/releases/rtk/", "validation-evidence", {"stage_produced": "release", "bundle_type": "rtk"}),
    ("docs/releases/bootstrap/", "validation-evidence", {"stage_produced": "release", "bundle_type": "bootstrap"}),
    ("tests/cards/", "test-card", {"stage_produced": "plan"}),
]


def register_collection(conn: sqlite3.Connection) -> str:
    """Create or refresh the ai-dev-governance collection. Returns collection_id.

    On a pre-existing collection, the stored ``config_json`` is reconciled against
    the in-code ``COLLECTION_CONFIG`` so newly-added doc_types, lifecycle stages,
    statuses, or tag keys take effect without requiring operators to drop the DB.
    Removals are deliberately NOT propagated — only additions — so retired doc
    types continue to validate against historical documents.
    """
    existing = get_collection(conn, COLLECTION_NAME)
    if existing is None:
        return create_collection(
            conn, COLLECTION_NAME, "SDLC artifacts for ai-dev-governance methodology", COLLECTION_CONFIG
        )

    stored = existing.get("config") or {}
    merged = dict(stored)
    changed = False
    for key in ("doc_types", "lifecycle_stages", "statuses", "tag_keys"):
        in_code = list(COLLECTION_CONFIG.get(key, []))
        if not in_code:
            continue
        current = list(merged.get(key, []))
        additions = [v for v in in_code if v not in current]
        if additions:
            merged[key] = current + additions
            changed = True

    if changed:
        with transaction(conn) as cur:
            cur.execute(
                "UPDATE collection SET config_json = ? WHERE collection_id = ?",
                (json.dumps(merged), existing["collection_id"]),
            )
        logger.info("Refreshed collection %r config (additions only)", COLLECTION_NAME)

    return existing["collection_id"]


# Backward-compatible alias
register_ai_dev_governance = register_collection


def scan_and_register(
    conn: sqlite3.Connection,
    root_dir: str | Path,
) -> list[dict]:
    """Scan the project directory and register governance artifacts.

    Matches files against SCAN_RULES. Extracts phase and chunk tags from
    filenames where possible. Skips files already registered (by file_path).

    Returns list of {"document_id", "file_path", "doc_type", "title"} for newly registered docs.
    """
    root = Path(root_dir)
    registered = []

    # Get existing file paths to skip duplicates
    col = get_collection(conn, COLLECTION_NAME)
    if col is None:
        raise ValueError(f"Collection {COLLECTION_NAME!r} does not exist. Call register_ai_dev_governance() first.")

    existing_paths = set()
    rows = conn.execute(
        "SELECT file_path FROM document WHERE collection_id = ?",
        (col["collection_id"],),
    ).fetchall()
    for row in rows:
        existing_paths.add(row["file_path"])

    for rule_pattern, doc_type, base_tags in SCAN_RULES:
        full_pattern = root / rule_pattern

        if str(rule_pattern).endswith("/"):
            # Explicit directory patterns should recurse so nested chunk trees and
            # board/member directories are discoverable in downstream repos.
            files = _glob_dir_recursive(full_pattern)
        elif full_pattern.is_dir():
            # Pattern resolved to a directory (no trailing slash but is a dir).
            files = _glob_dir_recursive(full_pattern)
        else:
            # Prefix or exact filename match inside the parent directory
            parent = full_pattern.parent
            prefix = full_pattern.name
            files = sorted(f for f in parent.glob(f"{prefix}*") if f.is_file()) if parent.exists() else []

        for filepath in files:
            if not filepath.is_file():
                continue
            if filepath.suffix in (".pyc", ".pyo"):
                continue

            path_str = str(filepath)
            if path_str in existing_paths:
                continue

            tags = dict(base_tags)
            external_id = _extract_external_id(filepath, doc_type)
            _extract_phase_chunk_tags(filepath, tags)
            fm_external = _apply_test_card_frontmatter(filepath, doc_type, tags)
            if fm_external is not None:
                external_id = fm_external
            title = _derive_title(filepath, doc_type)

            doc_id = register_document(
                conn,
                COLLECTION_NAME,
                filepath,
                doc_type,
                title,
                tags=tags if tags else None,
                external_id=external_id,
                status="active",
            )
            existing_paths.add(path_str)
            registered.append({
                "document_id": doc_id,
                "file_path": path_str,
                "doc_type": doc_type,
                "title": title,
            })

    _refresh_test_card_frontmatter_tags(conn, col["collection_id"])

    for filepath, doc_type, base_tags in _scan_governance_board(root):
        path_str = str(filepath)
        if path_str in existing_paths:
            continue

        tags = dict(base_tags)
        external_id = _extract_external_id(filepath, doc_type)
        _extract_phase_chunk_tags(filepath, tags)
        title = _derive_title(filepath, doc_type)

        doc_id = register_document(
            conn,
            COLLECTION_NAME,
            filepath,
            doc_type,
            title,
            tags=tags if tags else None,
            external_id=external_id,
            status="active",
        )
        existing_paths.add(path_str)
        registered.append({
            "document_id": doc_id,
            "file_path": path_str,
            "doc_type": doc_type,
            "title": title,
        })

    logger.info("Scanned and registered %d new documents in %s", len(registered), COLLECTION_NAME)
    return registered


def _glob_dir_recursive(directory: Path) -> list[Path]:
    """Return all files under a directory, excluding hidden paths."""
    if not directory.is_dir():
        return []
    return sorted(
        f for f in directory.rglob("*")
        if f.is_file() and not any(part.startswith(".") for part in f.parts)
    )


def _scan_governance_board(root: Path) -> list[tuple[Path, str, dict[str, str]]]:
    """Classify project board artifacts under docs/governance/board."""
    board_dir = root / "docs" / "governance" / "board"
    if not board_dir.is_dir():
        return []

    staged_files: list[tuple[Path, str, dict[str, str]]] = []
    for filepath in _glob_dir_recursive(board_dir):
        name = filepath.stem
        if name.endswith("meeting-minutes"):
            staged_files.append((filepath, "meeting-record", {"stage_produced": "board-review"}))
        elif name.endswith("review-packet"):
            staged_files.append((filepath, "board-packet", {"stage_produced": "board-review"}))
        elif name.endswith("followup-note") or name.endswith("decision-note") or name.endswith("note"):
            staged_files.append((filepath, "board-decision", {"stage_produced": "board-review"}))
        elif name.endswith("memo") or "handoff-memo" in name:
            staged_files.append((filepath, "implementation-handoff", {"stage_produced": "board-review"}))
    return staged_files


_TEST_CARD_FRONTMATTER_KEYS = ("id", "paradigm")


def _refresh_test_card_frontmatter_tags(
    conn: sqlite3.Connection, collection_id: str
) -> None:
    """Backfill id/paradigm tags + external_id on already-registered test-cards.

    Existing repos that registered test-cards before the frontmatter-aware
    patch landed will carry only ``stage_produced=plan``. This pass walks every
    registered test-card document, re-reads frontmatter from disk, and upserts
    the derived tags. Idempotent — INSERT OR IGNORE keeps repeat scans cheap.
    """
    rows = conn.execute(
        "SELECT document_id, file_path, external_id FROM document "
        "WHERE collection_id = ? AND doc_type = 'test-card'",
        (collection_id,),
    ).fetchall()
    if not rows:
        return

    with transaction(conn) as cur:
        for row in rows:
            path = Path(row["file_path"])
            if not path.is_file():
                continue
            promoted: dict[str, str | list[str]] = {}
            fm_id = _apply_test_card_frontmatter(path, "test-card", promoted)
            for key, value in promoted.items():
                if isinstance(value, list):
                    continue
                cur.execute(
                    "INSERT OR IGNORE INTO document_tag (document_id, tag_key, tag_value) VALUES (?, ?, ?)",
                    (row["document_id"], key, value),
                )
            if fm_id and not row["external_id"]:
                cur.execute(
                    "UPDATE document SET external_id = ? WHERE document_id = ?",
                    (fm_id, row["document_id"]),
                )


def _apply_test_card_frontmatter(
    filepath: Path,
    doc_type: str,
    tags: dict[str, str | list[str]],
) -> str | None:
    """Promote test-card YAML frontmatter into document tags.

    For ``doc_type == "test-card"`` only, parse the YAML frontmatter at the head
    of the file and lift ``id`` and ``paradigm`` into the document_tag set.
    Returns the frontmatter ``id`` (used as ``external_id``) or ``None`` if the
    file is not a test-card or has no parseable frontmatter.

    No-op for every other doc_type — chunk-plan, gherkin, pool-question, etc.
    remain unchanged.
    """
    if doc_type != "test-card":
        return None

    frontmatter = _read_yaml_frontmatter(filepath)
    if frontmatter is None:
        return None

    for key in _TEST_CARD_FRONTMATTER_KEYS:
        value = frontmatter.get(key)
        if isinstance(value, str) and value:
            tags[key] = value

    fm_id = frontmatter.get("id")
    return fm_id if isinstance(fm_id, str) and fm_id else None


def _read_yaml_frontmatter(filepath: Path) -> dict[str, str] | None:
    """Parse top-level scalar YAML frontmatter from a markdown file.

    Recognises the standard ``---\\n...\\n---\\n`` block and returns a dict of
    top-level ``key: value`` scalars. Skips list/nested keys (lines that end in
    ``:`` with no value, or indented continuation lines) because test-card tag
    promotion only needs ``id`` and ``paradigm``. Returns ``None`` when the file
    has no frontmatter or cannot be read.

    Intentionally minimal: avoids a YAML dependency to keep Astaire's
    zero-extra-dep stance (see astaire/CLAUDE.md "No external dependencies
    except tiktoken").
    """
    try:
        with filepath.open("r", encoding="utf-8") as fh:
            first_line = fh.readline()
            if first_line.strip() != "---":
                return None
            parsed: dict[str, str] = {}
            for raw in fh:
                line = raw.rstrip("\n")
                if line.strip() == "---":
                    return parsed
                if not line or line.startswith(("#", " ", "\t", "-")):
                    continue
                if ":" not in line:
                    continue
                key, _, value = line.partition(":")
                key = key.strip()
                value = value.strip()
                if not key or not value:
                    continue
                if value.startswith(("'", '"')) and value.endswith(value[0]) and len(value) >= 2:
                    value = value[1:-1]
                parsed[key] = value
            return None
    except OSError:
        return None


def _extract_external_id(filepath: Path, doc_type: str) -> str | None:
    """Extract external ID from filename conventions."""
    name = filepath.stem

    # SCN-X.Y-NN from Gherkin filenames
    m = re.match(r"(SCN-\d+\.\d+-\d+)", name)
    if m:
        return m.group(1)

    # BM-NNN from board member profiles
    m = re.match(r"(BM-\d+)", name)
    if m:
        return m.group(1)

    # MTG-NNNN, PKT-NNNN, HOF-NNNN patterns
    m = re.match(r"((?:MTG|PKT|HOF|FND|DEC|ACT|COM)-\d+)", name)
    if m:
        return m.group(1)

    return None


def _extract_phase_chunk_tags(filepath: Path, tags: dict[str, str | list[str]]) -> None:
    """Extract phase and chunk numbers from filename patterns."""
    name = filepath.stem

    # phase-N from risk log filenames
    m = re.search(r"phase-(\d+)", name)
    if m:
        tags["phase"] = m.group(1)

    # chunk-X.Y from chunk plan filenames
    m = re.search(r"chunk-(\d+\.\d+)", name)
    if m:
        tags["chunk"] = m.group(1)

    # SCN-X.Y from Gherkin — extract chunk
    m = re.match(r"SCN-(\d+)\.(\d+)", name)
    if m:
        tags["phase"] = m.group(1)
        tags["chunk"] = f"{m.group(1)}.{m.group(2)}"


def _derive_title(filepath: Path, doc_type: str) -> str:
    """Derive a human-readable title from the filename."""
    name = filepath.stem
    # Replace hyphens/underscores with spaces, title case
    title = name.replace("-", " ").replace("_", " ")
    # Capitalize first letter of each word but preserve uppercase acronyms
    words = title.split()
    result = []
    for w in words:
        if w.isupper() and len(w) <= 4:
            result.append(w)
        else:
            result.append(w.capitalize())
    return " ".join(result)
