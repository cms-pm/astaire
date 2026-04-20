"""governance-authoring collection — indexes ai-dev-governance source artifacts.

Covers the normative policy tree (core/, adapters/, contracts/, templates/,
runbooks/) so agents can query governance source docs without direct file reads.
Honors the collectionStrategy field in governance.yaml when present.
"""

import logging
import re
import sqlite3
from pathlib import Path

from src.registry import create_collection, get_collection, register_document

logger = logging.getLogger(__name__)

COLLECTION_NAME = "governance-authoring"

COLLECTION_CONFIG = {
    "doc_types": [
        "core-policy",
        "adapter-spec",
        "contract-schema",
        "template",
        "runbook",
        "compatibility-entry",
        "changelog-entry",
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
        "policy_area",
        "version",
        "status",
        "provider",
    ],
    "statuses": [
        "draft",
        "active",
        "superseded",
        "archived",
    ],
}

SCAN_RULES: list[tuple[str, str, dict[str, str]]] = [
    ("core/",                    "core-policy",        {"policy_area": "core"}),
    ("adapters/providers/",      "adapter-spec",       {"policy_area": "provider"}),
    ("adapters/tooling/",        "adapter-spec",       {"policy_area": "tooling"}),
    ("contracts/",               "contract-schema",    {"policy_area": "contracts"}),
    ("templates/",               "template",           {"policy_area": "templates"}),
    ("runbooks/",                "runbook",            {"policy_area": "runbooks"}),
    ("CHANGELOG.md",             "changelog-entry",    {"policy_area": "releases"}),
]


def register_collection(conn: sqlite3.Connection) -> str:
    existing = get_collection(conn, COLLECTION_NAME)
    if existing:
        return existing["collection_id"]
    return create_collection(
        conn,
        COLLECTION_NAME,
        "ai-dev-governance normative source artifacts",
        COLLECTION_CONFIG,
    )


def scan_and_register(
    conn: sqlite3.Connection,
    root_dir: str | Path,
) -> list[dict]:
    """Scan governance source tree and register authoring artifacts.

    Respects collectionStrategy from governance.yaml if present:
    - 'split'   (default): registers only this collection's paths
    - 'unified': same behaviour for this plugin (ai-dev-governance collection
                 handles the SDLC artifact paths; no overlap)
    """
    root = Path(root_dir)
    registered = []

    col = get_collection(conn, COLLECTION_NAME)
    if col is None:
        raise ValueError(
            f"Collection {COLLECTION_NAME!r} not registered. "
            "Call register_collection() first."
        )

    existing_paths: set[str] = set()
    for row in conn.execute(
        "SELECT file_path FROM document WHERE collection_id = ?",
        (col["collection_id"],),
    ).fetchall():
        existing_paths.add(row["file_path"])

    for rule_pattern, doc_type, base_tags in SCAN_RULES:
        full_pattern = root / rule_pattern

        if str(rule_pattern).endswith("/"):
            files = _glob_dir_recursive(full_pattern)
        elif full_pattern.is_dir():
            files = _glob_dir_recursive(full_pattern)
        else:
            parent = full_pattern.parent
            prefix = full_pattern.name
            files = (
                sorted(f for f in parent.glob(f"{prefix}*") if f.is_file())
                if parent.exists()
                else []
            )

        for filepath in files:
            if not filepath.is_file():
                continue
            if filepath.suffix in (".pyc", ".pyo", ".db", ".db-shm", ".db-wal"):
                continue
            if filepath.name.startswith("."):
                continue

            path_str = str(filepath)
            if path_str in existing_paths:
                continue

            tags = dict(base_tags)
            _extract_provider_tag(filepath, tags)
            title = _derive_title(filepath)

            doc_id = register_document(
                conn,
                COLLECTION_NAME,
                filepath,
                doc_type,
                title,
                tags=tags or None,
                status="active",
            )
            existing_paths.add(path_str)
            registered.append(
                {
                    "document_id": doc_id,
                    "file_path": path_str,
                    "doc_type": doc_type,
                    "title": title,
                }
            )

    logger.info(
        "Scanned and registered %d new documents in %s",
        len(registered),
        COLLECTION_NAME,
    )
    return registered


def _glob_dir_recursive(directory: Path) -> list[Path]:
    """Return all files under directory, excluding hidden and generated paths."""
    if not directory.is_dir():
        return []
    return sorted(
        f for f in directory.rglob("*")
        if f.is_file() and not any(p.startswith(".") for p in f.parts)
    )


def _extract_provider_tag(filepath: Path, tags: dict) -> None:
    """Tag adapter specs with the provider name derived from their path."""
    parts = filepath.parts
    if "providers" in parts:
        idx = parts.index("providers")
        if idx + 1 < len(parts):
            tags["provider"] = parts[idx + 1].lower().replace("_", "-")
    elif "tooling" in parts:
        idx = parts.index("tooling")
        if idx + 1 < len(parts):
            tags["provider"] = parts[idx + 1].lower().replace("_", "-")


def _derive_title(filepath: Path) -> str:
    name = filepath.stem.replace("-", " ").replace("_", " ")
    words = name.split()
    return " ".join(
        w if w.isupper() and len(w) <= 4 else w.capitalize() for w in words
    )
