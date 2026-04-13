# Collection Authoring Guide

This guide explains how to create a new Astaire collection for your domain.

## What is a Collection?

A collection is a named group of related document types with its own configuration. Examples:
- `ai-dev-governance` — SDLC artifacts (Gherkin, chunk plans, board findings)
- `project-docs` — ADRs, specs, RFCs
- `research-papers` — academic papers, notes, annotations

The core is collection-agnostic — you define what document types, statuses, and tags mean for your domain.

## Creating a Collection Module

Create a Python file in `src/collections/`:

```python
# src/collections/my_project.py

import logging
import sqlite3
from pathlib import Path

from src.registry import create_collection, get_collection, register_document

logger = logging.getLogger(__name__)

COLLECTION_NAME = "my-project"

COLLECTION_CONFIG = {
    "doc_types": ["spec", "adr", "rfc", "design-doc"],
    "statuses": ["draft", "active", "superseded", "archived"],
    "tag_keys": ["component", "phase", "priority"],
    "lifecycle_stages": ["draft", "review", "approved", "implemented"],
}

# Maps path patterns to (doc_type, base_tags)
SCAN_RULES = [
    ("docs/specs/", "spec", {"component": "core"}),
    ("docs/adrs/", "adr", {}),
    ("docs/rfcs/", "rfc", {"component": "api"}),
]


def register_collection(conn: sqlite3.Connection) -> str:
    """Create the collection if it doesn't exist. Returns collection_id."""
    existing = get_collection(conn, COLLECTION_NAME)
    if existing:
        return existing["collection_id"]
    return create_collection(
        conn, COLLECTION_NAME, "Project documentation", COLLECTION_CONFIG
    )


def scan_and_register(
    conn: sqlite3.Connection,
    root_dir: str | Path,
) -> list[dict]:
    """Scan the project directory and register matching files.

    Returns list of {"document_id", "file_path", "doc_type", "title"}
    for newly registered documents.
    """
    root = Path(root_dir)
    registered = []

    col = get_collection(conn, COLLECTION_NAME)
    if col is None:
        raise ValueError(f"Collection {COLLECTION_NAME!r} does not exist")

    # Get existing file paths to skip duplicates
    existing_paths = set()
    rows = conn.execute(
        "SELECT file_path FROM document WHERE collection_id = ?",
        (col["collection_id"],),
    ).fetchall()
    for row in rows:
        existing_paths.add(row["file_path"])

    for rule_pattern, doc_type, base_tags in SCAN_RULES:
        scan_dir = root / rule_pattern
        if not scan_dir.exists():
            continue

        for filepath in sorted(scan_dir.glob("*")):
            if not filepath.is_file():
                continue
            path_str = str(filepath)
            if path_str in existing_paths:
                continue

            tags = dict(base_tags)
            title = filepath.stem.replace("-", " ").replace("_", " ").title()

            doc_id = register_document(
                conn, COLLECTION_NAME, filepath, doc_type, title,
                tags=tags if tags else None,
                status="active",
            )
            existing_paths.add(path_str)
            registered.append({
                "document_id": doc_id,
                "file_path": path_str,
                "doc_type": doc_type,
                "title": title,
            })

    logger.info("Scanned %d new documents in %s", len(registered), COLLECTION_NAME)
    return registered
```

## Required Interface

Your module must expose these four attributes:

| Attribute | Type | Purpose |
|-----------|------|---------|
| `COLLECTION_NAME` | `str` | Unique collection identifier |
| `COLLECTION_CONFIG` | `dict` | Configuration with allowed types, statuses, tags |
| `register_collection(conn)` | callable | Create the collection (idempotent) |
| `scan_and_register(conn, root)` | callable | Scan filesystem, register new docs |

## Auto-Discovery

Collection modules are automatically discovered by `src/collections/discovery.py` using `pkgutil`. Any module in `src/collections/` with the required interface will be picked up on `astaire startup` and `astaire scan`.

No explicit registration is needed — just drop the file in and it works.

## Collection Config

The `COLLECTION_CONFIG` dict controls validation:

```python
{
    "doc_types": ["spec", "adr"],     # Allowed document types
    "statuses": ["draft", "active"],   # Allowed statuses
    "tag_keys": ["component"],         # Informational — allowed tag keys
    "lifecycle_stages": ["draft"],     # Informational — lifecycle stages
}
```

If `doc_types` or `statuses` are defined, `register_document()` validates against them. If omitted, any value is accepted (permissive mode).

## Querying Your Collection

Once registered:

```bash
# List all documents in your collection
uv run astaire query -c my-project

# Filter by type
uv run astaire query -c my-project -t adr

# Filter by tag
uv run astaire query --tag component=core

# Assemble context for LLM consumption
uv run astaire context -c my-project --budget 4000

# Full-text search
uv run astaire query --fts "authentication"
```

## Scan Rules Pattern

The `SCAN_RULES` list maps filesystem paths to document types and tags. Each rule is a tuple: `(path_pattern, doc_type, base_tags)`.

- **path_pattern**: Relative path from project root. If it ends with `/`, all files in that directory are matched.
- **doc_type**: Must match a value in `COLLECTION_CONFIG["doc_types"]`.
- **base_tags**: Dict of tags applied to all files matching this rule.

You can add additional tags dynamically in `scan_and_register()` based on filename patterns (e.g., extracting version numbers or component names from filenames).
