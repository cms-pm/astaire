"""Document registry — collection management, document registration, query, sync, and context assembly.

This is the core value-add module. It is entirely generic — no application-specific logic.
Collections define their own document types, statuses, and tag vocabularies via config_json.
"""

import json
import logging
import sqlite3
from pathlib import Path

from src.db import transaction
from src.utils import hashing, tokens, ulid

logger = logging.getLogger(__name__)


# ── Collection management ──────────────────────────────────────


def create_collection(
    conn: sqlite3.Connection,
    name: str,
    description: str | None = None,
    config: dict | None = None,
) -> str:
    """Create a new collection. Returns the collection_id."""
    collection_id = ulid.generate()
    config_json = json.dumps(config or {})
    with transaction(conn) as cur:
        cur.execute(
            "INSERT INTO collection (collection_id, name, description, config_json) VALUES (?, ?, ?, ?)",
            (collection_id, name, description, config_json),
        )
    logger.info("Created collection %r (%s)", name, collection_id)
    return collection_id


def get_collection(conn: sqlite3.Connection, name: str) -> dict | None:
    """Look up a collection by name. Returns dict or None."""
    row = conn.execute(
        "SELECT * FROM collection WHERE name = ?", (name,)
    ).fetchone()
    if row is None:
        return None
    d = dict(row)
    d["config"] = json.loads(d.pop("config_json", "{}"))
    return d


# ── Validation and normalization ────────────────────────────────


def validate_document(
    config: dict,
    doc_type: str | None = None,
    status: str | None = None,
) -> None:
    """Validate doc_type and status against collection config.

    Only validates fields that have constraints defined in config.
    If config has no "doc_types" or "statuses" key, validation is skipped
    for that field — the core is permissive by default.

    Raises ValueError if a field violates its constraint.
    """
    if doc_type is not None and "doc_types" in config:
        allowed = [t.lower() for t in config["doc_types"]]
        if doc_type not in allowed:
            raise ValueError(
                f"doc_type {doc_type!r} not allowed in this collection. "
                f"Allowed: {allowed}"
            )

    if status is not None and "statuses" in config:
        allowed = [s.lower() for s in config["statuses"]]
        if status not in allowed:
            raise ValueError(
                f"status {status!r} not allowed in this collection. "
                f"Allowed: {allowed}"
            )


def _normalize_tags(
    tags: dict[str, str | list[str]],
) -> dict[str, str | list[str]]:
    """Normalize tag keys and values to lowercase."""
    result: dict[str, str | list[str]] = {}
    for key, values in tags.items():
        nkey = key.lower()
        if nkey == "routing_hint":
            result[nkey] = values
            continue
        if isinstance(values, str):
            result[nkey] = values.lower()
        else:
            result[nkey] = [v.lower() for v in values]
    return result


# ── Document registration ─────────────────────────────────────


def register_document(
    conn: sqlite3.Connection,
    collection_name: str,
    file_path: str | Path,
    doc_type: str,
    title: str,
    tags: dict[str, str | list[str]] | None = None,
    external_id: str | None = None,
    metadata: dict | None = None,
    status: str = "draft",
    encoding: str = "cl100k_base",
) -> str:
    """Register a document in a collection. Returns the document_id.

    file_path must point to an existing file. Its content is hashed and
    token-counted but not stored in the database.

    tags is a dict mapping tag_key to a single value or list of values:
        {"stage": "implementation", "chunk": ["1.2", "1.3"]}
    """
    col = get_collection(conn, collection_name)
    if col is None:
        raise ValueError(f"Collection {collection_name!r} does not exist")

    # Normalize free-text fields to lowercase (ACT-011)
    doc_type = doc_type.lower()
    status = status.lower()
    if tags:
        tags = _normalize_tags(tags)

    # Validate against collection config if constraints exist (ACT-010)
    validate_document(col["config"], doc_type=doc_type, status=status)

    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Document file not found: {path}")

    content_hash = hashing.hash_file(path)
    content = path.read_text(encoding="utf-8")
    token_count = tokens.count_tokens(content, encoding)
    document_id = ulid.generate()
    metadata_json = json.dumps(metadata or {})

    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO document
               (document_id, collection_id, external_id, doc_type, title, status,
                file_path, content_hash, token_count, metadata_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                document_id,
                col["collection_id"],
                external_id,
                doc_type,
                title,
                status,
                str(path),
                content_hash,
                token_count,
                metadata_json,
            ),
        )

        if tags:
            _insert_tags(cur, document_id, tags)

    logger.info("Registered document %r (%s) in %r", title, document_id, collection_name)
    return document_id


def _insert_tags(
    cur: sqlite3.Cursor,
    document_id: str,
    tags: dict[str, str | list[str]],
) -> None:
    """Insert tag rows for a document."""
    for key, values in tags.items():
        if isinstance(values, str):
            values = [values]
        for val in values:
            cur.execute(
                "INSERT OR IGNORE INTO document_tag (document_id, tag_key, tag_value) VALUES (?, ?, ?)",
                (document_id, key, val),
            )


def register_dependency(
    conn: sqlite3.Connection,
    from_document_id: str,
    to_document_id: str,
    dep_type: str,
) -> str:
    """Register a typed dependency between two documents. Returns the dep_id."""
    dep_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO document_dependency (dep_id, from_document_id, to_document_id, dep_type)
               VALUES (?, ?, ?, ?)""",
            (dep_id, from_document_id, to_document_id, dep_type),
        )
    return dep_id


# ── Query ──────────────────────────────────────────────────────


def get_document(conn: sqlite3.Connection, document_id: str) -> dict | None:
    """Look up a document by ID. Returns dict with tags included, or None."""
    row = conn.execute(
        "SELECT * FROM document WHERE document_id = ?", (document_id,)
    ).fetchone()
    if row is None:
        return None
    return _enrich_document(conn, dict(row))


def get_by_external_id(
    conn: sqlite3.Connection, collection_name: str, external_id: str
) -> dict | None:
    """Look up a document by its external_id within a collection."""
    col = get_collection(conn, collection_name)
    if col is None:
        return None
    row = conn.execute(
        "SELECT * FROM document WHERE collection_id = ? AND external_id = ?",
        (col["collection_id"], external_id),
    ).fetchone()
    if row is None:
        return None
    return _enrich_document(conn, dict(row))


def query_documents(
    conn: sqlite3.Connection,
    collection_name: str | None = None,
    doc_type: str | None = None,
    tags: dict[str, str] | None = None,
    status: str | None = None,
) -> list[dict]:
    """Query documents with optional filters. Returns list of dicts with tags.

    tags filter: {"stage": "implementation"} matches documents tagged stage=implementation.
    """
    sql = "SELECT d.* FROM document d"
    joins = []
    conditions = []
    params: list = []

    if collection_name:
        joins.append("JOIN collection c ON c.collection_id = d.collection_id")
        conditions.append("c.name = ?")
        params.append(collection_name)

    if doc_type:
        conditions.append("d.doc_type = ?")
        params.append(doc_type)

    if status:
        conditions.append("d.status = ?")
        params.append(status)

    if tags:
        for i, (key, value) in enumerate(tags.items()):
            alias = f"t{i}"
            joins.append(
                f"JOIN document_tag {alias} ON {alias}.document_id = d.document_id"
            )
            conditions.append(f"{alias}.tag_key = ? AND {alias}.tag_value = ?")
            params.extend([key, value])

    sql += " " + " ".join(joins)
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY d.created_at DESC"

    rows = conn.execute(sql, params).fetchall()
    return [_enrich_document(conn, dict(r)) for r in rows]


def _sanitize_fts_query(query: str) -> str:
    """Normalise a user-supplied FTS5 query.

    FTS5 treats many punctuation characters as syntax: hyphens become
    column-qualifier prefixes, periods/colons/asterisks/carets trigger
    phrase or column operators, and unmatched parens/quotes cause parse
    errors.  Replace every non-alphanumeric, non-whitespace character
    with a space so callers can pass raw identifiers like 'SCN-3.2'
    without triggering FTS5 syntax errors.
    """
    import re
    return re.sub(r"[^\w\s]", " ", query)


def search_documents(conn: sqlite3.Connection, query: str) -> list[dict]:
    """Full-text search across documents using FTS5. Returns matching documents."""
    safe_query = _sanitize_fts_query(query)
    rows = conn.execute(
        """SELECT d.* FROM document d
           JOIN document_fts ON document_fts.rowid = d.rowid
           WHERE document_fts MATCH ?
           ORDER BY rank""",
        (safe_query,),
    ).fetchall()
    return [_enrich_document(conn, dict(r)) for r in rows]


def _enrich_document(conn: sqlite3.Connection, doc: dict) -> dict:
    """Add tags and parsed metadata to a document dict."""
    tag_rows = conn.execute(
        "SELECT tag_key, tag_value FROM document_tag WHERE document_id = ?",
        (doc["document_id"],),
    ).fetchall()

    tags: dict[str, list[str]] = {}
    for row in tag_rows:
        tags.setdefault(row["tag_key"], []).append(row["tag_value"])
    doc["tags"] = tags

    if "metadata_json" in doc:
        doc["metadata"] = json.loads(doc.pop("metadata_json", "{}"))

    return doc


# ── Change detection (sync) ────────────────────────────────────


def sync_document(
    conn: sqlite3.Connection, document_id: str
) -> dict:
    """Check a single document for changes on disk.

    Returns {"changed": bool, "old_hash": str, "new_hash": str, "missing": bool,
             "title": str, "file_path": str}.
    If the file is missing, marks status='archived'.
    """
    row = conn.execute(
        "SELECT file_path, content_hash, status, title FROM document WHERE document_id = ?",
        (document_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Document {document_id!r} not found")

    path = Path(row["file_path"])
    old_hash = row["content_hash"]
    base = {"title": row["title"], "file_path": str(row["file_path"])}

    if not path.exists():
        if row["status"] != "archived":
            with transaction(conn) as cur:
                cur.execute(
                    "UPDATE document SET status = 'archived', updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE document_id = ?",
                    (document_id,),
                )
            logger.warning("Document file missing, archived: %s", path)
        return {**base, "changed": True, "old_hash": old_hash, "new_hash": None, "missing": True}

    new_hash = hashing.hash_file(path)
    if new_hash == old_hash:
        return {**base, "changed": False, "old_hash": old_hash, "new_hash": new_hash, "missing": False}

    content = path.read_text(encoding="utf-8")
    token_count = tokens.count_tokens(content)
    with transaction(conn) as cur:
        cur.execute(
            """UPDATE document
               SET content_hash = ?, token_count = ?,
                   updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
               WHERE document_id = ?""",
            (new_hash, token_count, document_id),
        )
    logger.info("Document updated (hash changed): %s", document_id)
    return {**base, "changed": True, "old_hash": old_hash, "new_hash": new_hash, "missing": False}


def sync_collection(
    conn: sqlite3.Connection, collection_name: str
) -> list[dict]:
    """Sync all documents in a collection. Returns list of change dicts."""
    col = get_collection(conn, collection_name)
    if col is None:
        raise ValueError(f"Collection {collection_name!r} does not exist")

    rows = conn.execute(
        "SELECT document_id FROM document WHERE collection_id = ?",
        (col["collection_id"],),
    ).fetchall()

    changes = []
    for row in rows:
        result = sync_document(conn, row["document_id"])
        if result["changed"]:
            result["document_id"] = row["document_id"]
            changes.append(result)
    return changes


def sync_all(conn: sqlite3.Connection) -> list[dict]:
    """Sync all documents across all collections. Returns list of change dicts."""
    rows = conn.execute("SELECT document_id FROM document").fetchall()
    changes = []
    for row in rows:
        result = sync_document(conn, row["document_id"])
        if result["changed"]:
            result["document_id"] = row["document_id"]
            changes.append(result)
    return changes


# ── Context assembly ───────────────────────────────────────────


def assemble_context(
    conn: sqlite3.Connection,
    collection_name: str | None = None,
    doc_type: str | None = None,
    tags: dict[str, str] | None = None,
    token_budget: int = 8000,
    encoding: str = "cl100k_base",
) -> str:
    """Assemble matching documents into a markdown context block within a token budget.

    Documents are prioritized: smallest first (to fit more), then most recent.
    """
    docs = query_documents(conn, collection_name=collection_name, doc_type=doc_type, tags=tags)
    return _assemble_from_docs(docs, token_budget, encoding)


def assemble_tagged_context(
    conn: sqlite3.Connection,
    tag_key: str,
    tag_value: str,
    token_budget: int = 12000,
    encoding: str = "cl100k_base",
) -> str:
    """Assemble all documents matching a specific tag into a context block.

    Convenience for "everything tagged chunk=1.2" across all collections.
    """
    docs = query_documents(conn, tags={tag_key: tag_value})
    return _assemble_from_docs(docs, token_budget, encoding)


def _assemble_from_docs(
    docs: list[dict],
    token_budget: int,
    encoding: str = "cl100k_base",
) -> str:
    """Read document files and concatenate into markdown, respecting token budget.

    Priority: smallest token_count first (fit more documents), then most recent.
    """
    # Sort: smallest first, then most recent as tiebreaker
    docs.sort(key=lambda d: (d.get("token_count", 0), d.get("created_at", "")))

    sections: list[str] = []
    used_tokens = 0

    for doc in docs:
        path = Path(doc["file_path"])
        if not path.exists():
            continue

        content = path.read_text(encoding="utf-8")
        doc_tokens = tokens.count_tokens(content, encoding)

        if used_tokens + doc_tokens > token_budget:
            # Try to fit a truncated version
            remaining = token_budget - used_tokens
            if remaining > 50:  # only include if meaningful
                content = tokens.truncate_to_budget(content, remaining - 10, encoding)
                doc_tokens = tokens.count_tokens(content, encoding)
            else:
                break

        header = f"## {doc['title']}"
        if doc.get("external_id"):
            header += f" ({doc['external_id']})"
        header += f"\n_type: {doc['doc_type']} | status: {doc['status']}_\n"

        section = f"{header}\n{content}\n"
        section_tokens = tokens.count_tokens(section, encoding)

        if used_tokens + section_tokens > token_budget:
            break

        sections.append(section)
        used_tokens += section_tokens

    return "\n---\n\n".join(sections) if sections else ""
