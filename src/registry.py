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

#: Default per-document size gate for opt-in FTS content indexing (bytes,
#: UTF-8 encoded). A collection overrides this via
#: `config["fts_content_max_bytes"]`. See `_content_index_body`.
DEFAULT_FTS_CONTENT_MAX_BYTES = 200_000


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


def _content_index_body(col_config: dict, content: str) -> str | None:
    """Whether/what to index into `document_fts.body` for this collection.

    Opt-in, size-gated content indexing (Proposal B item 1): a collection
    must set `config["fts_content_index"] = true`; documents whose UTF-8
    byte length exceeds `config["fts_content_max_bytes"]` (default
    `DEFAULT_FTS_CONTENT_MAX_BYTES`) are skipped rather than truncated, so a
    partial-content search hit never silently misrepresents what was
    actually indexed. Returns `content` verbatim to index, or `None` to
    leave `body` empty (the default — see the `document_fts` schema
    comment).
    """
    if not col_config.get("fts_content_index"):
        return None
    max_bytes = col_config.get("fts_content_max_bytes", DEFAULT_FTS_CONTENT_MAX_BYTES)
    if len(content.encode("utf-8")) > max_bytes:
        return None
    return content


def _set_fts_body(cur: sqlite3.Cursor, document_id: str, body: str) -> None:
    """Write opt-in indexed content into the FTS row for `document_id`.

    Always a separate, explicit statement — never folded into the
    trigger-driven title/external_id/doc_type population (see the
    `document_fts` schema comment for why: triggers only ever see
    `document`'s own columns, and content lives on disk, not in a column).
    """
    cur.execute(
        "UPDATE document_fts SET body = ? "
        "WHERE rowid = (SELECT rowid FROM document WHERE document_id = ?)",
        (body, document_id),
    )


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
    fts_body = _content_index_body(col["config"], content)

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

        if fts_body is not None:
            _set_fts_body(cur, document_id, fts_body)

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


def _escape_like(value: str) -> str:
    """Escape SQLite LIKE wildcards (`%`, `_`) in a caller-supplied literal.

    Used for tag-value prefix matching (Proposal B item 3): a real tag value
    that happens to contain a literal `%` or `_` must not be interpreted as
    an unintended wildcard once it is turned into a `LIKE 'prefix%'` pattern.
    Pairs with `ESCAPE '\\'` on the SQL side.
    """
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def query_documents(
    conn: sqlite3.Connection,
    collection_name: str | None = None,
    doc_type: str | None = None,
    tags: dict[str, str] | None = None,
    tag_prefixes: dict[str, str] | None = None,
    status: str | None = None,
) -> list[dict]:
    """Query documents with optional filters. Returns list of dicts with tags.

    tags filter: {"stage": "implementation"} matches documents tagged
    stage=implementation exactly. This remains the default, unchanged
    behavior.

    tag_prefixes filter (Proposal B item 3): {"chunk": "7.1"} matches any
    document tagged chunk=<value> where <value> starts with "7.1" — e.g.
    "7.1", "7.1.16", "7.1.2-rc". This exists for hierarchical/dotted
    vocabularies where real deployments tag documents coarser ("chunk=7.1")
    than a caller's query is scoped ("chunk=7.1.16"): an exact-match query
    against the fine-grained value would otherwise return zero rows even
    though the coarser tag is a legitimate match. Implemented as a bound
    `LIKE` parameter (never string-concatenated SQL); any `%` or `_`
    literally present in the supplied prefix is escaped via `_escape_like`
    so it cannot behave as an unintended wildcard. `tags` and
    `tag_prefixes` may be combined (AND semantics across both, same as
    combining `tags` with `doc_type`/`status`); a key present in both is
    treated as two independent conditions.
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

    if tag_prefixes:
        for i, (key, prefix) in enumerate(tag_prefixes.items()):
            alias = f"tp{i}"
            joins.append(
                f"JOIN document_tag {alias} ON {alias}.document_id = d.document_id"
            )
            conditions.append(f"{alias}.tag_key = ? AND {alias}.tag_value LIKE ? ESCAPE '\\'")
            params.extend([key, _escape_like(prefix) + "%"])

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


#: The document_fts columns actually searched by every `--fts`/`search_documents`
#: call. `body` only has content for documents in a collection that opted into
#: content indexing (`config["fts_content_index"]`) and were within the size
#: gate — see `_content_index_body`. Exposed as a constant (rather than only
#: prose) so a caller/CLI can report exactly what was searched, closing the
#: honesty gap at both the mechanism and the documentation layer (Proposal B
#: item 1).
FTS_SEARCHED_FIELDS = ("title", "external_id", "doc_type", "body (opt-in, per-collection)")


def search_documents(conn: sqlite3.Connection, query: str) -> list[dict]:
    """Full-text search across documents using FTS5.

    Searches `title`, `external_id`, `doc_type`, and — for documents in a
    collection that opted into content indexing — `body` (see
    `FTS_SEARCHED_FIELDS`, `_content_index_body`). A collection that has not
    opted in is searched on title/external_id/doc_type only; this was the
    *entire* prior behavior, silently, for every collection — now it is an
    explicit, reportable per-collection choice rather than an undocumented
    universal limitation.
    """
    safe_query = _sanitize_fts_query(query)
    rows = conn.execute(
        """SELECT d.* FROM document d
           JOIN document_fts ON document_fts.rowid = d.rowid
           WHERE document_fts MATCH ?
           ORDER BY rank""",
        (safe_query,),
    ).fetchall()
    return [_enrich_document(conn, dict(r)) for r in rows]


def _or_relax(safe_query: str) -> str:
    """Turn implicit-AND bareword tokens into an OR query, for the zero-result retry.

    `_sanitize_fts_query` already strips FTS5-significant punctuation to
    spaces; FTS5 implicitly ANDs bareword tokens, so a multi-word or
    hyphenated query (`"phase 5 rollout"`, sanitized `"AC-C0-4"` -> `"AC C0
    4"`) frequently zeros out even when a document matches most of the
    tokens. Joining with `OR` is strictly broader — used only for the
    zero-result diagnostic retry, never as the primary search semantics
    (an OR-widened *primary* result set would be a silent behavior change).
    """
    parts = safe_query.split()
    return " OR ".join(parts) if parts else safe_query


def diagnose_zero_results(
    conn: sqlite3.Connection,
    *,
    fts_query: str | None = None,
    collection_name: str | None = None,
    limit: int = 10,
) -> dict:
    """Assemble actionable diagnostics after a query/search call returns zero hits.

    A bare `0 document(s) found` is the direct trigger of the fallback
    pattern (agents abandon after 1-2 retries and fall through to
    grep/find/full-file reads — see the evidence in
    `astaire-schema-reform-plan.md` §1b) — this exists to give the caller
    (typically the CLI) something actionable *before* it gives up.

    Returns:
        fields_searched: FTS_SEARCHED_FIELDS, for FTS calls (else omitted).
        or_relaxed_hits: docs found by an OR-relaxed retry of the same FTS
            terms (only when `fts_query` is given) — capped at `limit`.
        nearest_tags: the `limit` most common (tag_key, tag_value, count)
            triples actually present, scoped to `collection_name` if given.
        doc_type_counts: (doc_type, count) pairs actually present, scoped to
            `collection_name` if given, most common first.
    """
    diagnostics: dict = {}

    if fts_query is not None:
        diagnostics["fields_searched"] = FTS_SEARCHED_FIELDS
        relaxed = _or_relax(_sanitize_fts_query(fts_query))
        rows = conn.execute(
            """SELECT d.* FROM document d
               JOIN document_fts ON document_fts.rowid = d.rowid
               WHERE document_fts MATCH ?
               ORDER BY rank LIMIT ?""",
            (relaxed, limit),
        ).fetchall()
        diagnostics["or_relaxed_hits"] = [_enrich_document(conn, dict(r)) for r in rows]

    tag_sql = "SELECT t.tag_key, t.tag_value, COUNT(*) AS n FROM document_tag t"
    type_sql = "SELECT d.doc_type, COUNT(*) AS n FROM document d"
    params: list = []
    if collection_name:
        tag_sql += (
            " JOIN document d ON d.document_id = t.document_id"
            " JOIN collection c ON c.collection_id = d.collection_id"
            " WHERE c.name = ?"
        )
        type_sql += " JOIN collection c ON c.collection_id = d.collection_id WHERE c.name = ?"
        params = [collection_name]
    tag_sql += " GROUP BY t.tag_key, t.tag_value ORDER BY n DESC LIMIT ?"
    type_sql += " GROUP BY d.doc_type ORDER BY n DESC LIMIT ?"

    diagnostics["nearest_tags"] = [
        (r["tag_key"], r["tag_value"], r["n"])
        for r in conn.execute(tag_sql, [*params, limit]).fetchall()
    ]
    diagnostics["doc_type_counts"] = [
        (r["doc_type"], r["n"]) for r in conn.execute(type_sql, [*params, limit]).fetchall()
    ]
    return diagnostics


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
        """SELECT d.file_path, d.content_hash, d.status, d.title, c.config_json
           FROM document d JOIN collection c ON c.collection_id = d.collection_id
           WHERE d.document_id = ?""",
        (document_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Document {document_id!r} not found")

    path = Path(row["file_path"])
    old_hash = row["content_hash"]
    col_config = json.loads(row["config_json"] or "{}")
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
    fts_body = _content_index_body(col_config, content)
    with transaction(conn) as cur:
        cur.execute(
            """UPDATE document
               SET content_hash = ?, token_count = ?,
                   updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
               WHERE document_id = ?""",
            (new_hash, token_count, document_id),
        )
        # content changed: re-index (or clear, if the collection opted out or
        # the file now exceeds the size gate) the opt-in FTS body in step —
        # never leave a stale body indexed against changed content.
        _set_fts_body(cur, document_id, fts_body or "")
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


def reindex_content(
    conn: sqlite3.Connection, collection_name: str | None = None
) -> dict:
    """Force-recompute `document_fts.body` for every eligible document.

    `sync_document`/`sync_all` only re-index `body` as a side effect of a
    detected `content_hash` change — a real gap found while live-testing
    this feature against a populated database: a collection that opts into
    `fts_content_index` (or raises/lowers `fts_content_max_bytes`) *after*
    its documents are already registered has no other path to actually get
    them indexed, since editing `collection.config_json` never changes any
    document's file content or hash. This is that backfill path — it honors
    each document's own collection's *current* config, regardless of
    whether the file has changed since registration.

    Returns `{"indexed": N, "skipped_over_gate": N, "skipped_not_opted_in":
    N, "missing_files": [document_id, ...]}`. A document whose collection is
    not (or no longer) opted in, or whose content now exceeds the size gate,
    has its `body` explicitly cleared (never left stale) and is counted in
    the corresponding `skipped_*` bucket, not `indexed`.
    """
    sql = (
        "SELECT d.document_id, d.file_path, c.config_json "
        "FROM document d JOIN collection c ON c.collection_id = d.collection_id"
    )
    params: list = []
    if collection_name:
        sql += " WHERE c.name = ?"
        params.append(collection_name)
    rows = conn.execute(sql, params).fetchall()

    indexed = 0
    skipped_over_gate = 0
    skipped_not_opted_in = 0
    missing_files: list[str] = []
    with transaction(conn) as cur:
        for row in rows:
            config = json.loads(row["config_json"] or "{}")
            if not config.get("fts_content_index"):
                skipped_not_opted_in += 1
                # A prior opt-in may have left real content indexed; clear
                # it so an opt-out actually takes effect (never leave a
                # stale body indexed against a collection's current config).
                _set_fts_body(cur, row["document_id"], "")
                continue
            path = Path(row["file_path"])
            if not path.exists():
                missing_files.append(row["document_id"])
                continue
            content = path.read_text(encoding="utf-8")
            body = _content_index_body(config, content)
            if body is None:
                skipped_over_gate += 1
                _set_fts_body(cur, row["document_id"], "")
                continue
            _set_fts_body(cur, row["document_id"], body)
            indexed += 1

    logger.info(
        "Reindexed content for %d document(s)%s",
        indexed,
        f" in collection {collection_name!r}" if collection_name else "",
    )
    return {
        "indexed": indexed,
        "skipped_over_gate": skipped_over_gate,
        "skipped_not_opted_in": skipped_not_opted_in,
        "missing_files": missing_files,
    }


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
