"""Ingest pipeline — document registration wrapper and source ingestion with claim processing.

Chunk 4.1: ingest_document() and scan_directory() — high-level wrappers around the
generic registry that also handle ingest_log entries, L0 regeneration, and optional
source linkage.

Chunk 4.2: ingest_source() — source ingestion with pre-extracted claim processing,
entity dedup (exact match on canonical_name), and contradiction detection.
"""

import logging
import sqlite3
from pathlib import Path

from src.db import transaction
from src.project import generate_l0, invalidate_cache
from src.registry import get_collection, query_documents, register_document
from src.utils import hashing, tokens, ulid

logger = logging.getLogger(__name__)


# ── Document ingestion (chunk 4.1) ────────────────────────────


def ingest_document(
    conn: sqlite3.Connection,
    collection_name: str,
    file_path: str | Path,
    doc_type: str,
    title: str,
    tags: dict[str, str | list[str]] | None = None,
    external_id: str | None = None,
    metadata: dict | None = None,
    status: str = "draft",
    extract_claims: bool = False,
    regen_l0: bool = True,
    encoding: str = "cl100k_base",
) -> dict:
    """Ingest a document: register + ingest_log + L0 regen + optional source linkage.

    Returns dict with keys: document_id, duplicate, source_id (if extract_claims).

    Trade-off (FND-0013, MTG-0002): When extract_claims=True, document registration
    and source linkage are two separate transactions. register_document() commits its
    own transaction internally, then a second transaction creates the source row and
    links it via document.source_id. If the process fails between these two writes,
    the document will exist without a source_id. This is accepted because:
    (1) register_document() is a standalone API with its own transaction semantics,
    (2) the failure window is milliseconds on a local filesystem, and
    (3) Phase 5 lint can detect orphaned documents missing expected source linkage.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Document file not found: {path}")

    # Dedup by content_hash within collection
    content_hash = hashing.hash_file(path)
    col = get_collection(conn, collection_name)
    if col is None:
        raise ValueError(f"Collection {collection_name!r} does not exist")

    existing = conn.execute(
        "SELECT document_id FROM document WHERE collection_id = ? AND content_hash = ?",
        (col["collection_id"], content_hash),
    ).fetchone()
    if existing:
        return {"document_id": existing["document_id"], "duplicate": True}

    # Register the document
    document_id = register_document(
        conn,
        collection_name,
        path,
        doc_type,
        title,
        tags=tags,
        external_id=external_id,
        metadata=metadata,
        status=status,
        encoding=encoding,
    )

    result: dict = {"document_id": document_id, "duplicate": False}

    # Optional source linkage for claim extraction.
    # FND-0013: This is a second transaction, separate from register_document() above.
    # See docstring for the trade-off rationale.
    if extract_claims:
        content = path.read_text(encoding="utf-8")
        token_count = tokens.count_tokens(content, encoding)
        source_id = ulid.generate()
        with transaction(conn) as cur:
            cur.execute(
                """INSERT INTO source (source_id, title, source_type, content_hash, file_path, token_count)
                   VALUES (?, ?, 'note', ?, ?, ?)""",
                (source_id, title, content_hash, str(path), token_count),
            )
            cur.execute(
                "UPDATE document SET source_id = ? WHERE document_id = ?",
                (source_id, document_id),
            )
        result["source_id"] = source_id

    # Write ingest_log
    _write_ingest_log(conn, "register", documents_registered=1,
                      summary=f"Registered {title!r} ({doc_type}) in {collection_name}")

    # FND-0014 (MTG-0002): Invalidate stale L1 caches for the affected collection
    invalidate_cache(conn, f"collection:{collection_name}")

    # FND-0017 (MTG-0002): L0 failure must not propagate after successful registration.
    if regen_l0:
        try:
            generate_l0(conn, encoding)
        except Exception:
            logger.warning("L0 regeneration failed; will be caught by staleness check", exc_info=True)

    return result


def scan_directory(
    conn: sqlite3.Connection,
    collection_name: str,
    directory: str | Path,
    type_rules: list[tuple[str, str]] | None = None,
    tags: dict[str, str | list[str]] | None = None,
    status: str = "draft",
    encoding: str = "cl100k_base",
) -> dict:
    """Bulk-register files from a directory into a collection.

    type_rules is a list of (glob_pattern, doc_type) tuples, e.g.:
        [("*.feature", "gherkin"), ("*.md", "spec")]
    Files not matching any rule are skipped.

    Returns {"registered": int, "skipped": int, "errors": list[dict], "documents": list[dict]}.
    """
    dirpath = Path(directory)
    if not dirpath.is_dir():
        raise NotADirectoryError(f"Not a directory: {dirpath}")

    col = get_collection(conn, collection_name)
    if col is None:
        raise ValueError(f"Collection {collection_name!r} does not exist")

    # Build set of already-registered content hashes in this collection
    existing_hashes = set()
    rows = conn.execute(
        "SELECT content_hash FROM document WHERE collection_id = ?",
        (col["collection_id"],),
    ).fetchall()
    for row in rows:
        existing_hashes.add(row["content_hash"])

    type_rules = type_rules or [("*", "document")]
    registered = []
    skipped = 0
    errors: list[dict] = []

    for filepath in sorted(dirpath.rglob("*")):
        if not filepath.is_file():
            continue

        # Match against type_rules
        matched_type = None
        for pattern, doc_type in type_rules:
            if filepath.match(pattern):
                matched_type = doc_type
                break
        if matched_type is None:
            skipped += 1
            continue

        # Dedup by content hash
        content_hash = hashing.hash_file(filepath)
        if content_hash in existing_hashes:
            skipped += 1
            continue

        # FND-0015 (MTG-0002): Per-file error handling so a single bad file
        # (binary, unreadable, encoding error) does not abort the entire scan.
        try:
            title = filepath.stem.replace("-", " ").replace("_", " ").title()
            doc_id = register_document(
                conn,
                collection_name,
                filepath,
                matched_type,
                title,
                tags=tags,
                status=status,
                encoding=encoding,
            )
            existing_hashes.add(content_hash)
            registered.append({
                "document_id": doc_id,
                "file_path": str(filepath),
                "doc_type": matched_type,
                "title": title,
            })
        except Exception as exc:
            logger.warning("Skipping %s: %s", filepath, exc)
            errors.append({"file_path": str(filepath), "error": str(exc)})

    # Single L0 regen after bulk scan (not per document)
    if registered:
        _write_ingest_log(
            conn, "register",
            documents_registered=len(registered),
            summary=f"Scanned {directory}: {len(registered)} registered, {skipped} skipped",
        )
        # FND-0014 (MTG-0002): Invalidate stale L1 cache for this collection
        invalidate_cache(conn, f"collection:{collection_name}")
        # FND-0017 (MTG-0002): L0 failure must not propagate after successful scan.
        try:
            generate_l0(conn, encoding)
        except Exception:
            logger.warning("L0 regeneration failed; will be caught by staleness check", exc_info=True)

    return {
        "registered": len(registered),
        "skipped": skipped,
        "errors": errors,
        "documents": registered,
    }


# ── Source ingestion with claim processing (chunk 4.2) ────────


def ingest_source(
    conn: sqlite3.Connection,
    file_path: str | Path,
    source_type: str,
    title: str,
    claims: list[dict] | None = None,
    encoding: str = "cl100k_base",
) -> dict:
    """Ingest a source document with optional pre-extracted claims.

    Claims are dicts: {"entity": str, "predicate": str, "value": str,
                       "claim_type": str, "confidence": float, "source_span": str?}

    Returns dict with keys: source_id, duplicate, entities_created, claims_created,
                            contradictions_found.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")

    # FND-0016 (MTG-0002): Single-read to eliminate TOCTOU race.
    # Read content once, then derive hash and token count from it.
    content = path.read_text(encoding="utf-8")
    content_hash = hashing.hash_content(content)

    # Dedup by content hash
    existing = conn.execute(
        "SELECT source_id FROM source WHERE content_hash = ?", (content_hash,)
    ).fetchone()
    if existing:
        return {
            "source_id": existing["source_id"],
            "duplicate": True,
            "entities_created": 0,
            "claims_created": 0,
            "contradictions_found": 0,
        }

    # Create source row
    token_count = tokens.count_tokens(content, encoding)
    source_id = ulid.generate()

    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO source (source_id, title, source_type, content_hash, file_path, token_count)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (source_id, title, source_type, content_hash, str(path), token_count),
        )

    # Process claims
    entities_created = 0
    claims_created = 0
    contradictions_found = 0
    affected_entity_ids: set[str] = set()

    if claims:
        for claim_data in claims:
            entity_name = claim_data["entity"]
            entity_type = claim_data.get("entity_type", "concept")
            entity_id, created = _find_or_create_entity(conn, entity_name, entity_type)
            if created:
                entities_created += 1
            affected_entity_ids.add(entity_id)

            claim_id = _create_claim(conn, entity_id, claim_data, source_id)
            claims_created += 1

            # Contradiction detection: same entity+predicate, different value
            contradictions = _detect_contradictions(
                conn, entity_id, claim_data["predicate"], claim_data["value"], claim_id
            )
            contradictions_found += contradictions

    # FND-0014 (MTG-0002): Invalidate stale L1 caches for affected entities
    for eid in affected_entity_ids:
        invalidate_cache(conn, f"entity:{eid}")

    # Write ingest_log
    _write_ingest_log(
        conn, "ingest",
        source_id=source_id,
        claims_created=claims_created,
        entities_created=entities_created,
        contradictions_found=contradictions_found,
        summary=f"Ingested {title!r} ({source_type}): {claims_created} claims, {entities_created} entities, {contradictions_found} contradictions",
    )

    # FND-0017 (MTG-0002): L0 failure must not propagate after successful ingest.
    try:
        generate_l0(conn, encoding)
    except Exception:
        logger.warning("L0 regeneration failed; will be caught by staleness check", exc_info=True)

    return {
        "source_id": source_id,
        "duplicate": False,
        "entities_created": entities_created,
        "claims_created": claims_created,
        "contradictions_found": contradictions_found,
    }


# ── Internal helpers ──────────────────────────────────────────


def _find_or_create_entity(
    conn: sqlite3.Connection,
    canonical_name: str,
    entity_type: str = "concept",
) -> tuple[str, bool]:
    """Find entity by canonical_name or create a new one. Returns (entity_id, created).

    Uses INSERT OR IGNORE + SELECT to be atomic within a single transaction.
    FND-0012 (MTG-0002): eliminates check-then-act race condition.

    Note: This relies on the single-writer concurrency model documented in
    CLAUDE.md. Under concurrent writers, the UNIQUE constraint on canonical_name
    still prevents duplicates, but the "created" flag could be inaccurate.
    """
    entity_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            "INSERT OR IGNORE INTO entity (entity_id, canonical_name, entity_type) VALUES (?, ?, ?)",
            (entity_id, canonical_name, entity_type),
        )
        row = cur.execute(
            "SELECT entity_id FROM entity WHERE canonical_name = ?",
            (canonical_name,),
        ).fetchone()
    # If our INSERT was ignored, a different entity_id was returned
    created = row["entity_id"] == entity_id
    return row["entity_id"], created


def _create_claim(
    conn: sqlite3.Connection,
    entity_id: str,
    claim_data: dict,
    source_id: str,
) -> str:
    """Create a claim row. Returns the claim_id."""
    claim_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO claim
               (claim_id, entity_id, predicate, value, claim_type, confidence,
                source_id, source_span)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                claim_id,
                entity_id,
                claim_data["predicate"],
                claim_data["value"],
                claim_data.get("claim_type", "fact"),
                claim_data.get("confidence", 0.5),
                source_id,
                claim_data.get("source_span"),
            ),
        )
    return claim_id


def _detect_contradictions(
    conn: sqlite3.Connection,
    entity_id: str,
    predicate: str,
    value: str,
    new_claim_id: str,
) -> int:
    """Check for existing claims on the same entity+predicate with a different value.

    Creates contradiction rows for each conflict found. Returns count.
    """
    existing = conn.execute(
        """SELECT claim_id, value FROM claim
           WHERE entity_id = ? AND predicate = ? AND claim_id != ?
             AND superseded_by IS NULL AND epistemic_tag != 'retracted'""",
        (entity_id, predicate, new_claim_id),
    ).fetchall()

    count = 0
    for row in existing:
        if row["value"] != value:
            contradiction_id = ulid.generate()
            # FND-0011 (MTG-0002): INSERT contradiction + UPDATE claim tags
            # must be atomic to prevent inconsistent state on crash.
            with transaction(conn) as cur:
                cur.execute(
                    """INSERT INTO contradiction
                       (contradiction_id, claim_a_id, claim_b_id, description)
                       VALUES (?, ?, ?, ?)""",
                    (
                        contradiction_id,
                        row["claim_id"],
                        new_claim_id,
                        f"Conflicting values for {predicate}: {row['value']!r} vs {value!r}",
                    ),
                )
                cur.execute(
                    "UPDATE claim SET epistemic_tag = 'contested' WHERE claim_id IN (?, ?)",
                    (row["claim_id"], new_claim_id),
                )
            count += 1
    return count


def _write_ingest_log(
    conn: sqlite3.Connection,
    operation: str,
    source_id: str | None = None,
    claims_created: int = 0,
    claims_updated: int = 0,
    claims_superseded: int = 0,
    entities_created: int = 0,
    relationships_created: int = 0,
    contradictions_found: int = 0,
    documents_registered: int = 0,
    documents_updated: int = 0,
    summary: str | None = None,
) -> str:
    """Write an ingest_log entry. Returns the log_id."""
    log_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO ingest_log
               (log_id, operation, source_id, summary,
                claims_created, claims_updated, claims_superseded,
                entities_created, relationships_created, contradictions_found,
                documents_registered, documents_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                log_id, operation, source_id, summary,
                claims_created, claims_updated, claims_superseded,
                entities_created, relationships_created, contradictions_found,
                documents_registered, documents_updated,
            ),
        )
    return log_id
