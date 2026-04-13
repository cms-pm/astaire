"""Lint — health checks for the knowledge base.

Chunk 5.1: Individual check functions and run_all_checks() aggregator.
Each check returns a list of issue dicts with severity, relevant ID, and message.
Optional fix=True enables safe auto-repairs (L0 regen, L1 cache generation).
"""

import logging
import time
import sqlite3

from src.db import transaction
from src.project import build_l0_content, generate_l0, generate_l1_entity, read_cache
from src.utils import hashing, ulid

logger = logging.getLogger(__name__)


def check_orphan_entities(conn: sqlite3.Connection) -> list[dict]:
    """SCN-5.1-01: Detect entities with zero active claims."""
    rows = conn.execute(
        """SELECT e.entity_id, e.canonical_name FROM entity e
           LEFT JOIN claim c ON c.entity_id = e.entity_id
             AND c.superseded_by IS NULL AND c.epistemic_tag != 'retracted'
           GROUP BY e.entity_id
           HAVING COUNT(c.claim_id) = 0"""
    ).fetchall()
    return [
        {"severity": "warning", "entity_id": r["entity_id"],
         "message": f"Orphan entity with zero claims: {r['canonical_name']!r}"}
        for r in rows
    ]


def check_orphan_claims(conn: sqlite3.Connection) -> list[dict]:
    """SCN-5.1-02: Detect claims referencing non-existent entities."""
    rows = conn.execute(
        """SELECT c.claim_id, c.entity_id FROM claim c
           LEFT JOIN entity e ON e.entity_id = c.entity_id
           WHERE e.entity_id IS NULL"""
    ).fetchall()
    return [
        {"severity": "error", "claim_id": r["claim_id"],
         "message": f"Orphan claim references missing entity_id: {r['entity_id']}"}
        for r in rows
    ]


def check_open_contradictions(conn: sqlite3.Connection) -> list[dict]:
    """SCN-5.1-03: List all open contradictions."""
    rows = conn.execute("SELECT * FROM v_open_contradictions").fetchall()
    return [
        {"severity": "warning", "contradiction_id": r["contradiction_id"],
         "claim_a_id": r["entity_a_name"], "claim_b_id": r["entity_b_name"],
         "message": r["description"] or f"Contradiction between {r['entity_a_name']} claims"}
        for r in rows
    ]


def check_stale_claims(conn: sqlite3.Connection, days: int = 90) -> list[dict]:
    """SCN-5.1-04: Flag provisional claims older than threshold."""
    rows = conn.execute(
        """SELECT claim_id, entity_id, predicate, updated_at FROM claim
           WHERE epistemic_tag = 'provisional'
             AND superseded_by IS NULL
             AND updated_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', ?)""",
        (f"-{days} days",),
    ).fetchall()
    return [
        {"severity": "warning", "claim_id": r["claim_id"],
         "message": f"Stale provisional claim on {r['predicate']!r} "
                    f"(last updated {r['updated_at']})"}
        for r in rows
    ]


def check_hub_score_anomalies(
    conn: sqlite3.Connection, fix: bool = False,
) -> list[dict]:
    """SCN-5.1-05: Flag high-hub entities without L1 cache. Optionally generate it."""
    rows = conn.execute(
        """SELECT entity_id, canonical_name, hub_score FROM v_entity_hub_scores
           WHERE hub_score >= 5"""
    ).fetchall()
    issues = []
    for r in rows:
        scope_key = f"entity:{r['entity_id']}"
        cached = read_cache(conn, "L1", scope_key)
        if cached is None:
            issue = {
                "severity": "warning", "entity_id": r["entity_id"],
                "message": f"Entity {r['canonical_name']!r} has hub_score={r['hub_score']} "
                           f"but no L1 cache",
            }
            if fix:
                generate_l1_entity(conn, r["entity_id"])
                issue["fixed"] = True
                issue["message"] += " — L1 cache generated"
            issues.append(issue)
    return issues


def check_l0_staleness(
    conn: sqlite3.Connection, fix: bool = False,
) -> list[dict]:
    """SCN-5.1-06: Detect stale L0 without unconditional side effects.

    FND-0025: Uses build_l0_content() to generate fresh content without writing,
    then compares its hash against the cached hash. Only writes (via generate_l0)
    if the cache is actually stale or missing, so lint is read-only when L0 is fresh.
    """
    cached = conn.execute(
        "SELECT content_hash FROM projection_cache WHERE tier = 'L0' AND scope_key = 'global'"
    ).fetchone()
    old_hash = cached["content_hash"] if cached else None

    if old_hash is None:
        generate_l0(conn)
        return [{"severity": "warning", "message": "L0 cache was missing — regenerated"}]

    # Build fresh content without writing, then compare data portion
    # (skip the first line which contains a timestamp)
    cached_content = conn.execute(
        "SELECT content_md FROM projection_cache WHERE tier = 'L0' AND scope_key = 'global'"
    ).fetchone()["content_md"]
    fresh_content = build_l0_content(conn)

    def _data_hash(content: str) -> str:
        """Hash everything after the first line (timestamp header)."""
        _, _, body = content.partition("\n")
        return hashing.hash_content(body)

    if _data_hash(cached_content) != _data_hash(fresh_content):
        issue = {"severity": "error", "message": "L0 cache was stale — content hash mismatch"}
        if fix:
            generate_l0(conn)
            issue["fixed"] = True
            issue["message"] += " — regenerated"
        return [issue]

    return []


def check_unbounded_clusters(
    conn: sqlite3.Connection, threshold: int = 200,
) -> list[dict]:
    """SCN-5.1-07: Flag topic clusters exceeding claim count threshold."""
    rows = conn.execute(
        "SELECT cluster_id, label, claim_count FROM topic_cluster WHERE claim_count > ?",
        (threshold,),
    ).fetchall()
    return [
        {"severity": "warning", "cluster_id": r["cluster_id"],
         "message": f"Cluster {r['label']!r} has {r['claim_count']} claims "
                    f"(threshold: {threshold}). Consider splitting."}
        for r in rows
    ]


def check_document_drift(conn: sqlite3.Connection) -> list[dict]:
    """SCN-5.1-08: Detect documents whose file content changed since registration."""
    from pathlib import Path
    rows = conn.execute(
        "SELECT document_id, title, file_path, content_hash FROM document "
        "WHERE status NOT IN ('superseded', 'archived')"
    ).fetchall()
    issues = []
    for r in rows:
        path = Path(r["file_path"])
        if not path.exists():
            continue  # handled by check_missing_documents
        current_hash = hashing.hash_file(path)
        if current_hash != r["content_hash"]:
            issues.append({
                "severity": "warning", "document_id": r["document_id"],
                "message": f"Document {r['title']!r} has drifted "
                           f"(registered hash: {r['content_hash'][:12]}..., "
                           f"current: {current_hash[:12]}...)",
            })
    return issues


def check_missing_documents(conn: sqlite3.Connection) -> list[dict]:
    """SCN-5.1-09: Detect documents whose files no longer exist on disk."""
    from pathlib import Path
    rows = conn.execute(
        "SELECT document_id, title, file_path FROM document "
        "WHERE status NOT IN ('superseded', 'archived')"
    ).fetchall()
    return [
        {"severity": "error", "document_id": r["document_id"],
         "message": f"Document {r['title']!r} file missing: {r['file_path']}"}
        for r in rows if not Path(r["file_path"]).exists()
    ]


def check_l0_performance(
    conn: sqlite3.Connection, threshold_ms: float = 100,
) -> list[dict]:
    """SCN-5.1-10: Time L0 generation and flag if it exceeds threshold."""
    start = time.perf_counter()
    generate_l0(conn)
    elapsed_ms = (time.perf_counter() - start) * 1000

    issue = {
        "severity": "info",
        "message": f"L0 generation took {elapsed_ms:.1f}ms",
        "elapsed_ms": elapsed_ms,
    }
    if elapsed_ms > threshold_ms:
        issue["severity"] = "warning"
        issue["message"] = (
            f"L0 generation took {elapsed_ms:.1f}ms "
            f"(threshold: {threshold_ms}ms)"
        )
    return [issue]


def run_all_checks(
    conn: sqlite3.Connection, fix: bool = False,
) -> dict:
    """SCN-5.1-11, SCN-5.1-12: Aggregate all checks, write ingest_log entry."""
    results: dict = {}
    results["orphan_entities"] = check_orphan_entities(conn)
    results["orphan_claims"] = check_orphan_claims(conn)
    results["open_contradictions"] = check_open_contradictions(conn)
    results["stale_claims"] = check_stale_claims(conn)
    results["hub_score_anomalies"] = check_hub_score_anomalies(conn, fix=fix)
    results["l0_staleness"] = check_l0_staleness(conn, fix=fix)
    results["unbounded_clusters"] = check_unbounded_clusters(conn)
    results["document_drift"] = check_document_drift(conn)
    results["missing_documents"] = check_missing_documents(conn)
    results["l0_performance"] = check_l0_performance(conn)

    total_warnings = 0
    total_errors = 0
    for issues in results.values():
        if isinstance(issues, list):
            for issue in issues:
                if issue.get("severity") == "warning":
                    total_warnings += 1
                elif issue.get("severity") == "error":
                    total_errors += 1

    results["total_warnings"] = total_warnings
    results["total_errors"] = total_errors

    # Write ingest_log
    log_id = ulid.generate()
    summary = f"Lint: {total_warnings} warnings, {total_errors} errors"
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO ingest_log
               (log_id, operation, summary,
                claims_created, claims_updated, claims_superseded,
                entities_created, relationships_created, contradictions_found,
                documents_registered, documents_updated)
               VALUES (?, 'lint', ?, 0, 0, 0, 0, 0, ?, 0, 0)""",
            (log_id, summary, total_warnings + total_errors),
        )

    return results
