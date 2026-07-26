"""Lint — health checks for the knowledge base.

Chunk 5.1: Individual check functions and run_all_checks() aggregator.
Each check returns a list of issue dicts with severity, relevant ID, and message.
Optional fix=True enables safe auto-repairs (L0 regen, L1 cache generation).
Read-only lint must not mutate the knowledge base.
"""

import json
import logging
import time
import sqlite3

from src.db import claims_module_present, transaction
from src.project import build_l0_content, generate_l0, generate_l1_entity, read_cache
from src.utils import hashing, tokens, ulid

logger = logging.getLogger(__name__)

# The public v0.6.0 dogfood workspace generates L0 in roughly ~220ms once
# graphify outputs and release-evidence activity are present. Keep the default
# threshold aligned with that observed release shape so lint highlights real
# regressions instead of a known-good steady state.
DEFAULT_L0_PERFORMANCE_THRESHOLD_MS = 300.0

# Tag-vocabulary drift thresholds (Proposal B item 4). A tag_key is
# considered "common" for a doc_type once it covers more than
# TAG_DRIFT_HIGH_THRESHOLD of that doc_type's instances, and "rare" once it
# covers fewer than TAG_DRIFT_LOW_THRESHOLD. Flagging requires *both*: a
# tag_key that is common for one doc_type but rare for another, within the
# same collection. The gap between the two thresholds (50% vs 10%) is
# deliberately wide so a handful of stray tagged/untagged documents doesn't
# trip the check — it's sized to catch the evidenced case this lint targets
# ("phase= tags chunk-plans only 4/201 times, ~2%, while other doc_types in
# the same collection cluster well above 50%"), not marginal noise.
TAG_DRIFT_HIGH_THRESHOLD = 0.5
TAG_DRIFT_LOW_THRESHOLD = 0.1


def check_orphan_entities(conn: sqlite3.Connection) -> list[dict]:
    """SCN-5.1-01: Detect entities with zero active claims.

    Claims-module check (Proposal A) — returns [] when the module isn't
    installed rather than raising sqlite3.OperationalError on the missing
    `entity`/`claim` tables.
    """
    if not claims_module_present(conn):
        return []
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
    """SCN-5.1-02: Detect claims referencing non-existent entities.

    Claims-module check (Proposal A) — returns [] when the module isn't
    installed. See `check_orphan_entities`.
    """
    if not claims_module_present(conn):
        return []
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
    """SCN-5.1-03: List all open contradictions.

    Claims-module check (Proposal A) — returns [] when the module isn't
    installed. See `check_orphan_entities`.
    """
    if not claims_module_present(conn):
        return []
    rows = conn.execute("SELECT * FROM v_open_contradictions").fetchall()
    return [
        {"severity": "warning", "contradiction_id": r["contradiction_id"],
         "claim_a_id": r["entity_a_name"], "claim_b_id": r["entity_b_name"],
         "message": r["description"] or f"Contradiction between {r['entity_a_name']} claims"}
        for r in rows
    ]


def check_stale_claims(conn: sqlite3.Connection, days: int = 90) -> list[dict]:
    """SCN-5.1-04: Flag provisional claims older than threshold.

    Claims-module check (Proposal A) — returns [] when the module isn't
    installed. See `check_orphan_entities`.
    """
    if not claims_module_present(conn):
        return []
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
    """SCN-5.1-05: Flag high-hub entities without L1 cache. Optionally generate it.

    Claims-module check (Proposal A) — returns [] when the module isn't
    installed. See `check_orphan_entities`.
    """
    if not claims_module_present(conn):
        return []
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
        issue = {"severity": "warning", "message": "L0 cache is missing"}
        if fix:
            generate_l0(conn)
            issue["fixed"] = True
            issue["message"] += " — regenerated"
        return [issue]

    # Build fresh content without writing, then compare data portion
    # (skip the first line which contains a timestamp)
    cached_content = conn.execute(
        "SELECT content_md FROM projection_cache WHERE tier = 'L0' AND scope_key = 'global'"
    ).fetchone()["content_md"]
    fresh_content = build_l0_content(conn)

    def _data_hash(content: str) -> str:
        """Hash content minus fields that record the tool's own execution.

        Strips the first line (timestamp header) and any "Last lint" line.
        `Last lint` is written to `ingest_log` by this very check's caller
        (`run_all_checks`) *after* this comparison runs, so including it in
        the hash would make the cache perpetually one lint-run behind: every
        `lint` (even with --fix) would report freshly-regenerated content as
        stale on the very next invocation. See astaire#28.
        """
        _, _, body = content.partition("\n")
        body = "\n".join(
            line for line in body.splitlines() if not line.startswith("- Last lint:")
        )
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
    """SCN-5.1-07: Flag topic clusters exceeding claim count threshold.

    Claims-module check (Proposal A) — returns [] when the module isn't
    installed. See `check_orphan_entities`.
    """
    if not claims_module_present(conn):
        return []
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


def check_tag_vocabulary_drift(
    conn: sqlite3.Connection,
    high_threshold: float = TAG_DRIFT_HIGH_THRESHOLD,
    low_threshold: float = TAG_DRIFT_LOW_THRESHOLD,
) -> list[dict]:
    """SCN-5.1-13: Flag tag-usage drift across doc_types within a collection.

    Mechanizes a manual audit pattern: collections declare their tag
    vocabulary as a flat `config["tag_keys"]` list (no per-doc_type
    requirement mapping — the core stays collection-agnostic), so nothing
    today catches a tag_key that is heavily used on one doc_type but almost
    never used on another doc_type in the *same* collection, even though
    both doc_types share the same declared vocabulary. That drift is easy
    to miss by hand and easy to compute mechanically.

    For each collection with a non-empty `config["tag_keys"]`, and for each
    of its declared tag_keys, compute the fraction of each doc_type's
    *active* documents (`v_active_documents` — excludes superseded/archived)
    that actually carry that tag_key. A doc_type is only compared against
    other doc_types in the same collection; a collection with fewer than
    two doc_types has nothing to compare and is skipped. Flag every
    (high, low) doc_type pair where the tag_key covers more than
    `high_threshold` of the high doc_type's instances but less than
    `low_threshold` of the low doc_type's instances.
    """
    issues: list[dict] = []
    collections = conn.execute(
        "SELECT collection_id, name, config_json FROM collection"
    ).fetchall()

    for col in collections:
        config = json.loads(col["config_json"] or "{}")
        tag_keys = config.get("tag_keys")
        if not tag_keys:
            continue

        doc_type_rows = conn.execute(
            "SELECT doc_type, COUNT(*) AS n FROM v_active_documents "
            "WHERE collection_name = ? GROUP BY doc_type",
            (col["name"],),
        ).fetchall()
        doc_type_totals = {r["doc_type"]: r["n"] for r in doc_type_rows}
        if len(doc_type_totals) < 2:
            continue  # nothing to compare a single doc_type's usage against

        for tag_key in tag_keys:
            coverage: dict[str, float] = {}
            for doc_type, total in doc_type_totals.items():
                covered = conn.execute(
                    """SELECT COUNT(DISTINCT d.document_id) AS n
                       FROM v_active_documents d
                       JOIN document_tag t ON t.document_id = d.document_id
                       WHERE d.collection_name = ? AND d.doc_type = ? AND t.tag_key = ?""",
                    (col["name"], doc_type, tag_key),
                ).fetchone()["n"]
                coverage[doc_type] = covered / total if total else 0.0

            high_types = [dt for dt, frac in coverage.items() if frac > high_threshold]
            low_types = [dt for dt, frac in coverage.items() if frac < low_threshold]

            for high_dt in high_types:
                for low_dt in low_types:
                    if high_dt == low_dt:
                        continue
                    issues.append({
                        "severity": "warning",
                        "collection": col["name"],
                        "tag_key": tag_key,
                        "message": (
                            f"Tag {tag_key!r} in collection {col['name']!r} covers "
                            f"{coverage[high_dt]:.0%} of {high_dt!r} documents but "
                            f"only {coverage[low_dt]:.0%} of {low_dt!r} documents "
                            f"— possible tag-vocabulary drift."
                        ),
                    })

    return issues


def check_l0_performance(
    conn: sqlite3.Connection,
    threshold_ms: float = DEFAULT_L0_PERFORMANCE_THRESHOLD_MS,
) -> list[dict]:
    """SCN-5.1-10: Time L0 generation logic without mutating cached state."""
    start = time.perf_counter()
    content = build_l0_content(conn)
    tokens.count_tokens(content)
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


def check_claims_module_status(conn: sqlite3.Connection) -> dict:
    """Informational (not pass/fail) report on the optional claims module.

    Returns `{"installed": False}` when the module (Proposal A) hasn't been
    opted into via `astaire init --with-claims`. When installed, also
    reports the total row count across the claim-side tables and the most
    recent write timestamp: the max `updated_at` across `entity`/`claim`,
    or (if no claim-side rows exist yet) the most recent `ingest_log` row
    that actually wrote claims/entities (`entities_created > 0` OR
    `claims_created > 0`). Either may be None if the module is installed
    but has never been written to.
    """
    if not claims_module_present(conn):
        return {"installed": False}

    row_count = 0
    for table in ("entity", "claim", "relationship", "contradiction", "topic_cluster"):
        row_count += conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]

    last_write = conn.execute(
        """SELECT MAX(ts) AS last_write FROM (
               SELECT MAX(updated_at) AS ts FROM entity
               UNION ALL
               SELECT MAX(updated_at) AS ts FROM claim
           )"""
    ).fetchone()["last_write"]

    if last_write is None:
        last_write = conn.execute(
            """SELECT created_at FROM ingest_log
               WHERE entities_created > 0 OR claims_created > 0
               ORDER BY created_at DESC LIMIT 1"""
        ).fetchone()
        last_write = last_write["created_at"] if last_write else None

    return {
        "installed": True,
        "row_count": row_count,
        "last_write": last_write,
    }


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
    results["tag_vocabulary_drift"] = check_tag_vocabulary_drift(conn)
    results["l0_performance"] = check_l0_performance(conn)
    results["claims_module_status"] = check_claims_module_status(conn)

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
