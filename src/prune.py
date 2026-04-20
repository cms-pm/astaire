"""Prune — TTL-based claim expiry and orphan cleanup.

Chunk 5.3: Conservative deletion model — only claims with an explicit expires_at
in the past are removed. Orphaned join table rows are cleaned up. Cache is
refreshed post-prune.
"""

import logging
import sqlite3

from src.db import transaction
from src.project import generate_l0, invalidate_cache
from src.utils import ulid

logger = logging.getLogger(__name__)


def prune_expired_claims(
    conn: sqlite3.Connection,
    encoding: str = "cl100k_base",
) -> dict:
    """Remove expired claims and clean up related data.

    Returns dict: claims_pruned, clusters_cleaned, l0_regenerated.
    """
    # Find expired claims
    expired = conn.execute(
        """SELECT claim_id, entity_id FROM claim
           WHERE expires_at IS NOT NULL
             AND expires_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"""
    ).fetchall()

    if not expired:
        return {"claims_pruned": 0, "clusters_cleaned": 0, "l0_regenerated": False}

    expired_ids = [r["claim_id"] for r in expired]
    affected_entity_ids = {r["entity_id"] for r in expired}

    # Find affected clusters before deletion
    placeholders = ",".join("?" * len(expired_ids))
    affected_clusters = conn.execute(
        f"SELECT DISTINCT cluster_id FROM claim_cluster WHERE claim_id IN ({placeholders})",
        expired_ids,
    ).fetchall()
    affected_cluster_ids = [r["cluster_id"] for r in affected_clusters]

    with transaction(conn) as cur:
        # Delete claim_cluster rows for expired claims first so cluster counts
        # can be recomputed from the surviving assignments.
        clusters_cleaned = cur.execute(
            f"DELETE FROM claim_cluster WHERE claim_id IN ({placeholders})",
            expired_ids,
        ).rowcount

        # The schema maintains claim_fts through ordinary DELETE triggers, so a
        # direct claim delete keeps the index in sync without special handling.
        cur.execute(
            f"DELETE FROM claim WHERE claim_id IN ({placeholders})",
            expired_ids,
        )

        # Update topic_cluster.claim_count for affected clusters
        for cid in affected_cluster_ids:
            cur.execute(
                """UPDATE topic_cluster SET claim_count = (
                       SELECT COUNT(*) FROM claim_cluster WHERE cluster_id = ?
                   ) WHERE cluster_id = ?""",
                (cid, cid),
            )

    # Invalidate L1 caches for affected entities
    for eid in affected_entity_ids:
        invalidate_cache(conn, f"entity:{eid}")

    # Regenerate L0
    generate_l0(conn, encoding)

    # Write ingest_log
    log_id = ulid.generate()
    summary = (
        f"Pruned {len(expired_ids)} expired claim(s), "
        f"cleaned {clusters_cleaned} cluster assignment(s)"
    )
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO ingest_log
               (log_id, operation, summary,
                claims_created, claims_updated, claims_superseded,
                entities_created, relationships_created, contradictions_found,
                documents_registered, documents_updated)
               VALUES (?, 'prune', ?, 0, 0, ?, 0, 0, 0, 0, 0)""",
            (log_id, summary, len(expired_ids)),
        )

    logger.info("Prune: %s", summary)

    return {
        "claims_pruned": len(expired_ids),
        "clusters_cleaned": clusters_cleaned,
        "l0_regenerated": True,
    }
