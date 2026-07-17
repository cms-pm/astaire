"""Projection engine — L0/L1/L2 cache generation and context assembly.

The projection cache stores pre-compiled context blocks at three tiers:
  L0 — global summary (~2-4K tokens), regenerated after every write operation
  L1 — per-scope digests (~1-2K each): cluster, entity, collection
  L2 — per-query detail (on demand, may be cached)

The key optimization: retrieval is pure Python/SQLite, zero LLM tokens.
The LLM only sees the final assembled context document.
"""

import logging
import sqlite3
from datetime import datetime, timezone

from src.db import claims_module_present, transaction
from src.utils import hashing, tokens, ulid

logger = logging.getLogger(__name__)

_L1_TOKEN_BUDGET = 2000


# ── L0: Global summary ────────────────────────────────────────


def build_l0_content(conn: sqlite3.Connection) -> str:
    """Build the L0 global summary string from current database state.

    Pure function — does not write to the database. Used by generate_l0()
    and check_l0_staleness() (FND-0025) to compare without side effects.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Module split (Proposal A): the claim/entity/relationship/contradiction
    # subsystem is optional. When it isn't installed, the sections and
    # metrics below that depend on it are omitted entirely (not shown as
    # zero) — a "0" would misleadingly read as "empty knowledge base"
    # rather than "this feature isn't turned on". claims_module_present()
    # is checked once up front so no claim-side table is ever queried
    # against a core-only database.
    claims_installed = claims_module_present(conn)

    # Key metrics
    source_count = conn.execute("SELECT COUNT(*) FROM source").fetchone()[0]
    collection_count = conn.execute("SELECT COUNT(*) FROM collection").fetchone()[0]
    document_count = conn.execute(
        "SELECT COUNT(*) FROM document WHERE status NOT IN ('superseded', 'archived')"
    ).fetchone()[0]

    entity_count = claim_count = rel_count = contradiction_count = 0
    entity_section = topic_section = contra_section = None

    if claims_installed:
        entity_count = conn.execute("SELECT COUNT(*) FROM entity").fetchone()[0]
        claim_count = conn.execute(
            "SELECT COUNT(*) FROM claim WHERE superseded_by IS NULL AND epistemic_tag != 'retracted'"
        ).fetchone()[0]
        rel_count = conn.execute("SELECT COUNT(*) FROM relationship").fetchone()[0]
        contradiction_count = conn.execute(
            "SELECT COUNT(*) FROM contradiction WHERE resolution_status = 'open'"
        ).fetchone()[0]

        # Entity registry (top 30 by hub score)
        entity_lines = []
        hub_rows = conn.execute(
            "SELECT canonical_name, entity_type, claim_count, hub_score FROM v_entity_hub_scores LIMIT 30"
        ).fetchall()
        for row in hub_rows:
            entity_lines.append(
                f"- **{row['canonical_name']}** ({row['entity_type']}): "
                f"{row['claim_count']} claims, hub score {row['hub_score']}"
            )
        entity_section = "\n".join(entity_lines) if entity_lines else "- (none)"

    # Document registry stats per collection
    doc_lines = []
    col_rows = conn.execute(
        """SELECT c.name, COUNT(d.document_id) AS doc_count,
                  COUNT(DISTINCT d.doc_type) AS type_count,
                  MAX(d.updated_at) AS last_updated
           FROM collection c
           LEFT JOIN document d ON d.collection_id = c.collection_id
               AND d.status NOT IN ('superseded', 'archived')
           GROUP BY c.collection_id
           HAVING COUNT(d.document_id) > 0
           ORDER BY doc_count DESC"""
    ).fetchall()
    for row in col_rows:
        doc_lines.append(
            f"- **{row['name']}**: {row['doc_count']} documents, "
            f"{row['type_count']} types — last updated {row['last_updated'] or 'never'}"
        )
    doc_section = "\n".join(doc_lines) if doc_lines else "- (none)"

    if claims_installed:
        # Hot topics (top 5 clusters by recent activity)
        topic_lines = []
        cluster_rows = conn.execute(
            """SELECT label, summary, claim_count, updated_at
               FROM topic_cluster
               ORDER BY updated_at DESC, claim_count DESC
               LIMIT 5"""
        ).fetchall()
        for row in cluster_rows:
            summary = row["summary"] or "no summary"
            topic_lines.append(
                f"- **{row['label']}**: {summary} — {row['claim_count']} claims, last updated {row['updated_at']}"
            )
        topic_section = "\n".join(topic_lines) if topic_lines else "- (none)"

    # Routing hints (from graphify-outputs and any other collection with routing_hint tags)
    routing_rows = conn.execute(
        """SELECT DISTINCT dt.tag_value
           FROM document_tag dt
           JOIN document d ON d.document_id = dt.document_id
           WHERE dt.tag_key = 'routing_hint'
             AND d.status NOT IN ('superseded', 'archived')
           ORDER BY dt.tag_value"""
    ).fetchall()
    routing_section = (
        "\n".join(row["tag_value"] for row in routing_rows)
        if routing_rows
        else ""
    )

    if claims_installed:
        # Open contradictions
        contra_lines = []
        contra_rows = conn.execute(
            """SELECT entity_a_name, entity_b_name, description
               FROM v_open_contradictions
               LIMIT 10"""
        ).fetchall()
        for row in contra_rows:
            desc = row["description"] or "no description"
            contra_lines.append(f"- {row['entity_a_name']} vs {row['entity_b_name']}: {desc}")
        contra_section = "\n".join(contra_lines) if contra_lines else "- (none)"

    # Recent activity (last 5 ingest_log entries). 'query' is excluded
    # alongside the pre-existing 'recompile' exclusion: query-operation
    # logging (Proposal B item 5) fires on every read-only `astaire query`
    # call, unlike every other operation here, which represents a real
    # state change — without this exclusion, a handful of queries push
    # every genuine register/ingest/lint entry out of this 5-row window.
    # The full history (queries included) is still available in the
    # ingest_log table itself and the wiki timeline (src/export.py) — this
    # only trims the short "what just happened" summary.
    activity_lines = []
    log_rows = conn.execute(
        "SELECT operation, summary, created_at FROM ingest_log "
        "WHERE operation NOT IN ('recompile', 'query') ORDER BY created_at DESC LIMIT 5"
    ).fetchall()
    for row in log_rows:
        activity_lines.append(f"- [{row['created_at']}] {row['operation']}: {row['summary'] or 'no summary'}")
    activity_section = "\n".join(activity_lines) if activity_lines else "- (none)"

    # Last ingest/lint timestamps
    last_ingest = conn.execute(
        "SELECT created_at FROM ingest_log WHERE operation = 'ingest' ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    last_lint = conn.execute(
        "SELECT created_at FROM ingest_log WHERE operation = 'lint' ORDER BY created_at DESC LIMIT 1"
    ).fetchone()

    routing_block = f"\n## Routing hints\n{routing_section}\n" if routing_section else ""

    # Claims-module-dependent sections/metric lines are omitted entirely
    # (not zeroed) when the module isn't installed — see the note above
    # claims_installed for rationale.
    entity_block = (
        f"\n## Entity registry ({entity_count} entities)\n{entity_section}\n"
        if claims_installed
        else ""
    )
    topics_block = (
        f"\n## Hot topics\n{topic_section}\n" if claims_installed else ""
    )
    contradictions_block = (
        f"\n## Open contradictions ({contradiction_count})\n{contra_section}\n"
        if claims_installed
        else ""
    )
    # Kept as two separate blocks (rather than one claims_metrics_block
    # including "Open contradictions") so the Key metrics line order
    # matches the documented L0 template: Total sources, [claims metrics],
    # Total documents, Total collections, Open contradictions, Last
    # ingest, Last lint. Folding "Open contradictions" into the earlier
    # block would move it ahead of Total documents/collections, which
    # would flip every existing with-claims DB's L0 content hash on the
    # next generate_l0() call and trip a spurious one-time
    # check_l0_staleness() warning on upgrade.
    claims_metrics_block = (
        f"- Total active claims: {claim_count}\n"
        f"- Total entities: {entity_count}\n"
        f"- Total relationships: {rel_count}\n"
        if claims_installed
        else ""
    )
    contradictions_metric_line = (
        f"- Open contradictions: {contradiction_count}\n" if claims_installed else ""
    )

    content = f"""# Knowledge base state — {now}
{entity_block}
## Document registry ({document_count} documents across {collection_count} collections)
{doc_section}
{routing_block}{topics_block}{contradictions_block}
## Recent activity
{activity_section}

## Key metrics
- Total sources: {source_count}
{claims_metrics_block}- Total documents: {document_count}
- Total collections: {collection_count}
{contradictions_metric_line}- Last ingest: {last_ingest['created_at'] if last_ingest else 'never'}
- Last lint: {last_lint['created_at'] if last_lint else 'never'}
"""

    return content


def generate_l0(conn: sqlite3.Connection, encoding: str = "cl100k_base") -> str:
    """Generate the L0 global summary, write to cache, and log the operation.

    Calls build_l0_content() for the pure generation, then upserts to
    projection_cache and writes an ingest_log audit entry (FND-0018).
    """
    content = build_l0_content(conn)
    _upsert_cache(conn, "L0", "global", content, encoding)

    # FND-0018 (MTG-0002): L0 audit trail in ingest_log
    token_count = tokens.count_tokens(content, encoding)
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO ingest_log
               (log_id, operation, summary,
                claims_created, claims_updated, claims_superseded,
                entities_created, relationships_created, contradictions_found,
                documents_registered, documents_updated)
               VALUES (?, 'recompile', ?, 0, 0, 0, 0, 0, 0, 0, 0)""",
            (ulid.generate(), f"L0 regenerated: {token_count} tokens"),
        )

    logger.info("L0 cache generated")
    return content


def read_l0(conn: sqlite3.Connection) -> str | None:
    """Read the L0 global summary from cache. Returns None if not generated yet."""
    row = conn.execute(
        "SELECT content_md FROM projection_cache WHERE tier = 'L0' AND scope_key = 'global'"
    ).fetchone()
    return row["content_md"] if row else None


# ── Cache management ───────────────────────────────────────────


def invalidate_cache(conn: sqlite3.Connection, scope_key: str) -> None:
    """Delete a cache entry by scope_key, forcing regeneration on next read."""
    with transaction(conn) as cur:
        cur.execute(
            "DELETE FROM projection_cache WHERE scope_key = ?", (scope_key,)
        )


def read_cache(conn: sqlite3.Connection, tier: str, scope_key: str) -> str | None:
    """Read a cache entry. Returns content_md or None."""
    row = conn.execute(
        "SELECT content_md FROM projection_cache WHERE tier = ? AND scope_key = ?",
        (tier, scope_key),
    ).fetchone()
    return row["content_md"] if row else None


# ── L1: Per-scope digests ──────────────────────────────────────


def generate_l1_cluster(conn: sqlite3.Connection, cluster_id: str, encoding: str = "cl100k_base") -> str:
    """Generate an L1 digest for a topic cluster.

    Includes cluster label, summary, and claims sorted by confidence DESC.
    """
    cluster = conn.execute(
        "SELECT label, summary, claim_count FROM topic_cluster WHERE cluster_id = ?",
        (cluster_id,),
    ).fetchone()
    if cluster is None:
        raise ValueError(f"Cluster {cluster_id!r} not found")

    claim_rows = conn.execute(
        """SELECT c.predicate, c.value, c.confidence, c.claim_type,
                  c.epistemic_tag, e.canonical_name AS entity_name
           FROM claim c
           JOIN claim_cluster cc ON cc.claim_id = c.claim_id
           JOIN entity e ON e.entity_id = c.entity_id
           WHERE cc.cluster_id = ?
             AND c.superseded_by IS NULL
             AND c.epistemic_tag != 'retracted'
           ORDER BY c.confidence DESC, c.updated_at DESC""",
        (cluster_id,),
    ).fetchall()

    lines = [
        f"# Cluster: {cluster['label']}",
        f"_{cluster['summary'] or 'No summary'}_",
        f"Claims: {cluster['claim_count']}",
        "",
    ]
    for row in claim_rows:
        lines.append(
            f"- [{row['confidence']:.2f}] **{row['entity_name']}** {row['predicate']}: "
            f"{row['value']} ({row['claim_type']}, {row['epistemic_tag']})"
        )

    content = "\n".join(lines)
    content = tokens.truncate_to_budget(content, _L1_TOKEN_BUDGET, encoding)
    scope_key = f"cluster:{cluster_id}"
    _upsert_cache(conn, "L1", scope_key, content, encoding)
    logger.info("L1 cluster cache generated: %s", cluster["label"])
    return content


def generate_l1_entity(conn: sqlite3.Connection, entity_id: str, encoding: str = "cl100k_base") -> str:
    """Generate an L1 digest for an entity.

    Includes entity type, description, claims, and relationships.
    """
    entity = conn.execute(
        "SELECT canonical_name, entity_type, description FROM entity WHERE entity_id = ?",
        (entity_id,),
    ).fetchone()
    if entity is None:
        raise ValueError(f"Entity {entity_id!r} not found")

    claim_rows = conn.execute(
        """SELECT predicate, value, confidence, claim_type, epistemic_tag
           FROM claim
           WHERE entity_id = ?
             AND superseded_by IS NULL
             AND epistemic_tag != 'retracted'
           ORDER BY confidence DESC, updated_at DESC""",
        (entity_id,),
    ).fetchall()

    rel_rows = conn.execute(
        """SELECT r.rel_type, e2.canonical_name AS target_name, r.weight
           FROM relationship r
           JOIN entity e2 ON e2.entity_id = r.to_entity_id
           WHERE r.from_entity_id = ?
           UNION ALL
           SELECT r.rel_type, e2.canonical_name AS target_name, r.weight
           FROM relationship r
           JOIN entity e2 ON e2.entity_id = r.from_entity_id
           WHERE r.to_entity_id = ?""",
        (entity_id, entity_id),
    ).fetchall()

    lines = [
        f"# Entity: {entity['canonical_name']}",
        f"Type: {entity['entity_type']}",
    ]
    if entity["description"]:
        lines.append(f"_{entity['description']}_")
    lines.append("")

    if claim_rows:
        lines.append("## Claims")
        for row in claim_rows:
            lines.append(
                f"- [{row['confidence']:.2f}] {row['predicate']}: "
                f"{row['value']} ({row['claim_type']}, {row['epistemic_tag']})"
            )
        lines.append("")

    if rel_rows:
        lines.append("## Relationships")
        for row in rel_rows:
            lines.append(f"- {row['rel_type']} → {row['target_name']} (weight: {row['weight']:.2f})")
        lines.append("")

    content = "\n".join(lines)
    content = tokens.truncate_to_budget(content, _L1_TOKEN_BUDGET, encoding)
    scope_key = f"entity:{entity_id}"
    _upsert_cache(conn, "L1", scope_key, content, encoding)
    logger.info("L1 entity cache generated: %s", entity["canonical_name"])
    return content


def generate_l1_collection(conn: sqlite3.Connection, collection_name: str, encoding: str = "cl100k_base") -> str:
    """Generate an L1 digest for a collection.

    Includes document count, type breakdown, and recent documents.
    """
    col = conn.execute(
        "SELECT collection_id, name, description FROM collection WHERE name = ?",
        (collection_name,),
    ).fetchone()
    if col is None:
        raise ValueError(f"Collection {collection_name!r} not found")

    total = conn.execute(
        """SELECT COUNT(*) FROM document
           WHERE collection_id = ? AND status NOT IN ('superseded', 'archived')""",
        (col["collection_id"],),
    ).fetchone()[0]

    type_rows = conn.execute(
        """SELECT doc_type, COUNT(*) AS cnt
           FROM document
           WHERE collection_id = ? AND status NOT IN ('superseded', 'archived')
           GROUP BY doc_type
           ORDER BY cnt DESC""",
        (col["collection_id"],),
    ).fetchall()

    recent_rows = conn.execute(
        """SELECT title, doc_type, external_id, status, updated_at
           FROM document
           WHERE collection_id = ? AND status NOT IN ('superseded', 'archived')
           ORDER BY updated_at DESC
           LIMIT 10""",
        (col["collection_id"],),
    ).fetchall()

    lines = [
        f"# Collection: {col['name']}",
    ]
    if col["description"]:
        lines.append(f"_{col['description']}_")
    lines.append(f"Total active documents: {total}")
    lines.append("")

    if type_rows:
        lines.append("## Document types")
        for row in type_rows:
            lines.append(f"- {row['doc_type']}: {row['cnt']}")
        lines.append("")

    if recent_rows:
        lines.append("## Recent documents")
        for row in recent_rows:
            ext = f" ({row['external_id']})" if row["external_id"] else ""
            lines.append(f"- **{row['title']}**{ext} — {row['doc_type']}, {row['status']}, {row['updated_at']}")
        lines.append("")

    content = "\n".join(lines)
    content = tokens.truncate_to_budget(content, _L1_TOKEN_BUDGET, encoding)
    scope_key = f"collection:{collection_name}"
    _upsert_cache(conn, "L1", scope_key, content, encoding)
    logger.info("L1 collection cache generated: %s", collection_name)
    return content


# ── Unified query context assembly ─────────────────────────────


def assemble_query_context(
    conn: sqlite3.Connection,
    collection_name: str | None = None,
    tags: dict[str, str] | None = None,
    token_budget: int = 8000,
    encoding: str = "cl100k_base",
) -> str:
    """Assemble a unified context block: L0 + relevant L1 + documents.

    Always includes L0 in full. Remaining budget goes to:
    1. L1 cache hits for the relevant scope
    2. Document content assembled from registry (fallback)

    Returns assembled markdown within the token budget.
    """
    # Always include L0
    l0 = read_l0(conn)
    if l0 is None:
        l0 = generate_l0(conn, encoding)

    l0_tokens = tokens.count_tokens(l0, encoding)
    remaining = token_budget - l0_tokens
    sections = [l0]

    if remaining <= 0:
        return l0

    # Try L1 cache for collection scope
    if collection_name:
        scope_key = f"collection:{collection_name}"
        l1 = read_cache(conn, "L1", scope_key)
        if l1 is None:
            # Generate on demand
            try:
                l1 = generate_l1_collection(conn, collection_name, encoding)
            except ValueError:
                l1 = None

        if l1:
            l1_tokens = tokens.count_tokens(l1, encoding)
            if l1_tokens <= remaining:
                sections.append(l1)
                remaining -= l1_tokens

    # Fill remaining budget with document content
    if remaining > 50 and (collection_name or tags):
        from src.registry import assemble_context
        doc_context = assemble_context(
            conn,
            collection_name=collection_name,
            tags=tags,
            token_budget=remaining,
            encoding=encoding,
        )
        if doc_context:
            sections.append(doc_context)

    return "\n\n---\n\n".join(sections)


# ── Internal helpers ───────────────────────────────────────────


def _upsert_cache(
    conn: sqlite3.Connection,
    tier: str,
    scope_key: str,
    content: str,
    encoding: str = "cl100k_base",
) -> None:
    """Insert or update a projection cache entry."""
    content_hash = hashing.hash_content(content)
    token_count = tokens.count_tokens(content, encoding)

    with transaction(conn) as cur:
        cache_id = ulid.generate()
        cur.execute(
            """INSERT INTO projection_cache
               (cache_id, tier, scope_key, content_md, token_count, content_hash, encoding)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tier, scope_key) DO UPDATE SET
                   content_md = excluded.content_md,
                   token_count = excluded.token_count,
                   content_hash = excluded.content_hash,
                   encoding = excluded.encoding,
                   generated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')""",
            (cache_id, tier, scope_key, content, token_count, content_hash, encoding),
        )
