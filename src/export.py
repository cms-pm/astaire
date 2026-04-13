"""Export — wiki generation from database state.

Chunk 5.2: Full-rebuild wiki export producing entity pages, collection indexes,
contradictions, timeline, and master index. All output is markdown suitable for
Obsidian browsing.
"""

import logging
import re
import shutil
import sqlite3
from pathlib import Path

from src.db import transaction
from src.utils import ulid

logger = logging.getLogger(__name__)


def export_wiki(
    conn: sqlite3.Connection, output_dir: str | Path = "wiki/",
) -> dict:
    """SCN-5.2-06: Full rebuild — delete and recreate the wiki directory.

    Returns dict with page counts: entities, collections, total_pages.
    """
    out = Path(output_dir)

    # Clean slate
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)

    # Entity pages
    entities_dir = out / "entities"
    entities_dir.mkdir()
    entity_rows = conn.execute(
        "SELECT entity_id, canonical_name FROM v_entity_hub_scores"
    ).fetchall()
    entity_count = 0
    for row in entity_rows:
        page = export_entity_page(conn, row["entity_id"])
        slug = _slugify(row["canonical_name"])
        (entities_dir / f"{slug}.md").write_text(page, encoding="utf-8")
        entity_count += 1

    # Collection indexes
    collections_dir = out / "collections"
    collections_dir.mkdir()
    col_rows = conn.execute("SELECT name FROM collection").fetchall()
    col_count = 0
    for row in col_rows:
        page = export_collection_index(conn, row["name"])
        col_dir = collections_dir / _slugify(row["name"])
        col_dir.mkdir(exist_ok=True)
        (col_dir / "index.md").write_text(page, encoding="utf-8")
        col_count += 1

    # Static pages
    (out / "contradictions.md").write_text(export_contradictions(conn), encoding="utf-8")
    (out / "timeline.md").write_text(export_timeline(conn), encoding="utf-8")
    (out / "index.md").write_text(export_index(conn), encoding="utf-8")

    total = entity_count + col_count + 3  # 3 static pages
    return {"entities": entity_count, "collections": col_count, "total_pages": total}


def export_entity_page(conn: sqlite3.Connection, entity_id: str) -> str:
    """SCN-5.2-01: Generate a single entity page with YAML frontmatter, claims, relationships."""
    entity = conn.execute(
        "SELECT * FROM v_entity_hub_scores WHERE entity_id = ?", (entity_id,)
    ).fetchone()
    if entity is None:
        return f"# Unknown Entity\n\nEntity `{entity_id}` not found.\n"

    name = entity["canonical_name"]
    etype = entity["entity_type"]
    hub = entity["hub_score"]
    claims_count = entity["claim_count"]

    lines = [
        "---",
        f"entity_type: {etype}",
        f"hub_score: {hub}",
        f"claim_count: {claims_count}",
        "---",
        "",
        f"# {name}",
        "",
    ]

    # Claims grouped by predicate
    claims = conn.execute(
        """SELECT predicate, value, confidence, epistemic_tag, source_id
           FROM claim WHERE entity_id = ? AND superseded_by IS NULL
             AND epistemic_tag != 'retracted'
           ORDER BY predicate, confidence DESC""",
        (entity_id,),
    ).fetchall()

    if claims:
        lines.append("## Claims")
        lines.append("")
        current_pred = None
        for c in claims:
            if c["predicate"] != current_pred:
                current_pred = c["predicate"]
                lines.append(f"### {current_pred}")
                lines.append("")
            conf = f"{c['confidence']:.0%}"
            tag = c["epistemic_tag"]
            lines.append(f"- {c['value']} (confidence: {conf}, status: {tag})")
        lines.append("")

    # Relationships as wikilinks
    rels_out = conn.execute(
        """SELECT r.rel_type, e.canonical_name FROM relationship r
           JOIN entity e ON e.entity_id = r.to_entity_id
           WHERE r.from_entity_id = ?""",
        (entity_id,),
    ).fetchall()
    rels_in = conn.execute(
        """SELECT r.rel_type, e.canonical_name FROM relationship r
           JOIN entity e ON e.entity_id = r.from_entity_id
           WHERE r.to_entity_id = ?""",
        (entity_id,),
    ).fetchall()

    if rels_out or rels_in:
        lines.append("## Relationships")
        lines.append("")
        for r in rels_out:
            lines.append(f"- {r['rel_type']} -> [[{r['canonical_name']}]]")
        for r in rels_in:
            lines.append(f"- [[{r['canonical_name']}]] -> {r['rel_type']}")
        lines.append("")

    if not claims and not rels_out and not rels_in:
        lines.append("_No claims or relationships recorded._")
        lines.append("")

    return "\n".join(lines)


def export_collection_index(conn: sqlite3.Connection, collection_name: str) -> str:
    """SCN-5.2-02: Generate a collection document catalog with type breakdown."""
    col = conn.execute(
        "SELECT * FROM collection WHERE name = ?", (collection_name,)
    ).fetchone()
    if col is None:
        return f"# {collection_name}\n\n_Collection not found._\n"

    docs = conn.execute(
        """SELECT d.document_id, d.title, d.doc_type, d.status, d.token_count,
                  d.external_id, d.created_at
           FROM document d
           WHERE d.collection_id = ? AND d.status NOT IN ('superseded', 'archived')
           ORDER BY d.created_at DESC""",
        (col["collection_id"],),
    ).fetchall()

    lines = [
        f"# {collection_name}",
        "",
    ]

    # Type breakdown
    type_counts: dict[str, int] = {}
    for d in docs:
        type_counts[d["doc_type"]] = type_counts.get(d["doc_type"], 0) + 1

    if type_counts:
        lines.append("## Type Breakdown")
        lines.append("")
        for dtype, count in sorted(type_counts.items()):
            lines.append(f"- **{dtype}**: {count}")
        lines.append("")

    # Document catalog
    lines.append(f"## Documents ({len(docs)})")
    lines.append("")
    if docs:
        lines.append("| Title | Type | Status | Tokens | Created |")
        lines.append("|---|---|---|---|---|")
        for d in docs:
            ext = f" ({d['external_id']})" if d["external_id"] else ""
            lines.append(
                f"| {d['title']}{ext} | {d['doc_type']} | {d['status']} "
                f"| {d['token_count']} | {d['created_at'][:10]} |"
            )
    else:
        lines.append("_No documents registered._")
    lines.append("")

    return "\n".join(lines)


def export_contradictions(conn: sqlite3.Connection) -> str:
    """SCN-5.2-03: Generate the contradictions page listing all open contradictions."""
    rows = conn.execute("SELECT * FROM v_open_contradictions").fetchall()

    lines = [
        "# Contradictions",
        "",
    ]

    if rows:
        lines.append(f"**{len(rows)} open contradiction(s)**")
        lines.append("")
        for r in rows:
            lines.append(f"## {r['entity_a_name']} — {r['predicate_a']}")
            lines.append("")
            lines.append(f"- **Claim A** ([[{r['entity_a_name']}]]): {r['value_a']}")
            lines.append(f"- **Claim B** ([[{r['entity_b_name']}]]): {r['value_b']}")
            if r["description"]:
                lines.append(f"- _{r['description']}_")
            lines.append(f"- Detected: {r['detected_at']}")
            lines.append("")
    else:
        lines.append("_No open contradictions. The knowledge base is consistent._")
        lines.append("")

    return "\n".join(lines)


def export_timeline(conn: sqlite3.Connection) -> str:
    """SCN-5.2-04: Generate timeline from ingest_log in reverse chronological order."""
    rows = conn.execute(
        "SELECT * FROM ingest_log ORDER BY created_at DESC"
    ).fetchall()

    lines = [
        "# Timeline",
        "",
    ]

    if rows:
        for r in rows:
            ts = r["created_at"][:10] if r["created_at"] else "unknown"
            op = r["operation"]
            summary = r["summary"] or f"{op} operation"
            lines.append(f"- **[{ts}]** {op} | {summary}")
        lines.append("")
    else:
        lines.append("_No activity recorded._")
        lines.append("")

    return "\n".join(lines)


def export_index(conn: sqlite3.Connection) -> str:
    """SCN-5.2-05: Generate master index cataloging all pages."""
    entity_rows = conn.execute(
        "SELECT canonical_name, entity_type, hub_score FROM v_entity_hub_scores"
    ).fetchall()
    col_rows = conn.execute("SELECT name FROM collection").fetchall()
    contra_count = conn.execute(
        "SELECT COUNT(*) FROM contradiction WHERE resolution_status = 'open'"
    ).fetchone()[0]

    lines = [
        "# Knowledge Base Index",
        "",
    ]

    # Entity pages
    lines.append(f"## Entities ({len(entity_rows)})")
    lines.append("")
    if entity_rows:
        for r in entity_rows:
            slug = _slugify(r["canonical_name"])
            lines.append(
                f"- [[entities/{slug}|{r['canonical_name']}]] "
                f"({r['entity_type']}, hub: {r['hub_score']})"
            )
    else:
        lines.append("_No entities registered._")
    lines.append("")

    # Collection pages
    lines.append(f"## Collections ({len(col_rows)})")
    lines.append("")
    if col_rows:
        for r in col_rows:
            slug = _slugify(r["name"])
            lines.append(f"- [[collections/{slug}/index|{r['name']}]]")
    else:
        lines.append("_No collections registered._")
    lines.append("")

    # Links to static pages
    lines.append("## Reports")
    lines.append("")
    lines.append(f"- [[contradictions]] ({contra_count} open)")
    lines.append("- [[timeline]]")
    lines.append("")

    return "\n".join(lines)


# ── Helpers ──────────────────────────────────────────────────────

def _slugify(name: str) -> str:
    """Convert a name to a filesystem-safe slug."""
    slug = name.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug or "unnamed"
