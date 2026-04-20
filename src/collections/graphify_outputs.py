"""graphify-outputs collection — registers graphify graph artifacts with routing hints."""

import hashlib
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from src.governance import derive_source_repo, load_graphify_config
from src.project import generate_l0, invalidate_cache
from src.registry import create_collection, get_collection, register_document
from src.routing import format_route_hint
from src.utils import hashing, tokens

logger = logging.getLogger(__name__)

COLLECTION_NAME = "graphify-outputs"
GRAPHIFY_DIR = "graphify-out"
DEFAULT_ROUTING_HINT = format_route_hint(
    tentacle="graphify.query",
    target="graphify-out/graph.json",
    budget=6000,
    returns="path-traversal",
    reason="policy-graph",
)
REPORT_ROUTING_HINT = format_route_hint(
    tentacle="graphify.report",
    target="graphify-out/GRAPH_REPORT.md",
    budget=2000,
    returns="report-snippet",
    reason="structural-summary",
)

COLLECTION_CONFIG = {
    "doc_types": ["graph-report", "graph-data"],
    "statuses": ["active", "superseded", "archived"],
    "tag_keys": ["source_repo", "graph_version", "graph_schema_version", "run_date", "routing_hint"],
}


def register_collection(conn: sqlite3.Connection) -> str:
    """Create the graphify-outputs collection if it doesn't exist. Returns collection_id."""
    existing = get_collection(conn, COLLECTION_NAME)
    if existing:
        return existing["collection_id"]
    return create_collection(
        conn,
        COLLECTION_NAME,
        "Graphify graph artifacts with routing hints for agent path traversal",
        COLLECTION_CONFIG,
    )


def scan_and_register(
    conn: sqlite3.Connection,
    root_dir: str | Path,
    collection_strategy: str = "split",
    target_collection: str | None = None,
) -> list[dict]:
    """Scan for graphify-out/ artifacts and register them.

    collection_strategy:
      'split'   (default) — dedicated 'graphify-outputs' collection
      'unified' — register into target_collection instead

    Invalidates scope_key='global' on any new registration so the next L0
    generation includes the routing-hint line.

    Returns list of newly registered document dicts.
    """
    root = Path(root_dir)
    graphify_dir = root / GRAPHIFY_DIR
    graphify_config = load_graphify_config(root)

    if not graphify_dir.is_dir():
        logger.debug("No graphify-out/ at %s — skipping", root)
        return []

    collection_strategy = (
        graphify_config.get("collectionStrategy")
        if graphify_config.get("collectionStrategy") in {"split", "unified"}
        else collection_strategy
    )
    if collection_strategy == "unified":
        col_name = target_collection or _default_unified_collection_name(conn, root)
    else:
        col_name = COLLECTION_NAME
        register_collection(conn)

    if collection_strategy == "unified" and target_collection:
        col_name = target_collection

    col = get_collection(conn, col_name)
    if col is None:
        raise ValueError(
            f"Collection {col_name!r} does not exist. "
            "Call register_collection() first or pass a valid target_collection."
        )

    graph_json_path = graphify_dir / "graph.json"
    graph_report_path = graphify_dir / "GRAPH_REPORT.md"

    graph_meta = _extract_graph_metadata(
        graph_json_path=graph_json_path,
        graph_report_path=graph_report_path,
        root=root,
        graphify_config=graphify_config,
    )

    existing_rows = {
        row["file_path"]: row
        for row in conn.execute(
            "SELECT document_id, file_path, content_hash FROM document WHERE collection_id = ?",
            (col["collection_id"],),
        ).fetchall()
    }

    registered = []
    updated = []
    for filepath, doc_type, title in [
        (graph_report_path, "graph-report", "Graph Report"),
        (graph_json_path, "graph-data", "Graph Data"),
    ]:
        if not filepath.is_file():
            continue
        path_str = str(filepath)

        tags = {
            "source_repo": graph_meta["source_repo"],
            "graph_version": graph_meta["graph_version"],
            "graph_schema_version": graph_meta["graph_schema_version"],
            "run_date": graph_meta["run_date"],
            "routing_hint": graph_meta["routing_hint"],
        }

        if path_str in existing_rows:
            changed = _refresh_document(conn, existing_rows[path_str]["document_id"], filepath, tags)
            if changed:
                updated.append({
                    "document_id": existing_rows[path_str]["document_id"],
                    "file_path": path_str,
                    "doc_type": doc_type,
                    "title": title,
                })
            continue

        doc_id = register_document(conn, col_name, filepath, doc_type, title, tags=tags, status="active")
        registered.append({"document_id": doc_id, "file_path": path_str, "doc_type": doc_type, "title": title})

    if registered or updated:
        invalidate_cache(conn, "global")
        try:
            generate_l0(conn)
        except Exception:
            logger.warning(
                "L0 regeneration failed after graphify-outputs scan", exc_info=True
            )

    logger.info(
        "Scanned graphify artifacts in %s: %d registered, %d updated",
        col_name,
        len(registered),
        len(updated),
    )
    return registered


# ── Internal helpers ──────────────────────────────────────────


def _extract_graph_metadata(
    graph_json_path: Path,
    graph_report_path: Path,
    root: Path,
    graphify_config: dict,
) -> dict:
    """Extract source_repo, graph_version, graph_schema_version, run_date, routing hint."""
    source_repo = derive_source_repo(root, graphify_config.get("sourceRepoTag"))
    graph_version = "unknown"
    graph_schema_version = "graphify-legacy"
    run_date = _mtime_utc(graph_json_path)
    routing_hint = DEFAULT_ROUTING_HINT if graph_json_path.is_file() else REPORT_ROUTING_HINT

    if graph_json_path.is_file():
        try:
            data = json.loads(graph_json_path.read_text(encoding="utf-8"))
            if not graphify_config.get("sourceRepoTag"):
                source_repo = (
                    data.get("source_repo")
                    or data.get("graph", {}).get("source_repo")
                    or source_repo
                )
            graph_version = (
                data.get("graph_version")
                or data.get("graph", {}).get("graph_version")
                or _hash_file(graph_json_path)
            )
            graph_schema_version = (
                data.get("graph_schema_version")
                or data.get("graph", {}).get("graph_schema_version")
                or graph_schema_version
            )
            run_date = data.get("run_date") or run_date
        except (json.JSONDecodeError, OSError):
            logger.warning(
                "Could not parse graph.json at %s; using fallbacks", graph_json_path
            )
            graph_version = _hash_file(graph_json_path)
    else:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return {
        "source_repo": source_repo,
        "graph_version": graph_version,
        "graph_schema_version": graph_schema_version,
        "run_date": run_date,
        "routing_hint": routing_hint if graph_json_path.is_file() or graph_report_path.is_file() else "",
    }


def _refresh_document(
    conn: sqlite3.Connection,
    document_id: str,
    filepath: Path,
    tags: dict[str, str],
) -> bool:
    content = filepath.read_text(encoding="utf-8")
    content_hash = hashing.hash_file(filepath)
    token_count = tokens.count_tokens(content)

    row = conn.execute(
        "SELECT content_hash FROM document WHERE document_id = ?",
        (document_id,),
    ).fetchone()
    existing_tags = {
        tag_row["tag_key"]: tag_row["tag_value"]
        for tag_row in conn.execute(
            "SELECT tag_key, tag_value FROM document_tag WHERE document_id = ?",
            (document_id,),
        ).fetchall()
    }
    changed = row is None or row["content_hash"] != content_hash or existing_tags != tags
    if not changed:
        return False

    conn.execute(
        """UPDATE document
           SET content_hash = ?, token_count = ?,
               updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
           WHERE document_id = ?""",
        (content_hash, token_count, document_id),
    )
    conn.execute("DELETE FROM document_tag WHERE document_id = ?", (document_id,))
    for key, value in tags.items():
        conn.execute(
            "INSERT INTO document_tag (document_id, tag_key, tag_value) VALUES (?, ?, ?)",
            (document_id, key, value),
        )
    conn.commit()
    return True


def _hash_file(path: Path) -> str:
    """Return first 16 hex chars of SHA-256 of file content."""
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


def _mtime_utc(path: Path) -> str:
    if not path.exists():
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).strftime("%Y-%m-%d")


def _default_unified_collection_name(conn: sqlite3.Connection, root: Path) -> str:
    preferred = root.name
    if get_collection(conn, preferred) is not None:
        return preferred

    rows = conn.execute(
        "SELECT name FROM collection WHERE name != ? ORDER BY name",
        (COLLECTION_NAME,),
    ).fetchall()
    if len(rows) == 1:
        return rows[0]["name"]

    raise ValueError(
        "Unified graphify collection target is ambiguous. "
        "Pass target_collection explicitly or ensure a root-named collection exists."
    )
