"""Graphify skeleton promotion importer."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import sqlite3
from pathlib import Path
from typing import Any

from src.db import get_connection, transaction
from src.project import build_l0_content, generate_l0, invalidate_cache, read_cache
from src.utils import hashing, tokens, ulid

logger = logging.getLogger(__name__)

_NODE_TYPE_MAP: dict[str, str] = {
    "module": "system",
    "service": "system",
    "library": "system",
    "contract": "concept",
    "interface": "concept",
    "schema": "concept",
    "event": "event",
    "org": "org",
    "person": "person",
    "place": "place",
    "code": "system",
    "doc": "concept",
}

_EDGE_TYPE_MAP: dict[str, str] = {
    "imports": "depends_on",
    "imports_from": "depends_on",
    "depends_on": "depends_on",
    "consumes": "depends_on",
    "implements": "part_of",
    "contains": "part_of",
    "part_of": "part_of",
    "extends": "evolved_into",
    "evolved_into": "evolved_into",
    "exposes": "related_to",
    "related_to": "related_to",
    "supports": "supports",
    "contradicts": "contradicts",
    "tested_by": "tested_by",
}
_EDGE_TYPE_DEFAULT = "related_to"
_CONTRACT_LIKE_TYPES = {"contract", "schema", "interface"}
_GRAPHIFY_ORIGIN_PREDICATE = "graphify_origin"
_IMPORT_CONTRACT_VERSION = "graphify-import-v2"
_MUTATION_COUNT_FIELDS = (
    "entities_created",
    "entities_existing",
    "relationships_created",
    "relationships_updated",
    "relationships_removed",
    "claims_created",
    "claims_updated",
    "claims_superseded",
    "skipped_inferred",
    "skipped_ambiguous",
)


def import_graphify(
    conn: sqlite3.Connection,
    graph_json_path: str | Path,
    threshold: str = "p90",
    floor: int = 3,
    ceiling: int = 100,
    pinned_nodes: list[str] | None = None,
    inferred_edge_threshold: float | None = None,
    source_repo: str | None = None,
    cross_repo_authority: list[dict] | None = None,
    annotate_approval_status: bool = False,
    contract_registry_path: str | Path | None = None,
    auto_tune: bool = False,
    l0_token_budget: int = 2000,
    encoding: str = "cl100k_base",
) -> dict:
    """Import promoted graphify nodes and relationships into Astaire."""
    path = Path(graph_json_path)
    if not path.exists():
        raise FileNotFoundError(f"graph.json not found: {path}")

    graph = _load_graph(path, source_repo=source_repo)
    pinned_nodes = pinned_nodes or []
    cross_repo_authority = cross_repo_authority or []
    contract_registry = (
        _load_contract_registry(contract_registry_path)
        if annotate_approval_status and contract_registry_path
        else {}
    )
    cache_key = _build_import_cache_key(
        graph=graph,
        threshold=threshold,
        floor=floor,
        ceiling=ceiling,
        pinned_nodes=pinned_nodes,
        inferred_edge_threshold=inferred_edge_threshold,
        cross_repo_authority=cross_repo_authority,
        annotate_approval_status=annotate_approval_status,
        contract_registry_path=contract_registry_path,
        auto_tune=auto_tune,
        l0_token_budget=l0_token_budget,
        encoding=encoding,
    )
    cached = read_cache(conn, "L1", cache_key)
    if cached is not None:
        source_id = _ensure_graphify_source(conn, path, graph)
        logger.info(
            "Graphify import is a no-op: source_repo=%s graph_version=%s cache_key=%s",
            graph["source_repo"],
            graph["graph_version"],
            cache_key,
        )
        result = _parse_cache_summary(cached, graph, cache_key)
        result["source_id"] = source_id

        invalidate_cache(conn, "global")
        _write_ingest_log(
            conn,
            source_id,
            result,
            _build_verification_summary(result),
        )
        try:
            generate_l0(conn, encoding)
        except Exception:
            logger.warning("L0 regeneration failed after graphify verification", exc_info=True)
        return result

    degree = _compute_degree(graph["nodes"], graph["edges"], inferred_edge_threshold)
    base_total = _compute_target_total(
        node_count=len(graph["nodes"]),
        threshold=threshold,
        floor=floor,
        ceiling=ceiling,
    )
    sorted_node_ids = _sorted_node_ids(graph["nodes"], degree)
    selected_node_ids = _materialize_selection(
        sorted_node_ids=sorted_node_ids,
        valid_node_ids={node["id"] for node in graph["nodes"]},
        pinned_nodes=pinned_nodes,
        target_total=base_total,
    )
    selected_node_ids = _pad_selection_for_structure(
        graph=graph,
        selected_node_ids=selected_node_ids,
        sorted_node_ids=sorted_node_ids,
        inferred_edge_threshold=inferred_edge_threshold,
        ceiling=ceiling,
    )

    if auto_tune:
        selected_node_ids = _auto_tune_selection(
            conn=conn,
            graph=graph,
            sorted_node_ids=sorted_node_ids,
            pinned_nodes=pinned_nodes,
            base_total=base_total,
            inferred_edge_threshold=inferred_edge_threshold,
            cross_repo_authority=cross_repo_authority,
            annotate_approval_status=annotate_approval_status,
            contract_registry=contract_registry,
            ceiling=ceiling,
            l0_token_budget=l0_token_budget,
            encoding=encoding,
        )

    result = _apply_graph_import(
        conn=conn,
        graph=graph,
        selected_node_ids=selected_node_ids,
        inferred_edge_threshold=inferred_edge_threshold,
        cross_repo_authority=cross_repo_authority,
        annotate_approval_status=annotate_approval_status,
        contract_registry=contract_registry,
        encoding=encoding,
    )
    result["graph_schema_version"] = graph["graph_schema_version"]
    result["selected_nodes"] = len(selected_node_ids)
    result["effective_threshold"] = f"absolute:{len(selected_node_ids)}" if auto_tune else threshold
    result["auto_tuned"] = auto_tune
    result["source_repo"] = graph["source_repo"]
    result["cache_key"] = cache_key

    invalidate_cache(conn, "global")
    try:
        generate_l0(conn, encoding)
    except Exception:
        logger.warning("L0 regeneration failed after graphify import", exc_info=True)

    summary = _build_summary(result)
    _write_cache(conn, cache_key, result, encoding)
    _write_ingest_log(conn, result["source_id"], result, summary)

    logger.info(summary)
    return result


def _load_graph(path: Path, source_repo: str | None = None) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    graph_meta = raw.get("graph", {}) if isinstance(raw.get("graph"), dict) else {}

    nodes = [_normalize_node(node) for node in raw.get("nodes", [])]
    edges = [_normalize_edge(edge) for edge in raw.get("edges") or raw.get("links") or []]

    return {
        "graph_version": raw.get("graph_version") or graph_meta.get("graph_version") or hashing.hash_file(path)[:16],
        "graph_schema_version": raw.get("graph_schema_version") or graph_meta.get("graph_schema_version") or "graphify-legacy",
        "source_repo": source_repo or raw.get("source_repo") or graph_meta.get("source_repo") or path.parent.parent.name,
        "nodes": [node for node in nodes if node["id"]],
        "edges": [edge for edge in edges if edge["from"] and edge["to"]],
        "path": path,
    }


def _normalize_node(node: dict) -> dict:
    node_id = str(node.get("id") or "").strip()
    label = (
        node.get("name")
        or node.get("label")
        or node.get("title")
        or node_id
    )
    node_type = (node.get("node_type") or node.get("file_type") or "").lower()
    if not node_type and str(node.get("source_file", "")).endswith(".md"):
        node_type = "doc"
    return {
        "id": node_id,
        "label": str(label),
        "node_type": node_type,
        "source_file": str(node.get("source_file") or ""),
        "source_location": str(node.get("source_location") or ""),
        "approval_status": node.get("approval_status"),
    }


def _normalize_edge(edge: dict) -> dict:
    source = edge.get("source")
    target = edge.get("target")
    if isinstance(source, dict):
        source = source.get("id")
    if isinstance(target, dict):
        target = target.get("id")
    return {
        "from": str(edge.get("from") or edge.get("_src") or source or "").strip(),
        "to": str(edge.get("to") or edge.get("_tgt") or target or "").strip(),
        "relation": str(edge.get("relation") or edge.get("rel_type") or "").strip(),
        "confidence": str(edge.get("confidence") or "EXTRACTED").upper(),
        "confidence_score": _coerce_float(edge.get("confidence_score") or edge.get("score")),
        "weight": _coerce_float(edge.get("weight")) or 1.0,
    }


def _compute_degree(nodes: list[dict], edges: list[dict], inferred_edge_threshold: float | None) -> dict[str, int]:
    degree = {node["id"]: 0 for node in nodes}
    for edge in edges:
        if not _edge_is_admitted(edge, inferred_edge_threshold):
            continue
        degree[edge["from"]] = degree.get(edge["from"], 0) + 1
        degree[edge["to"]] = degree.get(edge["to"], 0) + 1
    return degree


def _compute_target_total(node_count: int, threshold: str, floor: int, ceiling: int) -> int:
    if node_count == 0:
        return 0
    if threshold.startswith("absolute:"):
        total = int(threshold.split(":", 1)[1])
    elif threshold.startswith("p"):
        percentile = int(threshold[1:])
        top_fraction = max(0, min(100, 100 - percentile)) / 100
        total = math.ceil(node_count * top_fraction)
    else:
        raise ValueError(f"Unknown threshold format: {threshold!r}")
    return max(floor, min(ceiling, total))


def _sorted_node_ids(nodes: list[dict], degree: dict[str, int]) -> list[str]:
    return [
        node["id"]
        for node in sorted(
            nodes,
            key=lambda node: (-degree.get(node["id"], 0), node["id"]),
        )
    ]


def _materialize_selection(
    sorted_node_ids: list[str],
    valid_node_ids: set[str],
    pinned_nodes: list[str],
    target_total: int,
) -> set[str]:
    selected = [node_id for node_id in pinned_nodes if node_id in valid_node_ids]
    selected_set = set(selected)
    target_total = max(target_total, len(selected))
    for node_id in sorted_node_ids:
        if len(selected) >= target_total:
            break
        if node_id in selected_set:
            continue
        selected.append(node_id)
        selected_set.add(node_id)
    return selected_set


def _pad_selection_for_structure(
    graph: dict[str, Any],
    selected_node_ids: set[str],
    sorted_node_ids: list[str],
    inferred_edge_threshold: float | None,
    ceiling: int,
) -> set[str]:
    selected = set(selected_node_ids)
    admitted_edges = [edge for edge in graph["edges"] if _edge_is_admitted(edge, inferred_edge_threshold)]
    if _selected_edge_count(selected, admitted_edges) > 0:
        return selected

    rank = {node_id: idx for idx, node_id in enumerate(sorted_node_ids)}
    while len(selected) < ceiling:
        connected_candidates = {
            edge["to"]
            for edge in admitted_edges
            if edge["from"] in selected and edge["to"] not in selected
        }
        connected_candidates.update(
            edge["from"]
            for edge in admitted_edges
            if edge["to"] in selected and edge["from"] not in selected
        )
        if not connected_candidates:
            break

        next_node = min(connected_candidates, key=lambda node_id: (rank.get(node_id, math.inf), node_id))
        selected.add(next_node)
        if _selected_edge_count(selected, admitted_edges) > 0:
            break

    return selected


def _selected_edge_count(selected_node_ids: set[str], edges: list[dict]) -> int:
    return sum(
        1
        for edge in edges
        if edge["from"] in selected_node_ids and edge["to"] in selected_node_ids
    )


def _auto_tune_selection(
    conn: sqlite3.Connection,
    graph: dict,
    sorted_node_ids: list[str],
    pinned_nodes: list[str],
    base_total: int,
    inferred_edge_threshold: float | None,
    cross_repo_authority: list[dict],
    annotate_approval_status: bool,
    contract_registry: dict[str, str],
    ceiling: int,
    l0_token_budget: int,
    encoding: str,
) -> set[str]:
    valid_node_ids = {node["id"] for node in graph["nodes"]}
    min_total = max(len([node_id for node_id in pinned_nodes if node_id in valid_node_ids]), 1)
    for target_total in range(base_total, min_total - 1, -1):
        preview_conn = get_connection(":memory:")
        conn.backup(preview_conn)
        try:
            preview_selected = _materialize_selection(
                sorted_node_ids=sorted_node_ids,
                valid_node_ids=valid_node_ids,
                pinned_nodes=pinned_nodes,
                target_total=target_total,
            )
            preview_selected = _pad_selection_for_structure(
                graph=graph,
                selected_node_ids=preview_selected,
                sorted_node_ids=sorted_node_ids,
                inferred_edge_threshold=inferred_edge_threshold,
                ceiling=ceiling,
            )
            _apply_graph_import(
                conn=preview_conn,
                graph=graph,
                selected_node_ids=preview_selected,
                inferred_edge_threshold=inferred_edge_threshold,
                cross_repo_authority=cross_repo_authority,
                annotate_approval_status=annotate_approval_status,
                contract_registry=contract_registry,
                encoding=encoding,
            )
            l0_tokens = tokens.count_tokens(build_l0_content(preview_conn), encoding)
            if l0_tokens <= l0_token_budget:
                return preview_selected
        finally:
            preview_conn.close()
    selected = _materialize_selection(
        sorted_node_ids=sorted_node_ids,
        valid_node_ids=valid_node_ids,
        pinned_nodes=pinned_nodes,
        target_total=min_total,
    )
    return _pad_selection_for_structure(
        graph=graph,
        selected_node_ids=selected,
        sorted_node_ids=sorted_node_ids,
        inferred_edge_threshold=inferred_edge_threshold,
        ceiling=ceiling,
    )


def _apply_graph_import(
    conn: sqlite3.Connection,
    graph: dict,
    selected_node_ids: set[str],
    inferred_edge_threshold: float | None,
    cross_repo_authority: list[dict],
    annotate_approval_status: bool,
    contract_registry: dict[str, str],
    encoding: str,
) -> dict:
    source_id = _ensure_graphify_source(conn, graph["path"], graph)
    previous_source_ids = _graphify_source_ids_for_repo(conn, graph["source_repo"])

    node_map = {node["id"]: node for node in graph["nodes"]}
    node_id_to_entity_id: dict[str, str] = {}
    entities_created = 0
    entities_existing = 0

    for node_id in sorted(selected_node_ids):
        node = node_map[node_id]
        entity_id, created = _upsert_entity(
            conn,
            node=node,
            source_repo=graph["source_repo"],
            cross_repo_authority=cross_repo_authority,
        )
        node_id_to_entity_id[node_id] = entity_id
        if created:
            entities_created += 1
        else:
            entities_existing += 1

    admitted_edges = []
    skipped_inferred = 0
    skipped_ambiguous = 0
    for edge in graph["edges"]:
        if edge["confidence"] == "AMBIGUOUS":
            skipped_ambiguous += 1
            continue
        if edge["confidence"] == "INFERRED" and not _edge_is_admitted(edge, inferred_edge_threshold):
            skipped_inferred += 1
            continue
        if edge["confidence"] not in {"EXTRACTED", "INFERRED"} and not _edge_is_admitted(edge, inferred_edge_threshold):
            continue
        admitted_edges.append(edge)

    desired_relationships: dict[tuple[str, str, str], dict] = {}
    for edge in admitted_edges:
        from_id = node_id_to_entity_id.get(edge["from"])
        to_id = node_id_to_entity_id.get(edge["to"])
        if from_id is None or to_id is None:
            continue
        signature = (from_id, to_id, _map_edge_type(edge["relation"]))
        desired_relationships[signature] = {
            "weight": edge["weight"],
            "source_id": source_id,
        }

    rel_stats = _reconcile_relationships(conn, desired_relationships, previous_source_ids, source_id)

    desired_claims: dict[tuple[str, str], dict[str, str]] = {}
    for node_id in selected_node_ids:
        node = node_map[node_id]
        desired_claims[(node_id_to_entity_id[node_id], _GRAPHIFY_ORIGIN_PREDICATE)] = {
            "value": _graphify_origin_value(node, graph["source_repo"]),
            "claim_type": "fact",
            "epistemic_tag": "confirmed",
        }

    if annotate_approval_status:
        for node_id in selected_node_ids:
            node = node_map[node_id]
            if node["node_type"] not in _CONTRACT_LIKE_TYPES:
                continue
            status = _approval_status_for_node(node, contract_registry)
            if status:
                desired_claims[(node_id_to_entity_id[node_id], "approval_status")] = {
                    "value": status,
                    "claim_type": "status",
                    "epistemic_tag": "confirmed",
                }

    claim_stats = _reconcile_graphify_claims(conn, desired_claims, previous_source_ids, source_id)

    return {
        "duplicate": False,
        "source_id": source_id,
        "graph_version": graph["graph_version"],
        "entities_created": entities_created,
        "entities_existing": entities_existing,
        "relationships_created": rel_stats["relationships_created"],
        "relationships_updated": rel_stats["relationships_updated"],
        "relationships_removed": rel_stats["relationships_removed"],
        "claims_created": claim_stats["claims_created"],
        "claims_updated": claim_stats["claims_updated"],
        "claims_superseded": claim_stats["claims_superseded"],
        "skipped_inferred": skipped_inferred,
        "skipped_ambiguous": skipped_ambiguous,
    }


def _edge_is_admitted(edge: dict, inferred_edge_threshold: float | None) -> bool:
    if edge["confidence"] == "AMBIGUOUS":
        return False
    if edge["confidence"] == "INFERRED":
        if inferred_edge_threshold is None:
            return False
        return edge["confidence_score"] is not None and edge["confidence_score"] >= inferred_edge_threshold
    return True


def _upsert_entity(
    conn: sqlite3.Connection,
    node: dict,
    source_repo: str,
    cross_repo_authority: list[dict],
) -> tuple[str, bool]:
    canonical = _normalize_name(node["label"] or node["id"])
    node_namespaces = _node_namespaces(node)

    exact_candidates = []
    alias_candidates = []
    for row in conn.execute("SELECT entity_id, canonical_name, aliases_json FROM entity").fetchall():
        row_canonical = _normalize_name(row["canonical_name"])
        if row_canonical == canonical:
            exact_candidates.append(row)
            continue
        try:
            aliases = json.loads(row["aliases_json"] or "[]")
        except json.JSONDecodeError:
            aliases = []
        if any(_normalize_name(alias) == canonical for alias in aliases):
            alias_candidates.append(row)

    for row in exact_candidates:
        repos = _entity_source_repos(conn, row["entity_id"])
        if not repos or source_repo in repos:
            return row["entity_id"], False
        if _can_merge_cross_repo(source_repo, repos, node_namespaces, cross_repo_authority):
            return row["entity_id"], False

    for row in alias_candidates:
        repos = _entity_source_repos(conn, row["entity_id"])
        if not repos or source_repo in repos:
            return row["entity_id"], False

    entity_id = ulid.generate()
    entity_type = _NODE_TYPE_MAP.get(node["node_type"], "concept")
    aliases = sorted({node["id"], node["label"], canonical, f"repo::{source_repo}"} - {""})

    canonical_name = canonical
    if exact_candidates:
        canonical_name = f"{canonical} ({source_repo})"
        aliases = sorted(set(aliases + [canonical]))

    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO entity (entity_id, canonical_name, entity_type, aliases_json)
               VALUES (?, ?, ?, ?)""",
            (entity_id, canonical_name, entity_type, json.dumps(aliases)),
        )
    return entity_id, True


def _entity_source_repos(conn: sqlite3.Connection, entity_id: str) -> set[str]:
    repos = set()
    source_rows = conn.execute(
        """SELECT DISTINCT s.metadata_json
           FROM source s
           LEFT JOIN relationship r ON r.source_id = s.source_id
           LEFT JOIN claim c ON c.source_id = s.source_id
           WHERE (r.from_entity_id = ? OR r.to_entity_id = ? OR c.entity_id = ?)
             AND s.source_type = 'synthesis'""",
        (entity_id, entity_id, entity_id),
    ).fetchall()
    for row in source_rows:
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            metadata = {}
        if metadata.get("importer") == "graphify" and metadata.get("source_repo"):
            repos.add(metadata["source_repo"])
    if repos:
        return repos

    alias_rows = conn.execute(
        "SELECT aliases_json FROM entity WHERE entity_id = ?",
        (entity_id,),
    ).fetchall()
    for row in alias_rows:
        try:
            aliases = json.loads(row["aliases_json"] or "[]")
        except json.JSONDecodeError:
            aliases = []
        for alias in aliases:
            if isinstance(alias, str) and alias.startswith("repo::"):
                repos.add(alias.split("::", 1)[1])
    return repos


def _can_merge_cross_repo(
    source_repo: str,
    existing_repos: set[str],
    namespaces: list[str],
    cross_repo_authority: list[dict],
) -> bool:
    authority = _authority_repo_for_namespaces(namespaces, cross_repo_authority)
    if authority in (None, "*"):
        return True
    if source_repo == authority:
        return True
    return authority in existing_repos


def _authority_repo_for_namespaces(namespaces: list[str], cross_repo_authority: list[dict]) -> str | None:
    for entry in cross_repo_authority:
        repo = str(entry.get("repo") or "")
        for pattern in entry.get("namespaces") or []:
            regex = "^" + re.escape(str(pattern)).replace(r"\*", ".*") + "$"
            if any(re.match(regex, namespace) for namespace in namespaces):
                return repo
    return None


def _node_namespaces(node: dict) -> list[str]:
    namespaces = []
    source_file = node.get("source_file") or ""
    if source_file:
        path = Path(source_file)
        if len(path.parts) >= 2:
            namespaces.append("/".join(path.parts[:2]))
        namespaces.append(str(path))
    if node.get("id"):
        namespaces.append(str(node["id"]))
    return namespaces


def _normalize_name(name: str) -> str:
    return re.sub(r"[\s\-_]+", " ", str(name)).strip().lower()


def _reconcile_relationships(
    conn: sqlite3.Connection,
    desired_relationships: dict[tuple[str, str, str], dict],
    previous_source_ids: list[str],
    source_id: str,
) -> dict:
    existing_rows = {}
    if previous_source_ids:
        placeholders = ",".join("?" for _ in previous_source_ids)
        rows = conn.execute(
            f"""SELECT rel_id, from_entity_id, to_entity_id, rel_type, weight, source_id
                FROM relationship
                WHERE source_id IN ({placeholders})""",
            previous_source_ids,
        ).fetchall()
        existing_rows = {
            (row["from_entity_id"], row["to_entity_id"], row["rel_type"]): row
            for row in rows
        }

    created = 0
    updated = 0
    removed = 0

    for signature, payload in desired_relationships.items():
        row = existing_rows.pop(signature, None)
        if row is None:
            rel_id = ulid.generate()
            with transaction(conn) as cur:
                cur.execute(
                    """INSERT INTO relationship
                       (rel_id, from_entity_id, to_entity_id, rel_type, weight, source_id)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (rel_id, signature[0], signature[1], signature[2], payload["weight"], source_id),
                )
            created += 1
            continue

        if row["weight"] != payload["weight"] or row["source_id"] != source_id:
            with transaction(conn) as cur:
                cur.execute(
                    "UPDATE relationship SET weight = ?, source_id = ? WHERE rel_id = ?",
                    (payload["weight"], source_id, row["rel_id"]),
                )
            updated += 1

    for row in existing_rows.values():
        with transaction(conn) as cur:
            cur.execute("DELETE FROM relationship WHERE rel_id = ?", (row["rel_id"],))
        removed += 1

    return {
        "relationships_created": created,
        "relationships_updated": updated,
        "relationships_removed": removed,
    }


def _reconcile_graphify_claims(
    conn: sqlite3.Connection,
    desired_claims: dict[tuple[str, str], dict[str, str]],
    previous_source_ids: list[str],
    source_id: str,
) -> dict:
    existing_rows = {}
    if previous_source_ids:
        placeholders = ",".join("?" for _ in previous_source_ids)
        rows = conn.execute(
            f"""SELECT claim_id, entity_id, predicate, value, epistemic_tag
                FROM claim
                WHERE source_id IN ({placeholders})
                  AND superseded_by IS NULL""",
            previous_source_ids,
        ).fetchall()
        existing_rows = {(row["entity_id"], row["predicate"]): row for row in rows}

    created = 0
    updated = 0
    superseded = 0

    for signature, payload in desired_claims.items():
        row = existing_rows.pop(signature, None)
        if row is None:
            _create_claim(
                conn,
                signature[0],
                signature[1],
                payload["value"],
                source_id,
                claim_type=payload["claim_type"],
                epistemic_tag=payload["epistemic_tag"],
            )
            created += 1
            continue
        if row["value"] != payload["value"] or row["epistemic_tag"] != payload["epistemic_tag"]:
            new_claim_id = _create_claim(
                conn,
                signature[0],
                signature[1],
                payload["value"],
                source_id,
                claim_type=payload["claim_type"],
                epistemic_tag=payload["epistemic_tag"],
            )
            with transaction(conn) as cur:
                cur.execute(
                    "UPDATE claim SET superseded_by = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE claim_id = ?",
                    (new_claim_id, row["claim_id"]),
                )
            updated += 1
            superseded += 1

    for row in existing_rows.values():
        with transaction(conn) as cur:
            cur.execute(
                "UPDATE claim SET epistemic_tag = 'retracted', updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE claim_id = ?",
                (row["claim_id"],),
            )
        superseded += 1

    return {
        "claims_created": created,
        "claims_updated": updated,
        "claims_superseded": superseded,
    }


def _create_claim(
    conn: sqlite3.Connection,
    entity_id: str,
    predicate: str,
    value: str,
    source_id: str,
    claim_type: str = "fact",
    epistemic_tag: str = "confirmed",
) -> str:
    claim_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO claim
               (claim_id, entity_id, predicate, value, claim_type, confidence, epistemic_tag, source_id)
               VALUES (?, ?, ?, ?, ?, 1.0, ?, ?)""",
            (claim_id, entity_id, predicate, value, claim_type, epistemic_tag, source_id),
        )
    return claim_id


def _graphify_origin_value(node: dict, source_repo: str) -> str:
    parts = [
        f"repo={source_repo}",
        f"node_id={node['id']}",
        f"label={node['label']}",
        f"node_type={node['node_type'] or 'unknown'}",
    ]
    if node.get("source_file"):
        parts.append(f"source_file={node['source_file']}")
    if node.get("source_location"):
        parts.append(f"source_location={node['source_location']}")
    return "; ".join(parts)


def _approval_status_for_node(node: dict, contract_registry: dict[str, str]) -> str | None:
    direct = node.get("approval_status")
    if isinstance(direct, str) and direct:
        return direct

    candidates = [
        node.get("id"),
        node.get("label"),
        _normalize_name(node.get("label") or ""),
    ]
    for candidate in candidates:
        if candidate and candidate in contract_registry:
            return contract_registry[candidate]
    return None


def _load_contract_registry(path: str | Path | None) -> dict[str, str]:
    if not path:
        return {}
    registry_path = Path(path)
    if not registry_path.is_file():
        return {}

    text = registry_path.read_text(encoding="utf-8")
    if registry_path.suffix == ".json":
        data = json.loads(text)
        return _contract_registry_from_json(data)
    return _contract_registry_from_yaml(text.splitlines())


def _contract_registry_from_json(data: Any) -> dict[str, str]:
    if isinstance(data, dict):
        return {
            str(key): str(value)
            for key, value in data.items()
            if isinstance(value, str)
        }
    result = {}
    for entry in data if isinstance(data, list) else []:
        if not isinstance(entry, dict):
            continue
        status = entry.get("approval_status") or entry.get("status")
        if not status:
            continue
        for key in ("id", "name", "label"):
            if entry.get(key):
                result[str(entry[key])] = str(status)
    return result


def _contract_registry_from_yaml(lines: list[str]) -> dict[str, str]:
    result = {}
    current: dict[str, str] = {}
    for raw in lines:
        line = raw.rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if re.match(r"^\s*-\s+", line):
            if current.get("status") and current.get("key"):
                result[current["key"]] = current["status"]
            current = {}
            match = re.match(r"^\s*-\s*(id|name|label):\s*(.+?)\s*$", line)
            if match:
                current["key"] = _strip_quotes(match.group(2))
            continue
        match = re.match(r"^\s*(id|name|label|approval_status|status):\s*(.+?)\s*$", line)
        if not match:
            continue
        key = match.group(1)
        value = _strip_quotes(match.group(2))
        if key in {"id", "name", "label"}:
            current["key"] = value
        else:
            current["status"] = value
    if current.get("status") and current.get("key"):
        result[current["key"]] = current["status"]
    return result


def _strip_quotes(value: str) -> str:
    raw = value.strip()
    if raw.startswith(("'", '"')) and raw.endswith(("'", '"')):
        return raw[1:-1]
    return raw


def _graphify_source_ids_for_repo(
    conn: sqlite3.Connection,
    source_repo: str,
    exclude_source_id: str | None = None,
) -> list[str]:
    source_ids = []
    for row in conn.execute("SELECT source_id, metadata_json FROM source WHERE source_type = 'synthesis'").fetchall():
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            metadata = {}
        if metadata.get("importer") != "graphify":
            continue
        if metadata.get("source_repo") != source_repo:
            continue
        if exclude_source_id and row["source_id"] == exclude_source_id:
            continue
        source_ids.append(row["source_id"])
    return source_ids


def _map_edge_type(graphify_type: str) -> str:
    return _EDGE_TYPE_MAP.get(str(graphify_type).lower(), _EDGE_TYPE_DEFAULT)


def _ensure_graphify_source(conn: sqlite3.Connection, path: Path, graph: dict) -> str:
    content_hash = hashing.hash_file(path)
    existing = conn.execute(
        "SELECT source_id FROM source WHERE content_hash = ?",
        (content_hash,),
    ).fetchone()
    title = f"graphify:{graph['source_repo']}@{graph['graph_version'][:8]}"
    metadata = json.dumps(
        {
            "importer": "graphify",
            "graph_version": graph["graph_version"],
            "graph_schema_version": graph["graph_schema_version"],
            "source_repo": graph["source_repo"],
        }
    )
    if existing:
        with transaction(conn) as cur:
            cur.execute(
                """UPDATE source
                   SET title = ?, file_path = ?, metadata_json = ?
                   WHERE source_id = ?""",
                (title, str(path), metadata, existing["source_id"]),
            )
        return existing["source_id"]

    source_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO source
               (source_id, title, source_type, content_hash, file_path, token_count, metadata_json)
               VALUES (?, ?, 'synthesis', ?, ?, 0, ?)""",
            (source_id, title, content_hash, str(path), metadata),
        )
    return source_id


def _build_import_cache_key(
    graph: dict[str, Any],
    threshold: str,
    floor: int,
    ceiling: int,
    pinned_nodes: list[str],
    inferred_edge_threshold: float | None,
    cross_repo_authority: list[dict],
    annotate_approval_status: bool,
    contract_registry_path: str | Path | None,
    auto_tune: bool,
    l0_token_budget: int,
    encoding: str,
) -> str:
    options = {
        "import_contract_version": _IMPORT_CONTRACT_VERSION,
        "threshold": threshold,
        "floor": floor,
        "ceiling": ceiling,
        "pinned_nodes": pinned_nodes,
        "inferred_edge_threshold": inferred_edge_threshold,
        "cross_repo_authority": cross_repo_authority,
        "annotate_approval_status": annotate_approval_status,
        "contract_registry_path": str(contract_registry_path) if contract_registry_path else None,
        "contract_registry_signature": _registry_signature(contract_registry_path),
        "auto_tune": auto_tune,
        "l0_token_budget": l0_token_budget,
        "encoding": encoding,
    }
    raw = json.dumps(
        {
            "source_repo": graph["source_repo"],
            "graph_version": graph["graph_version"],
            "options": options,
        },
        sort_keys=True,
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"graphify:{graph['source_repo']}:{graph['graph_version']}:{digest}"


def _registry_signature(path: str | Path | None) -> str | None:
    if not path:
        return None
    registry_path = Path(path)
    if not registry_path.is_file():
        return None
    return hashing.hash_file(registry_path)


def _build_summary(result: dict[str, Any]) -> str:
    return (
        f"graphify import: source_repo={result['source_repo']} graph_version={result['graph_version']} "
        f"graph_schema_version={result['graph_schema_version']} | "
        f"selected_nodes={result['selected_nodes']} effective_threshold={result['effective_threshold']} "
        f"auto_tuned={result['auto_tuned']} | "
        f"entities_created={result['entities_created']} entities_existing={result['entities_existing']} | "
        f"relationships_created={result['relationships_created']} relationships_updated={result['relationships_updated']} "
        f"relationships_removed={result['relationships_removed']} | "
        f"claims_created={result['claims_created']} claims_updated={result['claims_updated']} "
        f"claims_superseded={result['claims_superseded']} | "
        f"skipped_inferred={result['skipped_inferred']} skipped_ambiguous={result['skipped_ambiguous']}"
    )


def _build_verification_summary(result: dict[str, Any]) -> str:
    return (
        f"graphify verification: source_repo={result['source_repo']} graph_version={result['graph_version']} "
        f"graph_schema_version={result['graph_schema_version']} | "
        f"selected_nodes={result['selected_nodes']} effective_threshold={result['effective_threshold']} "
        f"cached_verification=true"
    )


def _write_cache(conn: sqlite3.Connection, scope_key: str, result: dict[str, Any], encoding: str) -> None:
    from src.project import _upsert_cache

    _upsert_cache(conn, "L1", scope_key, json.dumps(result, sort_keys=True), encoding)


def _default_cached_result(graph: dict[str, Any], cache_key: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "duplicate": True,
        "graph_version": graph["graph_version"],
        "graph_schema_version": graph["graph_schema_version"],
        "source_repo": graph["source_repo"],
        "cache_key": cache_key,
        "selected_nodes": 0,
        "effective_threshold": "unknown",
        "auto_tuned": False,
    }
    for field in _MUTATION_COUNT_FIELDS:
        result[field] = 0
    return result


def _parse_cache_summary(cached: str, graph: dict[str, Any], cache_key: str) -> dict:
    result = _default_cached_result(graph, cache_key)
    try:
        payload = json.loads(cached)
    except json.JSONDecodeError:
        payload = None

    if isinstance(payload, dict):
        result.update(payload)
    else:
        legacy = dict(result)
        for part in cached.split("|"):
            for key in legacy:
                if key in {"duplicate", "graph_version", "graph_schema_version", "source_repo", "cache_key", "effective_threshold"}:
                    continue
                if f"{key}=" not in part:
                    continue
                raw_value = part.strip().split(f"{key}=", 1)[1].split()[0]
                if key == "auto_tuned":
                    legacy[key] = raw_value.lower() == "true"
                    continue
                try:
                    legacy[key] = int(raw_value)
                except ValueError:
                    legacy[key] = raw_value
        result.update(legacy)

    result["duplicate"] = True
    result["graph_version"] = graph["graph_version"]
    result["graph_schema_version"] = graph["graph_schema_version"]
    result["source_repo"] = graph["source_repo"]
    result["cache_key"] = cache_key
    for field in _MUTATION_COUNT_FIELDS:
        result[field] = 0
    return result


def _write_ingest_log(conn: sqlite3.Connection, source_id: str, result: dict, summary: str) -> None:
    log_id = ulid.generate()
    with transaction(conn) as cur:
        cur.execute(
            """INSERT INTO ingest_log
               (log_id, operation, source_id, summary,
                claims_created, claims_updated, claims_superseded,
                entities_created, relationships_created, contradictions_found,
                documents_registered, documents_updated)
               VALUES (?, 'ingest', ?, ?, ?, ?, ?, ?, ?, 0, 0, 0)""",
            (
                log_id,
                source_id,
                summary,
                result["claims_created"],
                result["claims_updated"],
                result["claims_superseded"],
                result["entities_created"],
                result["relationships_created"],
            ),
        )


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
