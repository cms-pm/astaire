"""scenario-predicates collection — indexes scn-*.json predicate files.

Issue #17. Each scenario block inside a `docs/validation/**/predicates/*.json`
file becomes one document, keyed by SCN external_id. Predicate hash is the
SHA-256 of the canonical (sorted-keys, no whitespace) scenario JSON; it
crosses to the scenario-ledger collection (issue #16) for evidence audit.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from pathlib import Path

from src.registry import create_collection, get_collection, register_document

logger = logging.getLogger(__name__)

COLLECTION_NAME = "scenario-predicates"

COLLECTION_CONFIG = {
    "doc_types": ["predicate"],
    "tag_keys": [
        "chunk",
        "feature",
        "predicate_kind",
        "haiku_eligible",
        "schema_version",
        "owner_chunk_plan",
        "surface",
    ],
    "statuses": ["active", "superseded", "archived"],
}

PREDICATE_GLOB = "docs/validation/**/predicates/*.json"


def register_collection(conn: sqlite3.Connection) -> str:
    existing = get_collection(conn, COLLECTION_NAME)
    if existing:
        return existing["collection_id"]
    return create_collection(
        conn,
        COLLECTION_NAME,
        "Per-scenario predicate definitions for HiL/SiL routing",
        COLLECTION_CONFIG,
    )


def scan_and_register(
    conn: sqlite3.Connection,
    root_dir: str | Path,
) -> list[dict]:
    root = Path(root_dir)
    col = get_collection(conn, COLLECTION_NAME)
    if col is None:
        raise ValueError(
            f"Collection {COLLECTION_NAME!r} not registered. "
            "Call register_collection() first."
        )

    existing_ext_ids: set[str] = set()
    for row in conn.execute(
        "SELECT external_id FROM document "
        "WHERE collection_id = ? AND external_id IS NOT NULL",
        (col["collection_id"],),
    ).fetchall():
        existing_ext_ids.add(row["external_id"])

    registered: list[dict] = []
    for predicate_path in sorted(root.glob(PREDICATE_GLOB)):
        if not predicate_path.is_file():
            continue
        try:
            payload = json.loads(predicate_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("skip predicate file %s: %s", predicate_path, exc)
            continue

        scenarios = payload.get("scenarios")
        if not isinstance(scenarios, dict):
            logger.warning("skip %s: missing scenarios object", predicate_path)
            continue

        chunk = str(payload.get("chunk", "")).strip()
        feature = str(payload.get("feature", "")).strip()
        owner_plan = str(payload.get("owner_chunk_plan", "")).strip()
        schema_version = str(payload.get("schema_version", "")).strip()
        routing = payload.get("routing_hint") or {}
        haiku_eligible = set(routing.get("haiku_eligible") or [])

        for scn_id, scn_block in scenarios.items():
            if not isinstance(scn_block, dict):
                continue
            if scn_id in existing_ext_ids:
                continue

            phash = _predicate_hash(scn_block)
            tags: dict[str, str | list[str]] = {}
            if chunk:
                tags["chunk"] = chunk
            if feature:
                tags["feature"] = feature
            if schema_version:
                tags["schema_version"] = schema_version
            if owner_plan:
                tags["owner_chunk_plan"] = owner_plan
            kind = scn_block.get("predicate_kind")
            if kind:
                tags["predicate_kind"] = str(kind)
            surface = scn_block.get("surface")
            if surface:
                tags["surface"] = str(surface)
            tags["haiku_eligible"] = "true" if scn_id in haiku_eligible else "false"

            metadata = {
                "scenario_id": scn_id,
                "predicate_hash": phash,
                "scenario": scn_block,
                "predicate_file": str(predicate_path),
            }
            title = str(scn_block.get("intent") or scn_id)

            doc_id = register_document(
                conn,
                COLLECTION_NAME,
                predicate_path,
                "predicate",
                title,
                tags=tags,
                external_id=scn_id,
                metadata=metadata,
                status="active",
            )
            existing_ext_ids.add(scn_id)
            registered.append(
                {
                    "document_id": doc_id,
                    "scenario_id": scn_id,
                    "predicate_hash": phash,
                    "file_path": str(predicate_path),
                }
            )

    logger.info(
        "Scanned and registered %d new scenarios in %s",
        len(registered),
        COLLECTION_NAME,
    )
    return registered


def _predicate_hash(scenario_block: dict) -> str:
    """SHA-256 of canonical (sorted-keys, no whitespace) scenario JSON."""
    canonical = json.dumps(scenario_block, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
