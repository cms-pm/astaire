"""scenario-ledger collection — indexes the append-only scenario evidence ledger.

Issue #16. Reads `artifacts/validation/scenario_ledger.jsonl` (JSONL, one
record per scenario observation). Latest record per `scenario_id` wins; the
collection exposes that current status keyed by SCN. The `predicate_hash`
field cross-references rows in the scenario-predicates collection (#17) so
drift between predicate definition and observed evidence becomes a join
question.

Status enum mirrors `core/EVIDENCE_CONTRACT.md` (governance PR #20):
not-started, expected-fail, passing, failing, waived.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

from src.registry import create_collection, get_collection, register_document

logger = logging.getLogger(__name__)

COLLECTION_NAME = "scenario-ledger"

LEDGER_PATH = "artifacts/validation/scenario_ledger.jsonl"

ALLOWED_STATUSES = {
    "not-started",
    "expected-fail",
    "passing",
    "failing",
    "waived",
}

COLLECTION_CONFIG = {
    "doc_types": ["ledger-entry"],
    "tag_keys": [
        "scenario_id",
        "chunk",
        "status",
        "predicate_hash",
        "actor",
        "evidence_kind",
    ],
    "statuses": sorted(ALLOWED_STATUSES),
}


def register_collection(conn: sqlite3.Connection) -> str:
    existing = get_collection(conn, COLLECTION_NAME)
    if existing:
        return existing["collection_id"]
    return create_collection(
        conn,
        COLLECTION_NAME,
        "Append-only scenario-evidence ledger (latest record per SCN wins)",
        COLLECTION_CONFIG,
    )


def scan_and_register(
    conn: sqlite3.Connection,
    root_dir: str | Path,
) -> list[dict]:
    root = Path(root_dir)
    ledger_path = root / LEDGER_PATH
    if not ledger_path.is_file():
        logger.debug("scenario-ledger not present at %s; nothing to scan", ledger_path)
        return []

    col = get_collection(conn, COLLECTION_NAME)
    if col is None:
        raise ValueError(
            f"Collection {COLLECTION_NAME!r} not registered. "
            "Call register_collection() first."
        )

    latest: dict[str, dict] = {}
    for line_no, raw in enumerate(ledger_path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            logger.warning("ledger line %d malformed: %s", line_no, exc)
            continue
        scn_id = record.get("scenario_id")
        if not isinstance(scn_id, str):
            logger.warning("ledger line %d missing scenario_id; skipped", line_no)
            continue
        timestamp = record.get("timestamp") or ""
        prior = latest.get(scn_id)
        if prior is None or str(timestamp) >= str(prior.get("timestamp") or ""):
            latest[scn_id] = record

    existing_ext_ids: set[str] = set()
    for row in conn.execute(
        "SELECT external_id FROM document "
        "WHERE collection_id = ? AND external_id IS NOT NULL",
        (col["collection_id"],),
    ).fetchall():
        existing_ext_ids.add(row["external_id"])

    registered: list[dict] = []
    for scn_id, record in latest.items():
        if scn_id in existing_ext_ids:
            continue
        status = str(record.get("status") or "not-started").lower()
        if status not in ALLOWED_STATUSES:
            logger.warning(
                "ledger entry %s has unknown status %r; storing as not-started",
                scn_id, status,
            )
            status = "not-started"

        tags: dict[str, str | list[str]] = {"scenario_id": scn_id, "status": status}
        for key in ("chunk", "predicate_hash", "actor", "evidence_kind"):
            val = record.get(key)
            if val:
                tags[key] = str(val)

        title = f"{scn_id} :: {status}"
        metadata = {"latest_record": record, "ledger_path": str(ledger_path)}

        doc_id = register_document(
            conn,
            COLLECTION_NAME,
            ledger_path,
            "ledger-entry",
            title,
            tags=tags,
            external_id=scn_id,
            metadata=metadata,
            status=status,
        )
        existing_ext_ids.add(scn_id)
        registered.append(
            {
                "document_id": doc_id,
                "scenario_id": scn_id,
                "status": status,
                "predicate_hash": record.get("predicate_hash"),
            }
        )

    logger.info(
        "Scanned ledger %s: %d unique SCN(s), %d newly registered",
        ledger_path, len(latest), len(registered),
    )
    return registered
