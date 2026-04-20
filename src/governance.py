"""Helpers for reading a consumer governance.yaml without external YAML deps."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
import hashlib


def load_graphify_config(
    root_dir: str | Path,
    manifest_path: str | Path | None = None,
) -> dict:
    """Read the graphify block from governance.yaml using a narrow parser.

    Supports the specific manifest shapes used by ai-dev-governance fixtures:
    scalars, scalar lists, and `crossRepoAuthority` entries with `repo` plus
    `namespaces`.
    """
    root = Path(root_dir)
    manifest = _manifest_path(root, manifest_path)
    if not manifest.is_file():
        return {}

    lines = manifest.read_text(encoding="utf-8").splitlines()
    block = _slice_block(lines, "graphify")
    if not block:
        return {}

    config: dict = {}
    i = 0
    while i < len(block):
        line = block[i]
        scalar = re.match(r"^\s{2}([A-Za-z0-9_]+):\s*(.+?)\s*$", line)
        nested = re.match(r"^\s{2}([A-Za-z0-9_]+):\s*$", line)

        if scalar:
            key = scalar.group(1)
            config[key] = _parse_scalar(scalar.group(2))
            i += 1
            continue

        if nested:
            key = nested.group(1)
            if key == "crossRepoAuthority":
                values, i = _parse_authority_list(block, i + 1)
            else:
                values, i = _parse_scalar_list(block, i + 1)
            config[key] = values
            continue

        i += 1

    return config


def load_governance_context(
    root_dir: str | Path,
    manifest_path: str | Path | None = None,
) -> dict:
    """Return the manifest-derived context shared by Astaire and root scripts."""
    root = Path(root_dir)
    manifest = _manifest_path(root, manifest_path)
    graphify = load_graphify_config(root, manifest)
    source_repo = derive_source_repo(root, graphify.get("sourceRepoTag"))
    return {
        "rootDir": str(root),
        "manifestPath": str(manifest),
        "graphify": graphify,
        "exceptionsRegistryPath": load_exceptions_registry_path(root, manifest),
        "contractRegistryPath": load_contract_registry_path(root, manifest),
        "sourceRepo": source_repo,
    }


def load_exceptions_registry_path(
    root_dir: str | Path,
    manifest_path: str | Path | None = None,
) -> str | None:
    root = Path(root_dir)
    manifest = _manifest_path(root, manifest_path)
    if not manifest.is_file():
        return None

    lines = manifest.read_text(encoding="utf-8").splitlines()
    block = _slice_block(lines, "exceptions")
    if not block:
        return None

    for line in block:
        match = re.match(r"^\s{2}registryPath:\s*(.+?)\s*$", line)
        if match:
            return str(_resolve_path(root, _parse_scalar(match.group(1))))
    return None


def load_contract_registry_path(
    root_dir: str | Path,
    manifest_path: str | Path | None = None,
) -> str | None:
    """Resolve the contract registry path using manifest-driven conventions."""
    root = Path(root_dir)
    manifest = _manifest_path(root, manifest_path)
    if manifest.is_file():
        lines = manifest.read_text(encoding="utf-8").splitlines()
        block = _slice_block(lines, "contracts")
        if block:
            for line in block:
                match = re.match(r"^\s{2}registryPath:\s*(.+?)\s*$", line)
                if match:
                    resolved = _resolve_path(root, _parse_scalar(match.group(1)))
                    return str(resolved) if resolved.is_file() else None

    candidates: list[Path] = []
    exceptions_path = load_exceptions_registry_path(root, manifest)
    if exceptions_path:
        exceptions_file = Path(exceptions_path)
        candidates.extend(
            [
                exceptions_file.with_name("contracts.yaml"),
                exceptions_file.with_name("contracts.json"),
            ]
        )

    gov_dir = root / "docs" / "governance"
    candidates.extend([gov_dir / "contracts.yaml", gov_dir / "contracts.json"])

    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve() if candidate.is_absolute() else candidate
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.is_file():
            return str(resolved)
    return None


def derive_source_repo(root_dir: str | Path, source_repo_tag: str | None = None) -> str:
    """Prefer manifest sourceRepoTag, then hashed git remote, then repo name."""
    if source_repo_tag:
        return source_repo_tag

    root = Path(root_dir)
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "config", "--get", "remote.origin.url"],
            check=False,
            capture_output=True,
            text=True,
        )
        remote = proc.stdout.strip()
    except OSError:
        remote = ""

    if remote:
        return hashlib.sha256(remote.encode("utf-8")).hexdigest()[:16]
    return root.name


def _manifest_path(root: Path, manifest_path: str | Path | None) -> Path:
    if manifest_path is None:
        return root / "governance.yaml"
    manifest = Path(manifest_path)
    return manifest if manifest.is_absolute() else root / manifest


def _resolve_path(root: Path, value) -> Path:
    path = Path(str(value))
    return path if path.is_absolute() else root / path


def _slice_block(lines: list[str], block_name: str) -> list[str]:
    start = None
    for idx, line in enumerate(lines):
        if re.match(rf"^{re.escape(block_name)}:\s*$", line):
            start = idx + 1
            break
    if start is None:
        return []

    block: list[str] = []
    for line in lines[start:]:
        if line and not line.startswith(" "):
            break
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        block.append(line)
    return block


def _parse_scalar(value: str):
    raw = value.strip()
    if raw in {"null", "~"}:
        return None
    if raw in {"true", "false"}:
        return raw == "true"
    if raw.startswith("[") and raw.endswith("]"):
        inner = raw[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(part.strip()) for part in inner.split(",")]
    if raw.startswith("{") and raw.endswith("}"):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw
    if raw.startswith(("'", '"')) and raw.endswith(("'", '"')) and len(raw) >= 2:
        return raw[1:-1]
    if re.fullmatch(r"-?\d+", raw):
        return int(raw)
    if re.fullmatch(r"-?\d+\.\d+", raw):
        return float(raw)
    return raw


def _parse_scalar_list(block: list[str], start_idx: int) -> tuple[list, int]:
    values = []
    i = start_idx
    while i < len(block):
        line = block[i]
        match = re.match(r"^\s{4}-\s*(.+?)\s*$", line)
        if not match:
            break
        values.append(_parse_scalar(match.group(1)))
        i += 1
    return values, i


def _parse_authority_list(block: list[str], start_idx: int) -> tuple[list[dict], int]:
    values: list[dict] = []
    i = start_idx
    while i < len(block):
        repo_match = re.match(r"^\s{4}-\s*repo:\s*(.+?)\s*$", block[i])
        if not repo_match:
            break
        entry = {"repo": _parse_scalar(repo_match.group(1)), "namespaces": []}
        i += 1
        while i < len(block):
            ns_header = re.match(r"^\s{6}namespaces:\s*$", block[i])
            ns_item = re.match(r"^\s{8}-\s*(.+?)\s*$", block[i])
            if ns_header:
                i += 1
                continue
            if ns_item:
                entry["namespaces"].append(_parse_scalar(ns_item.group(1)))
                i += 1
                continue
            break
        values.append(entry)
    return values, i
