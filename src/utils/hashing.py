"""SHA-256 content hashing for dedup and staleness detection."""

import hashlib
from pathlib import Path


def hash_content(content: str | bytes) -> str:
    """Return the hex SHA-256 digest of content."""
    if isinstance(content, str):
        content = content.encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def hash_file(path: str | Path) -> str:
    """Return the hex SHA-256 digest of a file's contents."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
