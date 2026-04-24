"""Token counting utilities backed by tiktoken."""

from __future__ import annotations

import os

import tiktoken


APPROX_CHARS_PER_TOKEN = 4


class TokenizerUnavailable(RuntimeError):
    """Raised when the requested tokenizer encoding is unavailable."""


def _allow_approx_tokens() -> bool:
    value = os.getenv("ASTAIRE_ALLOW_APPROX_TOKENS", "1").strip().lower()
    return value not in {"0", "false", "no", "off"}


def _approx_count_tokens(text: str) -> int:
    if not text:
        return 0
    return max(1, (len(text) + APPROX_CHARS_PER_TOKEN - 1) // APPROX_CHARS_PER_TOKEN)


def _approx_truncate(text: str, budget: int) -> str:
    if budget <= 0 or not text:
        return ""
    max_chars = budget * APPROX_CHARS_PER_TOKEN
    if len(text) <= max_chars:
        return text
    return text[:max_chars]


def get_encoder(encoding: str = "cl100k_base"):
    """Return a tiktoken encoder or raise a deterministic readiness error."""
    try:
        return tiktoken.get_encoding(encoding)
    except Exception as exc:  # pragma: no cover - exercised via monkeypatch/tests
        raise TokenizerUnavailable(
            f"Tokenizer encoding '{encoding}' is unavailable. "
            "Run Astaire bootstrap/doctor while online to prewarm tokenizer assets."
        ) from exc


def check_tokenizer_health(encoding: str = "cl100k_base") -> dict[str, str | bool]:
    """Report whether the tokenizer encoding is ready for exact token counts."""
    try:
        get_encoder(encoding)
    except TokenizerUnavailable as exc:
        return {
            "ok": False,
            "encoding": encoding,
            "message": str(exc),
            "approx_tokens_enabled": _allow_approx_tokens(),
        }
    return {
        "ok": True,
        "encoding": encoding,
        "message": f"Tokenizer encoding '{encoding}' is ready.",
        "approx_tokens_enabled": _allow_approx_tokens(),
    }


def count_tokens(text: str, encoding: str = "cl100k_base", allow_approx: bool | None = None) -> int:
    """Count tokens in text using the specified tiktoken encoding."""
    if allow_approx is None:
        allow_approx = _allow_approx_tokens()
    try:
        enc = get_encoder(encoding)
    except TokenizerUnavailable:
        if allow_approx:
            return _approx_count_tokens(text)
        raise
    return len(enc.encode(text))


def truncate_to_budget(
    text: str,
    budget: int,
    encoding: str = "cl100k_base",
    allow_approx: bool | None = None,
) -> str:
    """Truncate text to fit within a token budget, preserving whole tokens."""
    if allow_approx is None:
        allow_approx = _allow_approx_tokens()
    try:
        enc = get_encoder(encoding)
    except TokenizerUnavailable:
        if allow_approx:
            return _approx_truncate(text, budget)
        raise
    tokens = enc.encode(text)
    if len(tokens) <= budget:
        return text
    return enc.decode(tokens[:budget])
