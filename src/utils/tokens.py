"""Token counting via tiktoken. The only external dependency."""

import tiktoken


def count_tokens(text: str, encoding: str = "cl100k_base") -> int:
    """Count tokens in text using the specified tiktoken encoding."""
    enc = tiktoken.get_encoding(encoding)
    return len(enc.encode(text))


def truncate_to_budget(text: str, budget: int, encoding: str = "cl100k_base") -> str:
    """Truncate text to fit within a token budget, preserving whole tokens."""
    enc = tiktoken.get_encoding(encoding)
    tokens = enc.encode(text)
    if len(tokens) <= budget:
        return text
    return enc.decode(tokens[:budget])
