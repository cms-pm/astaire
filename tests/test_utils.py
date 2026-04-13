"""Tests for src/utils/ modules."""

import time
from pathlib import Path

from src.utils import hashing, ulid, tokens


class TestULID:
    def test_generate_returns_26_chars(self):
        u = ulid.generate()
        assert len(u) == 26

    def test_generate_unique(self):
        ids = {ulid.generate() for _ in range(100)}
        assert len(ids) == 100

    def test_time_sortable(self):
        a = ulid.generate()
        time.sleep(0.002)
        b = ulid.generate()
        assert a < b

    def test_timestamp_roundtrip(self):
        before = int(time.time() * 1000)
        u = ulid.generate()
        after = int(time.time() * 1000)
        ts = ulid.timestamp_ms(u)
        assert before <= ts <= after

    def test_invalid_length_raises(self):
        import pytest
        with pytest.raises(ValueError):
            ulid.timestamp_ms("short")


class TestHashing:
    def test_hash_content_deterministic(self):
        h1 = hashing.hash_content("hello world")
        h2 = hashing.hash_content("hello world")
        assert h1 == h2

    def test_hash_content_different_inputs(self):
        h1 = hashing.hash_content("hello")
        h2 = hashing.hash_content("world")
        assert h1 != h2

    def test_hash_content_bytes(self):
        h1 = hashing.hash_content("hello")
        h2 = hashing.hash_content(b"hello")
        assert h1 == h2

    def test_hash_file(self, tmp_path):
        p = tmp_path / "test.txt"
        p.write_text("hello world")
        h = hashing.hash_file(p)
        assert h == hashing.hash_content("hello world")

    def test_hash_is_sha256_hex(self):
        h = hashing.hash_content("test")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


class TestTokens:
    def test_count_tokens_nonempty(self):
        count = tokens.count_tokens("Hello, world!")
        assert count > 0

    def test_count_tokens_empty(self):
        assert tokens.count_tokens("") == 0

    def test_count_tokens_encoding_param(self):
        count = tokens.count_tokens("Hello", encoding="cl100k_base")
        assert count > 0

    def test_truncate_within_budget(self):
        text = "Hello, world!"
        result = tokens.truncate_to_budget(text, budget=100)
        assert result == text

    def test_truncate_over_budget(self):
        text = "word " * 1000
        result = tokens.truncate_to_budget(text, budget=10)
        result_count = tokens.count_tokens(result)
        assert result_count <= 10

    def test_truncate_preserves_valid_text(self):
        text = "The quick brown fox jumps over the lazy dog. " * 50
        result = tokens.truncate_to_budget(text, budget=20)
        assert len(result) > 0
        assert isinstance(result, str)
