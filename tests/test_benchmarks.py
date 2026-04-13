"""Smoke tests for benchmarks — verify they complete and produce valid results."""

import pytest

from benchmarks.bench_context import (
    bench_assembly_latency,
    bench_l0_vs_full,
    bench_tag_query,
    bench_token_savings_scoped,
    format_results,
    run_all_benchmarks,
)
from src.collections.discovery import register_all_collections, scan_all_collections
from src.db import get_connection, init_db


@pytest.fixture
def bench_db(tmp_path):
    """Set up a DB with sample artifacts for benchmarking."""
    db_path = str(tmp_path / "bench.db")
    root = tmp_path / "project"
    root.mkdir()

    # Create sample files matching discovered collection scan rules
    pq = root / "docs" / "planning" / "pool_questions"
    pq.mkdir(parents=True)
    (pq / "phase-1-test.md").write_text("# Pool Questions: Phase 1\n\nQ1: How?\n")
    (pq / "phase-2-test.md").write_text("# Pool Questions: Phase 2\n\nQ1: What?\n")

    scn = root / "docs" / "planning" / "scenarios"
    scn.mkdir(parents=True)
    (scn / "SCN-1.1-01-test.feature").write_text("Feature: Test\n  Scenario: One\n")

    chunks = root / "docs" / "planning" / "chunks"
    chunks.mkdir(parents=True)
    (chunks / "chunk-1.1-test.md").write_text("# Chunk 1.1\n\nImplementation plan.\n")

    (root / "governance.yaml").write_text("profile: strict-baseline\n")

    conn = get_connection(db_path)
    init_db(conn)
    register_all_collections(conn)
    scan_all_collections(conn, root)
    conn.close()

    return db_path, root


class TestBenchmarks:
    def test_tag_query(self, bench_db):
        db_path, root = bench_db
        conn = get_connection(db_path)
        result = bench_tag_query(conn, root)
        conn.close()
        assert "astaire_ms" in result
        assert result["astaire_ms"] >= 0

    def test_token_savings_scoped(self, bench_db):
        db_path, root = bench_db
        conn = get_connection(db_path)
        result = bench_token_savings_scoped(conn, root)
        conn.close()
        assert "astaire_tokens" in result
        assert "raw_fs_tokens" in result

    def test_l0_vs_full(self, bench_db):
        db_path, _ = bench_db
        conn = get_connection(db_path)
        from src.project import generate_l0
        generate_l0(conn)
        result = bench_l0_vs_full(conn)
        conn.close()
        assert result["l0_tokens"] > 0
        assert result["all_doc_tokens"] > 0
        # savings_pct may be negative for tiny test datasets (L0 overhead > content)

    def test_assembly_latency(self, bench_db):
        db_path, root = bench_db
        conn = get_connection(db_path)
        result = bench_assembly_latency(conn, root)
        conn.close()
        assert "astaire_ms" in result
        assert "raw_fs_ms" in result

    def test_run_all(self, bench_db):
        db_path, root = bench_db
        results = run_all_benchmarks(db_path, str(root))
        assert len(results) == 4

    def test_format_results(self, bench_db):
        db_path, root = bench_db
        results = run_all_benchmarks(db_path, str(root))
        report = format_results(results)
        assert "# Astaire Performance Benchmarks" in report
        assert "Token savings" in report
