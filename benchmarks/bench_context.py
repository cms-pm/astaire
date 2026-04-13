"""Performance benchmarks: Astaire vs raw filesystem reads.

Measures time and token savings to demonstrate Astaire's value proposition.
Can be run standalone or via `astaire bench`.
"""

import time
from pathlib import Path

from src.db import get_connection, init_db
from src.registry import query_documents, assemble_context
from src.project import generate_l0, read_l0
from src.utils import tokens


def _get_first_collection(conn) -> str | None:
    """Get the name of the first registered collection."""
    row = conn.execute("SELECT name FROM collection LIMIT 1").fetchone()
    return row["name"] if row else None


def _glob_and_read(root: Path, patterns: list[str]) -> tuple[float, int, int]:
    """Simulate raw FS approach: glob for files, read them all, count tokens.
    Returns (elapsed_seconds, total_tokens, file_count)."""
    start = time.perf_counter()
    total_tokens = 0
    file_count = 0
    for pattern in patterns:
        for f in sorted(root.rglob(pattern)):
            if f.is_file():
                try:
                    content = f.read_text(encoding="utf-8")
                    total_tokens += tokens.count_tokens(content)
                    file_count += 1
                except (UnicodeDecodeError, PermissionError):
                    pass
    elapsed = time.perf_counter() - start
    return elapsed, total_tokens, file_count


def bench_tag_query(conn, root: Path) -> dict:
    """Compare tag-based query latency: Astaire vs glob."""
    # Astaire: query by tag
    start = time.perf_counter()
    docs = query_documents(conn, tags={"phase": "1"})
    astaire_time = time.perf_counter() - start

    # Raw FS: glob for phase-1 related files
    fs_time, _, fs_count = _glob_and_read(root, ["*phase-1*", "*phase*1*"])

    return {
        "metric": "Tag query latency",
        "astaire_ms": round(astaire_time * 1000, 2),
        "raw_fs_ms": round(fs_time * 1000, 2),
        "speedup": round(fs_time / astaire_time, 1) if astaire_time > 0 else float("inf"),
        "astaire_results": len(docs),
        "raw_fs_files": fs_count,
    }


def bench_token_savings_scoped(conn, root: Path) -> dict:
    """Compare tokens: assembled context (budget=4000) vs all raw files for a scope."""
    # Astaire: assemble with budget
    start = time.perf_counter()
    ctx = assemble_context(conn, collection_name=_get_first_collection(conn),
                           tags={"phase": "1"}, token_budget=4000)
    astaire_time = time.perf_counter() - start
    astaire_tokens = tokens.count_tokens(ctx) if ctx else 0

    # Raw FS: read all phase-1 planning files
    fs_time, fs_tokens, fs_count = _glob_and_read(
        root, ["docs/planning/pool_questions/phase-1*",
               "docs/planning/scenarios/SCN-1.*",
               "docs/planning/chunks/chunk-1.*",
               "docs/planning/phase-1*"]
    )

    savings_pct = round((1 - astaire_tokens / fs_tokens) * 100, 1) if fs_tokens > 0 else 0

    return {
        "metric": "Token savings (scoped context, budget=4000)",
        "astaire_tokens": astaire_tokens,
        "raw_fs_tokens": fs_tokens,
        "savings_pct": savings_pct,
        "astaire_ms": round(astaire_time * 1000, 2),
        "raw_fs_ms": round(fs_time * 1000, 2),
        "raw_fs_files": fs_count,
    }


def bench_l0_vs_full(conn) -> dict:
    """Compare tokens: L0 summary vs sum of all document token_counts."""
    l0 = read_l0(conn)
    if l0 is None:
        l0 = generate_l0(conn)

    l0_tokens = tokens.count_tokens(l0)

    total_doc_tokens = conn.execute(
        "SELECT COALESCE(SUM(token_count), 0) FROM document "
        "WHERE status NOT IN ('superseded', 'archived')"
    ).fetchone()[0]

    savings_pct = round((1 - l0_tokens / total_doc_tokens) * 100, 1) if total_doc_tokens > 0 else 0

    return {
        "metric": "Token savings (L0 vs all documents)",
        "l0_tokens": l0_tokens,
        "all_doc_tokens": total_doc_tokens,
        "savings_pct": savings_pct,
        "compression_ratio": f"{total_doc_tokens // l0_tokens}:1" if l0_tokens > 0 else "N/A",
    }


def bench_assembly_latency(conn, root: Path) -> dict:
    """Compare assembly time: Astaire query+assemble vs sequential file reads."""
    # Warm up
    assemble_context(conn, collection_name=_get_first_collection(conn), token_budget=8000)

    # Astaire: assemble context
    start = time.perf_counter()
    ctx = assemble_context(conn, collection_name=_get_first_collection(conn), token_budget=8000)
    astaire_time = time.perf_counter() - start
    astaire_tokens = tokens.count_tokens(ctx) if ctx else 0

    # Raw FS: read all governance files
    fs_time, fs_tokens, fs_count = _glob_and_read(
        root, ["docs/planning/**/*.md", "docs/planning/**/*.feature",
               "governance.yaml"]
    )

    return {
        "metric": "Assembly latency (full collection)",
        "astaire_ms": round(astaire_time * 1000, 2),
        "raw_fs_ms": round(fs_time * 1000, 2),
        "speedup": round(fs_time / astaire_time, 1) if astaire_time > 0 else float("inf"),
        "astaire_tokens": astaire_tokens,
        "raw_fs_tokens": fs_tokens,
        "raw_fs_files": fs_count,
    }


def run_all_benchmarks(db_path: str, root: str) -> list[dict]:
    """Run all benchmarks. Returns list of result dicts."""
    conn = get_connection(db_path)
    root_path = Path(root).resolve()
    results = []

    results.append(bench_tag_query(conn, root_path))
    results.append(bench_token_savings_scoped(conn, root_path))
    results.append(bench_l0_vs_full(conn))
    results.append(bench_assembly_latency(conn, root_path))

    conn.close()
    return results


def format_results(results: list[dict]) -> str:
    """Format benchmark results as a markdown report."""
    lines = ["# Astaire Performance Benchmarks", ""]

    for r in results:
        lines.append(f"## {r['metric']}")
        for k, v in r.items():
            if k == "metric":
                continue
            label = k.replace("_", " ").title()
            lines.append(f"- **{label}**: {v}")
        lines.append("")

    return "\n".join(lines)


def main():
    """CLI entry point for benchmarks."""
    import argparse
    parser = argparse.ArgumentParser(description="Astaire performance benchmarks")
    parser.add_argument("--db", default="db/memory_palace.db", help="Database path")
    parser.add_argument("--root", default=".", help="Project root")
    args = parser.parse_args()

    results = run_all_benchmarks(args.db, args.root)
    print(format_results(results))


if __name__ == "__main__":
    main()
