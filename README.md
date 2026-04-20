# Astaire

A hybrid memory palace — structured, persistent knowledge store that sits between raw source documents and LLM reasoning. Astaire provides two complementary subsystems: a **claim store** for structured knowledge extraction (entities, claims, relationships, contradictions) and a **document registry** for fast, indexed storage and retrieval of any file set. The core is document-type agnostic; applications plug in as "collections" with their own document types, tags, and lifecycle conventions.

## Why Astaire?

LLM workflows typically read raw files for context. This is slow, expensive, and unstructured. Astaire provides:

- **159:1 token compression** — L0 summary captures full knowledge base state in 563 tokens vs 89,900 tokens of raw documents
- **57.8% token savings** on scoped context assembly with budget enforcement
- **133.1x faster** tag-based queries vs filesystem glob+read
- **8.9x faster** full collection assembly vs sequential file reads

These are measured results from the `ai-dev-governance` dogfood workspace on `v0.6.0`, using a `96`-document dataset across `3` collections. See [benchmarks](#benchmarks) for details.

## Installation

```bash
git clone <repo-url> astaire
cd astaire
uv sync
```

Requirements: Python 3.11+, [uv](https://docs.astral.sh/uv/).

## Quick Start

```bash
# Initialize the database
uv run astaire init

# Scan and register collection artifacts
uv run astaire scan --root .

# Session startup (init + scan + sync + status)
uv run astaire startup --root .

# Query documents by collection, type, or tag
uv run astaire query -c my-collection
uv run astaire query -t gherkin
uv run astaire query --tag phase=1

# Full-text search
uv run astaire query --fts "keyword"

# Assemble context for LLM consumption (with token budget)
uv run astaire context -c my-collection --budget 4000
uv run astaire context --tag chunk=1.2

# Health checks
uv run astaire lint
uv run astaire lint --fix

# Export wiki (Obsidian-compatible markdown)
uv run astaire export

# Prune expired claims
uv run astaire prune

# Detect file drift
uv run astaire sync

# Ingest a source document with claims
uv run astaire ingest raw/article.md --title "Article Title"
```

## Architecture

Astaire separates three concerns that are typically coupled:

| Layer | Purpose | Implementation |
|-------|---------|----------------|
| **Storage** | Structured data persistence | SQLite + FTS5 |
| **Query** | Projection engine, context assembly | Python/SQL (zero LLM tokens) |
| **Presentation** | Human-readable output | Markdown wiki export |

The projection cache provides tiered context at three levels:

- **L0** — Global summary (~500 tokens in the `v0.6.0` dogfood workspace). Regenerated after every write. It carries registry state, key metrics, recent activity, and canonical `route:` lines for deeper tentacles.
- **L1** — Per-scope digests (~2K tokens each). Per-entity, per-cluster, per-collection.
- **L2** — Per-query detail with source excerpts. Generated on demand.

See `docs/diagrams/` for Mermaid diagrams of the full architecture.

## Astaire As Broker

Astaire is meant to be the broker of context, not just a database. In the `ai-dev-governance` integration:

- **Astaire L0** is the port of first resort for low-token orientation and recent activity.
- **Astaire L1/L2** handle scoped recall when a question needs more than the global summary.
- **graphify** contributes structural routes so agents can jump directly into codebase topology instead of re-reading the filesystem.

In the current `v0.6.0` dogfood release snapshot, Astaire carries:

- `5` active claims
- `5` entities
- `1` relationship
- a live `route: tentacle=graphify.query; ...` hand-off in L0
- a fresh no-op graphify verification entry in recent activity so release evidence stays current even when imports are unchanged

## Collections

A **collection** is a named group of related document types with its own configuration. The core is collection-agnostic — you define what document types, statuses, tags, and lifecycle stages mean for your domain.

To create a collection, add a module in `src/collections/`:

```python
# src/collections/my_project.py
COLLECTION_NAME = "my-project"
COLLECTION_CONFIG = {
    "doc_types": ["spec", "adr", "rfc"],
    "statuses": ["draft", "active", "archived"],
    "tag_keys": ["phase", "component"],
}

def register_collection(conn):
    from src.registry import create_collection, get_collection
    existing = get_collection(conn, COLLECTION_NAME)
    if existing:
        return existing["collection_id"]
    return create_collection(conn, COLLECTION_NAME, "My project docs", COLLECTION_CONFIG)

def scan_and_register(conn, root_dir):
    # Scan filesystem and register documents
    # See docs/guides/collection-authoring.md for full example
    ...
```

Collections are auto-discovered on startup and scan. See [Collection Authoring Guide](docs/guides/collection-authoring.md) for details.

## CLI Reference

| Command | Purpose |
|---------|---------|
| `astaire init` | Initialize database schema |
| `astaire startup --root .` | Full session startup (init + scan + sync + status) |
| `astaire status` | Print L0 summary |
| `astaire scan [--root .] [-c COLLECTION]` | Register new artifacts |
| `astaire sync [-c COLLECTION]` | Detect file drift |
| `astaire query [-c COL] [-t TYPE] [--tag K=V] [--fts TERM] [--json]` | Query documents |
| `astaire context [-c COL] [--tag K=V] [--budget N]` | Assemble context |
| `astaire lint [--fix]` | Health checks |
| `astaire export [-o DIR]` | Generate wiki |
| `astaire prune` | Remove expired claims |
| `astaire ingest FILE --title T [--claims FILE]` | Ingest source document |

All commands accept `--db PATH` to use a non-default database and `-v` for verbose logging.

## Benchmarks

### Running

```bash
uv run python -m benchmarks.bench_context --db db/memory_palace.db --root .
```

In restricted or sandboxed environments, set a writable `uv` cache explicitly:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run --project astaire python -m benchmarks.bench_context --db .astaire/memory_palace.db --root .
```

### Results

Measured against the `ai-dev-governance` dogfood workspace on a MacBook (`v0.6.0`, 2026-04-20), with `96` registered documents across `3` collections:

| Metric | Astaire | Raw FS | Improvement |
|--------|---------|--------|-------------|
| Tag query latency | 3.03ms | 403.12ms | **133.1x faster** |
| Scoped context tokens (budget=4K) | 2,125 | 5,035 | **57.8% fewer tokens** |
| L0 vs all documents | 563 tokens | 89,900 tokens | **99.4% savings / 159:1** |
| Full collection assembly | 21.9ms | 194.88ms | **8.9x faster** |

The full captured benchmark artifact lives at [docs/benchmarks/v0.6.0-ai-dev-governance.md](docs/benchmarks/v0.6.0-ai-dev-governance.md).

### Methodology

Each benchmark pairs an Astaire operation against its nearest raw-filesystem equivalent, using the same dataset and measuring wall-clock time with `time.perf_counter()` and token counts with `tiktoken` (cl100k_base):

- **Tag query**: `query_documents(conn, tags={"phase": "1"})` (single SQL lookup) vs `Path.rglob("*phase-1*")` + `rglob("*phase*1*")` with file reads and token counting.
- **Scoped context**: `assemble_context(conn, tags={"phase": "1"}, token_budget=4000)` (projection cache hit) vs reading all matching planning files (pool questions, scenarios, chunks) from disk.
- **L0 vs all documents**: token count of the cached L0 global summary vs `SUM(token_count)` across all active documents — no filesystem I/O needed on either side, purely a compression ratio measurement.
- **Full assembly**: `assemble_context(conn, token_budget=8000)` after one warmup call vs recursive glob of all planning docs (`**/*.md`, `**/*.feature`) with file reads.

The raw-FS baseline intentionally mimics what a naive agent would do: glob for plausible filenames, read everything, count tokens. Astaire's advantage compounds as the collection grows — the SQL indexes and projection cache are O(log n) and O(1) respectively, while raw glob+read is O(n·file_size).

## Development

```bash
# Run tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific module tests
uv run pytest tests/test_registry.py
```

## License

Apache 2.0
