# Astaire — CLAUDE.md

You are building and maintaining a **hybrid memory palace** called **Astaire**: a structured, persistent knowledge store that sits between raw source documents and LLM reasoning. The key architectural principle is **separation of concerns** — storage (SQLite), query (projection engine), and presentation (markdown export) are independent layers, never coupled.

**The core is document-type agnostic.** Astaire has two complementary subsystems: a **claim store** for structured knowledge extraction (entities, claims, relationships, contradictions) and a **document registry** for fast, indexed storage and retrieval of any file set. Different applications plug in as "collections" — ai-dev-governance SDLC artifacts, project documentation, design decisions, etc. — each with their own document types, tags, and lifecycle conventions. The core never hardcodes application-specific enums or constraints.

Read this file completely before performing any operation. It is the authoritative reference for all conventions, workflows, and architectural decisions.

---

## Governance and methodology

This project follows the **ai-dev-governance** methodology (pinned as a git submodule at `ai-dev-governance/`). All development work MUST comply with the governance policies, adapter conventions, and evidence contracts defined there.

### Governance manifest

The project governance manifest is `governance.yaml` (project root). It declares:
- **Profile:** `strict-baseline` with `providers/claude` and `tooling/rtk`
- **Automation:** Risk-tiered 8-state lifecycle (`ingest → plan → artifact-generation → implementation → validation → board-review → gate → release`)
- **Evidence paths:** `docs/planning/`, `docs/validation/`, `docs/releases/`
- **Exceptions:** `docs/governance/exceptions.yaml`

### Key governance policies

Consult these authoritative sources in the submodule — do not duplicate their content here:

| Policy domain | Reference |
|---|---|
| Planning gates, pool questions, ambiguity scoring | `ai-dev-governance/core/PLANNING_METHODOLOGY.md` |
| Test-driven requirements, Gherkin, acceptance IDs | `ai-dev-governance/core/AI_ASSISTED_TDR_METHODOLOGY.md` |
| Automation state machine, risk tiers, stop rules | `ai-dev-governance/core/AUTONOMOUS_DELIVERY_GOVERNANCE.md` |
| Board review, expert-agent selection, critique | `ai-dev-governance/core/BOARD_REVIEW_GOVERNANCE_METHODOLOGY.md` |
| Branch model, atomic scope, merge gates | `ai-dev-governance/core/GIT_BRANCH_STRATEGY.md` |
| Evidence fields for planning, validation, release | `ai-dev-governance/core/EVIDENCE_CONTRACT.md` |
| Exceptions and time-bound waivers | `ai-dev-governance/core/EXCEPTIONS_AND_WAIVERS.md` |
| Security baseline | `ai-dev-governance/core/SECURITY_CONTROLS.md` |

### RTK — token governance (mandatory)

This project uses **RTK (Rust Token Killer)** for token-optimized CLI output, as required by `strict-baseline` + `providers/claude`. RTK is installed and configured via `rtk init -g`.

**Operating rules:**

- Use **shell-first workflows** for high-volume repository inspection, git, test, lint, package, and container commands. RTK's Claude Code hook transparently rewrites these (`git status` → `rtk git status`).
- Prefer commands RTK already rewrites: `git`, `gh`, `rg`, `grep`, `cat`, `head`, `tail`, `ls`, `pytest`, `docker`, `kubectl`.
- Claude built-in `Read`, `Grep`, `Glob` are allowed for **narrow targeted inspection** but SHOULD NOT be the default for broad repo exploration — RTK cannot rewrite them.
- `rtk gain`, `rtk gain --history`, `rtk discover` are meta-commands — always call `rtk` directly for these.

**Release evidence requirements** (per `ai-dev-governance/core/EVIDENCE_CONTRACT.md`):
- `rtk init --show` — setup verification
- `rtk gain` — token savings evidence
- `rtk discover` — missed opportunity evidence (or documented no-op)

See `ai-dev-governance/adapters/tooling/RTK_CONTEXT_ADAPTER.md` for full RTK adapter specification and `ai-dev-governance/runbooks/RTK_ADOPTION_RUNBOOK.md` for bootstrap procedures.

### Automation state machine

Transitions are **fail-closed**: if any required artifact is missing or any check is red, the state machine halts.

| Tier | Auto-merge | Human approvals | Board required |
|---|---|---|---|
| low | yes | 0 | no |
| medium | no | 1 | no |
| high | no | 2 + chair | yes |
| critical | no | 2 + chair + accountable | yes (mandatory) |

### Branch model

- Work is isolated on short-lived **chunk branches** (`chunk-X.Y.Z-description`).
- Each PR maps to exactly one acceptance target (`SCN-*`) or one approved SCN prefix.
- Chunk-scope CI check MUST run pre-merge and fail on mixed SCN scope.
- Changed-file count SHOULD stay bounded (default <= 40).

---

## Project structure

```
astaire/
├── CLAUDE.md                        ← You are here
├── governance.yaml                  ← Governance manifest
├── ai-dev-governance/               ← Submodule: governance methodology
├── raw/                             ← Immutable source documents
│   ├── articles/
│   ├── papers/
│   ├── transcripts/
│   ├── notes/
│   └── assets/
├── db/
│   └── memory_palace.db             ← SQLite source of truth
├── src/
│   ├── db.py                        ← Connection, init, transactions
│   ├── registry.py                  ← Document registry (register, query, sync, assemble)
│   ├── ingest.py                    ← Source → claims pipeline + document registration
│   ├── project.py                   ← Projection engine (L0/L1/L2 assembly)
│   ├── export.py                    ← DB → markdown wiki export
│   ├── lint.py                      ← Health checks and contradiction detection
│   ├── prune.py                     ← TTL-based claim expiry
│   ├── collections/                 ← Auto-discovered collection plugins
│   │   ├── discovery.py             ← Convention-based plugin loading
│   │   └── ai_dev_governance.py     ← Example: governance SDLC collection
│   └── utils/
│       ├── ulid.py                  ← ULID generation
│       ├── tokens.py                ← Token counting (tiktoken)
│       └── hashing.py              ← SHA-256 content hashing
├── wiki/                            ← Read-only markdown export (generated)
│   ├── index.md
│   ├── entities/
│   ├── topics/
│   ├── collections/                 ← Per-collection document catalogs
│   ├── contradictions.md
│   ├── timeline.md
│   └── graph_report.md
├── docs/
│   ├── diagrams/
│   │   ├── erd_schema.mermaid
│   │   ├── system_architecture.mermaid
│   │   ├── query_projection_flow.mermaid
│   │   ├── claim_lifecycle.mermaid
│   │   ├── architecture_comparison.mermaid
│   │   └── schema_class_diagram.mermaid
│   ├── schema/
│   │   └── memory_palace_schema.sql  ← Executable DDL with FTS5 + triggers
│   ├── planning/                     ← Governance planning artifacts
│   │   ├── pool_questions/
│   │   ├── scenarios/                ← Gherkin .feature files
│   │   ├── chunks/                   ← Chunk implementation plans
│   │   ├── signoffs.md
│   │   └── traceability.md
│   ├── validation/                   ← Validation evidence
│   ├── releases/                     ← Release evidence bundles
│   ├── governance/
│   │   ├── exceptions.yaml           ← Exception/waiver registry
│   │   └── amendments/               ← Local governance overlay
│   └── plan/
│       └── implementation-plan.md    ← Current implementation plan
├── log.md                           ← Append-only operation log
└── tests/
    ├── conftest.py
    ├── test_utils.py
    ├── test_db.py
    ├── test_registry.py
    ├── test_ingest.py
    ├── test_project.py
    ├── test_lint.py
    └── fixtures/
        └── sample_docs/
```

---

## Architectural reference

Before writing or modifying any code, consult the relevant diagram:

| Question | Diagram |
|---|---|
| What tables exist, what are their columns and relationships? | `docs/diagrams/erd_schema.mermaid` |
| How do the three layers (storage, projection, presentation) connect? | `docs/diagrams/system_architecture.mermaid` |
| What happens at query time? What is the critical path? | `docs/diagrams/query_projection_flow.mermaid` |
| How does a claim move through its lifecycle? | `docs/diagrams/claim_lifecycle.mermaid` |
| How does this differ from Karpathy's wiki or glaucobrito's system? | `docs/diagrams/architecture_comparison.mermaid` |
| What is the full table schema including types and constraints? | `docs/diagrams/schema_class_diagram.mermaid` |

The executable SQL DDL is at `docs/schema/memory_palace_schema.sql`. Initialize a fresh database with:

```bash
sqlite3 db/memory_palace.db < docs/schema/memory_palace_schema.sql
```

---

## Core concepts

### Two subsystems, one database

**Claim store** — structured knowledge extraction from documents. Entities, claims with epistemic status, typed relationships, contradiction detection. Optimized for knowledge queries ("what do we know about X?").

**Document registry** — indexed file tracking organized into collections. Any file set can be registered with flexible tagging. Optimized for agent workflows ("give me everything for chunk 1.2").

Both subsystems share the same SQLite database and projection cache. A document MAY be linked to the claim pipeline (via optional `source_id` FK) when its content should also be extracted as claims.

### Collections and documents

A **collection** is a named group of related document types with its own configuration (allowed types, lifecycle stages, statuses, metadata schemas) stored in `collection.config_json`. Examples:

- `project-docs` — ADRs, specs, RFCs
- `design-decisions` — trade studies, architecture records
- `sdlc-artifacts` — Gherkin scenarios, chunk plans, board findings

A **document** is a registered file in a collection. It has:
- `collection_id` — which collection it belongs to
- `external_id` — any external identifier (SCN-1.2-01, FND-0001, ADR-003, etc.)
- `doc_type` — free-text, meaning defined by the collection
- `status` — free-text, meaning defined by the collection
- `file_path`, `content_hash`, `token_count` — for retrieval and change detection
- `metadata_json` — arbitrary structured data

**Document tags** are flexible key-value pairs (e.g., `stage=implementation`, `chunk=1.2`, `phase=1`, `risk_tier=low`). Tags enable queries like "all documents where stage=implementation and chunk=1.2" without schema changes for new dimensions.

**Document dependencies** are typed edges between documents (produces, consumes, supersedes, validates, references).

### The claim is the atomic unit

Everything in the knowledge subsystem reduces to **claims**. A claim is a single assertion: `(entity, predicate, value)` with provenance (`source_id`, `source_span`), confidence, and epistemic status. Claims are not sentences from documents — they are structured extractions that the LLM produces during ingest.

Good claim:
```
entity: "Alfvén wave"
predicate: "propagation speed depends on"
value: "magnetic field strength and plasma density (v_A = B / sqrt(μ₀ρ))"
claim_type: "fact"
confidence: 0.95
epistemic_tag: "confirmed"
```

Bad claim (too vague, not atomic):
```
entity: "plasma physics"
predicate: "is about"
value: "the study of ionized gases and their behavior in electromagnetic fields"
```

When extracting claims from a source, ask: **could this claim be independently verified, contradicted, or superseded?** If not, it is not atomic enough.

### Entities are de-duplicated subjects

Every claim has exactly one entity as its subject. Entities have a `canonical_name` and an `aliases_json` array. During ingest, always check for existing entities before creating new ones. Use fuzzy matching on aliases — "GPT-4o", "gpt4o", "GPT 4o" are the same entity.

Entity types: `person`, `org`, `system`, `concept`, `place`, `event`.

### Relationships are typed edges

Relationships connect two entities with a typed edge. They optionally reference an evidence claim. The relationship types are:

- `supports` — entity A provides evidence for entity B
- `contradicts` — entity A conflicts with entity B
- `depends_on` — entity A requires entity B
- `evolved_into` — entity A was replaced by entity B
- `part_of` — entity A is a component of entity B
- `related_to` — weak association (use sparingly)
- `tested_by` — entity A has been empirically validated by entity B

### Contradictions are first-class objects

When two claims about the same entity and predicate disagree, create a `contradiction` row. Do not silently discard either claim. Contradictions surface in lint reports and wiki exports. They can be resolved by a new claim that supersedes both, or deferred for human review.

### Projection tiers (L0 / L1 / L2)

The projection cache stores pre-compiled context blocks at three tiers:

| Tier | Scope | Token budget | Regeneration trigger |
|---|---|---|---|
| **L0** | Global summary — entity registry, document registry stats, hot topics | ~2-4K tokens | Every ingest, every lint pass |
| **L1** | Per-cluster, per-entity, or per-collection digest | ~1-2K tokens each | When claims/documents in scope change |
| **L2** | Per-query or per-entity detail with source excerpts | ~2-4K tokens | On demand (may be cached) |

The L0 cache is the single most important optimization. It must be regenerated after every ingest operation. 80% of queries should be answerable from L0 alone.

Scope key conventions for `projection_cache.scope_key`:
- `global` — L0 global summary
- `cluster:{id}` — L1 topic cluster digest
- `entity:{id}` — L1/L2 entity digest
- `collection:{name}` — L1 collection document summary
- `doctype:{collection}:{type}` — L1 document type summary
- `tag:{key}:{value}` — L1 tag-based context bundle
- `query:{hash}` — L2 query-specific cache

---

## Operations

### 1. Ingest

**Trigger:** A new file appears in `raw/` or the user says "ingest [source]".

**Workflow:**

1. Hash the source content (SHA-256). Check `source.content_hash` for duplicates. If already ingested, stop.
2. Read the source. Count tokens. Create a `source` row.
3. Extract entities. For each, check `entity.canonical_name` and `entity.aliases_json` for existing matches. Create or merge.
4. Extract claims. For each:
   a. Check for existing claims on the same `(entity_id, predicate)`.
   b. If values agree → increase confidence on existing claim. Do not duplicate.
   c. If values disagree → create new claim, create `contradiction` row, evaluate temporal ordering. If the new source is clearly newer, set `superseded_by` on the old claim.
   d. Assign `claim_type`, `confidence`, `epistemic_tag`.
   e. If the claim is tactical (time-sensitive, operational), set `expires_at` to 30 days from now.
5. Extract relationships between entities found in this source.
6. Assign claims to topic clusters. Create new clusters if no existing cluster fits.
7. **Regenerate L0 cache.** This is mandatory — never skip it.
8. Invalidate any L1 caches whose clusters were touched.
9. Append to `ingest_log`.
10. Append to `log.md`:
    ```
    ## [YYYY-MM-DD] ingest | Source Title
    - Source: raw/articles/filename.md
    - Claims: 12 created, 3 updated, 1 superseded
    - Entities: 2 created, 4 existing
    - Contradictions: 1 found (claim_id_a vs claim_id_b)
    ```

**Claim extraction prompt pattern:**

```
You are extracting structured claims from a source document.

For each distinct assertion in the source, produce a JSON object:
{
  "entity": "canonical name of the subject",
  "predicate": "verb or relation (be specific)",
  "value": "the assertion (include units, formulas, specifics)",
  "claim_type": "fact|opinion|metric|status|definition",
  "confidence": 0.0-1.0,
  "source_span": "page/section/paragraph locator"
}

Rules:
- One claim per assertion. Do not merge multiple facts.
- Use existing entity names when possible: [list current entities]
- Include quantitative details: numbers, units, equations, dates.
- Mark opinions and interpretations as claim_type "opinion" with lower confidence.
- If the source says "X is approximately Y", confidence should reflect the approximation.
```

### 2. Register (document registry)

**Trigger:** A governance artifact or project document is created or updated.

**Workflow:**

1. Hash the file content (SHA-256). Check `document.content_hash` for existing match in this collection.
2. Count tokens. Create a `document` row with collection, type, title, tags.
3. Register tag entries in `document_tag`.
4. Register dependencies in `document_dependency` if applicable.
5. Optionally link to the claim pipeline by also creating a `source` row and setting `document.source_id`.
6. Invalidate any relevant L1 caches (`collection:{name}`, `tag:{key}:{value}`).
7. Append to `ingest_log` with `operation = 'register'`.

### 3. Query (Projection)

**Trigger:** The user asks a question, or an agent requests context for a stage/chunk.

**Workflow (see `docs/diagrams/query_projection_flow.mermaid` for full sequence):**

1. **Always read L0.** Load `projection_cache` where `tier = 'L0'` and `scope_key = 'global'`. This is a single SQL read, not a file read.
2. **Evaluate L0 sufficiency.** If the L0 context contains enough information to answer the question with confidence, answer directly. Do not drill deeper unnecessarily.
3. **If stage/chunk context requested — use document registry.**
   a. Query documents by collection, type, tags.
   b. Check for L1 cache hits on `collection:{name}` or `tag:{key}:{value}`.
   c. If cache miss, assemble from documents and write to cache.
4. **If knowledge query — use claim pipeline.**
   a. Run FTS5 query: `SELECT * FROM claim_fts WHERE claim_fts MATCH ?`
   b. Check for L1 cache hits on the relevant `cluster_id` or `entity_id`.
   c. If L1 cache miss, assemble from claims and write to cache.
5. **If cross-cutting or comparative — use graph traversal.**
   a. Query `relationship` table for edges between relevant entities.
   b. Query `contradiction` table for open contradictions.
   c. Load L2 source excerpts if provenance matters.
6. **Assemble context document.** Concatenate L0 + relevant L1/L2 blocks. Enforce token budget by prioritizing: `confidence DESC, updated_at DESC, claim_type = 'fact' first`.
7. **Answer the question** using the assembled context.
8. **Evaluate writeback.** If the answer contains novel synthesis (a new connection, a resolved contradiction, a comparative insight), extract claims from the answer and write them back with `source_type = 'synthesis'`.

**Critical rule:** The projection engine runs as Python/SQLite. It consumes **zero LLM tokens** for retrieval. The LLM only sees the final assembled context document. This is the key latency and cost optimization.

### 4. Lint

**Trigger:** Weekly, or user says "lint" / "health check".

**Checks:**

- **Orphan entities:** Entities with zero claims. Flag for removal or investigation.
- **Orphan claims:** Claims whose `entity_id` references a missing entity. Should never happen — indicates a bug.
- **Open contradictions:** List all `contradiction` rows where `resolution_status = 'open'`. Suggest resolution paths.
- **Stale claims:** Claims where `updated_at` is older than 90 days and `epistemic_tag = 'provisional'`. Suggest re-evaluation.
- **Hub score anomalies:** Entities with high `hub_score` (many claims + relationships) but no L1 cache. Generate one.
- **Missing relationships:** Entities that co-occur in many sources but have no `relationship` row. Suggest creating one.
- **L0 staleness:** Compare `projection_cache.content_hash` for L0 against a fresh generation. If they differ, something was ingested without regenerating L0 — fix immediately.
- **Unbounded growth:** Flag any topic cluster with `claim_count > 200`. Consider splitting.
- **Document drift:** Registered documents whose files have been modified on disk but registry not updated.
- **Missing documents:** Registered documents whose files no longer exist on disk.
- **Stage completeness:** For each governance stage, check whether all required artifact types are present.

**Output:** Write results to `wiki/health_report.md` and append to `log.md`.

### 5. Prune

**Trigger:** Weekly cron, or user says "prune".

**Workflow:**

1. Delete expired claims: `WHERE expires_at < datetime('now') AND expires_at IS NOT NULL`.
2. Remove from FTS5 (triggers handle this automatically).
3. Remove orphaned `claim_cluster` rows.
4. Update `topic_cluster.claim_count`.
5. Regenerate affected L1 caches.
6. If any L0-relevant claims were pruned, regenerate L0.
7. Log the operation.

### 6. Export (wiki generation)

**Trigger:** User says "export wiki" / "make wiki", or after significant ingest.

**Workflow:**

1. Query `v_entity_hub_scores` for all entities, sorted by `hub_score DESC`.
2. For each entity, generate a markdown page:
   - YAML frontmatter: `entity_type`, `hub_score`, `claim_count`, `last_updated`.
   - All active claims, grouped by `predicate`.
   - Relationships (incoming and outgoing), rendered as `[[wikilinks]]`.
   - Source citations.
3. For each topic cluster, generate a topic page with its summary and constituent claims.
4. For each collection, generate a document catalog at `wiki/collections/{name}/index.md`.
5. Generate `wiki/index.md` — catalog of all pages with one-line summaries.
6. Generate `wiki/contradictions.md` — all open contradictions.
7. Generate `wiki/timeline.md` — chronological view from `ingest_log`.
8. Generate `wiki/graph_report.md` — hub entities, orphans, clusters.

**The wiki directory is always a generated artifact.** Never edit files in `wiki/` directly. The source of truth is `db/memory_palace.db`.

---

## Database conventions

- **IDs:** Use ULIDs everywhere (time-sortable, unique). Generate with `src/utils/ulid.py`.
- **Timestamps:** ISO-8601 UTC strings: `2026-04-10T14:30:00Z`. SQLite stores these as TEXT.
- **JSON fields:** `metadata_json`, `aliases_json`, `config_json` are JSON-encoded TEXT columns. Always validate before writing.
- **Confidence scores:** 0.0 to 1.0. Default 0.5 for single-source claims. Increase when multiple sources corroborate. Decrease when contradicted.
- **Epistemic tags:**
  - `confirmed` — multiple independent sources agree, high confidence.
  - `provisional` — single source, moderate confidence. Default for new claims.
  - `contested` — actively contradicted by another claim. Both claims remain active.
  - `retracted` — source discredited or manually flagged. Excluded from projections.

### Active claims view

Always query through `v_active_claims` for projection. This view excludes superseded, retracted, and expired claims:

```sql
SELECT * FROM v_active_claims
WHERE entity_name = 'Alfvén wave'
ORDER BY confidence DESC, updated_at DESC;
```

### Active documents view

Query through `v_active_documents` for document registry lookups. This view excludes superseded and archived documents:

```sql
SELECT * FROM v_active_documents
WHERE collection_name = 'my-collection'
  AND doc_type = 'spec';
```

### FTS5 queries

```sql
-- Keyword search across claims
SELECT rowid, rank FROM claim_fts
WHERE claim_fts MATCH 'magnetic reconnection'
ORDER BY rank;

-- Entity search (includes aliases)
SELECT rowid, rank FROM entity_fts
WHERE entity_fts MATCH 'Josephson junction'
ORDER BY rank;

-- Document search
SELECT rowid, rank FROM document_fts
WHERE document_fts MATCH 'chunk-plan registry'
ORDER BY rank;
```

### Tag-based document queries

```sql
-- All documents for a specific chunk across all collections
SELECT d.* FROM document d
JOIN document_tag t1 ON t1.document_id = d.document_id
WHERE t1.tag_key = 'chunk' AND t1.tag_value = '1.2'
  AND d.status NOT IN ('superseded', 'archived');

-- All documents consumed at the implementation stage
SELECT d.* FROM document d
JOIN document_tag t1 ON t1.document_id = d.document_id
WHERE t1.tag_key = 'consumed_by' AND t1.tag_value = 'implementation'
  AND d.status NOT IN ('superseded', 'archived');
```

---

## L0 cache generation

The L0 cache is the most critical artifact. It must be a dense, token-efficient summary of the entire knowledge base state. Target: **2-4K tokens**.

**L0 structure:**

```markdown
# Knowledge base state — [timestamp]

## Entity registry ([N] entities)
[One line per entity, sorted by hub_score DESC, top 30]
- **[entity_name]** ([entity_type]): [description] — [claim_count] claims

## Document registry ([N] documents across [M] collections)
[One line per collection]
- **[collection_name]**: [doc_count] documents, [type_count] types — last updated [date]

## Hot topics
[Top 5 topic clusters by recent activity]
- [cluster_label]: [summary] — [claim_count] claims, last updated [date]

## Open contradictions ([N])
[One line per open contradiction]
- [entity_a] vs [entity_b]: [description]

## Recent activity
[Last 5 ingest_log entries, one line each]

## Key metrics
- Total sources: [N]
- Total active claims: [N]
- Total entities: [N]
- Total relationships: [N]
- Total documents: [N]
- Total collections: [N]
- Open contradictions: [N]
- Last ingest: [timestamp]
- Last lint: [timestamp]
```

---

## Token budget enforcement

Every context assembly must respect a token budget. Default budgets:

| Query type | Total budget | L0 | L1 | L2 |
|---|---|---|---|---|
| Simple factual | 4K | 2K | 2K | 0 |
| Topic lookup | 8K | 2K | 4K | 2K |
| Stage/chunk context | 12K | 2K | 4K | 6K |
| Cross-cutting analysis | 12K | 2K | 4K | 6K |
| Deep provenance | 16K | 2K | 4K | 10K |

When the assembled context exceeds the budget, trim by:
1. Remove claims with `confidence < 0.3`.
2. Remove claims with `epistemic_tag = 'provisional'` if confirmed claims cover the same predicate.
3. Remove oldest claims first (by `updated_at`).
4. Truncate L2 source excerpts to first 500 tokens each.
5. For document context, prioritize smallest documents first (to fit more), then by recency.

Never trim L0. It is always included in full.

---

## Python environment (mandatory)

This project uses **uv** for dependency management and virtual environment.

**Rules:**

- **Always use `uv run`** to execute Python commands: `uv run pytest`, `uv run python -m src.registry`, etc. This ensures the `.venv` is active and dependencies are resolved. Never call bare `python` or `python3`.
- **Adding dependencies:** `uv add <package>` for runtime deps, `uv add --dev <package>` for dev/test deps.
- **Lock file:** `uv.lock` is checked into git. Run `uv sync` if it's out of date.
- **Python version:** 3.11+ (pinned in `pyproject.toml`).
- **No pip, no conda, no poetry.** uv is the only package manager.

---

## Coding conventions

- **Language:** Python 3.11+. No external dependencies except `tiktoken` for token counting.
- **Database access:** Use `sqlite3` stdlib. Always use parameterized queries. Never interpolate user input.
- **Error handling:** All database writes must be wrapped in transactions. Roll back on any failure.
- **Testing:** Every module in `src/` has a corresponding test in `tests/`. Use `pytest`. Fixtures provide a pre-populated test database.
- **Logging:** Use Python `logging` module. Log level INFO for operations, DEBUG for claim-level detail.
- **No external vector DB.** FTS5 is sufficient. If semantic search is later needed, add a `claim_embedding` BLOB column and use cosine similarity in Python — do not introduce ChromaDB, Pinecone, or similar.
- **Free-text normalization:** All free-text taxonomy fields (`doc_type`, `status`, `dep_type` in the document registry) are normalized to lowercase on write. This prevents case-variant duplicates ("Gherkin" vs "gherkin").

---

## Concurrency model

Astaire is **single-writer by design**. SQLite WAL mode allows concurrent reads but serializes writes. This is the correct model for a local-first CLI tool where one agent session operates at a time.

**Rules:**

- `db.py` sets `PRAGMA busy_timeout = 5000` (5 seconds) on every connection. This prevents immediate `SQLITE_BUSY` errors if a brief write overlap occurs.
- All write operations use short transactions via the `transaction()` context manager. Do not hold transactions open during LLM calls or file I/O.
- L0 regeneration, bulk `scan_and_register()`, and wiki export are the longest write operations. They should commit in batches rather than wrapping the entire operation in a single transaction.
- If multi-agent concurrent access is needed in the future, the solution is a write-ahead queue or a connection pool with retry logic — not removing WAL or changing the schema. Document this as a future option, do not implement it now.

**What NOT to do with concurrency:**

- Do not assume multiple agents can write simultaneously.
- Do not hold transactions open while reading files from disk or waiting for LLM responses.
- Do not retry indefinitely on `SQLITE_BUSY` — if `busy_timeout` expires, surface the error.

---

## What NOT to do

- **Do not edit files in `wiki/`.** They are generated artifacts. Edit the database, then re-export.
- **Do not skip L0 regeneration after ingest.** This is the most common source of stale query results.
- **Do not create claims without source provenance.** Every claim must have a `source_id`. Synthesis claims use `source_type = 'synthesis'` and reference the query that produced them.
- **Do not duplicate entities.** Always check aliases before creating. When in doubt, merge.
- **Do not use the LLM for retrieval.** The projection engine (FTS5 + SQL) handles retrieval. The LLM only sees the final assembled context. This is the core latency optimization.
- **Do not store raw source text in the database.** Sources stay as files in `raw/`. The database stores structured extractions (claims, entities, relationships) and source metadata (path, hash, token count).
- **Do not build a web UI.** The presentation layer is Obsidian reading the `wiki/` directory. The query interface is this Claude Code session.
- **Do not hardcode application-specific types in the core schema.** Document types, statuses, and tag keys are free-text validated at the application layer against `collection.config_json`. The core is collection-agnostic.
- **Do not weaken governance controls.** Local overlays may tighten but never relax the strict-baseline profile. Exceptions require the formal waiver process.

---

## Session startup checklist

**Automated:** The `UserPromptSubmit` hook in `.claude/settings.json` runs `astaire startup` on every prompt, which handles steps 1-4 automatically. The output is injected as hook context.

**Manual fallback** (if hooks are disabled or for debugging):

```bash
uv run astaire startup --root .
```

This single command does:

1. Verify `db/memory_palace.db` exists. If not, initialize from `docs/schema/memory_palace_schema.sql`.
2. Scan governance artifacts (idempotent — skips already-registered files).
3. Sync document registry: check for drift between registered documents and disk state.
4. Check L0 freshness, regenerate if needed, and print L0 status.

**Post-commit hook:** `.git/hooks/post-commit` runs `astaire scan` in the background after every commit, so new governance artifacts are registered automatically.

### CLI quick reference

| Command | Purpose |
|---|---|
| `uv run astaire startup` | Full session startup checklist |
| `uv run astaire status` | Print L0 summary |
| `uv run astaire scan` | Register new collection artifacts |
| `uv run astaire sync` | Detect file drift |
| `uv run astaire query -c my-collection -t spec` | Query by collection/type |
| `uv run astaire query --tag chunk=1.2` | Query by tag |
| `uv run astaire query --fts "keyword"` | Full-text search |
| `uv run astaire context --tag chunk=1.2` | Assemble context for LLM |
| `uv run astaire lint --fix` | Health checks with auto-fix |
| `uv run astaire export` | Generate wiki |
| `uv run astaire prune` | Remove expired claims |
| `uv run astaire ingest FILE --title "..."` | Ingest a source document |

---

## Appendix: Design rationale

This system is a hybrid of three prior approaches:

1. **Karpathy's LLM Wiki** — the wiki-as-compiled-artifact concept, ingest-time synthesis, the index+log pattern. We keep the philosophy but replace markdown-as-storage with structured data.

2. **glaucobrito's Unified Memory** — tiered loading (L0/L1/L2), `wip.md` for session continuity, tactical claim expiry, feedback loops. We adopt the tier model but implement it as cached projections from a database rather than curated markdown files.

3. **Structured claim store** — SQLite with FTS5, typed claims with epistemic status, relationship graph, contradiction detection. This is the novel layer that eliminates file-read penalties and enables O(1) retrieval for most queries.

4. **Document registry** — generic, collection-based file tracking with flexible tagging. Optimized for agent SDLC workflows where structured artifacts need fast retrieval. Collections are auto-discovered plugins; the core is agnostic.

See `docs/diagrams/architecture_comparison.mermaid` for a visual comparison of the first three approaches.

The key insight: **Karpathy's design makes the presentation layer also the storage layer and the query layer. Separating those three concerns unlocks the latency and token savings.** The database is storage. The projection engine is query. The markdown export is presentation. Each is optimized independently.
