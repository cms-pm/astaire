# ai-dev-governance Integration Guide

This guide explains how to use Astaire as the artifact backend for the [ai-dev-governance](https://github.com/anthropics/ai-dev-governance) methodology.

## What Astaire Provides

Astaire gives ai-dev-governance workflows:

- **Indexed artifact tracking** — every governance artifact (Gherkin scenarios, chunk plans, board findings, signoffs, traceability matrices) is registered with type, tags, and content hash
- **Full-text search** — find any artifact by keyword across all phases and chunks
- **Tag-based queries** — retrieve all artifacts for a specific phase, chunk, or lifecycle stage
- **Context assembly** — assemble token-budgeted context for LLM consumption
- **Drift detection** — detect when registered files have been modified on disk
- **Health checks** — lint for missing documents, orphan entities, stale caches

## Setup

### 1. Install Astaire

```bash
cd your-project
git clone <astaire-repo-url> astaire/
cd astaire
uv sync
```

### 2. Initialize and Scan

```bash
# Initialize the database
uv run astaire init

# Scan governance artifacts
uv run astaire scan --root /path/to/your/project

# Or run the full startup checklist
uv run astaire startup --root /path/to/your/project
```

### 3. Configure Claude Code Hooks

Add to `.claude/settings.json` in your project:

```json
{
  "hooks": {
    "UserPromptSubmit": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": "uv run astaire startup --root \"$(git rev-parse --show-toplevel)\" 2>/dev/null || true",
            "timeout": 10000
          }
        ]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "Bash(git commit*)",
        "hooks": [
          {
            "type": "command",
            "command": "uv run astaire scan --root \"$(git rev-parse --show-toplevel)\" 2>/dev/null || true",
            "timeout": 10000
          }
        ]
      }
    ]
  }
}
```

This automatically:
- Runs the full startup checklist on every Claude Code prompt
- Scans for new governance artifacts after every git commit

## Scan Rules

The ai-dev-governance collection maps these file patterns:

| Path Pattern | Document Type | Base Tags |
|-------------|---------------|-----------|
| `docs/planning/pool_questions/` | pool-question | stage_produced=plan |
| `docs/planning/scenarios/` | gherkin | stage_produced=artifact-generation |
| `docs/planning/chunks/` | chunk-plan | stage_produced=plan |
| `docs/planning/signoffs.md` | signoff | stage_produced=plan |
| `docs/planning/traceability.md` | traceability | stage_produced=artifact-generation |
| `docs/planning/phase-*` | risk-log | stage_produced=plan |
| `docs/planning/board/board-selection-*` | board-selection | stage_produced=plan |
| `docs/planning/board/committee-review-packet-*` | board-packet | stage_produced=board-review |
| `docs/planning/board/committee-virtual-meeting-*` | meeting-record | stage_produced=board-review |
| `docs/planning/board/members/` | board-member-profile | stage_produced=plan |
| `docs/plan/implementation-plan.md` | implementation-plan | stage_produced=plan |
| `docs/governance/exceptions.yaml` | exception-registry | — |
| `governance.yaml` | governance-manifest | stage_produced=ingest |

Additional tags are extracted automatically:
- **phase** — from `phase-N` in filenames
- **chunk** — from `chunk-X.Y` or `SCN-X.Y` in filenames

## Query Patterns

### All artifacts for a specific chunk

```bash
uv run astaire query --tag chunk=1.2
```

### All Gherkin scenarios

```bash
uv run astaire query -c ai-dev-governance -t gherkin
```

### All artifacts for a phase

```bash
uv run astaire query --tag phase=3
```

### Assemble context for a chunk implementation

```bash
uv run astaire context --tag chunk=1.2 --budget 8000
```

This assembles:
1. L0 global summary (always included)
2. L1 collection digest (if cached)
3. Document content for matching files (within budget)

### Full-text search

```bash
uv run astaire query --fts "contradiction detection"
```

### Board artifacts

```bash
uv run astaire query -c ai-dev-governance -t meeting-record
uv run astaire query -c ai-dev-governance -t board-packet
```

## Health Checks

```bash
# Run all health checks
uv run astaire lint

# Auto-fix safe issues (regenerate stale caches)
uv run astaire lint --fix
```

Checks relevant to governance workflows:
- **Document drift** — files modified since registration
- **Missing documents** — registered files deleted from disk
- **L0 staleness** — projection cache out of date
- **Stage completeness** — all required artifact types present

## How It Works

1. **Scan** — `astaire scan` walks the project directory, matches files against scan rules, and registers them as documents with types and tags
2. **Index** — each document gets a content hash, token count, and FTS5 index entry
3. **Project** — the projection engine compiles L0/L1/L2 summaries from the indexed data
4. **Query** — queries run against SQLite indexes (zero LLM tokens for retrieval)
5. **Assemble** — context assembly reads files within a token budget, prioritizing by size and recency
