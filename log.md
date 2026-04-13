# Memory Palace — Operation Log

Append-only record of all operations. Each entry uses a consistent prefix for parseability.

```bash
# Last 5 entries
grep "^## \[" log.md | tail -5

# All ingests
grep "ingest" log.md

# All lint passes
grep "lint" log.md
```

---

