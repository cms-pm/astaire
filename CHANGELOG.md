# Changelog

All notable changes to this project are documented in this file.

## [0.6.1] - 2026-07-26

### Fixed
- `lint` no longer reports a false `l0_staleness` error on every run after
  the first. `run_all_checks()` writes its own `ingest_log(operation='lint')`
  row after the L0 cache is generated, so that entry's timestamp and its
  appearance in the L0 "Recent activity" feed made the cache the tool had
  just written look stale on the very next invocation, regardless of
  `--fix`. `check_l0_staleness()`'s hash comparison now excludes the
  `Last lint` field, and `build_l0_content()`'s Recent Activity query now
  excludes `operation='lint'` rows, matching the existing exclusion for
  `recompile`/`query`.
