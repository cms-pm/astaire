# Changelog

All notable changes to this project are documented in this file.

## [0.6.2] - 2026-07-26

Note: `0.6.1` was written to `CHANGELOG.md` and `pyproject.toml` but never
tagged. `v0.6.2` is therefore the first tag since `v0.6.0` and carries the
`0.6.1` lint fix below in addition to the changes in this entry.

### Added
- Hexagonal pilot for the claims/projection slice (SCN-9.4): `src/domain/claims/`
  holds framework-free `models.py` and `ports.py`; `src/adapters/sqlite/`
  implements those ports as `claim_repo.py`, `fts_index.py`, and
  `projection_cache.py`. This is a pilot only — no existing call site is
  rewired to the new layer, so runtime behaviour is unchanged.
  `tests/test_characterisation_phase9.py` pins current behaviour ahead of the
  Phase 10 refactors.
- Mutation baseline harness (SCN-9.5): `mutmut.ini`, `cosmic-ray.toml`, and the
  `[tool.mutmut]` block in `pyproject.toml`, scoped to
  `src/domain/claims/models.py`, establishing a mutation-score baseline for the
  new domain layer before it takes production traffic.
- Collection path-to-type entries for Phase 9 evidence artifacts and the
  provider-skill path (SCN-9.2, SCN-9.3), plus fractional `phase` tag support
  (`9.2`, `9.4`) and `evaluations/` + `profiles/` mappings, so Phase 9
  artifacts register with the correct doc-type instead of falling through to
  the default.

### Changed
- README and guide updates covering the optional claims module and the current
  CLI surface.

### Fixed
- `astaire doctor` characterisation expectations reconciled with the optional
  claims module, which inserts an `[INFO] Claims module: ...` line into that
  command's output.
- `uv.lock` version synced to `pyproject.toml`; the lock had been left behind
  at `0.6.0`.

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
