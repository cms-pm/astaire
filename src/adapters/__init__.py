"""Adapter implementations for the memory-palace domain ports.

Subpackages (e.g. `sqlite/`) wrap concrete I/O collaborators —
`sqlite3`, FTS5, the filesystem — and expose them through the
Protocols declared under `astaire/src/domain/claims/ports.py`.

The architecture-fitness validator (`scripts/validators/architecture_fitness.py
--audit` in the ADG repo) does not constrain this package: adapters are
the place where I/O imports live.
"""
