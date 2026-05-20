"""Astaire domain package — I/O-free types and ports.

Per the SCN-9.4 hexagonal pilot (memory-palace bounded context), modules
under this package MUST NOT import `sqlite3`, `src.db`, or any
FTS-coupled symbol. The architecture-fitness validator
(`scripts/validators/architecture_fitness.py --audit` in the ADG repo)
enforces this rule against `astaire/src/domain/claims/`.
"""
