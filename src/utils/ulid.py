"""ULID generation — pure Python, no external dependencies.

ULIDs are 26-character Crockford base32 strings: 10 chars timestamp + 16 chars randomness.
Time-sortable, globally unique, suitable for SQLite TEXT primary keys.
"""

import os
import time

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def generate() -> str:
    """Generate a new ULID using current time and OS randomness."""
    ts_ms = int(time.time() * 1000)
    rand_bytes = os.urandom(10)

    # Encode 48-bit timestamp as 10 Crockford base32 chars
    ts_chars = []
    for _ in range(10):
        ts_chars.append(_CROCKFORD[ts_ms & 0x1F])
        ts_ms >>= 5
    ts_part = "".join(reversed(ts_chars))

    # Encode 80-bit randomness as 16 Crockford base32 chars
    rand_int = int.from_bytes(rand_bytes, "big")
    rand_chars = []
    for _ in range(16):
        rand_chars.append(_CROCKFORD[rand_int & 0x1F])
        rand_int >>= 5
    rand_part = "".join(reversed(rand_chars))

    return ts_part + rand_part


def timestamp_ms(ulid: str) -> int:
    """Extract the millisecond timestamp from a ULID string."""
    if len(ulid) != 26:
        raise ValueError(f"ULID must be 26 characters, got {len(ulid)}")

    _DECODE = {c: i for i, c in enumerate(_CROCKFORD)}
    # Also accept lowercase
    _DECODE.update({c.lower(): i for i, c in enumerate(_CROCKFORD)})

    ts = 0
    for ch in ulid[:10]:
        ts = (ts << 5) | _DECODE[ch]
    return ts
