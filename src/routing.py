"""Routing-hint helpers for L0 hand-off lines."""

from __future__ import annotations


def format_route_hint(
    tentacle: str,
    target: str,
    budget: int,
    returns: str,
    reason: str,
) -> str:
    return (
        "route: "
        f"tentacle={tentacle}; "
        f"target={target}; "
        f"budget={budget}; "
        f"returns={returns}; "
        f"reason={reason}"
    )


def parse_route_hint(line: str) -> dict[str, str]:
    if not line.startswith("route: "):
        raise ValueError("routing hint must start with 'route: '")

    parts = [part.strip() for part in line[len("route: "):].split(";")]
    result: dict[str, str] = {}
    for part in parts:
        if not part:
            continue
        key, sep, value = part.partition("=")
        if not sep or not key or not value:
            raise ValueError(f"invalid routing segment: {part!r}")
        result[key.strip()] = value.strip()

    for required in ("tentacle", "target", "budget", "returns", "reason"):
        if required not in result:
            raise ValueError(f"missing routing field: {required}")
    return result
