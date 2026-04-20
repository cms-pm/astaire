"""Tests for routing-hint parsing."""

from pathlib import Path

from src.routing import parse_route_hint


def test_fixture_routing_hints_parse():
    fixture = Path(__file__).parent / "fixtures" / "routing_hints.md"
    parsed = [
        parse_route_hint(line)
        for line in fixture.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(parsed) == 6
    assert {entry["tentacle"] for entry in parsed} == {
        "astaire.l1",
        "astaire.l2",
        "graphify.report",
        "graphify.query",
        "graphify.mcp",
        "rtk.shell",
    }
