"""Tests for src.ingest_graphify — SCN-3.3."""

import json

from src.ingest_graphify import import_graphify
from src.routing import parse_route_hint


def _write_graph(path, *, source_repo="repo-a", graph_version="v1", graph_schema_version="gs1", nodes=None, links=None):
    graph = {
        "source_repo": source_repo,
        "graph_version": graph_version,
        "graph_schema_version": graph_schema_version,
        "nodes": nodes or [],
        "links": links or [],
    }
    path.write_text(json.dumps(graph))
    return path


def test_import_supports_links_shape_and_idempotence(db_conn, tmp_path):
    graph_path = _write_graph(
        tmp_path / "graph.json",
        nodes=[
            {"id": "svc", "label": "Service", "node_type": "service"},
            {"id": "contract", "label": "Contract", "node_type": "contract"},
        ],
        links=[
            {"source": "svc", "target": "contract", "relation": "depends_on", "confidence": "EXTRACTED"},
        ],
    )

    result = import_graphify(db_conn, graph_path, threshold="absolute:2", floor=1, ceiling=10)
    assert result["entities_created"] == 2
    assert result["relationships_created"] == 1
    assert result["claims_created"] == 2

    duplicate = import_graphify(db_conn, graph_path, threshold="absolute:2", floor=1, ceiling=10)
    assert duplicate["duplicate"] is True
    assert duplicate["selected_nodes"] == 2
    assert duplicate["effective_threshold"] == "absolute:2"
    assert duplicate["graph_schema_version"] == "gs1"
    assert duplicate["source_repo"] == "repo-a"
    assert duplicate["entities_created"] == 0
    assert duplicate["relationships_created"] == 0
    assert duplicate["claims_created"] == 0

    ingest_logs = db_conn.execute(
        "SELECT operation, summary FROM ingest_log WHERE operation = 'ingest' ORDER BY created_at"
    ).fetchall()
    assert len(ingest_logs) == 2
    assert ingest_logs[-1]["summary"].startswith("graphify verification:")

    l0 = db_conn.execute(
        "SELECT content_md FROM projection_cache WHERE tier = 'L0' AND scope_key = 'global'"
    ).fetchone()["content_md"]
    assert "graphify verification:" in l0


def test_p90_base_selection_is_padded_when_needed_for_structure(db_conn, tmp_path):
    nodes = [{"id": f"n{i}", "label": f"Node {i}", "node_type": "service"} for i in range(10)]
    links = [{"source": "n0", "target": f"n{i}", "relation": "depends_on", "confidence": "EXTRACTED"} for i in range(1, 10)]
    graph_path = _write_graph(tmp_path / "graph.json", nodes=nodes, links=links)

    result = import_graphify(db_conn, graph_path, threshold="p90", floor=1, ceiling=100)
    assert result["selected_nodes"] == 2
    assert result["relationships_created"] == 1


def test_pinned_nodes_consume_slots(db_conn, tmp_path):
    nodes = [{"id": f"n{i}", "label": f"Node {i}", "node_type": "service"} for i in range(10)]
    links = [{"source": "n0", "target": f"n{i}", "relation": "depends_on", "confidence": "EXTRACTED"} for i in range(1, 10)]
    graph_path = _write_graph(tmp_path / "graph.json", nodes=nodes, links=links)

    result = import_graphify(
        db_conn,
        graph_path,
        threshold="p90",
        floor=1,
        ceiling=100,
        pinned_nodes=["n5"],
    )
    assert result["selected_nodes"] == 2
    names = {
        row["canonical_name"]
        for row in db_conn.execute("SELECT canonical_name FROM entity").fetchall()
    }
    assert "node 5" in names


def test_inferred_edges_require_numeric_threshold(db_conn, tmp_path):
    graph_path = _write_graph(
        tmp_path / "graph.json",
        nodes=[
            {"id": "a", "label": "A", "node_type": "service"},
            {"id": "b", "label": "B", "node_type": "service"},
            {"id": "c", "label": "C", "node_type": "service"},
        ],
        links=[
            {"source": "a", "target": "b", "relation": "depends_on", "confidence": "INFERRED", "confidence_score": 0.95},
            {"source": "a", "target": "c", "relation": "depends_on", "confidence": "INFERRED", "confidence_score": 0.50},
            {"source": "b", "target": "c", "relation": "depends_on", "confidence": "AMBIGUOUS"},
        ],
    )

    result = import_graphify(
        db_conn,
        graph_path,
        threshold="absolute:3",
        floor=1,
        ceiling=10,
        inferred_edge_threshold=0.90,
    )
    assert result["relationships_created"] == 1
    assert result["skipped_inferred"] == 1
    assert result["skipped_ambiguous"] == 1


def test_same_repo_fuzzy_match_merges(db_conn, tmp_path):
    graph_a = _write_graph(
        tmp_path / "graph-a.json",
        source_repo="repo-a",
        graph_version="v1",
        nodes=[{"id": "auth_a", "label": "Auth Service", "node_type": "service"}],
    )
    graph_b = _write_graph(
        tmp_path / "graph-b.json",
        source_repo="repo-a",
        graph_version="v2",
        nodes=[{"id": "auth_b", "label": "auth_service", "node_type": "service"}],
    )

    import_graphify(db_conn, graph_a, threshold="absolute:1", floor=1, ceiling=10)
    result = import_graphify(db_conn, graph_b, threshold="absolute:1", floor=1, ceiling=10)
    assert result["entities_existing"] == 1


def test_cross_repo_authority_can_preserve_distinct_entities(db_conn, tmp_path):
    graph_a = _write_graph(
        tmp_path / "graph-a.json",
        source_repo="repo-b",
        graph_version="v1",
        nodes=[{"id": "contract", "label": "Shared Contract", "node_type": "contract", "source_file": "contracts/shared.md"}],
    )
    graph_b = _write_graph(
        tmp_path / "graph-b.json",
        source_repo="repo-c",
        graph_version="v2",
        nodes=[{"id": "contract", "label": "Shared Contract", "node_type": "contract", "source_file": "contracts/shared.md"}],
    )

    import_graphify(db_conn, graph_a, threshold="absolute:1", floor=1, ceiling=10)
    result = import_graphify(
        db_conn,
        graph_b,
        threshold="absolute:1",
        floor=1,
        ceiling=10,
        cross_repo_authority=[{"repo": "repo-a", "namespaces": ["contracts/*"]}],
    )
    assert result["entities_created"] == 1
    assert db_conn.execute("SELECT COUNT(*) FROM entity").fetchone()[0] == 2


def test_annotate_approval_status_creates_claim(db_conn, tmp_path):
    registry = tmp_path / "contracts.json"
    registry.write_text(json.dumps([{"id": "shared_contract", "approval_status": "approved"}]))
    graph_path = _write_graph(
        tmp_path / "graph.json",
        nodes=[{"id": "shared_contract", "label": "Shared Contract", "node_type": "contract"}],
    )

    result = import_graphify(
        db_conn,
        graph_path,
        threshold="absolute:1",
        floor=1,
        ceiling=10,
        annotate_approval_status=True,
        contract_registry_path=registry,
    )
    assert result["claims_created"] == 2
    claims = db_conn.execute("SELECT predicate, value FROM claim ORDER BY predicate").fetchall()
    assert claims[0]["predicate"] == "approval_status"
    assert claims[0]["value"] == "approved"
    assert claims[1]["predicate"] == "graphify_origin"


def test_auto_tune_reduces_selected_nodes_for_tiny_budget(db_conn, tmp_path):
    nodes = [{"id": f"n{i}", "label": f"Node {i}", "node_type": "service"} for i in range(5)]
    links = [{"source": "n0", "target": f"n{i}", "relation": "depends_on", "confidence": "EXTRACTED"} for i in range(1, 5)]
    graph_path = _write_graph(tmp_path / "graph.json", nodes=nodes, links=links)

    result = import_graphify(
        db_conn,
        graph_path,
        threshold="absolute:5",
        floor=1,
        ceiling=10,
        auto_tune=True,
        l0_token_budget=1,
    )
    assert result["auto_tuned"] is True
    assert result["selected_nodes"] == 2


def test_promoted_nodes_emit_graphify_origin_claims(db_conn, tmp_path):
    graph_path = _write_graph(
        tmp_path / "graph.json",
        nodes=[
            {
                "id": "svc",
                "label": "Service",
                "node_type": "service",
                "source_file": "src/service.py",
                "source_location": "L10",
            }
        ],
    )
    result = import_graphify(db_conn, graph_path, threshold="absolute:1", floor=1, ceiling=10)
    assert result["claims_created"] == 1
    claim = db_conn.execute("SELECT predicate, value, claim_type, epistemic_tag FROM claim").fetchone()
    assert claim["predicate"] == "graphify_origin"
    assert "repo=repo-a" in claim["value"]
    assert "node_id=svc" in claim["value"]
    assert "source_file=src/service.py" in claim["value"]
    assert claim["claim_type"] == "fact"
    assert claim["epistemic_tag"] == "confirmed"


def test_changed_graph_reconciles_relationship_diff(db_conn, tmp_path):
    graph_v1 = _write_graph(
        tmp_path / "graph-v1.json",
        source_repo="repo-a",
        graph_version="v1",
        nodes=[
            {"id": "a", "label": "A", "node_type": "service"},
            {"id": "b", "label": "B", "node_type": "service"},
            {"id": "c", "label": "C", "node_type": "service"},
        ],
        links=[{"source": "a", "target": "b", "relation": "depends_on", "confidence": "EXTRACTED"}],
    )
    graph_v2 = _write_graph(
        tmp_path / "graph-v2.json",
        source_repo="repo-a",
        graph_version="v2",
        nodes=[
            {"id": "a", "label": "A", "node_type": "service"},
            {"id": "b", "label": "B", "node_type": "service"},
            {"id": "c", "label": "C", "node_type": "service"},
        ],
        links=[{"source": "a", "target": "c", "relation": "depends_on", "confidence": "EXTRACTED"}],
    )

    import_graphify(db_conn, graph_v1, threshold="absolute:3", floor=1, ceiling=10)
    result = import_graphify(db_conn, graph_v2, threshold="absolute:3", floor=1, ceiling=10)
    assert result["relationships_created"] == 1
    assert result["relationships_removed"] == 1
    rel_targets = {
        row["to_entity_id"]
        for row in db_conn.execute("SELECT to_entity_id FROM relationship").fetchall()
    }
    assert len(rel_targets) == 1


def test_changed_import_options_bypass_cache_and_reconcile_same_source(db_conn, tmp_path):
    graph_path = _write_graph(
        tmp_path / "graph.json",
        source_repo="repo-a",
        graph_version="v1",
        nodes=[
            {"id": "a", "label": "A", "node_type": "service"},
            {"id": "b", "label": "B", "node_type": "service"},
            {"id": "c", "label": "C", "node_type": "service"},
        ],
        links=[
            {"source": "a", "target": "b", "relation": "depends_on", "confidence": "EXTRACTED"},
            {"source": "a", "target": "c", "relation": "depends_on", "confidence": "EXTRACTED"},
        ],
    )

    initial = import_graphify(db_conn, graph_path, threshold="absolute:3", floor=1, ceiling=10)
    assert initial["duplicate"] is False
    assert db_conn.execute("SELECT COUNT(*) FROM relationship").fetchone()[0] == 2

    updated = import_graphify(db_conn, graph_path, threshold="absolute:2", floor=1, ceiling=10)
    assert updated["duplicate"] is False
    assert updated["selected_nodes"] == 2
    assert updated["relationships_created"] == 0
    assert updated["relationships_removed"] == 1
    assert db_conn.execute("SELECT COUNT(*) FROM relationship").fetchone()[0] == 1


def test_route_hint_fixture_parses(tmp_path):
    fixture = tmp_path / "routing_hints.md"
    fixture.write_text(
        "route: tentacle=astaire.l1; target=collection:ai-dev-governance; budget=4000; returns=context-bundle; reason=chunk-context\n"
        "route: tentacle=graphify.query; target=graphify-out/graph.json; budget=6000; returns=path-traversal; reason=policy-graph\n"
    )

    parsed = [parse_route_hint(line) for line in fixture.read_text().splitlines() if line.strip()]
    assert parsed[0]["tentacle"] == "astaire.l1"
    assert parsed[1]["tentacle"] == "graphify.query"
