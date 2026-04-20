"""Tests for manifest helpers in src.governance."""

from src.governance import load_graphify_config


def test_load_graphify_config_parses_inline_empty_lists(tmp_path):
    (tmp_path / "governance.yaml").write_text(
        "profile: strict-baseline\n"
        "graphify:\n"
        "  pinnedNodes: []\n"
        "  crossRepoAuthority: []\n"
    )

    config = load_graphify_config(tmp_path)
    assert config["pinnedNodes"] == []
    assert config["crossRepoAuthority"] == []


def test_load_graphify_config_parses_inline_scalar_lists(tmp_path):
    (tmp_path / "governance.yaml").write_text(
        "profile: strict-baseline\n"
        "graphify:\n"
        "  allowlist: [README.md, docs/**/*.md]\n"
    )

    config = load_graphify_config(tmp_path)
    assert config["allowlist"] == ["README.md", "docs/**/*.md"]
