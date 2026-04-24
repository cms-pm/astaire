"""Astaire CLI — command-line interface for the memory palace.

Subcommands: init, status, scan, query, lint, export, prune, sync, ingest, startup, bench.
All commands use the production DB at db/memory_palace.db by default.
"""

import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path

from src.db import DB_PATH, managed_connection, init_db


def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )


# ── Subcommands ──────────────────────────────────────────────────


def cmd_init(args: argparse.Namespace) -> None:
    """Initialize the database schema."""
    with managed_connection(args.db) as conn:
        init_db(conn)
    print(f"Database initialized: {args.db or DB_PATH}")


def cmd_status(args: argparse.Namespace) -> None:
    """Show knowledge base status (L0 summary)."""
    from src.lint import check_l0_staleness
    from src.project import generate_l0, read_l0

    with managed_connection(args.db) as conn:
        l0 = read_l0(conn)
        if l0 is None or check_l0_staleness(conn, fix=False):
            l0 = generate_l0(conn)
        print(l0)


def cmd_doctor(args: argparse.Namespace) -> None:
    """Check runtime readiness and common failure modes."""
    from src.utils import tokens

    db_target = Path(args.db).resolve() if args.db else DB_PATH
    db_dir = db_target.parent
    failures = 0

    if db_dir.exists():
        print(f"[PASS] Database directory exists: {db_dir}")
    else:
        print(f"[FAIL] Database directory missing: {db_dir}")
        failures += 1

    schema_ok = False
    try:
        with managed_connection(args.db) as conn:
            required_tables = {"collection", "document", "entity", "source", "claim"}
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            present = {row[0] for row in rows}
            missing = sorted(required_tables - present)
            schema_ok = not missing
            if schema_ok:
                print("[PASS] Database schema initialized")
            else:
                print("[WARN] Database schema incomplete or not initialized")
                print("       Run 'astaire init' or 'astaire startup' to create the schema.")
    except SystemExit as exc:
        print(str(exc))
        failures += 1
    except sqlite3.Error as exc:
        print(f"[FAIL] Database connection failed: {exc}")
        failures += 1

    tokenizer = tokens.check_tokenizer_health()
    if tokenizer["ok"]:
        print(f"[PASS] {tokenizer['message']}")
    else:
        status = "WARN" if tokenizer["approx_tokens_enabled"] else "FAIL"
        print(f"[{status}] {tokenizer['message']}")
        if tokenizer["approx_tokens_enabled"]:
            print("       Approximate token fallback is enabled; exact ingest budgets are degraded.")
        else:
            failures += 1

    approx_state = "enabled" if tokenizer["approx_tokens_enabled"] else "disabled"
    print(f"[INFO] Approximate token fallback: {approx_state}")

    if failures:
        raise SystemExit(1)


def cmd_scan(args: argparse.Namespace) -> None:
    """Scan and register collection artifacts."""
    from src.collections.discovery import (
        discover_collection_modules,
        register_all_collections,
        scan_all_collections,
    )

    with managed_connection(args.db) as conn:
        root = Path(args.root).resolve()

        if args.collection:
            # Scan a specific collection only
            modules = discover_collection_modules()
            target = [m for m in modules if m.COLLECTION_NAME == args.collection]
            if not target:
                print(f"Error: no collection module found for {args.collection!r}")
                sys.exit(1)
            mod = target[0]
            mod.register_collection(conn)
            results = mod.scan_and_register(conn, root)
        else:
            register_all_collections(conn)
            results = scan_all_collections(conn, root)

        print(f"Registered {len(results)} new document(s)")
        for doc in results:
            print(f"  {doc['doc_type']:20s} {doc['title']}")


def cmd_query(args: argparse.Namespace) -> None:
    """Query documents from the registry."""
    from src.registry import query_documents, search_documents

    with managed_connection(args.db) as conn:
        if args.fts:
            docs = search_documents(conn, args.fts)
        else:
            tags = None
            if args.tag:
                tags = {}
                for t in args.tag:
                    key, _, value = t.partition("=")
                    tags[key] = value
            docs = query_documents(
                conn,
                collection_name=args.collection,
                doc_type=args.type,
                tags=tags,
                status=args.status,
            )

        if args.json:
            for d in docs:
                if not isinstance(d.get("tags"), dict):
                    d.pop("tags", None)
            print(json.dumps(docs, indent=2, default=str))
        else:
            print(f"{len(docs)} document(s) found\n")
            for d in docs:
                ext = f" ({d.get('external_id')})" if d.get("external_id") else ""
                tags_str = ""
                if d.get("tags"):
                    tag_parts = [f"{k}={','.join(v) if isinstance(v, list) else v}"
                                 for k, v in d["tags"].items()]
                    tags_str = f"  [{', '.join(tag_parts)}]"
                print(f"  {d['doc_type']:20s} {d['title']}{ext}{tags_str}")


def cmd_context(args: argparse.Namespace) -> None:
    """Assemble context for a collection, tag, or chunk."""
    from src.project import assemble_query_context

    with managed_connection(args.db) as conn:
        tags = None
        if args.tag:
            tags = {}
            for t in args.tag:
                key, _, value = t.partition("=")
                tags[key] = value

        ctx = assemble_query_context(
            conn,
            collection_name=args.collection,
            tags=tags,
            token_budget=args.budget,
        )
        print(ctx)


def cmd_lint(args: argparse.Namespace) -> None:
    """Run health checks."""
    from src.lint import run_all_checks

    with managed_connection(args.db) as conn:
        results = run_all_checks(conn, fix=args.fix)

        for check_name, issues in results.items():
            if check_name in ("total_warnings", "total_errors"):
                continue
            if not isinstance(issues, list) or not issues:
                continue
            print(f"\n{check_name}:")
            for issue in issues:
                sev = issue.get("severity", "info")
                msg = issue.get("message", "")
                fixed = " [FIXED]" if issue.get("fixed") else ""
                print(f"  [{sev:7s}] {msg}{fixed}")

        print(f"\nTotal: {results['total_warnings']} warnings, {results['total_errors']} errors")


def cmd_export(args: argparse.Namespace) -> None:
    """Export wiki from database."""
    from src.export import export_wiki

    with managed_connection(args.db) as conn:
        output = args.output or "wiki/"
        stats = export_wiki(conn, output)
        print(f"Wiki exported to {output}")
        print(f"  {stats['entities']} entity pages, {stats['collections']} collection indexes, {stats['total_pages']} total")


def cmd_prune(args: argparse.Namespace) -> None:
    """Prune expired claims."""
    from src.prune import prune_expired_claims

    with managed_connection(args.db) as conn:
        stats = prune_expired_claims(conn)
        if stats["claims_pruned"] == 0:
            print("Nothing to prune.")
        else:
            print(f"Pruned {stats['claims_pruned']} claim(s), cleaned {stats['clusters_cleaned']} cluster assignment(s)")


def cmd_sync(args: argparse.Namespace) -> None:
    """Check all registered documents for drift."""
    from src.lint import check_l0_staleness
    from src.project import generate_l0, read_l0
    from src.registry import sync_all, sync_collection

    with managed_connection(args.db) as conn:
        if args.collection:
            changes = sync_collection(conn, args.collection)
        else:
            changes = sync_all(conn)

        if changes or read_l0(conn) is None or check_l0_staleness(conn, fix=False):
            generate_l0(conn)

        if not changes:
            print("All documents up to date.")
        else:
            print(f"{len(changes)} document(s) changed:")
            for c in changes:
                status = "MISSING" if c.get("missing") else "MODIFIED"
                title = c.get("title", c["document_id"])
                path = c.get("file_path", "")
                print(f"  [{status}] {title}  ({path})")


def cmd_startup(args: argparse.Namespace) -> None:
    """Session startup checklist: init if needed, scan, sync, status."""
    from src.collections.discovery import register_all_collections, scan_all_collections
    from src.lint import check_l0_staleness
    from src.project import generate_l0, read_l0
    from src.registry import sync_all

    with managed_connection(args.db) as conn:
        root = Path(args.root).resolve()

        # 1. Ensure schema exists
        init_db(conn)

        # 2. Scan all collection artifacts
        register_all_collections(conn)
        new_docs = scan_all_collections(conn, root)
        if new_docs:
            print(f"Scan: {len(new_docs)} new document(s) registered")

        # 3. Sync for drift
        changes = sync_all(conn)
        if changes:
            modified = [c for c in changes if not c.get("missing")]
            missing = [c for c in changes if c.get("missing")]
            parts = []
            if modified:
                parts.append(f"{len(modified)} modified")
            if missing:
                parts.append(f"{len(missing)} missing")
            print(f"Sync: {', '.join(parts)}")

        # 4. Check L0 freshness and regenerate if needed
        l0 = read_l0(conn)
        if new_docs or changes or l0 is None or check_l0_staleness(conn, fix=False):
            l0 = generate_l0(conn)

        # 5. Print status
        print(l0)


def cmd_ingest(args: argparse.Namespace) -> None:
    """Ingest a source document with optional pre-extracted claims."""
    from src.ingest import ingest_source

    with managed_connection(args.db) as conn:
        claims = None
        if args.claims:
            claims_path = Path(args.claims)
            if not claims_path.exists():
                raise SystemExit(f"Error: claims file not found: {claims_path}")
            claims = json.loads(claims_path.read_text())

        result = ingest_source(
            conn,
            file_path=args.file,
            source_type=args.source_type,
            title=args.title,
            claims=claims,
        )

        if result["duplicate"]:
            print(f"Duplicate source (already ingested): {result['source_id']}")
        else:
            print(f"Ingested: {result['source_id']}")
            print(f"  Entities created: {result['entities_created']}")
            print(f"  Claims created: {result['claims_created']}")
            print(f"  Contradictions found: {result['contradictions_found']}")


def cmd_graphify_import(args: argparse.Namespace) -> None:
    """Import graphify output into the claim store."""
    from src.governance import load_contract_registry_path, load_graphify_config
    from src.ingest_graphify import import_graphify

    root = Path(args.root).resolve()
    graphify_config = load_graphify_config(root)

    graph_path = Path(args.graph) if args.graph else root / "graphify-out" / "graph.json"
    threshold = args.threshold or graphify_config.get("promotionThreshold", "p90")
    floor = args.floor if args.floor is not None else graphify_config.get("promotionFloor", 3)
    ceiling = args.ceiling if args.ceiling is not None else graphify_config.get("promotionCeiling", 100)
    pinned_nodes = args.pinned_node or graphify_config.get("pinnedNodes") or []
    inferred_edge_threshold = (
        args.inferred_edge_threshold
        if args.inferred_edge_threshold is not None
        else graphify_config.get("inferredEdgeThreshold")
    )
    auto_tune = args.auto_tune or bool(graphify_config.get("autoTune", False))
    annotate_approval_status = (
        args.annotate_approval_status
        or bool(graphify_config.get("annotateApprovalStatus", False))
    )
    contract_registry_path = args.contract_registry or load_contract_registry_path(root)

    with managed_connection(args.db) as conn:
        result = import_graphify(
            conn,
            graph_json_path=graph_path,
            threshold=threshold,
            floor=floor,
            ceiling=ceiling,
            pinned_nodes=pinned_nodes,
            inferred_edge_threshold=inferred_edge_threshold,
            source_repo=graphify_config.get("sourceRepoTag"),
            cross_repo_authority=graphify_config.get("crossRepoAuthority") or [],
            annotate_approval_status=annotate_approval_status,
            contract_registry_path=contract_registry_path,
            auto_tune=auto_tune,
            l0_token_budget=args.l0_budget,
        )

    state = "no-op" if result["duplicate"] else "applied"
    print(f"Graphify import: {result['graph_version']} ({state})")
    print(f"  Selected nodes: {result['selected_nodes']}")
    print(f"  Effective threshold: {result['effective_threshold']}")
    print(f"  Entities created: {result['entities_created']}")
    print(f"  Relationships created: {result['relationships_created']}")
    print(f"  Relationships updated: {result['relationships_updated']}")
    print(f"  Relationships removed: {result['relationships_removed']}")
    print(f"  Claims created: {result['claims_created']}")
    print(f"  Claims updated: {result['claims_updated']}")
    print(f"  Claims superseded: {result['claims_superseded']}")
    print(f"  Skipped inferred: {result['skipped_inferred']}")
    print(f"  Skipped ambiguous: {result['skipped_ambiguous']}")


# ── Parser ───────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="astaire",
        description="Astaire — hybrid memory palace CLI",
    )
    parser.add_argument("--db", default=None, help="Database path (default: db/memory_palace.db)")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command")

    # init
    sub.add_parser("init", help="Initialize database schema")

    # startup
    p_startup = sub.add_parser("startup", help="Session startup: init + scan + sync + status")
    p_startup.add_argument("--root", default=".", help="Project root directory")

    # status
    sub.add_parser("status", help="Show knowledge base status")

    # doctor
    sub.add_parser("doctor", help="Check runtime readiness and common failure causes")

    # scan
    p_scan = sub.add_parser("scan", help="Scan and register collection artifacts")
    p_scan.add_argument("--root", default=".", help="Project root directory")
    p_scan.add_argument("-c", "--collection", help="Scan only this collection")

    # query
    p_query = sub.add_parser("query", help="Query documents")
    p_query.add_argument("-c", "--collection", help="Filter by collection name")
    p_query.add_argument("-t", "--type", help="Filter by doc_type")
    p_query.add_argument("-s", "--status", help="Filter by status")
    p_query.add_argument("--tag", action="append", help="Filter by tag (key=value), repeatable")
    p_query.add_argument("--fts", help="Full-text search query")
    p_query.add_argument("--json", action="store_true", help="Output as JSON")

    # context
    p_ctx = sub.add_parser("context", help="Assemble context for LLM consumption")
    p_ctx.add_argument("-c", "--collection", help="Collection name")
    p_ctx.add_argument("--tag", action="append", help="Filter by tag (key=value), repeatable")
    p_ctx.add_argument("--budget", type=int, default=8000, help="Token budget (default: 8000)")

    # lint
    p_lint = sub.add_parser("lint", help="Run health checks")
    p_lint.add_argument("--fix", action="store_true", help="Auto-fix safe issues")

    # export
    p_export = sub.add_parser("export", help="Export wiki")
    p_export.add_argument("-o", "--output", default="wiki/", help="Output directory")

    # prune
    sub.add_parser("prune", help="Prune expired claims")

    # sync
    p_sync = sub.add_parser("sync", help="Check for document drift")
    p_sync.add_argument("-c", "--collection", help="Sync only this collection")

    # ingest
    p_ingest = sub.add_parser("ingest", help="Ingest a source document")
    p_ingest.add_argument("file", help="Path to source file")
    p_ingest.add_argument("--title", required=True, help="Source title")
    p_ingest.add_argument("--source-type", default="note", help="Source type (default: note)")
    p_ingest.add_argument("--claims", help="Path to JSON file with pre-extracted claims")

    # graphify-import
    p_graph = sub.add_parser("graphify-import", help="Import graphify graph output")
    p_graph.add_argument("--root", default=".", help="Project root directory")
    p_graph.add_argument("--graph", help="Path to graph.json (default: <root>/graphify-out/graph.json)")
    p_graph.add_argument("--threshold", help="Promotion threshold, e.g. p90 or absolute:12")
    p_graph.add_argument("--floor", type=int, help="Promotion floor")
    p_graph.add_argument("--ceiling", type=int, help="Promotion ceiling")
    p_graph.add_argument("--pinned-node", action="append", help="Pinned node id, repeatable")
    p_graph.add_argument("--inferred-edge-threshold", type=float, help="Admit inferred edges at or above this score")
    p_graph.add_argument("--annotate-approval-status", action="store_true", help="Emit approval_status claims when registry data exists")
    p_graph.add_argument("--contract-registry", help="Optional path to contract registry JSON/YAML")
    p_graph.add_argument("--auto-tune", action="store_true", help="Auto-tune selected node count to fit the L0 token budget")
    p_graph.add_argument("--l0-budget", type=int, default=2000, help="Target L0 token budget when auto-tuning")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    _setup_logging(args.verbose)

    commands = {
        "init": cmd_init,
        "startup": cmd_startup,
        "status": cmd_status,
        "doctor": cmd_doctor,
        "scan": cmd_scan,
        "query": cmd_query,
        "context": cmd_context,
        "lint": cmd_lint,
        "export": cmd_export,
        "prune": cmd_prune,
        "sync": cmd_sync,
        "ingest": cmd_ingest,
        "graphify-import": cmd_graphify_import,
    }

    try:
        commands[args.command](args)
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            print(f"Error: database not initialized. Run 'astaire init' first.", file=sys.stderr)
            sys.exit(1)
        raise


if __name__ == "__main__":
    main()
