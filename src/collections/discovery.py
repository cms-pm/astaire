"""Collection module discovery — convention-based plugin loading.

Scans the src.collections package for modules that expose the collection
protocol: COLLECTION_NAME, register_collection(), scan_and_register().
"""

import importlib
import logging
import pkgutil
import sqlite3
from pathlib import Path
from types import ModuleType

import src.collections

logger = logging.getLogger(__name__)

_REQUIRED_ATTRS = ("COLLECTION_NAME", "register_collection", "scan_and_register")


def discover_collection_modules() -> list[ModuleType]:
    """Import and return all collection modules with the required interface."""
    modules = []
    package_path = src.collections.__path__
    for importer, modname, ispkg in pkgutil.iter_modules(package_path):
        if modname.startswith("_") or modname == "discovery":
            continue
        try:
            mod = importlib.import_module(f"src.collections.{modname}")
        except Exception:
            logger.warning("Failed to import collection module: %s", modname, exc_info=True)
            continue
        if all(hasattr(mod, attr) for attr in _REQUIRED_ATTRS):
            modules.append(mod)
        else:
            logger.debug("Skipping %s — missing required attributes", modname)
    return modules


def register_all_collections(conn: sqlite3.Connection) -> list[str]:
    """Register all discovered collections. Returns list of collection names."""
    names = []
    for mod in discover_collection_modules():
        try:
            mod.register_collection(conn)
            names.append(mod.COLLECTION_NAME)
        except Exception:
            logger.warning("Failed to register collection: %s", mod.COLLECTION_NAME, exc_info=True)
    return names


def scan_all_collections(
    conn: sqlite3.Connection, root: str | Path,
) -> list[dict]:
    """Scan and register documents for all discovered collections. Returns aggregated results."""
    results = []
    for mod in discover_collection_modules():
        try:
            new_docs = mod.scan_and_register(conn, root)
            results.extend(new_docs)
        except Exception:
            logger.warning("Failed to scan collection: %s", mod.COLLECTION_NAME, exc_info=True)
    return results
