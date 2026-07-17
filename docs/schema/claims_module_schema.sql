-- =============================================================
-- Memory Palace — SQLite DDL Schema (claims module)
-- Entity/claim/relationship/contradiction knowledge-extraction subsystem
-- =============================================================
--
-- Module split (Proposal A): this file is the OPTIONAL claims module.
-- It is NOT installed by a bare `init_db()` call — only by explicit
-- opt-in (`astaire init --with-claims`, which calls
-- `install_claims_module()` in `src/db.py`). The core registry
-- (collection/document/document_tag/document_dependency/document_fts/
-- projection_cache/ingest_log/source) lives in the sibling
-- `memory_palace_schema.sql` and must already be installed before this
-- file is applied: `claim.source_id` references `source(source_id)`,
-- a core table. All DDL below uses `IF NOT EXISTS` so this file is
-- safe to re-apply against a database that already has the module
-- installed (idempotent, matching `install_claims_module`'s contract).
--
-- Type discipline: the claim store (entity, claim, relationship,
-- contradiction) uses CHECK constraints on type enums because the claim
-- pipeline requires a closed, well-defined type vocabulary — unlike the
-- free-text document registry in the core schema. (FND-0009)
-- =============================================================

PRAGMA foreign_keys = ON;

-- ── Entities (de-duplicated subjects) ──

CREATE TABLE IF NOT EXISTS entity (
    entity_id      TEXT PRIMARY KEY,  -- ULID
    canonical_name TEXT NOT NULL UNIQUE,
    entity_type    TEXT NOT NULL CHECK (entity_type IN ('person','org','system','concept','place','event')),
    description    TEXT,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    aliases_json   TEXT DEFAULT '[]'  -- JSON array of alternate names
);

CREATE INDEX IF NOT EXISTS idx_entity_type ON entity(entity_type);
CREATE INDEX IF NOT EXISTS idx_entity_name ON entity(canonical_name);

-- ── Claims (atomic knowledge units) ──

CREATE TABLE IF NOT EXISTS claim (
    claim_id       TEXT PRIMARY KEY,  -- ULID
    entity_id      TEXT NOT NULL REFERENCES entity(entity_id),
    predicate      TEXT NOT NULL,     -- verb/relation label
    value          TEXT NOT NULL,     -- object (free text or structured)
    claim_type     TEXT NOT NULL CHECK (claim_type IN ('fact','opinion','metric','status','definition')),
    confidence     REAL NOT NULL DEFAULT 0.5 CHECK (confidence >= 0.0 AND confidence <= 1.0),
    epistemic_tag  TEXT NOT NULL DEFAULT 'provisional' CHECK (epistemic_tag IN ('confirmed','provisional','contested','retracted')),
    source_id      TEXT NOT NULL REFERENCES source(source_id),
    source_span    TEXT,              -- locator in raw source (page, paragraph, line)
    superseded_by  TEXT REFERENCES claim(claim_id),
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    expires_at     TEXT               -- ISO-8601, NULL = permanent
);

CREATE INDEX IF NOT EXISTS idx_claim_entity    ON claim(entity_id);
CREATE INDEX IF NOT EXISTS idx_claim_source    ON claim(source_id);
CREATE INDEX IF NOT EXISTS idx_claim_predicate ON claim(predicate);
CREATE INDEX IF NOT EXISTS idx_claim_epistemic ON claim(epistemic_tag);
CREATE INDEX IF NOT EXISTS idx_claim_active    ON claim(entity_id, epistemic_tag)
    WHERE superseded_by IS NULL AND epistemic_tag != 'retracted';

-- ── Relationships (typed edges between entities) ──

CREATE TABLE IF NOT EXISTS relationship (
    rel_id            TEXT PRIMARY KEY, -- ULID
    from_entity_id    TEXT NOT NULL REFERENCES entity(entity_id),
    to_entity_id      TEXT NOT NULL REFERENCES entity(entity_id),
    rel_type          TEXT NOT NULL CHECK (rel_type IN ('supports','contradicts','depends_on','evolved_into','part_of','related_to','tested_by')),
    weight            REAL DEFAULT 1.0 CHECK (weight >= 0.0 AND weight <= 1.0),
    evidence_claim_id TEXT REFERENCES claim(claim_id),
    source_id         TEXT REFERENCES source(source_id),
    created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_rel_from ON relationship(from_entity_id);
CREATE INDEX IF NOT EXISTS idx_rel_to   ON relationship(to_entity_id);
CREATE INDEX IF NOT EXISTS idx_rel_type ON relationship(rel_type);

-- ── Topic Clusters (L1 groupings) ──

CREATE TABLE IF NOT EXISTS topic_cluster (
    cluster_id        TEXT PRIMARY KEY, -- ULID
    label             TEXT NOT NULL,
    summary           TEXT,             -- 2-3 sentence digest
    parent_cluster_id TEXT REFERENCES topic_cluster(cluster_id),
    claim_count       INTEGER DEFAULT 0,
    created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

-- ── Claim ↔ Cluster junction table ──

CREATE TABLE IF NOT EXISTS claim_cluster (
    claim_id   TEXT NOT NULL REFERENCES claim(claim_id),
    cluster_id TEXT NOT NULL REFERENCES topic_cluster(cluster_id),
    relevance  REAL DEFAULT 1.0 CHECK (relevance >= 0.0 AND relevance <= 1.0),
    PRIMARY KEY (claim_id, cluster_id)
);

-- ── Contradictions (first-class objects) ──

CREATE TABLE IF NOT EXISTS contradiction (
    contradiction_id    TEXT PRIMARY KEY, -- ULID
    claim_a_id          TEXT NOT NULL REFERENCES claim(claim_id),
    claim_b_id          TEXT NOT NULL REFERENCES claim(claim_id),
    description         TEXT,             -- LLM-generated explanation
    resolution_status   TEXT NOT NULL DEFAULT 'open' CHECK (resolution_status IN ('open','resolved','deferred')),
    resolved_by_claim_id TEXT REFERENCES claim(claim_id),
    detected_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    resolved_at         TEXT
);

CREATE INDEX IF NOT EXISTS idx_contra_status ON contradiction(resolution_status);

-- ══════════════════════════════════════════════════════════════
-- FTS5 Virtual Tables (claim/entity full-text search)
-- ══════════════════════════════════════════════════════════════

CREATE VIRTUAL TABLE IF NOT EXISTS claim_fts USING fts5(
    predicate,
    value,
    entity_name,
    tokenize = 'porter unicode61'
);

CREATE VIRTUAL TABLE IF NOT EXISTS entity_fts USING fts5(
    canonical_name,
    description,
    aliases,
    tokenize = 'porter unicode61'
);

-- ── Triggers to keep FTS in sync ──

-- Claim FTS triggers
CREATE TRIGGER IF NOT EXISTS trg_claim_fts_insert AFTER INSERT ON claim
BEGIN
    INSERT INTO claim_fts(rowid, predicate, value, entity_name)
    SELECT NEW.rowid, NEW.predicate, NEW.value,
           (SELECT canonical_name FROM entity WHERE entity_id = NEW.entity_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_claim_fts_update AFTER UPDATE OF predicate, value, entity_id ON claim
BEGIN
    DELETE FROM claim_fts WHERE rowid = OLD.rowid;
    INSERT INTO claim_fts(rowid, predicate, value, entity_name)
    SELECT NEW.rowid, NEW.predicate, NEW.value,
           (SELECT canonical_name FROM entity WHERE entity_id = NEW.entity_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_claim_fts_delete AFTER DELETE ON claim
BEGIN
    DELETE FROM claim_fts WHERE rowid = OLD.rowid;
END;

-- Entity FTS triggers
CREATE TRIGGER IF NOT EXISTS trg_entity_fts_insert AFTER INSERT ON entity
BEGIN
    INSERT INTO entity_fts(rowid, canonical_name, description, aliases)
    VALUES (NEW.rowid, NEW.canonical_name, COALESCE(NEW.description,''), COALESCE(NEW.aliases_json,''));
END;

CREATE TRIGGER IF NOT EXISTS trg_entity_fts_update AFTER UPDATE OF canonical_name, description, aliases_json ON entity
BEGIN
    DELETE FROM entity_fts WHERE rowid = OLD.rowid;
    INSERT INTO entity_fts(rowid, canonical_name, description, aliases)
    VALUES (NEW.rowid, NEW.canonical_name, COALESCE(NEW.description,''), COALESCE(NEW.aliases_json,''));
END;

CREATE TRIGGER IF NOT EXISTS trg_entity_fts_delete AFTER DELETE ON entity
BEGIN
    DELETE FROM entity_fts WHERE rowid = OLD.rowid;
END;

-- ══════════════════════════════════════════════════════════════
-- Views
-- ══════════════════════════════════════════════════════════════

-- Active claims only (excludes superseded, retracted, expired)
CREATE VIEW IF NOT EXISTS v_active_claims AS
SELECT c.*, e.canonical_name AS entity_name, e.entity_type
FROM claim c
JOIN entity e ON e.entity_id = c.entity_id
WHERE c.superseded_by IS NULL
  AND c.epistemic_tag != 'retracted'
  AND (c.expires_at IS NULL OR c.expires_at > strftime('%Y-%m-%dT%H:%M:%SZ','now'));

-- Open contradictions with claim details
CREATE VIEW IF NOT EXISTS v_open_contradictions AS
SELECT
    con.contradiction_id,
    con.description,
    ca.entity_id  AS entity_a_id,
    ea.canonical_name AS entity_a_name,
    ca.predicate   AS predicate_a,
    ca.value       AS value_a,
    cb.entity_id  AS entity_b_id,
    eb.canonical_name AS entity_b_name,
    cb.predicate   AS predicate_b,
    cb.value       AS value_b,
    con.detected_at
FROM contradiction con
JOIN claim ca ON ca.claim_id = con.claim_a_id
JOIN claim cb ON cb.claim_id = con.claim_b_id
JOIN entity ea ON ea.entity_id = ca.entity_id
JOIN entity eb ON eb.entity_id = cb.entity_id
WHERE con.resolution_status = 'open';

-- Entity hub scores (count of relationships + claims)
CREATE VIEW IF NOT EXISTS v_entity_hub_scores AS
SELECT
    e.entity_id,
    e.canonical_name,
    e.entity_type,
    COUNT(DISTINCT c.claim_id) AS claim_count,
    COUNT(DISTINCT r1.rel_id) + COUNT(DISTINCT r2.rel_id) AS relationship_count,
    COUNT(DISTINCT c.claim_id) + COUNT(DISTINCT r1.rel_id) + COUNT(DISTINCT r2.rel_id) AS hub_score
FROM entity e
LEFT JOIN claim c ON c.entity_id = e.entity_id AND c.superseded_by IS NULL AND c.epistemic_tag != 'retracted'
LEFT JOIN relationship r1 ON r1.from_entity_id = e.entity_id
LEFT JOIN relationship r2 ON r2.to_entity_id = e.entity_id
GROUP BY e.entity_id, e.canonical_name, e.entity_type
ORDER BY hub_score DESC;
