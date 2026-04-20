-- =============================================================
-- Memory Palace — SQLite DDL Schema
-- Hybrid knowledge store with FTS5 and projection caching
-- =============================================================
--
-- DESIGN NOTES (from board review MTG-0001):
--
-- ID strategy: All primary keys use TEXT ULIDs (Crockford base32,
-- time-sortable, globally unique). Trade-off vs INTEGER rowids:
-- TEXT JOINs are slower at scale (100K+ rows), but ULIDs provide
-- time-sortability and global uniqueness across tables and systems
-- that INTEGER autoincrement cannot. Revisit with benchmark data
-- if claim count exceeds 100K. (FND-0003)
--
-- Type discipline asymmetry: The claim store (source, entity, claim)
-- uses CHECK constraints on type enums because the claim pipeline
-- requires a closed, well-defined type vocabulary. The document
-- registry (collection, document, document_tag) uses free-text for
-- doc_type, status, and tag values because the core must be
-- collection-agnostic — each collection defines its own vocabulary
-- in collection.config_json, validated at the application layer.
-- (FND-0009)
--
-- Concurrency: Astaire is single-writer by design. WAL mode allows
-- concurrent reads. busy_timeout is set in db.py (default 5000ms).
-- All writes use short transactions. (FND-0005)
-- =============================================================

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ══════════════════════════════════════════════════════════════
-- CLAIM STORE — structured knowledge extraction
-- ══════════════════════════════════════════════════════════════

-- ── Sources (immutable raw documents) ──

CREATE TABLE IF NOT EXISTS source (
    source_id     TEXT PRIMARY KEY,   -- ULID
    title         TEXT NOT NULL,
    source_type   TEXT NOT NULL CHECK (source_type IN ('article','paper','transcript','note','code','synthesis')),
    content_hash  TEXT NOT NULL,       -- SHA-256 of raw content
    file_path     TEXT,                -- path to raw file on disk
    media_type    TEXT DEFAULT 'text/markdown',
    token_count   INTEGER DEFAULT 0,
    ingested_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    metadata_json TEXT DEFAULT '{}'
);

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

-- ── Projection Cache (pre-compiled context tiers) ──
-- scope_key conventions:
--   'global'                    — L0 global summary
--   'cluster:{id}'             — L1 topic cluster digest
--   'entity:{id}'              — L1/L2 entity digest
--   'collection:{name}'        — L1 collection document summary
--   'doctype:{collection}:{t}' — L1 document type summary
--   'tag:{key}:{value}'        — L1 tag-based context bundle
--   'query:{hash}'             — L2 query-specific cache

CREATE TABLE IF NOT EXISTS projection_cache (
    cache_id        TEXT PRIMARY KEY,  -- ULID
    tier            TEXT NOT NULL CHECK (tier IN ('L0','L1','L2')),
    scope_key       TEXT NOT NULL,
    content_md      TEXT NOT NULL,     -- pre-rendered markdown
    token_count     INTEGER DEFAULT 0,
    content_hash    TEXT NOT NULL,     -- SHA-256 for staleness detection
    encoding        TEXT DEFAULT 'cl100k_base',  -- tiktoken encoding used for token_count (FND-0004)
    generated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    expires_at      TEXT,
    generator_model TEXT DEFAULT 'claude-sonnet-4-20250514',
    UNIQUE(tier, scope_key)
);

CREATE INDEX IF NOT EXISTS idx_cache_tier_scope ON projection_cache(tier, scope_key);

-- ── Ingest Log (append-only audit trail) ──

CREATE TABLE IF NOT EXISTS ingest_log (
    log_id                TEXT PRIMARY KEY, -- ULID
    operation             TEXT NOT NULL CHECK (operation IN ('ingest','query','lint','recompile','prune','export','register','sync')),
    source_id             TEXT REFERENCES source(source_id),
    summary               TEXT,
    claims_created        INTEGER DEFAULT 0,
    claims_updated        INTEGER DEFAULT 0,
    claims_superseded     INTEGER DEFAULT 0,
    entities_created      INTEGER DEFAULT 0,
    relationships_created INTEGER DEFAULT 0,
    contradictions_found  INTEGER DEFAULT 0,
    documents_registered  INTEGER DEFAULT 0,
    documents_updated     INTEGER DEFAULT 0,
    created_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

-- ══════════════════════════════════════════════════════════════
-- DOCUMENT REGISTRY — generic, collection-based file tracking
-- ══════════════════════════════════════════════════════════════

-- ── Collections (named groups of document types) ──

CREATE TABLE IF NOT EXISTS collection (
    collection_id TEXT PRIMARY KEY,    -- ULID
    name          TEXT NOT NULL UNIQUE,
    description   TEXT,
    config_json   TEXT DEFAULT '{}',   -- allowed types, lifecycle stages, metadata schemas
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

-- ── Documents (registered files in collections) ──
-- doc_type and status are free-text: validated at application layer
-- against collection.config_json. See design note at top of file.

CREATE TABLE IF NOT EXISTS document (
    document_id   TEXT PRIMARY KEY,     -- ULID
    collection_id TEXT NOT NULL REFERENCES collection(collection_id),
    external_id   TEXT,                 -- any external identifier (SCN-1.2-01, FND-0001, ADR-003, etc.)
    doc_type      TEXT NOT NULL,        -- free-text, meaning defined by collection
    title         TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'draft',
    file_path     TEXT NOT NULL,
    content_hash  TEXT NOT NULL,        -- SHA-256
    token_count   INTEGER DEFAULT 0,
    source_id     TEXT REFERENCES source(source_id),  -- optional link to claim pipeline
    metadata_json TEXT DEFAULT '{}',
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_document_ext_id ON document(collection_id, external_id) WHERE external_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_document_collection ON document(collection_id);
CREATE INDEX IF NOT EXISTS idx_document_type ON document(doc_type);
CREATE INDEX IF NOT EXISTS idx_document_status ON document(status);
CREATE INDEX IF NOT EXISTS idx_document_path ON document(file_path);

-- ── Document Tags (flexible key-value tagging) ──
-- Replaces hardcoded stage/chunk/phase columns. Each collection
-- defines its own tag vocabulary in config_json.

CREATE TABLE IF NOT EXISTS document_tag (
    document_id TEXT NOT NULL REFERENCES document(document_id),
    tag_key     TEXT NOT NULL,          -- e.g. 'stage', 'chunk', 'phase', 'priority', 'lens'
    tag_value   TEXT NOT NULL,          -- e.g. 'implementation', '1.2', '1', 'high', 'security'
    PRIMARY KEY (document_id, tag_key, tag_value)
);

-- Covering index for tag-based queries (COM-001 from MTG-0001)
CREATE INDEX IF NOT EXISTS idx_doctag_key_value ON document_tag(tag_key, tag_value, document_id);

-- ── Document Dependencies (typed edges between documents) ──

CREATE TABLE IF NOT EXISTS document_dependency (
    dep_id           TEXT PRIMARY KEY,   -- ULID
    from_document_id TEXT NOT NULL REFERENCES document(document_id),
    to_document_id   TEXT NOT NULL REFERENCES document(document_id),
    dep_type         TEXT NOT NULL,      -- free-text: 'produces', 'consumes', 'supersedes', 'validates', etc.
    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    UNIQUE(from_document_id, to_document_id, dep_type)
);

CREATE INDEX IF NOT EXISTS idx_docdep_from ON document_dependency(from_document_id);
CREATE INDEX IF NOT EXISTS idx_docdep_to ON document_dependency(to_document_id);

-- ══════════════════════════════════════════════════════════════
-- FTS5 Virtual Tables (full-text search, no external deps)
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

CREATE VIRTUAL TABLE IF NOT EXISTS document_fts USING fts5(
    title,
    external_id,
    doc_type,
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

-- Document FTS triggers
CREATE TRIGGER IF NOT EXISTS trg_document_fts_insert AFTER INSERT ON document
BEGIN
    INSERT INTO document_fts(rowid, title, external_id, doc_type)
    VALUES (NEW.rowid, NEW.title, COALESCE(NEW.external_id,''), NEW.doc_type);
END;

CREATE TRIGGER IF NOT EXISTS trg_document_fts_update AFTER UPDATE OF title, external_id, doc_type ON document
BEGIN
    DELETE FROM document_fts WHERE rowid = OLD.rowid;
    INSERT INTO document_fts(rowid, title, external_id, doc_type)
    VALUES (NEW.rowid, NEW.title, COALESCE(NEW.external_id,''), NEW.doc_type);
END;

CREATE TRIGGER IF NOT EXISTS trg_document_fts_delete AFTER DELETE ON document
BEGIN
    DELETE FROM document_fts WHERE rowid = OLD.rowid;
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

-- Active documents (excludes superseded and archived)
CREATE VIEW IF NOT EXISTS v_active_documents AS
SELECT d.*, c.name AS collection_name
FROM document d
JOIN collection c ON c.collection_id = d.collection_id
WHERE d.status NOT IN ('superseded', 'archived');
