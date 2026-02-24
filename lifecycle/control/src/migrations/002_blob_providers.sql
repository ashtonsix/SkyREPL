-- 002_blob_providers.sql - Blob provider config and payload tables

-- Per-tenant blob provider configuration
CREATE TABLE IF NOT EXISTS blob_provider_configs (
    tenant_id INTEGER PRIMARY KEY REFERENCES tenants(id),
    provider_name TEXT NOT NULL DEFAULT 'sql',
    config_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- BLOB-PROVIDER: blob_payloads is the backing store for SqlBlobProvider.
-- All access MUST go through the BlobProvider interface (getBlobProvider()).
-- Direct queries bypass provider abstraction and break provider substitutability.
CREATE TABLE IF NOT EXISTS blob_payloads (
    blob_key TEXT PRIMARY KEY,
    payload BLOB NOT NULL,
    size_bytes INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);

-- Default provider for existing default tenant
INSERT OR IGNORE INTO blob_provider_configs (tenant_id, provider_name, config_json, created_at, updated_at)
    VALUES (1, 'sql', '{}', strftime('%s','now') * 1000, strftime('%s','now') * 1000);

-- Log chunk storage (Tier 2 of 3-tier log streaming)
CREATE TABLE IF NOT EXISTS log_chunks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    stream TEXT NOT NULL DEFAULT 'stdout',
    chunk_seq INTEGER NOT NULL,
    byte_offset_start INTEGER NOT NULL DEFAULT 0,
    blob_id INTEGER NOT NULL REFERENCES blobs(id),
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_log_chunks_run ON log_chunks(run_id, stream, chunk_seq);
CREATE INDEX IF NOT EXISTS idx_log_chunks_time ON log_chunks(run_id, stream, start_ms, end_ms);

-- Update schema version
UPDATE schema_version SET version = 2;
