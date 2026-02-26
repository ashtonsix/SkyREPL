-- =============================================================================
-- 001_schema.sql
-- =============================================================================
-- Complete schema for SkyREPL control plane database.
-- tenant_id on every table, provenance columns on workflows/manifests/allocations.
-- CHECK constraints on all status/boolean columns.

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);
INSERT INTO schema_version (version) VALUES (1);

-- =============================================================================
-- Settings
-- =============================================================================

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- =============================================================================
-- Tenants & Users
-- =============================================================================

CREATE TABLE tenants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    seat_cap INTEGER NOT NULL DEFAULT 5,
    budget_usd REAL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER NOT NULL,
    email TEXT NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL DEFAULT 'member',
    budget_usd REAL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    UNIQUE(tenant_id, email)
);

CREATE INDEX idx_users_tenant ON users(tenant_id);

-- Default tenant for self-managed / single-tenant mode
INSERT INTO tenants (id, name, seat_cap, budget_usd, created_at, updated_at)
    VALUES (1, 'default', 5, NULL, strftime('%s','now') * 1000, strftime('%s','now') * 1000);

-- =============================================================================
-- API Keys (reverse FK: api_keys.user_id → users.id)
-- =============================================================================

CREATE TABLE api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER NOT NULL DEFAULT 1,
    user_id INTEGER,
    key_hash TEXT NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'admin',
    permissions TEXT NOT NULL DEFAULT 'all',
    created_at INTEGER NOT NULL,
    last_used_at INTEGER,
    expires_at INTEGER,
    revoked_at INTEGER,
    UNIQUE(key_hash),
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX idx_api_keys_hash ON api_keys(key_hash) WHERE revoked_at IS NULL;
CREATE INDEX idx_api_keys_tenant ON api_keys(tenant_id)
    WHERE revoked_at IS NULL;
CREATE INDEX idx_api_keys_user ON api_keys(user_id)
    WHERE user_id IS NOT NULL AND revoked_at IS NULL;

-- =============================================================================
-- Core Resources
-- =============================================================================

-- instances: raw records. Business reads go through resource/instance.ts materializer.
CREATE TABLE instances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER NOT NULL DEFAULT 1,
    provider TEXT NOT NULL,
    provider_id TEXT NOT NULL DEFAULT '',
    spec TEXT NOT NULL,
    region TEXT NOT NULL DEFAULT 'local',
    ip TEXT,
    workflow_state TEXT NOT NULL,
    workflow_error TEXT,
    current_manifest_id INTEGER,
    spawn_idempotency_key TEXT,
    is_spot INTEGER NOT NULL DEFAULT 0 CHECK(is_spot IN (0, 1)),
    spot_request_id TEXT,
    init_checksum TEXT,
    registration_token_hash TEXT,
    provider_metadata TEXT,
    created_at INTEGER NOT NULL,
    last_heartbeat INTEGER NOT NULL,
    FOREIGN KEY (current_manifest_id) REFERENCES manifests(id)
);

CREATE INDEX idx_instances_tenant ON instances(tenant_id);
CREATE INDEX idx_instances_workflow_state ON instances(workflow_state);
CREATE INDEX idx_instances_provider ON instances(provider, workflow_state);
CREATE INDEX idx_instances_manifest ON instances(current_manifest_id);
CREATE INDEX idx_instances_heartbeat ON instances(last_heartbeat)
    WHERE workflow_state NOT LIKE 'terminate:%';
CREATE INDEX idx_instances_spawn_pending ON instances(workflow_state, created_at)
    WHERE workflow_state = 'spawn:pending';
CREATE UNIQUE INDEX idx_instances_provider_id ON instances(provider, provider_id)
    WHERE provider_id != '';
CREATE UNIQUE INDEX idx_instances_spawn_key ON instances(spawn_idempotency_key)
    WHERE spawn_idempotency_key IS NOT NULL;
CREATE INDEX idx_instances_token_hash ON instances(registration_token_hash)
    WHERE registration_token_hash IS NOT NULL;

-- runs: raw records. Business reads go through resource/run.ts materializer.
CREATE TABLE runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER NOT NULL DEFAULT 1,
    command TEXT NOT NULL,
    workdir TEXT NOT NULL DEFAULT '/workspace',
    max_duration_ms INTEGER NOT NULL,
    workflow_state TEXT NOT NULL DEFAULT 'launch-run:pending',
    workflow_error TEXT,
    current_manifest_id INTEGER,
    exit_code INTEGER,
    init_checksum TEXT,
    create_snapshot INTEGER NOT NULL DEFAULT 0 CHECK(create_snapshot IN (0, 1)),
    spot_interrupted INTEGER NOT NULL DEFAULT 0 CHECK(spot_interrupted IN (0, 1)),
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER,
    FOREIGN KEY (current_manifest_id) REFERENCES manifests(id)
);

CREATE INDEX idx_runs_tenant ON runs(tenant_id, created_at DESC);
CREATE INDEX idx_runs_workflow_state ON runs(workflow_state);
CREATE INDEX idx_runs_manifest ON runs(current_manifest_id);
CREATE INDEX idx_runs_created ON runs(created_at DESC);
CREATE INDEX idx_runs_duration ON runs(started_at, finished_at);

-- allocations: raw records. Business reads go through resource/allocation.ts materializer.
CREATE TABLE allocations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER NOT NULL DEFAULT 1,
    run_id INTEGER,
    instance_id INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('AVAILABLE', 'CLAIMED', 'ACTIVE', 'COMPLETE', 'FAILED')),
    current_manifest_id INTEGER,
    claimed_by INTEGER,
    user TEXT NOT NULL DEFAULT 'ubuntu',
    workdir TEXT NOT NULL,
    debug_hold_until INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    completed_at INTEGER,
    FOREIGN KEY (run_id) REFERENCES runs(id),
    FOREIGN KEY (instance_id) REFERENCES instances(id),
    FOREIGN KEY (current_manifest_id) REFERENCES manifests(id),
    FOREIGN KEY (claimed_by) REFERENCES users(id)
);

CREATE INDEX idx_allocations_tenant ON allocations(tenant_id, status);
CREATE INDEX idx_allocations_status ON allocations(status);
CREATE INDEX idx_allocations_instance ON allocations(instance_id, status);
CREATE INDEX idx_allocations_run ON allocations(run_id);
CREATE INDEX idx_allocations_manifest ON allocations(current_manifest_id);
CREATE INDEX idx_allocations_debug_hold ON allocations(debug_hold_until)
    WHERE debug_hold_until IS NOT NULL;
CREATE INDEX idx_allocations_available ON allocations(instance_id, status, workdir)
    WHERE status = 'AVAILABLE';

-- =============================================================================
-- Workflow Execution
-- =============================================================================

-- workflows: raw records. Business reads go through resource/workflow.ts materializer.
CREATE TABLE workflows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER NOT NULL DEFAULT 1,
    type TEXT NOT NULL,
    parent_workflow_id INTEGER,
    depth INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK(status IN ('pending', 'running', 'paused', 'cancelling', 'completed', 'failed', 'cancelled', 'rolling_back')),
    current_node TEXT,
    input_json TEXT NOT NULL,
    output_json TEXT,
    error_json TEXT,
    manifest_id INTEGER,
    created_by INTEGER,
    trace_id TEXT,
    idempotency_key TEXT,
    timeout_ms INTEGER,
    timeout_at INTEGER,
    priority INTEGER NOT NULL DEFAULT 0,
    retry_of_workflow_id INTEGER,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (parent_workflow_id) REFERENCES workflows(id),
    FOREIGN KEY (manifest_id) REFERENCES manifests(id),
    FOREIGN KEY (created_by) REFERENCES users(id),
    FOREIGN KEY (retry_of_workflow_id) REFERENCES workflows(id)
);

CREATE INDEX idx_workflows_tenant ON workflows(tenant_id, status);
CREATE INDEX idx_workflows_status ON workflows(status, created_at DESC);
CREATE INDEX idx_workflows_type ON workflows(type, status);
CREATE INDEX idx_workflows_parent ON workflows(parent_workflow_id);
CREATE INDEX idx_workflows_manifest ON workflows(manifest_id);
CREATE INDEX idx_workflows_trace ON workflows(trace_id);
CREATE UNIQUE INDEX idx_workflows_idempotency ON workflows(idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE workflow_nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id INTEGER NOT NULL,
    node_id TEXT NOT NULL,
    node_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'running', 'completed', 'failed', 'skipped')),
    input_json TEXT NOT NULL,
    output_json TEXT,
    error_json TEXT,
    depends_on TEXT,
    attempt INTEGER NOT NULL DEFAULT 1,
    retry_reason TEXT,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (workflow_id) REFERENCES workflows(id) ON DELETE CASCADE,
    UNIQUE(workflow_id, node_id)
);

CREATE INDEX idx_workflow_nodes_workflow ON workflow_nodes(workflow_id, created_at);
CREATE INDEX idx_workflow_nodes_status ON workflow_nodes(workflow_id, status);
CREATE INDEX idx_workflow_nodes_type ON workflow_nodes(node_type, status);

-- =============================================================================
-- Object Storage
-- =============================================================================

CREATE TABLE blobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER NOT NULL DEFAULT 1,
    bucket TEXT NOT NULL,
    checksum TEXT NOT NULL,
    checksum_bytes INTEGER,
    s3_key TEXT,
    s3_bucket TEXT,
    payload BLOB,
    size_bytes INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    last_referenced_at INTEGER NOT NULL
);

CREATE INDEX idx_blobs_dedup ON blobs(bucket, checksum);
CREATE INDEX idx_blobs_last_ref ON blobs(last_referenced_at);
CREATE INDEX idx_blobs_s3 ON blobs(s3_bucket, s3_key)
    WHERE s3_key IS NOT NULL;

CREATE TABLE objects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER NOT NULL DEFAULT 1,
    type TEXT NOT NULL,
    blob_id INTEGER NOT NULL,
    provider TEXT,
    provider_object_id TEXT,
    metadata_json TEXT,
    expires_at INTEGER,
    current_manifest_id INTEGER,
    created_at INTEGER NOT NULL,
    accessed_at INTEGER,
    updated_at INTEGER,
    FOREIGN KEY (blob_id) REFERENCES blobs(id) ON DELETE RESTRICT,
    FOREIGN KEY (current_manifest_id) REFERENCES manifests(id)
);

CREATE INDEX idx_objects_tenant ON objects(tenant_id, type);
CREATE INDEX idx_objects_type ON objects(type, created_at DESC);
CREATE INDEX idx_objects_blob ON objects(blob_id);
CREATE INDEX idx_objects_provider ON objects(provider, type);
CREATE INDEX idx_objects_manifest ON objects(current_manifest_id);
CREATE INDEX idx_objects_expires ON objects(expires_at)
    WHERE expires_at IS NOT NULL;

CREATE TABLE object_tags (
    object_id INTEGER NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY (object_id, tag),
    FOREIGN KEY (object_id) REFERENCES objects(id) ON DELETE CASCADE
);

CREATE INDEX idx_object_tags_tag ON object_tags(tag);

CREATE TABLE blob_provider_configs (
    tenant_id INTEGER PRIMARY KEY REFERENCES tenants(id),
    provider_name TEXT NOT NULL DEFAULT 'sql',
    config_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- BLOB-PROVIDER: blob_payloads is the backing store for SqlBlobProvider.
-- All access MUST go through the BlobProvider interface (getBlobProvider()).
-- Direct queries bypass provider abstraction and break provider substitutability.
CREATE TABLE blob_payloads (
    blob_key TEXT PRIMARY KEY,
    payload BLOB NOT NULL,
    size_bytes INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);

-- Default provider for existing default tenant
INSERT INTO blob_provider_configs (tenant_id, provider_name, config_json, created_at, updated_at)
    VALUES (1, 'sql', '{}', strftime('%s','now') * 1000, strftime('%s','now') * 1000);

-- =============================================================================
-- Manifest Management
-- =============================================================================

-- manifests: raw records. Business reads go through resource/manifest.ts materializer.
CREATE TABLE manifests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER NOT NULL DEFAULT 1,
    workflow_id INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('DRAFT', 'SEALED')),
    created_by INTEGER,
    default_cleanup_priority INTEGER NOT NULL DEFAULT 50,
    retention_ms INTEGER,
    parent_manifest_id INTEGER,
    created_at INTEGER NOT NULL,
    released_at INTEGER,
    expires_at INTEGER,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (workflow_id) REFERENCES workflows(id),
    FOREIGN KEY (created_by) REFERENCES users(id),
    FOREIGN KEY (parent_manifest_id) REFERENCES manifests(id)
);

CREATE INDEX idx_manifests_tenant ON manifests(tenant_id, status);
CREATE INDEX idx_manifests_status ON manifests(status, expires_at);
CREATE INDEX idx_manifests_workflow ON manifests(workflow_id);
CREATE INDEX idx_manifests_expires ON manifests(expires_at)
    WHERE status = 'SEALED' AND expires_at IS NOT NULL;

CREATE TABLE manifest_resources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_id INTEGER NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    cleanup_priority INTEGER,
    added_at INTEGER NOT NULL,
    owner_type TEXT DEFAULT 'manifest' CHECK(owner_type IN ('manifest', 'released', 'workflow', 'policy')),
    cleanup_processed_at INTEGER,
    FOREIGN KEY (manifest_id) REFERENCES manifests(id) ON DELETE CASCADE,
    UNIQUE (manifest_id, resource_type, resource_id)
);

CREATE INDEX idx_manifest_resources_lookup ON manifest_resources(resource_type, resource_id);
CREATE INDEX idx_manifest_resources_priority ON manifest_resources(manifest_id, cleanup_priority);
CREATE INDEX idx_manifest_resources_owner ON manifest_resources(manifest_id, owner_type);

-- =============================================================================
-- Usage Tracking
-- =============================================================================

CREATE TABLE usage_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER NOT NULL DEFAULT 1,
    instance_id INTEGER NOT NULL,
    allocation_id INTEGER,
    run_id INTEGER,
    provider TEXT NOT NULL,
    spec TEXT NOT NULL,
    region TEXT,
    is_spot INTEGER NOT NULL DEFAULT 0 CHECK(is_spot IN (0, 1)),
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    duration_ms INTEGER,
    estimated_cost_usd REAL,
    FOREIGN KEY (instance_id) REFERENCES instances(id),
    FOREIGN KEY (allocation_id) REFERENCES allocations(id),
    FOREIGN KEY (run_id) REFERENCES runs(id)
);

CREATE INDEX idx_usage_tenant ON usage_records(tenant_id, started_at);
CREATE INDEX idx_usage_records_instance ON usage_records(instance_id);
CREATE INDEX idx_usage_records_allocation ON usage_records(allocation_id);
CREATE INDEX idx_usage_records_run ON usage_records(run_id);
CREATE INDEX idx_usage_records_time ON usage_records(started_at, finished_at);
CREATE INDEX idx_usage_records_cost ON usage_records(estimated_cost_usd DESC);

-- =============================================================================
-- Supporting Tables
-- =============================================================================

CREATE TABLE instance_panic_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER NOT NULL DEFAULT 1,
    instance_id INTEGER NOT NULL,
    reason TEXT NOT NULL,
    last_state_json TEXT NOT NULL,
    diagnostics_json TEXT NOT NULL,
    error_logs_json TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (instance_id) REFERENCES instances(id)
);

CREATE INDEX idx_panic_instance ON instance_panic_logs(instance_id);
CREATE INDEX idx_panic_timestamp ON instance_panic_logs(timestamp);
CREATE INDEX idx_instance_panic_logs_tenant ON instance_panic_logs(tenant_id);

-- Log chunk storage (Tier 2 of 3-tier log streaming)
CREATE TABLE log_chunks (
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

CREATE INDEX idx_log_chunks_run ON log_chunks(run_id, stream, chunk_seq);
CREATE INDEX idx_log_chunks_time ON log_chunks(run_id, stream, start_ms, end_ms);

CREATE TABLE idempotency_keys (
    key TEXT PRIMARY KEY,
    endpoint TEXT NOT NULL,
    params_hash TEXT NOT NULL,
    response_status INTEGER NOT NULL,
    response_body TEXT NOT NULL,
    workflow_id INTEGER,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    FOREIGN KEY (workflow_id) REFERENCES workflows(id)
);

CREATE INDEX idx_idempotency_expires ON idempotency_keys(expires_at);

CREATE TABLE orphan_whitelist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER NOT NULL DEFAULT 1,
    provider TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    reason TEXT NOT NULL,
    acknowledged_by TEXT NOT NULL,
    acknowledged_at INTEGER NOT NULL,
    expires_at INTEGER,
    UNIQUE(provider, provider_id)
);

CREATE INDEX idx_whitelist_expires ON orphan_whitelist(expires_at);
CREATE INDEX idx_whitelist_provider ON orphan_whitelist(provider, resource_type);

CREATE TABLE orphan_scans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER NOT NULL DEFAULT 1,
    provider TEXT NOT NULL,
    scanned_at INTEGER NOT NULL,
    orphans_found INTEGER NOT NULL,
    orphan_ids TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_orphan_scans_provider ON orphan_scans(provider, scanned_at DESC);

CREATE TABLE manifest_cleanup_state (
    manifest_id INTEGER PRIMARY KEY,
    tenant_id INTEGER NOT NULL DEFAULT 1,
    last_completed_priority INTEGER NOT NULL DEFAULT 0,
    last_completed_resource_id INTEGER NOT NULL DEFAULT 0,
    started_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (manifest_id) REFERENCES manifests(id)
);

-- =============================================================================
-- Cost Events (C0 DDL — no writers yet)
-- =============================================================================

CREATE TABLE cost_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL CHECK(event_type IN (
        'instance_start', 'instance_stop',
        'allocation_start', 'allocation_end',
        'run_start', 'run_end',
        'storage_snapshot', 'storage_artifact',
        'network_egress',
        'adjustment'
    )),
    tenant_id INTEGER NOT NULL DEFAULT 1,
    instance_id TEXT,
    manifest_id TEXT,
    run_id TEXT,
    payload TEXT NOT NULL,
    dedupe_key TEXT UNIQUE,
    event_version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_cost_events_tenant ON cost_events(tenant_id, created_at);
CREATE INDEX idx_cost_events_type ON cost_events(event_type, created_at);
CREATE INDEX idx_cost_events_instance ON cost_events(instance_id)
    WHERE instance_id IS NOT NULL;
