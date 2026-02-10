-- =============================================================================
-- 001_initial.sql
-- =============================================================================
-- Initial schema for SkyREPL control plane database.
-- Source: impl-pseudo/control/migrations/001_initial.sql.l2.txt

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);
INSERT INTO schema_version (version) VALUES (1);

-- =============================================================================
-- Core Resources
-- =============================================================================

CREATE TABLE instances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    provider_id TEXT NOT NULL DEFAULT '',
    spec TEXT NOT NULL,
    region TEXT NOT NULL DEFAULT 'local',
    ip TEXT,
    workflow_state TEXT NOT NULL,
    workflow_error TEXT,
    current_manifest_id INTEGER,
    spawn_idempotency_key TEXT,
    is_spot INTEGER NOT NULL DEFAULT 0,
    spot_request_id TEXT,
    init_checksum TEXT,
    created_at INTEGER NOT NULL,
    last_heartbeat INTEGER NOT NULL,
    FOREIGN KEY (current_manifest_id) REFERENCES manifests(id)
);

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

CREATE TABLE runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    command TEXT NOT NULL,
    workdir TEXT NOT NULL DEFAULT '/home/ubuntu/work',
    max_duration_ms INTEGER NOT NULL,
    workflow_state TEXT NOT NULL DEFAULT 'launch-run:pending',
    workflow_error TEXT,
    current_manifest_id INTEGER,
    exit_code INTEGER,
    init_checksum TEXT,
    create_snapshot INTEGER NOT NULL DEFAULT 0,
    spot_interrupted INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER,
    FOREIGN KEY (current_manifest_id) REFERENCES manifests(id)
);

CREATE INDEX idx_runs_workflow_state ON runs(workflow_state);
CREATE INDEX idx_runs_manifest ON runs(current_manifest_id);
CREATE INDEX idx_runs_created ON runs(created_at DESC);
CREATE INDEX idx_runs_duration ON runs(started_at, finished_at);

CREATE TABLE allocations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER,
    instance_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    current_manifest_id INTEGER,
    user TEXT NOT NULL DEFAULT 'ubuntu',
    workdir TEXT NOT NULL,
    debug_hold_until INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    completed_at INTEGER,
    FOREIGN KEY (run_id) REFERENCES runs(id),
    FOREIGN KEY (instance_id) REFERENCES instances(id),
    FOREIGN KEY (current_manifest_id) REFERENCES manifests(id)
);

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

CREATE TABLE workflows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    parent_workflow_id INTEGER,
    depth INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    current_node TEXT,
    input_json TEXT NOT NULL,
    output_json TEXT,
    error_json TEXT,
    manifest_id INTEGER,
    trace_id TEXT,
    idempotency_key TEXT,
    timeout_ms INTEGER,
    timeout_at INTEGER,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (parent_workflow_id) REFERENCES workflows(id),
    FOREIGN KEY (manifest_id) REFERENCES manifests(id)
);

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
    status TEXT NOT NULL,
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

-- =============================================================================
-- Manifest Management
-- =============================================================================

CREATE TABLE manifests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    default_cleanup_priority INTEGER NOT NULL DEFAULT 50,
    retention_ms INTEGER,
    created_at INTEGER NOT NULL,
    released_at INTEGER,
    expires_at INTEGER,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (workflow_id) REFERENCES workflows(id)
);

CREATE INDEX idx_manifests_status ON manifests(status, expires_at);
CREATE INDEX idx_manifests_workflow ON manifests(workflow_id);
CREATE INDEX idx_manifests_expires ON manifests(expires_at)
    WHERE status = 'SEALED' AND expires_at IS NOT NULL;

CREATE TABLE manifest_resources (
    manifest_id INTEGER NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    cleanup_priority INTEGER,
    added_at INTEGER NOT NULL,
    FOREIGN KEY (manifest_id) REFERENCES manifests(id) ON DELETE CASCADE,
    PRIMARY KEY (manifest_id, resource_type, resource_id)
);

CREATE INDEX idx_manifest_resources_lookup ON manifest_resources(resource_type, resource_id);
CREATE INDEX idx_manifest_resources_priority ON manifest_resources(manifest_id, cleanup_priority);

-- =============================================================================
-- Usage Tracking
-- =============================================================================

CREATE TABLE usage_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id INTEGER NOT NULL,
    allocation_id INTEGER,
    run_id INTEGER,
    provider TEXT NOT NULL,
    spec TEXT NOT NULL,
    region TEXT,
    is_spot INTEGER NOT NULL DEFAULT 0,
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    duration_ms INTEGER,
    estimated_cost_usd REAL,
    FOREIGN KEY (instance_id) REFERENCES instances(id),
    FOREIGN KEY (allocation_id) REFERENCES allocations(id),
    FOREIGN KEY (run_id) REFERENCES runs(id)
);

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
    provider TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_name TEXT,
    reason TEXT NOT NULL,
    acknowledged_by TEXT NOT NULL,
    acknowledged_at INTEGER NOT NULL,
    expires_at INTEGER,
    notes TEXT,
    UNIQUE(provider, provider_id)
);

CREATE INDEX idx_whitelist_expires ON orphan_whitelist(expires_at);
CREATE INDEX idx_whitelist_provider ON orphan_whitelist(provider, resource_type);

CREATE TABLE api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key_hash TEXT NOT NULL,
    name TEXT NOT NULL,
    permissions TEXT NOT NULL DEFAULT 'all',
    created_at INTEGER NOT NULL,
    last_used_at INTEGER,
    expires_at INTEGER,
    revoked_at INTEGER,
    UNIQUE(key_hash)
);

CREATE INDEX idx_api_keys_hash ON api_keys(key_hash) WHERE revoked_at IS NULL;

CREATE TABLE rate_limit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    window_ms INTEGER NOT NULL
);

CREATE INDEX idx_rate_limit_key ON rate_limit_log(key, endpoint, timestamp);
CREATE INDEX idx_rate_limit_cleanup ON rate_limit_log(timestamp);

CREATE TABLE orphan_scans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    scanned_at INTEGER NOT NULL,
    orphans_found INTEGER NOT NULL,
    orphan_ids TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_orphan_scans_provider ON orphan_scans(provider, scanned_at DESC);

CREATE TABLE manifest_cleanup_state (
    manifest_id INTEGER PRIMARY KEY,
    status TEXT NOT NULL,
    current_step TEXT,
    progress_json TEXT,
    started_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    finished_at INTEGER,
    FOREIGN KEY (manifest_id) REFERENCES manifests(id)
);
