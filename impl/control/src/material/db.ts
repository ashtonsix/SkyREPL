// material/db.ts - Database Layer

import { Database, type SQLQueryBindings } from "bun:sqlite";
import {
  SkyREPLError,
  ConflictError,
  NotFoundError,
  calculateBackoff,
} from "@skyrepl/shared";

// @ts-expect-error -- Bun handles `with { type: "text" }` at runtime; tsc lacks the declaration
import migrationSql from "../migrations/001_initial.sql" with { type: "text" };

// =============================================================================
// Types
// =============================================================================

export interface Instance {
  id: number;
  provider: string;
  provider_id: string;
  spec: string;
  region: string;
  ip: string | null;
  workflow_state: string;
  workflow_error: string | null;
  current_manifest_id: number | null;
  spawn_idempotency_key: string | null;
  is_spot: number;
  spot_request_id: string | null;
  init_checksum: string | null;
  created_at: number;
  last_heartbeat: number;
}

export interface Run {
  id: number;
  command: string;
  workdir: string;
  max_duration_ms: number;
  workflow_state: string;
  workflow_error: string | null;
  current_manifest_id: number | null;
  exit_code: number | null;
  init_checksum: string | null;
  create_snapshot: number;
  spot_interrupted: number;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
}

export interface Allocation {
  id: number;
  run_id: number | null;
  instance_id: number;
  status: "AVAILABLE" | "CLAIMED" | "ACTIVE" | "COMPLETE" | "FAILED";
  current_manifest_id: number | null;
  user: string;
  workdir: string;
  debug_hold_until: number | null;
  created_at: number;
  updated_at: number;
  completed_at: number | null;
}

export interface Manifest {
  id: number;
  workflow_id: number;
  status: "DRAFT" | "SEALED";
  default_cleanup_priority: number;
  retention_ms: number | null;
  created_at: number;
  released_at: number | null;
  expires_at: number | null;
  updated_at: number;
}

export interface ManifestResource {
  manifest_id: number;
  resource_type: string;
  resource_id: string;
  cleanup_priority: number | null;
  added_at: number;
}

export interface Blob {
  id: number;
  bucket: string;
  checksum: string;
  checksum_bytes: number | null;
  s3_key: string | null;
  s3_bucket: string | null;
  payload: Buffer | null;
  size_bytes: number;
  created_at: number;
  last_referenced_at: number;
}

export interface StorageObject {
  id: number;
  type: "snapshot" | "artifact" | "log" | "run_file" | "tailscale_machine" | "feature_installation";
  blob_id: number;
  provider: string | null;
  provider_object_id: string | null;
  metadata_json: string | null;
  expires_at: number | null;
  current_manifest_id: number | null;
  created_at: number;
  accessed_at: number | null;
}

export interface Workflow {
  id: number;
  type: string;
  parent_workflow_id: number | null;
  depth: number;
  status:
    | "pending"
    | "running"
    | "completed"
    | "failed"
    | "cancelled"
    | "rolling_back";
  current_node: string | null;
  input_json: string;
  output_json: string | null;
  error_json: string | null;
  manifest_id: number | null;
  trace_id: string | null;
  idempotency_key: string | null;
  timeout_ms: number | null;
  timeout_at: number | null;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  updated_at: number;
}

export interface WorkflowNode {
  id: number;
  workflow_id: number;
  node_id: string;
  node_type: string;
  status: "pending" | "running" | "completed" | "failed" | "skipped";
  input_json: string;
  output_json: string | null;
  error_json: string | null;
  depends_on: string | null;
  attempt: number;
  retry_reason: string | null;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  updated_at: number;
}

export interface UsageRecord {
  id: number;
  instance_id: number;
  allocation_id: number | null;
  run_id: number | null;
  provider: string;
  spec: string;
  region: string | null;
  is_spot: number;
  started_at: number;
  finished_at: number | null;
  duration_ms: number | null;
  estimated_cost_usd: number | null;
}

export interface OrphanScanResult {
  provider: string;
  scanned_at: number;
  orphans_found: number;
  orphan_ids: string[];
}

interface Migration {
  version: number;
  sql: string;
}

interface AddResourceOptions {
  cleanupPriority?: number;
  allowRecovery?: boolean;
}

// =============================================================================
// Connection Management
// =============================================================================

let connection: Database | null = null;

export function initDatabase(path: string): Database {
  connection = new Database(path);
  connection.exec("PRAGMA journal_mode = WAL");
  connection.exec("PRAGMA foreign_keys = ON");
  connection.exec("PRAGMA busy_timeout = 5000");
  return connection;
}

export function getDatabase(): Database {
  if (!connection) {
    throw new SkyREPLError("DATABASE_ERROR", "Database not initialized", "internal");
  }
  return connection;
}

export function closeDatabase(): void {
  if (connection) {
    connection.close();
    connection = null;
  }
}

// =============================================================================
// Migrations
// =============================================================================

const MIGRATIONS: Migration[] = [
  { version: 1, sql: migrationSql },
];

export function getMigrationVersion(): number {
  const db = getDatabase();
  try {
    const result = db.prepare("SELECT version FROM schema_version").get() as { version: number } | undefined;
    return result?.version ?? 0;
  } catch {
    return 0; // Table doesn't exist yet
  }
}

export function runMigrations(): void {
  const db = getDatabase();
  const current = getMigrationVersion();

  for (const migration of MIGRATIONS) {
    if (migration.version <= current) continue;
    db.exec(migration.sql);
  }
}

// =============================================================================
// Query Helpers
// =============================================================================

export function queryOne<T>(sql: string, params: unknown[] = []): T | null {
  const db = getDatabase();
  const row = db.prepare(sql).get(...(params as SQLQueryBindings[])) as T | undefined;
  return row ?? null;
}

export function queryMany<T>(sql: string, params: unknown[] = []): T[] {
  const db = getDatabase();
  return db.prepare(sql).all(...(params as SQLQueryBindings[])) as T[];
}

export function execute(sql: string, params: unknown[] = []): void {
  const db = getDatabase();
  db.prepare(sql).run(...(params as SQLQueryBindings[]));
}

export function transaction<T>(fn: () => T, retries = 3): T {
  const db = getDatabase();

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return db.transaction(fn)();
    } catch (e) {
      const error = e as { code?: string; message: string };

      if (error.code === "SQLITE_BUSY" && attempt < retries) {
        const delay = calculateBackoff(attempt);
        Bun.sleepSync(delay);
        continue;
      }

      throw new SkyREPLError("DATABASE_ERROR", error.message, "internal", { cause: e });
    }
  }

  // Unreachable, but TypeScript needs this
  throw new SkyREPLError("DATABASE_ERROR", "Transaction retry exhausted", "internal");
}

// =============================================================================
// Instance Operations
// =============================================================================

export function getInstance(id: number): Instance | null {
  return queryOne<Instance>("SELECT * FROM instances WHERE id = ?", [id]);
}

export function getInstanceByProviderId(
  provider: string,
  providerId: string
): Instance | null {
  return queryOne<Instance>(
    "SELECT * FROM instances WHERE provider = ? AND provider_id = ?",
    [provider, providerId]
  );
}

export function createInstance(
  data: Omit<Instance, "id" | "created_at">
): Instance {
  const now = Date.now();
  const db = getDatabase();

  const stmt = db.prepare(`
    INSERT INTO instances (provider, provider_id, spec, region, ip, workflow_state, workflow_error, current_manifest_id, spawn_idempotency_key, is_spot, spot_request_id, init_checksum, created_at, last_heartbeat)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const result = stmt.run(
    data.provider,
    data.provider_id,
    data.spec,
    data.region,
    data.ip,
    data.workflow_state,
    data.workflow_error,
    data.current_manifest_id,
    data.spawn_idempotency_key,
    data.is_spot,
    data.spot_request_id,
    data.init_checksum,
    now,
    data.last_heartbeat
  );

  return getInstance(result.lastInsertRowid as number)!;
}

export function updateInstance(
  id: number,
  updates: Partial<Instance>
): Instance {
  const db = getDatabase();

  const fields = Object.keys(updates).filter(k => k !== "id");
  const setClause = fields.map(f => `${f} = ?`).join(", ");
  const values = fields.map(f => updates[f as keyof Instance]);

  db.prepare(`UPDATE instances SET ${setClause} WHERE id = ?`)
    .run(...([...values, id] as SQLQueryBindings[]));

  return getInstance(id)!;
}

export function listInstances(filter?: {
  provider?: string;
  workflow_state?: string;
}): Instance[] {
  let sql = "SELECT * FROM instances WHERE 1=1";
  const params: unknown[] = [];

  if (filter?.provider) {
    sql += " AND provider = ?";
    params.push(filter.provider);
  }
  if (filter?.workflow_state) {
    sql += " AND workflow_state = ?";
    params.push(filter.workflow_state);
  }

  return queryMany<Instance>(sql, params);
}

export function deleteInstance(id: number): void {
  const db = getDatabase();

  // Check no active allocations
  const active = queryOne<{ id: number }>(
    `SELECT id FROM allocations WHERE instance_id = ? AND status NOT IN ('COMPLETE', 'FAILED')`,
    [id]
  );

  if (active) {
    throw new ConflictError("Cannot delete instance with active allocations");
  }

  db.prepare("DELETE FROM instances WHERE id = ?").run(id);
}

// =============================================================================
// Allocation Operations
// =============================================================================

export function getAllocation(id: number): Allocation | null {
  return queryOne<Allocation>("SELECT * FROM allocations WHERE id = ?", [id]);
}

export function createAllocation(
  data: Omit<Allocation, "id" | "created_at" | "updated_at">
): Allocation {
  const now = Date.now();
  const db = getDatabase();

  try {
    const stmt = db.prepare(`
      INSERT INTO allocations (run_id, instance_id, status, current_manifest_id, user, workdir, debug_hold_until, completed_at, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const result = stmt.run(
      data.run_id,
      data.instance_id,
      data.status,
      data.current_manifest_id,
      data.user,
      data.workdir,
      data.debug_hold_until,
      data.completed_at,
      now,
      now
    );

    return getAllocation(result.lastInsertRowid as number)!;
  } catch (e) {
    const error = e as { code?: string };
    if (error.code === "SQLITE_CONSTRAINT_FOREIGNKEY") {
      throw new ConflictError("Instance not found");
    }
    throw e;
  }
}

export function updateAllocationStatus(
  id: number,
  status: Allocation["status"]
): Allocation {
  const now = Date.now();

  execute(
    "UPDATE allocations SET status = ?, updated_at = ? WHERE id = ?",
    [status, now, id]
  );

  return getAllocation(id)!;
}

export function claimAllocation(
  id: number,
  runId: number
): Allocation | null {
  const db = getDatabase();

  return db.transaction(() => {
    // Phase 1: Read current state
    const allocation = queryOne<Allocation>(
      "SELECT * FROM allocations WHERE id = ? AND status = ? AND run_id IS NULL",
      [id, "AVAILABLE"]
    );

    if (!allocation) {
      // Not found, wrong status, or already claimed
      return null;
    }

    // Phase 2: CAS update with updated_at guard
    const now = Date.now();
    const result = db.prepare(`
      UPDATE allocations
      SET run_id = ?, status = 'CLAIMED', updated_at = ?
      WHERE id = ? AND updated_at = ?
    `).run(runId, now, id, allocation.updated_at);

    if (result.changes === 0) {
      // Race lost - another transaction modified the row
      return null;
    }

    return getAllocation(id);
  })();
}

export function findAvailableAllocation(spec: {
  spec: string;
  region?: string;
}): Allocation | null {
  let sql = `
    SELECT a.* FROM allocations a
    JOIN instances i ON a.instance_id = i.id
    WHERE a.status = 'AVAILABLE' AND a.run_id IS NULL
      AND i.spec = ?
  `;
  const params: unknown[] = [spec.spec];

  if (spec.region) {
    sql += " AND i.region = ?";
    params.push(spec.region);
  }

  sql += " LIMIT 1";

  return queryOne<Allocation>(sql, params);
}

export function deleteAllocation(id: number): void {
  const allocation = getAllocation(id);
  if (allocation && !["COMPLETE", "FAILED"].includes(allocation.status)) {
    throw new ConflictError("Cannot delete active allocation");
  }

  execute("DELETE FROM allocations WHERE id = ?", [id]);
}

// =============================================================================
// Run Operations
// =============================================================================

export function getRun(id: number): Run | null {
  return queryOne<Run>("SELECT * FROM runs WHERE id = ?", [id]);
}

export function createRun(data: Omit<Run, "id" | "created_at">): Run {
  const now = Date.now();
  const db = getDatabase();

  const stmt = db.prepare(`
    INSERT INTO runs (command, workdir, max_duration_ms, workflow_state, workflow_error, current_manifest_id, exit_code, init_checksum, create_snapshot, spot_interrupted, created_at, started_at, finished_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const result = stmt.run(
    data.command,
    data.workdir,
    data.max_duration_ms,
    data.workflow_state,
    data.workflow_error,
    data.current_manifest_id,
    data.exit_code,
    data.init_checksum,
    data.create_snapshot,
    data.spot_interrupted,
    now,
    data.started_at,
    data.finished_at
  );

  return getRun(result.lastInsertRowid as number)!;
}

export function updateRun(id: number, updates: Partial<Run>): Run {
  const db = getDatabase();

  const fields = Object.keys(updates).filter(k => k !== "id");
  const setClause = fields.map(f => `${f} = ?`).join(", ");
  const values = fields.map(f => updates[f as keyof Run]);

  db.prepare(`UPDATE runs SET ${setClause} WHERE id = ?`)
    .run(...([...values, id] as SQLQueryBindings[]));

  return getRun(id)!;
}

export function listRuns(filter?: {
  current_manifest_id?: number;
  workflow_state?: string;
}): Run[] {
  let sql = "SELECT * FROM runs WHERE 1=1";
  const params: unknown[] = [];

  if (filter?.current_manifest_id) {
    sql += " AND current_manifest_id = ?";
    params.push(filter.current_manifest_id);
  }
  if (filter?.workflow_state) {
    sql += " AND workflow_state = ?";
    params.push(filter.workflow_state);
  }

  return queryMany<Run>(sql, params);
}

export function deleteRun(id: number): void {
  const run = getRun(id);
  if (run && !["completed", "failed"].includes(run.workflow_state.split(":")[1] ?? "")) {
    throw new ConflictError("Cannot delete active run");
  }

  execute("DELETE FROM runs WHERE id = ?", [id]);
}

// =============================================================================
// Manifest Operations
// =============================================================================

export function getManifest(id: number): Manifest | null {
  return queryOne<Manifest>("SELECT * FROM manifests WHERE id = ?", [id]);
}

export function createManifest(
  workflowId: number,
  options?: { default_cleanup_priority?: number; retention_ms?: number }
): Manifest {
  const now = Date.now();
  const db = getDatabase();

  const stmt = db.prepare(`
    INSERT INTO manifests (workflow_id, status, default_cleanup_priority, retention_ms, created_at, released_at, expires_at, updated_at)
    VALUES (?, 'DRAFT', ?, ?, ?, NULL, NULL, ?)
  `);

  const result = stmt.run(
    workflowId,
    options?.default_cleanup_priority ?? 50,
    options?.retention_ms ?? null,
    now,
    now
  );

  return getManifest(result.lastInsertRowid as number)!;
}

export function sealManifest(id: number): void {
  const db = getDatabase();
  const now = Date.now();

  // First, get the manifest to check status and compute expires_at
  const manifest = getManifest(id);
  if (!manifest) {
    throw new NotFoundError("Manifest", id);
  }
  if (manifest.status === "SEALED") {
    throw new ConflictError("Manifest already sealed");
  }

  const expiresAt = manifest.retention_ms != null ? now + manifest.retention_ms : null;

  const result = db.prepare(`
    UPDATE manifests SET status = 'SEALED', released_at = ?, expires_at = ?, updated_at = ?
    WHERE id = ? AND status = 'DRAFT'
  `).run(now, expiresAt, now, id);

  if (result.changes === 0) {
    throw new ConflictError("Manifest could not be sealed");
  }
}

export function addResourceToManifest(
  manifestId: number,
  resourceType: string,
  resourceId: string,
  options?: AddResourceOptions
): void {
  const now = Date.now();

  const manifest = getManifest(manifestId);
  if (!manifest) {
    throw new ConflictError("Manifest not found");
  }

  // Enforce immutability: SEALED manifests reject additions
  // Exception: A13 recovery path can update provider_id for orphan matching
  if (manifest.status !== "DRAFT" && !options?.allowRecovery) {
    throw new ConflictError("Cannot add resources to sealed manifest");
  }

  execute(
    `INSERT INTO manifest_resources (manifest_id, resource_type, resource_id, cleanup_priority, added_at)
     VALUES (?, ?, ?, ?, ?)`,
    [manifestId, resourceType, resourceId, options?.cleanupPriority ?? null, now]
  );
}

export function getManifestResources(manifestId: number): ManifestResource[] {
  return queryMany<ManifestResource>(
    "SELECT * FROM manifest_resources WHERE manifest_id = ? ORDER BY cleanup_priority DESC",
    [manifestId]
  );
}

export function deleteManifest(id: number): void {
  const db = getDatabase();

  db.transaction(() => {
    // Cascade delete manifest_resources
    db.prepare("DELETE FROM manifest_resources WHERE manifest_id = ?").run(id);
    db.prepare("DELETE FROM manifests WHERE id = ?").run(id);
  })();
}

export function listExpiredManifests(cutoffTime: number): Manifest[] {
  return queryMany<Manifest>(
    "SELECT * FROM manifests WHERE status = ? AND expires_at < ?",
    ["SEALED", cutoffTime]
  );
}

export function getManifestObjectIds(manifestId: number): string[] {
  const rows = queryMany<{ resource_id: string }>(
    `SELECT resource_id FROM manifest_resources
     WHERE manifest_id = ? AND resource_type = 'object'`,
    [manifestId]
  );

  return rows.map(r => r.resource_id);
}

export function deleteObjectBatch(objectIds: number[]): void {
  if (objectIds.length === 0) return;

  const db = getDatabase();
  const placeholders = objectIds.map(() => "?").join(",");

  db.prepare(`DELETE FROM objects WHERE id IN (${placeholders})`).run(...objectIds);
}

// =============================================================================
// Workflow Operations
// =============================================================================

export function getWorkflow(id: number): Workflow | null {
  return queryOne<Workflow>("SELECT * FROM workflows WHERE id = ?", [id]);
}

export function createWorkflow(
  data: Omit<Workflow, "id" | "created_at">
): Workflow {
  const now = Date.now();
  const db = getDatabase();

  const stmt = db.prepare(`
    INSERT INTO workflows (type, parent_workflow_id, depth, status, current_node, input_json, output_json, error_json, manifest_id, trace_id, idempotency_key, timeout_ms, timeout_at, created_at, started_at, finished_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const result = stmt.run(
    data.type,
    data.parent_workflow_id,
    data.depth,
    data.status,
    data.current_node,
    data.input_json,
    data.output_json,
    data.error_json,
    data.manifest_id,
    data.trace_id,
    data.idempotency_key,
    data.timeout_ms,
    data.timeout_at,
    now,
    data.started_at,
    data.finished_at,
    data.updated_at
  );

  return getWorkflow(result.lastInsertRowid as number)!;
}

export function updateWorkflow(
  id: number,
  updates: Partial<Workflow>
): Workflow {
  const db = getDatabase();

  const fields = Object.keys(updates).filter(k => k !== "id");
  if (!fields.includes("updated_at")) {
    fields.push("updated_at");
    (updates as Record<string, unknown>).updated_at = Date.now();
  }
  const setClause = fields.map(f => `${f} = ?`).join(", ");
  const values = fields.map(f => updates[f as keyof Workflow]);

  db.prepare(`UPDATE workflows SET ${setClause} WHERE id = ?`)
    .run(...([...values, id] as SQLQueryBindings[]));

  return getWorkflow(id)!;
}

export function getWorkflowNodes(workflowId: number): WorkflowNode[] {
  return queryMany<WorkflowNode>(
    "SELECT * FROM workflow_nodes WHERE workflow_id = ?",
    [workflowId]
  );
}

export function createWorkflowNode(
  data: Omit<WorkflowNode, "id" | "created_at">
): WorkflowNode {
  const db = getDatabase();
  const now = Date.now();

  const stmt = db.prepare(`
    INSERT INTO workflow_nodes (workflow_id, node_id, node_type, status, input_json, output_json, error_json, depends_on, attempt, retry_reason, created_at, started_at, finished_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const result = stmt.run(
    data.workflow_id,
    data.node_id,
    data.node_type,
    data.status,
    data.input_json,
    data.output_json,
    data.error_json,
    data.depends_on,
    data.attempt,
    data.retry_reason,
    now,
    data.started_at,
    data.finished_at,
    data.updated_at
  );

  return queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [result.lastInsertRowid as number])!;
}

export function updateWorkflowNode(
  id: number,
  updates: Partial<WorkflowNode>
): WorkflowNode {
  const db = getDatabase();

  const fields = Object.keys(updates).filter(k => k !== "id");
  if (!fields.includes("updated_at")) {
    fields.push("updated_at");
    (updates as Record<string, unknown>).updated_at = Date.now();
  }
  const setClause = fields.map(f => `${f} = ?`).join(", ");
  const values = fields.map(f => updates[f as keyof WorkflowNode]);

  db.prepare(`UPDATE workflow_nodes SET ${setClause} WHERE id = ?`)
    .run(...([...values, id] as SQLQueryBindings[]));

  return queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [id])!;
}

export function findReadyNodes(workflowId: number): WorkflowNode[] {
  const nodes = getWorkflowNodes(workflowId);
  const completedOrSkipped = new Set(
    nodes
      .filter(n => n.status === 'completed' || n.status === 'skipped')
      .map(n => n.node_id)
  );
  return nodes.filter(node => {
    if (node.status !== 'pending') return false;
    if (!node.depends_on || node.depends_on === '[]') return true;
    const deps = JSON.parse(node.depends_on) as string[];
    return deps.every(depId => completedOrSkipped.has(depId));
  });
}

export function findActiveWorkflows(): Workflow[] {
  return queryMany<Workflow>("SELECT * FROM workflows WHERE status IN ('pending', 'running')");
}

export function deleteWorkflow(id: number): void {
  const db = getDatabase();

  db.transaction(() => {
    db.prepare("DELETE FROM workflow_nodes WHERE workflow_id = ?").run(id);
    db.prepare("DELETE FROM workflows WHERE id = ?").run(id);
  })();
}

// =============================================================================
// Blob Operations
// =============================================================================

export function getBlob(id: number): Blob | null {
  return queryOne<Blob>("SELECT * FROM blobs WHERE id = ?", [id]);
}

export function createBlob(data: Omit<Blob, "id" | "created_at">): Blob {
  const now = Date.now();
  const db = getDatabase();

  const DEDUPABLE_BUCKETS = ["run-files", "artifacts"];

  // For dedupable buckets, check if blob exists
  if (DEDUPABLE_BUCKETS.includes(data.bucket) && data.checksum) {
    const existing = findBlobByChecksum(data.bucket, data.checksum);
    if (existing) {
      // Update last_referenced_at and return existing
      updateBlobLastReferenced(existing.id);
      return existing;
    }
  }

  const stmt = db.prepare(`
    INSERT INTO blobs (bucket, checksum, checksum_bytes, s3_key, s3_bucket, payload, size_bytes, created_at, last_referenced_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const result = stmt.run(
    data.bucket,
    data.checksum,
    data.checksum_bytes,
    data.s3_key,
    data.s3_bucket,
    data.payload,
    data.size_bytes,
    now,
    data.last_referenced_at
  );

  return getBlob(result.lastInsertRowid as number)!;
}

export function findBlobByChecksum(
  bucket: string,
  checksum: string
): Blob | null {
  return queryOne<Blob>(
    "SELECT * FROM blobs WHERE bucket = ? AND checksum = ? LIMIT 1",
    [bucket, checksum]
  );
}

export function updateBlobLastReferenced(id: number): void {
  const now = Date.now();
  execute("UPDATE blobs SET last_referenced_at = ? WHERE id = ?", [now, id]);
}

export function findOrphanedBlobs(cutoff24hAgo: number): Blob[] {
  return queryMany<Blob>(
    `SELECT b.*
     FROM blobs b
     WHERE b.last_referenced_at < ?
       AND NOT EXISTS (
         SELECT 1 FROM objects o
         WHERE o.blob_id = b.id
       )`,
    [cutoff24hAgo]
  );
}

export function deleteBlobBatch(blobIds: number[]): void {
  if (blobIds.length === 0) return;

  const db = getDatabase();
  const placeholders = blobIds.map(() => "?").join(",");

  db.prepare(`DELETE FROM blobs WHERE id IN (${placeholders})`).run(...blobIds);
}

export function deleteBlob(id: number): void {
  // Used by GC only - checks no objects reference this blob
  const refs = queryOne<{ count: number }>(
    "SELECT COUNT(*) as count FROM objects WHERE blob_id = ?",
    [id]
  );

  if (refs && refs.count > 0) {
    throw new ConflictError("Cannot delete blob with referencing objects");
  }

  execute("DELETE FROM blobs WHERE id = ?", [id]);
}

// =============================================================================
// Object Operations
// =============================================================================

export function getObject(id: number): StorageObject | null {
  return queryOne<StorageObject>("SELECT * FROM objects WHERE id = ?", [id]);
}

export function createObject(
  data: Omit<StorageObject, "id" | "created_at">
): StorageObject {
  const now = Date.now();
  const db = getDatabase();

  const stmt = db.prepare(`
    INSERT INTO objects (type, blob_id, provider, provider_object_id, metadata_json, expires_at, current_manifest_id, created_at, accessed_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const result = stmt.run(
    data.type,
    data.blob_id,
    data.provider,
    data.provider_object_id,
    data.metadata_json,
    data.expires_at,
    data.current_manifest_id,
    now,
    data.accessed_at
  );

  // Touch blob last_referenced_at
  updateBlobLastReferenced(data.blob_id);

  return getObject(result.lastInsertRowid as number)!;
}

export function addObjectTag(
  objectId: number,
  key: string,
  value: string
): void {
  const tag = `${key}:${value}`;

  execute(
    `INSERT INTO object_tags (object_id, tag)
     VALUES (?, ?)
     ON CONFLICT (object_id, tag) DO NOTHING`,
    [objectId, tag]
  );
}

export function findObjectByTag(
  key: string,
  value: string
): StorageObject | null {
  const tag = `${key}:${value}`;
  return queryOne<StorageObject>(
    `SELECT o.* FROM objects o
     JOIN object_tags t ON o.id = t.object_id
     WHERE t.tag = ?`,
    [tag]
  );
}

export function deleteObject(id: number): void {
  execute("DELETE FROM objects WHERE id = ?", [id]);
}

// =============================================================================
// Warm Pool Operations
// =============================================================================

export function findWarmAllocation(
  spec: { spec: string; region?: string },
  initChecksum?: string
): Allocation | null {
  let sql = `
    SELECT a.* FROM allocations a
    JOIN instances i ON a.instance_id = i.id
    WHERE a.status = 'AVAILABLE' AND a.run_id IS NULL
      AND i.spec = ?
      AND i.workflow_state LIKE '%:complete'
  `;
  const params: unknown[] = [spec.spec];

  if (spec.region) {
    sql += " AND i.region = ?";
    params.push(spec.region);
  }

  if (initChecksum) {
    sql += " AND i.init_checksum = ?";
    params.push(initChecksum);
  }

  sql += " ORDER BY a.created_at ASC LIMIT 1";

  return queryOne<Allocation>(sql, params);
}

export function getWarmPoolStats(): {
  available: number;
  bySpec: Record<string, number>;
} {
  const total = queryOne<{ count: number }>(
    "SELECT COUNT(*) as count FROM allocations WHERE status = 'AVAILABLE'"
  );

  const bySpec = queryMany<{ spec: string; count: number }>(
    `SELECT i.spec || ':' || i.region as spec, COUNT(*) as count
     FROM allocations a
     JOIN instances i ON a.instance_id = i.id
     WHERE a.status = 'AVAILABLE'
     GROUP BY spec`
  );

  return {
    available: total?.count ?? 0,
    bySpec: Object.fromEntries(bySpec.map(r => [r.spec, r.count])),
  };
}

// =============================================================================
// Orphan Operations
// =============================================================================

export function getTrackedInstanceIds(): Set<number> {
  const rows = queryMany<{ id: number }>("SELECT id FROM instances");
  return new Set(rows.map(r => r.id));
}

export function getActiveManifestIds(): Set<number> {
  const now = Date.now();
  const rows = queryMany<{ id: number }>(
    `SELECT id FROM manifests
     WHERE status = 'DRAFT' OR (status = 'SEALED' AND expires_at > ?)`,
    [now]
  );
  return new Set(rows.map(r => r.id));
}

export function recordOrphanScan(result: OrphanScanResult): void {
  execute(
    `INSERT INTO orphan_scans (provider, scanned_at, orphans_found, orphan_ids, created_at)
     VALUES (?, ?, ?, ?, ?)`,
    [result.provider, result.scanned_at, result.orphans_found, JSON.stringify(result.orphan_ids), Date.now()]
  );
}

export function addToWhitelist(
  provider: string,
  providerId: string,
  resourceType: string,
  reason: string,
  acknowledgedBy: string
): void {
  const now = Date.now();

  execute(
    `INSERT INTO orphan_whitelist (provider, provider_id, resource_type, reason, acknowledged_by, acknowledged_at)
     VALUES (?, ?, ?, ?, ?, ?)
     ON CONFLICT (provider, provider_id) DO UPDATE SET reason = excluded.reason, acknowledged_by = excluded.acknowledged_by, acknowledged_at = excluded.acknowledged_at`,
    [provider, providerId, resourceType, reason, acknowledgedBy, now]
  );
}

export function isWhitelisted(
  provider: string,
  providerId: string
): boolean {
  const result = queryOne<{ provider_id: string }>(
    "SELECT provider_id FROM orphan_whitelist WHERE provider = ? AND provider_id = ?",
    [provider, providerId]
  );
  return result !== null;
}

// =============================================================================
// Usage Record Operations
// =============================================================================

export function createUsageRecord(
  data: Omit<UsageRecord, "id">
): UsageRecord {
  const db = getDatabase();

  const stmt = db.prepare(`
    INSERT INTO usage_records (instance_id, allocation_id, run_id, provider, spec, region, is_spot, started_at, finished_at, duration_ms, estimated_cost_usd)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const result = stmt.run(
    data.instance_id,
    data.allocation_id,
    data.run_id,
    data.provider,
    data.spec,
    data.region,
    data.is_spot,
    data.started_at,
    data.finished_at,
    data.duration_ms,
    data.estimated_cost_usd
  );

  return queryOne<UsageRecord>("SELECT * FROM usage_records WHERE id = ?", [result.lastInsertRowid as number])!;
}

export function finishUsageRecord(
  id: number,
  finishedAt: number
): UsageRecord {
  const record = queryOne<UsageRecord>("SELECT * FROM usage_records WHERE id = ?", [id]);
  if (!record) {
    throw new NotFoundError("UsageRecord", id);
  }

  const durationMs = finishedAt - record.started_at;

  execute(
    `UPDATE usage_records SET finished_at = ?, duration_ms = ? WHERE id = ?`,
    [finishedAt, durationMs, id]
  );

  return queryOne<UsageRecord>("SELECT * FROM usage_records WHERE id = ?", [id])!;
}

export function getMonthlyCostByProvider(
  monthStart: number,
  monthEnd: number
): { provider: string; total_cost: number }[] {
  return queryMany<{ provider: string; total_cost: number }>(
    `SELECT provider, SUM(estimated_cost_usd) as total_cost
     FROM usage_records
     WHERE started_at >= ? AND started_at < ?
     GROUP BY provider`,
    [monthStart, monthEnd]
  );
}

export function getActiveUsageRecords(): UsageRecord[] {
  return queryMany<UsageRecord>(
    "SELECT * FROM usage_records WHERE finished_at IS NULL"
  );
}
