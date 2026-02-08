// material/db.ts - Database Layer
// Stub: All function bodies throw "not implemented"

import { Database } from "bun:sqlite";

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

export function initDatabase(path: string): Database {
  throw new Error("not implemented");
}

export function getDatabase(): Database {
  throw new Error("not implemented");
}

export function closeDatabase(): void {
  throw new Error("not implemented");
}

// =============================================================================
// Migrations
// =============================================================================

export function getMigrationVersion(): number {
  throw new Error("not implemented");
}

export function runMigrations(): void {
  throw new Error("not implemented");
}

// =============================================================================
// Query Helpers
// =============================================================================

export function queryOne<T>(sql: string, params?: unknown[]): T | null {
  throw new Error("not implemented");
}

export function queryMany<T>(sql: string, params?: unknown[]): T[] {
  throw new Error("not implemented");
}

export function execute(sql: string, params?: unknown[]): void {
  throw new Error("not implemented");
}

export function transaction<T>(fn: () => T, retries?: number): T {
  throw new Error("not implemented");
}

// =============================================================================
// Instance Operations
// =============================================================================

export function getInstance(id: number): Instance | null {
  throw new Error("not implemented");
}

export function getInstanceByProviderId(
  provider: string,
  providerId: string
): Instance | null {
  throw new Error("not implemented");
}

export function createInstance(
  data: Omit<Instance, "id" | "created_at">
): Instance {
  throw new Error("not implemented");
}

export function updateInstance(
  id: number,
  updates: Partial<Instance>
): Instance {
  throw new Error("not implemented");
}

export function listInstances(filter?: {
  provider?: string;
  workflow_state?: string;
}): Instance[] {
  throw new Error("not implemented");
}

export function deleteInstance(id: number): void {
  throw new Error("not implemented");
}

// =============================================================================
// Allocation Operations
// =============================================================================

export function getAllocation(id: number): Allocation | null {
  throw new Error("not implemented");
}

export function createAllocation(
  data: Omit<Allocation, "id" | "created_at" | "updated_at">
): Allocation {
  throw new Error("not implemented");
}

export function updateAllocationStatus(
  id: number,
  status: Allocation["status"]
): Allocation {
  throw new Error("not implemented");
}

export function claimAllocation(
  id: number,
  runId: number
): Allocation | null {
  throw new Error("not implemented");
}

export function findAvailableAllocation(spec: {
  spec: string;
  region?: string;
}): Allocation | null {
  throw new Error("not implemented");
}

export function deleteAllocation(id: number): void {
  throw new Error("not implemented");
}

// =============================================================================
// Run Operations
// =============================================================================

export function getRun(id: number): Run | null {
  throw new Error("not implemented");
}

export function createRun(data: Omit<Run, "id" | "created_at">): Run {
  throw new Error("not implemented");
}

export function updateRun(id: number, updates: Partial<Run>): Run {
  throw new Error("not implemented");
}

export function listRuns(filter?: {
  current_manifest_id?: number;
  workflow_state?: string;
}): Run[] {
  throw new Error("not implemented");
}

export function deleteRun(id: number): void {
  throw new Error("not implemented");
}

// =============================================================================
// Manifest Operations
// =============================================================================

export function getManifest(id: number): Manifest | null {
  throw new Error("not implemented");
}

export function createManifest(
  workflowId: number,
  options?: { default_cleanup_priority?: number; retention_ms?: number }
): Manifest {
  throw new Error("not implemented");
}

export function sealManifest(id: number): void {
  throw new Error("not implemented");
}

export function addResourceToManifest(
  manifestId: number,
  resourceType: string,
  resourceId: string,
  options?: AddResourceOptions
): void {
  throw new Error("not implemented");
}

export function getManifestResources(manifestId: number): ManifestResource[] {
  throw new Error("not implemented");
}

export function deleteManifest(id: number): void {
  throw new Error("not implemented");
}

export function listExpiredManifests(cutoffTime: number): Manifest[] {
  throw new Error("not implemented");
}

export function getManifestObjectIds(manifestId: number): string[] {
  throw new Error("not implemented");
}

export function deleteObjectBatch(objectIds: number[]): void {
  throw new Error("not implemented");
}

// =============================================================================
// Workflow Operations
// =============================================================================

export function getWorkflow(id: number): Workflow | null {
  throw new Error("not implemented");
}

export function createWorkflow(
  data: Omit<Workflow, "id" | "created_at">
): Workflow {
  throw new Error("not implemented");
}

export function updateWorkflow(
  id: number,
  updates: Partial<Workflow>
): Workflow {
  throw new Error("not implemented");
}

export function getWorkflowNodes(workflowId: number): WorkflowNode[] {
  throw new Error("not implemented");
}

export function createWorkflowNode(
  data: Omit<WorkflowNode, "id" | "created_at">
): WorkflowNode {
  throw new Error("not implemented");
}

export function updateWorkflowNode(
  id: number,
  updates: Partial<WorkflowNode>
): WorkflowNode {
  throw new Error("not implemented");
}

export function deleteWorkflow(id: number): void {
  throw new Error("not implemented");
}

// =============================================================================
// Blob Operations
// =============================================================================

export function getBlob(id: number): Blob | null {
  throw new Error("not implemented");
}

export function createBlob(data: Omit<Blob, "id" | "created_at">): Blob {
  throw new Error("not implemented");
}

export function findBlobByChecksum(
  bucket: string,
  checksum: string
): Blob | null {
  throw new Error("not implemented");
}

export function updateBlobLastReferenced(id: number): void {
  throw new Error("not implemented");
}

export function findOrphanedBlobs(cutoff24hAgo: number): Blob[] {
  throw new Error("not implemented");
}

export function deleteBlobBatch(blobIds: number[]): void {
  throw new Error("not implemented");
}

export function deleteBlob(id: number): void {
  throw new Error("not implemented");
}

// =============================================================================
// Object Operations
// =============================================================================

export function getObject(id: number): StorageObject | null {
  throw new Error("not implemented");
}

export function createObject(
  data: Omit<StorageObject, "id" | "created_at">
): StorageObject {
  throw new Error("not implemented");
}

export function addObjectTag(
  objectId: number,
  key: string,
  value: string
): void {
  throw new Error("not implemented");
}

export function findObjectByTag(
  key: string,
  value: string
): StorageObject | null {
  throw new Error("not implemented");
}

export function deleteObject(id: number): void {
  throw new Error("not implemented");
}

// =============================================================================
// Warm Pool Operations
// =============================================================================

export function findWarmAllocation(
  spec: { spec: string; region?: string },
  initChecksum?: string
): Allocation | null {
  throw new Error("not implemented");
}

export function getWarmPoolStats(): {
  available: number;
  bySpec: Record<string, number>;
} {
  throw new Error("not implemented");
}

// =============================================================================
// Orphan Operations
// =============================================================================

export function getTrackedInstanceIds(): Set<number> {
  throw new Error("not implemented");
}

export function getActiveManifestIds(): Set<number> {
  throw new Error("not implemented");
}

export function recordOrphanScan(result: OrphanScanResult): void {
  throw new Error("not implemented");
}

export function addToWhitelist(
  provider: string,
  providerId: string,
  resourceType: string,
  reason: string,
  acknowledgedBy: string
): void {
  throw new Error("not implemented");
}

export function isWhitelisted(
  provider: string,
  providerId: string
): boolean {
  throw new Error("not implemented");
}

// =============================================================================
// Usage Record Operations
// =============================================================================

export function createUsageRecord(
  data: Omit<UsageRecord, "id">
): UsageRecord {
  throw new Error("not implemented");
}

export function finishUsageRecord(
  id: number,
  finishedAt: number
): UsageRecord {
  throw new Error("not implemented");
}

export function getMonthlyCostByProvider(
  monthStart: number,
  monthEnd: number
): { provider: string; total_cost: number }[] {
  throw new Error("not implemented");
}

export function getActiveUsageRecords(): UsageRecord[] {
  throw new Error("not implemented");
}
