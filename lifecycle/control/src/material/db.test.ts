import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../../../tests/harness";
import {
  runMigrations,
  getMigrationVersion,
  getInstance,
  createInstance,
  updateInstance,
  listInstances,
  deleteInstance,
  getInstanceByProviderId,
  getAllocation,
  createAllocation,
  deleteAllocation,
  getRun,
  createRun,
  updateRun,
  listRuns,
  deleteRun,
  getManifest,
  createManifest,
  addResourceToManifest,
  getManifestResources,
  deleteManifest,
  listExpiredManifests,
  getManifestObjectIds,
  deleteObjectBatch,
  getWorkflow,
  createWorkflow,
  updateWorkflow,
  getWorkflowNodes,
  createWorkflowNode,
  updateWorkflowNode,
  deleteWorkflow,
  getBlob,
  createBlob,
  findBlobByChecksum,
  updateBlobLastReferenced,
  findOrphanedBlobs,
  deleteBlobBatch,
  deleteBlob,
  getObject,
  createObject,
  addObjectTag,
  findObjectByTag,
  deleteObject,
  findWarmAllocation,
  getWarmPoolStats,
  getTrackedInstanceIds,
  getActiveManifestIds,
  recordOrphanScan,
  addToWhitelist,
  isWhitelisted,
  createUsageRecord,
  finishUsageRecord,
  getMonthlyCostByProvider,
  getActiveUsageRecords,
} from "./db";
import {
  claimAllocation,
  completeAllocation,
  sealManifest,
} from "../workflow/state-transitions";

// Helper: create a minimal workflow for FK references
function seedWorkflow(): number {
  const wf = createWorkflow({
    type: "launch-run",
    parent_workflow_id: null,
    depth: 0,
    status: "pending",
    current_node: null,
    input_json: "{}",
    output_json: null,
    error_json: null,
    manifest_id: null,
    trace_id: null,
    idempotency_key: null,
    timeout_ms: null,
    timeout_at: null,
    started_at: null,
    finished_at: null,
    updated_at: Date.now(),
  });
  return wf.id;
}

let seedCounter = 0;
function seedInstance(): number {
  const inst = createInstance({
    provider: "orbstack",
    provider_id: `test-vm-${++seedCounter}`,
    spec: "4vcpu-8gb",
    region: "local",
    ip: null,
    workflow_state: "spawn:pending",
    workflow_error: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 0,
    spot_request_id: null,
    init_checksum: null,
    registration_token_hash: null,
    last_heartbeat: Date.now(),
  });
  return inst.id;
}

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest();
});

afterEach(() => cleanup());

// =============================================================================
// Schema & Migrations
// =============================================================================

describe("schema & migrations", () => {
  test("runs migrations idempotently", () => {
    expect(getMigrationVersion()).toBe(1);
    runMigrations(); // second call should not fail
    expect(getMigrationVersion()).toBe(1);
  });
});

// =============================================================================
// Instance CRUD
// =============================================================================

describe("instances", () => {
  test("CRUD lifecycle: create, read, update, list, lookup by provider ID", () => {
    const inst = createInstance({
      provider: "orbstack",
      provider_id: "vm-abc",
      spec: "4vcpu-8gb",
      region: "local",
      ip: null,
      workflow_state: "spawn:pending",
      workflow_error: null,
      current_manifest_id: null,
      spawn_idempotency_key: null,
      is_spot: 0,
      spot_request_id: null,
      init_checksum: null,
      registration_token_hash: null,
      last_heartbeat: Date.now(),
    });

    expect(inst.id).toBeGreaterThan(0);
    expect(inst.provider).toBe("orbstack");
    expect(inst.created_at).toBeGreaterThan(0);

    const fetched = getInstance(inst.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.provider_id).toBe("vm-abc");

    const updated = updateInstance(inst.id, {
      ip: "192.168.1.1",
      workflow_state: "launch-run:provisioning",
    });
    expect(updated.ip).toBe("192.168.1.1");
    expect(updated.workflow_state).toBe("launch-run:provisioning");

    const found = getInstanceByProviderId("orbstack", "vm-abc");
    expect(found).not.toBeNull();
    expect(found!.provider_id).toBe("vm-abc");

    const notFound = getInstanceByProviderId("orbstack", "nope");
    expect(notFound).toBeNull();

    // Create another instance for list filtering
    createInstance({
      provider: "aws",
      provider_id: "b",
      spec: "4vcpu-8gb",
      region: "us-east-1",
      ip: null,
      workflow_state: "spawn:pending",
      workflow_error: null,
      current_manifest_id: null,
      spawn_idempotency_key: null,
      is_spot: 0,
      spot_request_id: null,
      init_checksum: null,
      registration_token_hash: null,
      last_heartbeat: Date.now(),
    });

    expect(listInstances().length).toBe(2);
    expect(listInstances({ provider: "orbstack" }).length).toBe(1);
    expect(listInstances({ provider: "lambda" }).length).toBe(0);
  });

  test("blocks deletion with active allocations", () => {
    const instId = seedInstance();
    createAllocation({
      run_id: null,
      instance_id: instId,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "ubuntu",
      workdir: "/home/ubuntu/work",
      debug_hold_until: null,
      completed_at: null,
    });

    expect(() => deleteInstance(instId)).toThrow("active allocations");
  });
});

// =============================================================================
// Allocation Operations
// =============================================================================

describe("allocations", () => {
  test("CRUD lifecycle and atomic claim transitions", () => {
    const instId = seedInstance();
    const alloc = createAllocation({
      run_id: null,
      instance_id: instId,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "ubuntu",
      workdir: "/home/ubuntu/work",
      debug_hold_until: null,
      completed_at: null,
    });

    expect(alloc.id).toBeGreaterThan(0);
    expect(alloc.status).toBe("AVAILABLE");
    expect(alloc.created_at).toBe(alloc.updated_at);

    const run = createRun({
      command: "echo test",
      workdir: "/home/ubuntu/work",
      max_duration_ms: 60000,
      workflow_state: "launch-run:pending",
      workflow_error: null,
      current_manifest_id: null,
      exit_code: null,
      init_checksum: null,
      create_snapshot: 0,
      spot_interrupted: 0,
      started_at: null,
      finished_at: null,
    });

    const result = claimAllocation(alloc.id, run.id);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.status).toBe("CLAIMED");
      expect(result.data.updated_at).toBeGreaterThanOrEqual(alloc.updated_at);
    }

    // Double-claim prevention
    const secondResult = claimAllocation(alloc.id, run.id);
    expect(secondResult.success).toBe(false);
  });

  test("deletion constraints: blocks active, allows terminal", () => {
    const instId = seedInstance();
    const alloc = createAllocation({
      run_id: null,
      instance_id: instId,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "ubuntu",
      workdir: "/home/ubuntu/work",
      debug_hold_until: null,
      completed_at: null,
    });

    expect(() => deleteAllocation(alloc.id)).toThrow("active allocation");

    // Transition to terminal state
    const run = createRun({
      command: "echo test",
      workdir: "/home/ubuntu/work",
      max_duration_ms: 60000,
      workflow_state: "launch-run:pending",
      workflow_error: null,
      current_manifest_id: null,
      exit_code: null,
      init_checksum: null,
      create_snapshot: 0,
      spot_interrupted: 0,
      started_at: null,
      finished_at: null,
    });
    claimAllocation(alloc.id, run.id);
    const { activateAllocation } = require("../workflow/state-transitions");
    activateAllocation(alloc.id);
    completeAllocation(alloc.id);

    deleteAllocation(alloc.id);
    expect(getAllocation(alloc.id)).toBeNull();
  });
});

// =============================================================================
// Run Operations
// =============================================================================

describe("runs", () => {
  test("CRUD lifecycle: create, read, update", () => {
    const run = createRun({
      command: "echo hello",
      workdir: "/home/ubuntu/work",
      max_duration_ms: 60000,
      workflow_state: "launch-run:pending",
      workflow_error: null,
      current_manifest_id: null,
      exit_code: null,
      init_checksum: null,
      create_snapshot: 0,
      spot_interrupted: 0,
      started_at: null,
      finished_at: null,
    });

    expect(run.id).toBeGreaterThan(0);
    expect(run.command).toBe("echo hello");

    const fetched = getRun(run.id);
    expect(fetched).not.toBeNull();

    const updated = updateRun(run.id, {
      exit_code: 0,
      workflow_state: "launch-run:completed",
      finished_at: Date.now(),
    });

    expect(updated.exit_code).toBe(0);
    expect(updated.workflow_state).toBe("launch-run:completed");
  });

  test("deletion constraint: blocks active runs", () => {
    const run = createRun({
      command: "echo hello",
      workdir: "/home/ubuntu/work",
      max_duration_ms: 60000,
      workflow_state: "launch-run:pending",
      workflow_error: null,
      current_manifest_id: null,
      exit_code: null,
      init_checksum: null,
      create_snapshot: 0,
      spot_interrupted: 0,
      started_at: null,
      finished_at: null,
    });

    expect(() => deleteRun(run.id)).toThrow("active run");
  });
});

// =============================================================================
// Workflow Operations
// =============================================================================

describe("workflows", () => {
  test("workflow CRUD: create, read, update with auto updated_at, cascade delete", () => {
    const wfId = seedWorkflow();
    const wf = getWorkflow(wfId);

    expect(wf).not.toBeNull();
    expect(wf!.type).toBe("launch-run");
    expect(wf!.status).toBe("pending");

    const original = getWorkflow(wfId)!;
    const updated = updateWorkflow(wfId, {
      status: "running",
      current_node: "resolve-instance",
    });

    expect(updated.status).toBe("running");
    expect(updated.current_node).toBe("resolve-instance");
    expect(updated.updated_at).toBeGreaterThanOrEqual(original.updated_at);

    createWorkflowNode({
      workflow_id: wfId,
      node_id: "test-node",
      node_type: "test",
      status: "pending",
      input_json: "{}",
      output_json: null,
      error_json: null,
      depends_on: null,
      attempt: 1,
      retry_reason: null,
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });

    deleteWorkflow(wfId);
    expect(getWorkflow(wfId)).toBeNull();
    expect(getWorkflowNodes(wfId).length).toBe(0);
  });

  test("workflow node CRUD: create, read, update with auto updated_at", () => {
    const wfId = seedWorkflow();

    const node = createWorkflowNode({
      workflow_id: wfId,
      node_id: "resolve-instance",
      node_type: "resolve-instance",
      status: "pending",
      input_json: "{}",
      output_json: null,
      error_json: null,
      depends_on: null,
      attempt: 1,
      retry_reason: null,
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });

    expect(node.id).toBeGreaterThan(0);
    expect(node.node_id).toBe("resolve-instance");

    const nodes = getWorkflowNodes(wfId);
    expect(nodes.length).toBe(1);

    const updated = updateWorkflowNode(node.id, {
      status: "running",
      started_at: Date.now(),
    });

    expect(updated.status).toBe("running");
    expect(updated.started_at).not.toBeNull();
  });
});

// =============================================================================
// Manifest Operations
// =============================================================================

describe("manifests", () => {
  test("manifest lifecycle: create, seal, resources, expiration", () => {
    const wfId = seedWorkflow();
    const manifest = createManifest(wfId, { retention_ms: 1 });

    expect(manifest.status).toBe("DRAFT");
    expect(manifest.default_cleanup_priority).toBe(50);
    expect(manifest.updated_at).toBeGreaterThan(0);

    addResourceToManifest(manifest.id, "instance", "1");
    addResourceToManifest(manifest.id, "run", "1", { cleanupPriority: 10 });

    const resources = getManifestResources(manifest.id);
    expect(resources.length).toBe(2);

    const expiresAt = Date.now() + 1;
    const result = sealManifest(manifest.id, expiresAt);
    expect(result.success).toBe(true);

    const sealed = getManifest(manifest.id);
    expect(sealed!.status).toBe("SEALED");
    expect(sealed!.released_at).not.toBeNull();
    expect(sealed!.expires_at).not.toBeNull();
    expect(sealed!.expires_at!).toBeGreaterThanOrEqual(sealed!.released_at!);

    const expired = listExpiredManifests(Date.now() + 100);
    expect(expired.length).toBe(1);
  });

  test("seal transitions: prevents double seal", () => {
    const wfId = seedWorkflow();
    const manifest = createManifest(wfId);
    const expiresAt = Date.now() + 3600000;

    const result = sealManifest(manifest.id, expiresAt);
    expect(result.success).toBe(true);

    const secondResult = sealManifest(manifest.id, expiresAt);
    expect(secondResult.success).toBe(false);
    if (!secondResult.success) {
      expect(secondResult.reason).toBe("WRONG_STATE");
    }
  });

  test("resource addition: blocks sealed manifest unless recovery", () => {
    const wfId = seedWorkflow();
    const manifest = createManifest(wfId);
    const expiresAt = Date.now() + 3600000;
    sealManifest(manifest.id, expiresAt);

    expect(() => addResourceToManifest(manifest.id, "instance", "1")).toThrow(
      "sealed manifest"
    );

    // Recovery path should work
    addResourceToManifest(manifest.id, "instance", "1", {
      allowRecovery: true,
    });
    const resources = getManifestResources(manifest.id);
    expect(resources.length).toBe(1);
  });
});

// =============================================================================
// Blob & Object Operations
// =============================================================================

describe("blobs", () => {
  test("CRUD lifecycle: create, read, find by checksum", () => {
    const blob = createBlob({
      bucket: "logs",
      checksum: "abc123",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("hello"),
      size_bytes: 5,
      last_referenced_at: Date.now(),
    });

    expect(blob.id).toBeGreaterThan(0);
    expect(blob.bucket).toBe("logs");

    const fetched = getBlob(blob.id);
    expect(fetched).not.toBeNull();

    createBlob({
      bucket: "run-files",
      checksum: "findme-abc",
      checksum_bytes: 3,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("abc"),
      size_bytes: 3,
      last_referenced_at: Date.now(),
    });

    const found = findBlobByChecksum("run-files", "findme-abc");
    expect(found).not.toBeNull();
    expect(found!.checksum).toBe("findme-abc");

    const notFound = findBlobByChecksum("run-files", "nope");
    expect(notFound).toBeNull();
  });

  test("deduplication: enabled for dedupable buckets, disabled otherwise", () => {
    const blob1 = createBlob({
      bucket: "run-files",
      checksum: "dedup-test-123",
      checksum_bytes: 5,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("hello"),
      size_bytes: 5,
      last_referenced_at: Date.now(),
    });

    const blob2 = createBlob({
      bucket: "run-files",
      checksum: "dedup-test-123",
      checksum_bytes: 5,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("hello"),
      size_bytes: 5,
      last_referenced_at: Date.now(),
    });

    expect(blob2.id).toBe(blob1.id);

    // Non-dedupable bucket
    const logBlob1 = createBlob({
      bucket: "logs",
      checksum: "log-123",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("log line 1"),
      size_bytes: 10,
      last_referenced_at: Date.now(),
    });

    const logBlob2 = createBlob({
      bucket: "logs",
      checksum: "log-123",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("log line 2"),
      size_bytes: 10,
      last_referenced_at: Date.now(),
    });

    expect(logBlob2.id).not.toBe(logBlob1.id);
  });

  test("orphan detection and deletion constraints", () => {
    const oldTime = Date.now() - 48 * 3600 * 1000;
    const blob = createBlob({
      bucket: "logs",
      checksum: "orphan-test",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("data"),
      size_bytes: 4,
      last_referenced_at: oldTime,
    });

    const cutoff = Date.now() - 24 * 3600 * 1000;
    const orphaned = findOrphanedBlobs(cutoff);
    expect(orphaned.length).toBeGreaterThanOrEqual(1);
    expect(orphaned.some(b => b.id === blob.id)).toBe(true);

    // Create a referenced blob
    const refBlob = createBlob({
      bucket: "logs",
      checksum: "ref-test",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("data"),
      size_bytes: 4,
      last_referenced_at: Date.now(),
    });

    createObject({
      type: "log",
      blob_id: refBlob.id,
      provider: null,
      provider_object_id: null,
      metadata_json: null,
      expires_at: null,
      current_manifest_id: null,
      accessed_at: null,
      updated_at: null,
    });

    expect(() => deleteBlob(refBlob.id)).toThrow("referencing objects");
  });
});

describe("objects", () => {
  test("CRUD lifecycle: create, read, tag, find by tag", () => {
    const blob = createBlob({
      bucket: "logs",
      checksum: "obj-test",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("data"),
      size_bytes: 4,
      last_referenced_at: Date.now(),
    });

    const obj = createObject({
      type: "log",
      blob_id: blob.id,
      provider: null,
      provider_object_id: null,
      metadata_json: null,
      expires_at: null,
      current_manifest_id: null,
      accessed_at: null,
      updated_at: null,
    });

    expect(obj.id).toBeGreaterThan(0);
    expect(obj.type).toBe("log");

    const fetched = getObject(obj.id);
    expect(fetched).not.toBeNull();

    addObjectTag(obj.id, "run_id", "42");
    addObjectTag(obj.id, "stream", "stdout");

    const found = findObjectByTag("run_id", "42");
    expect(found).not.toBeNull();
    expect(found!.id).toBe(obj.id);

    const notFound = findObjectByTag("run_id", "999");
    expect(notFound).toBeNull();

    // Idempotent tag addition
    addObjectTag(obj.id, "run_id", "42");
  });
});

// =============================================================================
// Warm Pool
// =============================================================================

describe("warm pool", () => {
  test("finds allocations by spec/checksum and returns stats", () => {
    const inst = createInstance({
      provider: "orbstack",
      provider_id: "warm-1",
      spec: "4vcpu-8gb",
      region: "local",
      ip: "10.0.0.1",
      workflow_state: "launch:complete",
      workflow_error: null,
      current_manifest_id: null,
      spawn_idempotency_key: null,
      is_spot: 0,
      spot_request_id: null,
      init_checksum: "abc123",
      registration_token_hash: null,
      last_heartbeat: Date.now(),
    });

    createAllocation({
      run_id: null,
      instance_id: inst.id,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "ubuntu",
      workdir: "/home/ubuntu/work",
      debug_hold_until: null,
      completed_at: null,
    });

    const found = findWarmAllocation({ spec: "4vcpu-8gb", tenantId: 1 });
    expect(found).not.toBeNull();
    expect(found!.instance_id).toBe(inst.id);

    const withChecksum = findWarmAllocation(
      { spec: "4vcpu-8gb", tenantId: 1 },
      "abc123"
    );
    expect(withChecksum).not.toBeNull();

    const wrongChecksum = findWarmAllocation(
      { spec: "4vcpu-8gb", tenantId: 1 },
      "wrong"
    );
    expect(wrongChecksum).toBeNull();

    const stats = getWarmPoolStats();
    expect(stats.available).toBe(1);
  });
});

// =============================================================================
// Orphan & Whitelist
// =============================================================================

describe("orphan operations", () => {
  test("tracking: instances, manifests, scan recording", () => {
    seedInstance();
    seedInstance();
    const ids = getTrackedInstanceIds();
    expect(ids.size).toBe(2);

    const wfId = seedWorkflow();
    createManifest(wfId);

    const manifestIds = getActiveManifestIds();
    expect(manifestIds.size).toBe(1);

    recordOrphanScan({
      provider: "orbstack",
      scanned_at: Date.now(),
      orphans_found: 2,
      orphan_ids: ["vm-a", "vm-b"],
    });
  });

  test("whitelist: add, check, upsert", () => {
    expect(isWhitelisted("orbstack", "orphan-1")).toBe(false);

    addToWhitelist("orbstack", "orphan-1", "instance", "test", "admin");
    expect(isWhitelisted("orbstack", "orphan-1")).toBe(true);

    // Upsert should not throw
    addToWhitelist("orbstack", "orphan-1", "instance", "updated", "admin2");
    expect(isWhitelisted("orbstack", "orphan-1")).toBe(true);
  });
});

// =============================================================================
// Usage Records
// =============================================================================

describe("usage records", () => {
  test("lifecycle: create, track active, finish", () => {
    const instId = seedInstance();
    const startedAt = Date.now();

    const record = createUsageRecord({
      instance_id: instId,
      allocation_id: null,
      run_id: null,
      provider: "orbstack",
      spec: "4vcpu-8gb",
      region: "local",
      is_spot: 0,
      started_at: startedAt,
      finished_at: null,
      duration_ms: null,
      estimated_cost_usd: null,
    });

    expect(record.id).toBeGreaterThan(0);
    expect(record.finished_at).toBeNull();

    const active = getActiveUsageRecords();
    expect(active.length).toBe(1);

    const finishedAt = startedAt + 60000;
    const finished = finishUsageRecord(record.id, finishedAt);
    expect(finished.finished_at).toBe(finishedAt);
    expect(finished.duration_ms).toBe(60000);

    const afterFinish = getActiveUsageRecords();
    expect(afterFinish.length).toBe(0);
  });
});
