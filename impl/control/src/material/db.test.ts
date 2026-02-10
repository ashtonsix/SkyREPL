import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import {
  initDatabase,
  closeDatabase,
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

beforeEach(() => {
  initDatabase(":memory:");
  runMigrations();
});

afterEach(() => {
  closeDatabase();
});

// =============================================================================
// Schema & Migrations
// =============================================================================

describe("schema & migrations", () => {
  it("runs migrations and reports version 2", () => {
    expect(getMigrationVersion()).toBe(2);
  });

  it("is idempotent on re-run", () => {
    runMigrations(); // second call
    expect(getMigrationVersion()).toBe(2);
  });
});

// =============================================================================
// Instance CRUD
// =============================================================================

describe("instances", () => {
  it("creates and reads an instance", () => {
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
  });

  it("looks up by provider id", () => {
    createInstance({
      provider: "orbstack",
      provider_id: "vm-xyz",
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

    const found = getInstanceByProviderId("orbstack", "vm-xyz");
    expect(found).not.toBeNull();
    expect(found!.provider_id).toBe("vm-xyz");

    const notFound = getInstanceByProviderId("orbstack", "nope");
    expect(notFound).toBeNull();
  });

  it("updates fields", () => {
    const inst = createInstance({
      provider: "orbstack",
      provider_id: "vm-upd",
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

    const updated = updateInstance(inst.id, {
      ip: "192.168.1.1",
      workflow_state: "spawn:complete",
    });

    expect(updated.ip).toBe("192.168.1.1");
    expect(updated.workflow_state).toBe("spawn:complete");
  });

  it("lists with filters", () => {
    createInstance({
      provider: "orbstack",
      provider_id: "a",
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
    expect(listInstances({ provider: "gcp" }).length).toBe(0);
  });

  it("blocks deletion with active allocations", () => {
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
  it("creates an allocation", () => {
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
  });

  it("claims allocation (atomic transition)", () => {
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
  });

  it("prevents double-claim (atomic transition)", () => {
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

    const result = claimAllocation(alloc.id, run.id);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.status).toBe("CLAIMED");
      expect(result.data.run_id).toBe(run.id);
    }

    // Second claim should fail (already claimed)
    const secondResult = claimAllocation(alloc.id, run.id);
    expect(secondResult.success).toBe(false);
  });

  it("blocks deletion of active allocation", () => {
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
  });

  it("allows deletion of terminal allocation", () => {
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

    // First claim it, then complete it
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
    // Need to transition through ACTIVE before COMPLETE
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
  it("creates and reads a run", () => {
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
  });

  it("updates run fields", () => {
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

    const updated = updateRun(run.id, {
      exit_code: 0,
      workflow_state: "launch-run:completed",
      finished_at: Date.now(),
    });

    expect(updated.exit_code).toBe(0);
    expect(updated.workflow_state).toBe("launch-run:completed");
  });

  it("blocks deletion of active run", () => {
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
  it("creates and reads a workflow", () => {
    const wfId = seedWorkflow();
    const wf = getWorkflow(wfId);

    expect(wf).not.toBeNull();
    expect(wf!.type).toBe("launch-run");
    expect(wf!.status).toBe("pending");
  });

  it("updates workflow with auto updated_at", () => {
    const wfId = seedWorkflow();
    const original = getWorkflow(wfId)!;

    const updated = updateWorkflow(wfId, {
      status: "running",
      current_node: "resolve-instance",
    });

    expect(updated.status).toBe("running");
    expect(updated.current_node).toBe("resolve-instance");
    expect(updated.updated_at).toBeGreaterThanOrEqual(original.updated_at);
  });

  it("creates and reads workflow nodes", () => {
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
  });

  it("updates workflow node with auto updated_at", () => {
    const wfId = seedWorkflow();
    const node = createWorkflowNode({
      workflow_id: wfId,
      node_id: "spawn-instance",
      node_type: "spawn-instance",
      status: "pending",
      input_json: "{}",
      output_json: null,
      error_json: null,
      depends_on: '["resolve-instance"]',
      attempt: 1,
      retry_reason: null,
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });

    const updated = updateWorkflowNode(node.id, {
      status: "running",
      started_at: Date.now(),
    });

    expect(updated.status).toBe("running");
    expect(updated.started_at).not.toBeNull();
  });

  it("cascade deletes workflow and nodes", () => {
    const wfId = seedWorkflow();
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
});

// =============================================================================
// Manifest Operations
// =============================================================================

describe("manifests", () => {
  it("creates a DRAFT manifest", () => {
    const wfId = seedWorkflow();
    const manifest = createManifest(wfId);

    expect(manifest.status).toBe("DRAFT");
    expect(manifest.default_cleanup_priority).toBe(50);
    expect(manifest.updated_at).toBeGreaterThan(0);
  });

  it("seals a manifest with expires_at", () => {
    const wfId = seedWorkflow();
    const manifest = createManifest(wfId, { retention_ms: 3600000 });
    const expiresAt = Date.now() + 3600000;

    const result = sealManifest(manifest.id, expiresAt);
    expect(result.success).toBe(true);

    const sealed = getManifest(manifest.id);
    expect(sealed!.status).toBe("SEALED");
    expect(sealed!.released_at).not.toBeNull();
    expect(sealed!.expires_at).not.toBeNull();
    expect(sealed!.expires_at!).toBeGreaterThan(sealed!.released_at!);
  });

  it("rejects double seal", () => {
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

  it("adds and reads manifest resources", () => {
    const wfId = seedWorkflow();
    const manifest = createManifest(wfId);

    addResourceToManifest(manifest.id, "instance", "1");
    addResourceToManifest(manifest.id, "run", "1", { cleanupPriority: 10 });

    const resources = getManifestResources(manifest.id);
    expect(resources.length).toBe(2);
  });

  it("blocks resource addition to SEALED manifest", () => {
    const wfId = seedWorkflow();
    const manifest = createManifest(wfId);
    const expiresAt = Date.now() + 3600000;
    sealManifest(manifest.id, expiresAt);

    expect(() => addResourceToManifest(manifest.id, "instance", "1")).toThrow(
      "sealed manifest"
    );
  });

  it("allows recovery addition to SEALED manifest", () => {
    const wfId = seedWorkflow();
    const manifest = createManifest(wfId);
    const expiresAt = Date.now() + 3600000;
    sealManifest(manifest.id, expiresAt);

    // Should not throw with allowRecovery
    addResourceToManifest(manifest.id, "instance", "1", {
      allowRecovery: true,
    });
    const resources = getManifestResources(manifest.id);
    expect(resources.length).toBe(1);
  });

  it("lists expired manifests", () => {
    const wfId = seedWorkflow();
    const manifest = createManifest(wfId, { retention_ms: 1 }); // 1ms retention
    const expiresAt = Date.now() + 1; // 1ms from now
    sealManifest(manifest.id, expiresAt);

    // After a tiny delay, it should be expired
    const expired = listExpiredManifests(Date.now() + 100);
    expect(expired.length).toBe(1);
  });
});

// =============================================================================
// Blob & Object Operations
// =============================================================================

describe("blobs", () => {
  it("creates and reads a blob", () => {
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
  });

  it("deduplicates blobs in dedupable buckets", () => {
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

    // Should return the same blob
    expect(blob2.id).toBe(blob1.id);
  });

  it("does NOT deduplicate in non-dedupable buckets", () => {
    const blob1 = createBlob({
      bucket: "logs",
      checksum: "log-123",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("log line 1"),
      size_bytes: 10,
      last_referenced_at: Date.now(),
    });

    const blob2 = createBlob({
      bucket: "logs",
      checksum: "log-123",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("log line 2"),
      size_bytes: 10,
      last_referenced_at: Date.now(),
    });

    expect(blob2.id).not.toBe(blob1.id);
  });

  it("finds blob by checksum", () => {
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

  it("finds orphaned blobs", () => {
    const oldTime = Date.now() - 48 * 3600 * 1000; // 48h ago
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

    // No object references this blob
    const cutoff = Date.now() - 24 * 3600 * 1000;
    const orphaned = findOrphanedBlobs(cutoff);
    expect(orphaned.length).toBeGreaterThanOrEqual(1);
    expect(orphaned.some(b => b.id === blob.id)).toBe(true);
  });

  it("blocks deletion of blob with references", () => {
    const blob = createBlob({
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
      blob_id: blob.id,
      provider: null,
      provider_object_id: null,
      metadata_json: null,
      expires_at: null,
      current_manifest_id: null,
      accessed_at: null,
      updated_at: null,
    });

    expect(() => deleteBlob(blob.id)).toThrow("referencing objects");
  });
});

describe("objects", () => {
  it("creates and reads a storage object", () => {
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
  });

  it("tags and finds by tag", () => {
    const blob = createBlob({
      bucket: "logs",
      checksum: "tag-test",
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

    addObjectTag(obj.id, "run_id", "42");
    addObjectTag(obj.id, "stream", "stdout");

    const found = findObjectByTag("run_id", "42");
    expect(found).not.toBeNull();
    expect(found!.id).toBe(obj.id);

    const notFound = findObjectByTag("run_id", "999");
    expect(notFound).toBeNull();
  });

  it("addObjectTag is idempotent", () => {
    const blob = createBlob({
      bucket: "logs",
      checksum: "idem-tag",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("x"),
      size_bytes: 1,
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

    addObjectTag(obj.id, "key", "val");
    addObjectTag(obj.id, "key", "val"); // should not throw
  });
});

// =============================================================================
// Warm Pool
// =============================================================================

describe("warm pool", () => {
  it("finds warm allocation matching spec", () => {
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

    const found = findWarmAllocation({ spec: "4vcpu-8gb" });
    expect(found).not.toBeNull();
    expect(found!.instance_id).toBe(inst.id);

    const withChecksum = findWarmAllocation(
      { spec: "4vcpu-8gb" },
      "abc123"
    );
    expect(withChecksum).not.toBeNull();

    const wrongChecksum = findWarmAllocation(
      { spec: "4vcpu-8gb" },
      "wrong"
    );
    expect(wrongChecksum).toBeNull();
  });

  it("returns pool stats", () => {
    const inst = createInstance({
      provider: "orbstack",
      provider_id: "stats-1",
      spec: "4vcpu-8gb",
      region: "local",
      ip: null,
      workflow_state: "spawn:complete",
      workflow_error: null,
      current_manifest_id: null,
      spawn_idempotency_key: null,
      is_spot: 0,
      spot_request_id: null,
      init_checksum: null,
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

    const stats = getWarmPoolStats();
    expect(stats.available).toBe(1);
  });
});

// =============================================================================
// Orphan & Whitelist
// =============================================================================

describe("orphan operations", () => {
  it("tracks instance IDs", () => {
    seedInstance();
    seedInstance();
    const ids = getTrackedInstanceIds();
    expect(ids.size).toBe(2);
  });

  it("tracks active manifest IDs", () => {
    const wfId = seedWorkflow();
    createManifest(wfId);

    const ids = getActiveManifestIds();
    expect(ids.size).toBe(1);
  });

  it("records orphan scans", () => {
    recordOrphanScan({
      provider: "orbstack",
      scanned_at: Date.now(),
      orphans_found: 2,
      orphan_ids: ["vm-a", "vm-b"],
    });
    // Should not throw — validates created_at is included
  });

  it("manages whitelist", () => {
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
  it("creates and finishes a usage record", () => {
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
