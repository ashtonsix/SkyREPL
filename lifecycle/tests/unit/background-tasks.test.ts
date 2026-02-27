// tests/unit/background-tasks.test.ts - Background Task Tests (#LIFE-02, #LIFE-03, #LIFE-04, #LIFE-05)
// Covers: heartbeatTimeoutCheck, holdExpiryTask, manifestCleanupCheck, storageGarbageCollection

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import {
  createInstance,
  createAllocation,
  createWorkflow,
  createManifest,
  getAllocation,
  getBlob,
  getObject,
  createBlob,
  createObject,
  execute,
  queryMany,
  listExpiredManifests,
  findOrphanedBlobs,
  deleteBlobBatch,
  deleteObjectBatch,
} from "../../control/src/material/db";
import {
  detectStaleHeartbeats,
} from "../../control/src/resource/instance";
import {
  failAllocation,
} from "../../control/src/workflow/state-transitions";
import {
  holdExpiryTask,
} from "../../control/src/resource/allocation";
import { TIMING } from "@skyrepl/contracts";

// =============================================================================
// Top-level test setup
// =============================================================================

let cleanup: () => Promise<void>;
beforeEach(() => { cleanup = setupTest(); });
afterEach(() => cleanup());

// =============================================================================
// Test Helpers
// =============================================================================

function createTestInstance(lastHeartbeat: number, workflowState = "launch-run:provisioning") {
  return createInstance({
    provider: "test",
    provider_id: `test-${Date.now()}-${Math.random()}`,
    spec: "test-spec",
    region: "local",
    ip: "10.0.0.1",
    workflow_state: workflowState,
    workflow_error: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 0,
    spot_request_id: null,
    init_checksum: null,
    registration_token_hash: null,
    last_heartbeat: lastHeartbeat,
    provider_metadata: null,
    display_name: null,
  });
}

function createTestAllocation(
  instanceId: number,
  status: "AVAILABLE" | "CLAIMED" | "ACTIVE" | "COMPLETE" | "FAILED",
  options?: {
    debugHoldUntil?: number | null;
    completedAt?: number | null;
  }
) {
  return createAllocation({
    run_id: null,
    instance_id: instanceId,
    status,
    current_manifest_id: null,
    user: "test",
    workdir: "/tmp/test",
    debug_hold_until: options?.debugHoldUntil ?? null,
    completed_at: options?.completedAt ?? null,
  });
}

function createTestWorkflow() {
  return createWorkflow({
    type: "test",
    parent_workflow_id: null,
    depth: 0,
    status: "completed",
    current_node: null,
    input_json: "{}",
    output_json: null,
    error_json: null,
    manifest_id: null,
    trace_id: null,
    idempotency_key: null,
    timeout_ms: null,
    timeout_at: null,
    started_at: Date.now(),
    finished_at: Date.now(),
    updated_at: Date.now(),
  });
}

function createTestBlob(checksum: string, lastReferencedAt: number) {
  return createBlob({
    bucket: "test",
    checksum,
    checksum_bytes: null,
    s3_key: null,
    s3_bucket: null,
    payload: Buffer.from("test data"),
    size_bytes: 9,
    last_referenced_at: lastReferencedAt,
  });
}

// =============================================================================
// Heartbeat Timeout Detection Tests (#LIFE-02)
// =============================================================================

describe("Background Tasks - Heartbeat Detection", () => {
  test("detects instances with stale heartbeats", () => {
    const staleTimestamp = Date.now() - TIMING.STALE_DETECTION_MS - 1000;
    const instance = createTestInstance(staleTimestamp);

    const staleInstances = detectStaleHeartbeats(TIMING.STALE_DETECTION_MS);

    expect(staleInstances.length).toBe(1);
    expect(staleInstances[0].id).toBe(instance.id);
  });

  test("ignores fresh heartbeat instances", () => {
    const freshTimestamp = Date.now() - 1000;
    createTestInstance(freshTimestamp);

    const staleInstances = detectStaleHeartbeats(TIMING.STALE_DETECTION_MS);

    expect(staleInstances.length).toBe(0);
  });

  test("ignores terminal state instances", () => {
    const staleTimestamp = Date.now() - TIMING.STALE_DETECTION_MS - 1000;
    createTestInstance(staleTimestamp, "terminate:complete");

    const staleInstances = detectStaleHeartbeats(TIMING.STALE_DETECTION_MS);

    expect(staleInstances.length).toBe(0);
  });

  test("failAllocation transitions ACTIVE to FAILED", () => {
    const instance = createTestInstance(Date.now());
    const allocation = createTestAllocation(instance.id, "ACTIVE");

    const result = failAllocation(allocation.id, "ACTIVE");

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.status).toBe("FAILED");
      expect(result.data.completed_at).not.toBeNull();
    }

    // Verify in DB
    const updated = getAllocation(allocation.id);
    expect(updated?.status).toBe("FAILED");
  });

  test("stale heartbeat → active allocations get failed (end-to-end)", () => {
    // Create stale instance with 2 ACTIVE allocations
    const staleTimestamp = Date.now() - TIMING.STALE_DETECTION_MS - 1000;
    const instance = createTestInstance(staleTimestamp);
    const alloc1 = createTestAllocation(instance.id, "ACTIVE");
    const alloc2 = createTestAllocation(instance.id, "ACTIVE");
    // Also a COMPLETE allocation that should NOT be affected
    const alloc3 = createTestAllocation(instance.id, "COMPLETE", {
      completedAt: Date.now() - 60000,
    });

    // Simulate heartbeatTimeoutCheck logic
    const staleInstances = detectStaleHeartbeats(TIMING.STALE_DETECTION_MS);
    expect(staleInstances.length).toBe(1);

    for (const inst of staleInstances) {
      const activeAllocations = queryMany<{ id: number }>(
        "SELECT id FROM allocations WHERE instance_id = ? AND status = 'ACTIVE'",
        [inst.id]
      );
      expect(activeAllocations.length).toBe(2);

      for (const alloc of activeAllocations) {
        failAllocation(alloc.id, "ACTIVE");
      }
    }

    // Verify: both ACTIVE allocations now FAILED
    expect(getAllocation(alloc1.id)?.status).toBe("FAILED");
    expect(getAllocation(alloc2.id)?.status).toBe("FAILED");
    // COMPLETE allocation untouched
    expect(getAllocation(alloc3.id)?.status).toBe("COMPLETE");
  });

  test("degraded detection finds instances between degraded and stale thresholds", () => {
    // Instance with heartbeat between DEGRADED (2m) and STALE (25m) — degraded
    const degradedTimestamp = Date.now() - TIMING.HEARTBEAT_DEGRADED_MS - 30000;
    const degradedInstance = createTestInstance(degradedTimestamp);

    // Instance that's truly stale (>25m)
    const staleTimestamp = Date.now() - TIMING.STALE_DETECTION_MS - 1000;
    const staleInstance = createTestInstance(staleTimestamp);

    // Fresh instance — should not appear
    createTestInstance(Date.now());

    // Single query with degraded threshold (as heartbeatTimeoutCheck does)
    const allDegraded = detectStaleHeartbeats(TIMING.HEARTBEAT_DEGRADED_MS);
    expect(allDegraded.length).toBe(2); // both degraded and stale

    // Partition: stale vs. just-degraded
    const now = Date.now();
    const stale = allDegraded.filter(i => now - i.last_heartbeat >= TIMING.STALE_DETECTION_MS);
    const justDegraded = allDegraded.filter(i => now - i.last_heartbeat < TIMING.STALE_DETECTION_MS);

    expect(stale.length).toBe(1);
    expect(stale[0].id).toBe(staleInstance.id);
    expect(justDegraded.length).toBe(1);
    expect(justDegraded[0].id).toBe(degradedInstance.id);
  });
});

// =============================================================================
// Debug Hold Expiry Tests (#LIFE-03)
// =============================================================================

describe("Background Tasks - Hold Expiry", () => {
  test("clears expired debug holds", async () => {
    const instance = createTestInstance(Date.now());
    const expiredHoldUntil = Date.now() - 1000; // 1s ago
    const completedAt = Date.now() - 60000; // 1 minute ago
    const allocation = createTestAllocation(instance.id, "COMPLETE", {
      debugHoldUntil: expiredHoldUntil,
      completedAt,
    });

    await holdExpiryTask();

    const updated = getAllocation(allocation.id);
    expect(updated?.debug_hold_until).toBeNull();
  });

  test("preserves active debug holds", async () => {
    const instance = createTestInstance(Date.now());
    const activeHoldUntil = Date.now() + 300000; // 5 minutes from now
    const completedAt = Date.now() - 60000; // 1 minute ago
    const allocation = createTestAllocation(instance.id, "COMPLETE", {
      debugHoldUntil: activeHoldUntil,
      completedAt,
    });

    await holdExpiryTask();

    const updated = getAllocation(allocation.id);
    expect(updated?.debug_hold_until).toBe(activeHoldUntil);
  });

  test("enforces absolute max hold (24h from completed_at)", async () => {
    const instance = createTestInstance(Date.now());
    // Completed 25h ago (past absolute max)
    const completedAt = Date.now() - TIMING.ABSOLUTE_MAX_HOLD_MS - 3600000;
    // Hold set to 1h from now (still "active" but exceeds absolute max from completed_at)
    const farFutureHold = Date.now() + 3600000;
    const allocation = createTestAllocation(instance.id, "COMPLETE", {
      debugHoldUntil: farFutureHold,
      completedAt,
    });

    await holdExpiryTask();

    const updated = getAllocation(allocation.id);
    expect(updated?.debug_hold_until).toBeNull();
  });

  test("ignores non-COMPLETE allocations", async () => {
    const instance = createTestInstance(Date.now());
    const holdUntil = Date.now() + 300000;
    const allocation = createTestAllocation(instance.id, "ACTIVE", {
      debugHoldUntil: holdUntil,
    });

    await holdExpiryTask();

    const updated = getAllocation(allocation.id);
    // Should be unchanged since allocation is not COMPLETE
    expect(updated?.debug_hold_until).toBe(holdUntil);
  });

  test("handles COMPLETE allocation with NULL completed_at gracefully", async () => {
    const instance = createTestInstance(Date.now());
    // Force a COMPLETE allocation without completed_at via direct SQL
    const alloc = createTestAllocation(instance.id, "COMPLETE", {
      debugHoldUntil: Date.now() + 300000,
      completedAt: null,
    });

    // Should not crash and should not clear the hold (absolute max
    // check is skipped when completed_at is NULL)
    await holdExpiryTask();

    const updated = getAllocation(alloc.id);
    // Hold not expired (future hold, absolute max skipped due to NULL completed_at)
    expect(updated?.debug_hold_until).not.toBeNull();
  });
});

// =============================================================================
// Manifest Cleanup Tests (#LIFE-04)
// =============================================================================

describe("Background Tasks - Manifest Cleanup", () => {
  test("finds sealed manifests past expiry", () => {
    const workflow = createTestWorkflow();
    const manifest = createManifest(workflow.id, { retention_ms: 3600000 });

    // Seal the manifest with expiry in the past
    const now = Date.now();
    const expiredAt = now - 1000;
    execute(
      "UPDATE manifests SET status = 'SEALED', released_at = ?, expires_at = ?, updated_at = ? WHERE id = ?",
      [now - 3600000, expiredAt, now, manifest.id]
    );

    const expired = listExpiredManifests(now);

    expect(expired.length).toBe(1);
    expect(expired[0].id).toBe(manifest.id);
  });

  test("ignores non-sealed manifests", () => {
    const workflow = createTestWorkflow();
    const manifest = createManifest(workflow.id, { retention_ms: 3600000 });

    // Leave as DRAFT, set expires_at to past
    const now = Date.now();
    const expiredAt = now - 1000;
    execute(
      "UPDATE manifests SET expires_at = ?, updated_at = ? WHERE id = ?",
      [expiredAt, now, manifest.id]
    );

    const expired = listExpiredManifests(now);

    expect(expired.length).toBe(0);
  });

  test("ignores manifests not yet expired", () => {
    const workflow = createTestWorkflow();
    const manifest = createManifest(workflow.id, { retention_ms: 3600000 });

    // Seal with expiry in the future
    const now = Date.now();
    const futureExpiry = now + 3600000;
    execute(
      "UPDATE manifests SET status = 'SEALED', released_at = ?, expires_at = ?, updated_at = ? WHERE id = ?",
      [now, futureExpiry, now, manifest.id]
    );

    const expired = listExpiredManifests(now);

    expect(expired.length).toBe(0);
  });

  test("ignores manifests with no expiry", () => {
    const workflow = createTestWorkflow();
    const manifest = createManifest(workflow.id, { retention_ms: 3600000 });

    // Seal but don't set expires_at
    const now = Date.now();
    execute(
      "UPDATE manifests SET status = 'SEALED', released_at = ?, updated_at = ? WHERE id = ?",
      [now, now, manifest.id]
    );

    const expired = listExpiredManifests(now);

    expect(expired.length).toBe(0);
  });
});

// =============================================================================
// Storage Garbage Collection Tests (#LIFE-05)
// =============================================================================

describe("Background Tasks - Storage GC", () => {
  test("finds orphaned blobs past grace period", () => {
    const oldTimestamp = Date.now() - 25 * 3600000; // 25h ago
    const blob = createTestBlob("abc123", oldTimestamp);

    const cutoff = Date.now() - 24 * 3600000; // 24h ago
    const orphans = findOrphanedBlobs(cutoff);

    expect(orphans.length).toBe(1);
    expect(orphans[0].id).toBe(blob.id);
  });

  test("preserves recently referenced blobs", () => {
    const recentTimestamp = Date.now() - 3600000; // 1h ago
    createTestBlob("xyz789", recentTimestamp);

    const cutoff = Date.now() - 24 * 3600000; // 24h ago
    const orphans = findOrphanedBlobs(cutoff);

    expect(orphans.length).toBe(0);
  });

  test("preserves blobs with referencing objects", () => {
    const oldTimestamp = Date.now() - 25 * 3600000;
    const blob = createTestBlob("referenced123", oldTimestamp);

    // Create object pointing to this blob
    createObject({
      type: "artifact",
      blob_id: blob.id,
      provider: null,
      provider_object_id: null,
      metadata_json: null,
      expires_at: null,
      current_manifest_id: null,
      accessed_at: null,
      updated_at: null,
    });

    const cutoff = Date.now() - 24 * 3600000;
    const orphans = findOrphanedBlobs(cutoff);

    // Should not be returned even though last_referenced_at is old
    expect(orphans.length).toBe(0);
  });

  test("deleteBlobBatch removes blobs", () => {
    const blob1 = createTestBlob("blob1", Date.now());
    const blob2 = createTestBlob("blob2", Date.now());
    const blob3 = createTestBlob("blob3", Date.now());

    // Delete 2 of them
    deleteBlobBatch([blob1.id, blob2.id]);

    // Only blob3 should remain
    expect(getBlob(blob1.id)).toBeNull();
    expect(getBlob(blob2.id)).toBeNull();
    expect(getBlob(blob3.id)).not.toBeNull();
  });

  test("deleteObjectBatch removes expired objects", () => {
    const blob = createTestBlob("obj-blob", Date.now());

    const obj1 = createObject({
      type: "artifact",
      blob_id: blob.id,
      provider: null,
      provider_object_id: null,
      metadata_json: null,
      expires_at: Date.now() - 1000, // expired
      current_manifest_id: null,
      accessed_at: null,
      updated_at: null,
    });

    const obj2 = createObject({
      type: "artifact",
      blob_id: blob.id,
      provider: null,
      provider_object_id: null,
      metadata_json: null,
      expires_at: Date.now() + 3600000, // not expired
      current_manifest_id: null,
      accessed_at: null,
      updated_at: null,
    });

    // Query and delete expired objects (mirrors storageGarbageCollection logic)
    const expiredObjects = queryMany<{ id: number }>(
      "SELECT id FROM objects WHERE expires_at IS NOT NULL AND expires_at < ? LIMIT 500",
      [Date.now()]
    );

    deleteObjectBatch(expiredObjects.map(o => o.id));

    // obj1 should be deleted, obj2 should remain
    expect(getObject(obj1.id)).toBeNull();
    expect(getObject(obj2.id)).not.toBeNull();
  });
});
