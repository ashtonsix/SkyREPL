// tests/unit/log-streaming.test.ts - Log Streaming Enhancement Tests
// Covers: S2.C2 (sequence numbering, gap detection),
//         S2.D3 (updated_at tracking), S2.C3 (manifest ownership wiring)

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import {
  createBlob,
  createObject,
  addObjectTag,
  addResourceToManifest,
  getObject,
  queryOne,
  queryMany,
  createRun,
  createWorkflow,
  createManifest,
  createInstance,
  createAllocation,
  updateObjectTimestamp,
  type StorageObject,
  type Blob,
  type ManifestResource,
} from "../../control/src/material/db";
import { SSEManager, type LogBroadcast } from "../../control/src/api/sse-protocol";

// =============================================================================
// Top-level harness setup
// =============================================================================

let cleanup: () => Promise<void>;
beforeEach(() => { cleanup = setupTest(); });
afterEach(() => cleanup());

// =============================================================================
// Helpers
// =============================================================================

function createTestRun(manifestId?: number | null) {
  return createRun({
    command: "echo test",
    workdir: "/workspace",
    max_duration_ms: 60000,
    workflow_state: "launch-run:running",
    workflow_error: null,
    current_manifest_id: manifestId ?? null,
    exit_code: null,
    init_checksum: null,
    create_snapshot: 0,
    spot_interrupted: 0,
    started_at: Date.now(),
    finished_at: null,
  });
}

function createTestWorkflow() {
  return createWorkflow({
    type: "launch-run",
    parent_workflow_id: null,
    depth: 0,
    status: "running",
    current_node: "start-run",
    input_json: "{}",
    output_json: null,
    error_json: null,
    manifest_id: null,
    trace_id: null,
    idempotency_key: null,
    timeout_ms: null,
    timeout_at: null,
    started_at: Date.now(),
    finished_at: null,
    updated_at: Date.now(),
  });
}

function createLogObject(runId: number, stream: string, manifestId?: number | null) {
  const now = Date.now();
  const blob = createBlob({
    bucket: "logs",
    checksum: "",
    checksum_bytes: null,
    s3_key: null,
    s3_bucket: null,
    payload: null,
    size_bytes: 0,
    last_referenced_at: now,
  });
  const obj = createObject({
    type: "log",
    blob_id: blob.id,
    provider: null,
    provider_object_id: null,
    metadata_json: JSON.stringify({ run_id: runId, stream }),
    expires_at: null,
    current_manifest_id: manifestId ?? null,
    accessed_at: null,
    updated_at: now,
  });
  return { blob, obj };
}

// =============================================================================
// S2.D3: objects.updated_at column tracking
// =============================================================================

describe("S2.D3: objects.updated_at tracking", () => {

  test("createObject sets updated_at to current time", () => {
    const before = Date.now();
    const blob = createBlob({
      bucket: "logs",
      checksum: "",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: null,
      size_bytes: 0,
      last_referenced_at: before,
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
    const after = Date.now();

    expect(obj.updated_at).toBeDefined();
    expect(obj.updated_at).toBeGreaterThanOrEqual(before);
    expect(obj.updated_at).toBeLessThanOrEqual(after);
  });

  test("createObject respects explicit updated_at value", () => {
    const explicit = 1000000;
    const blob = createBlob({
      bucket: "logs",
      checksum: "",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: null,
      size_bytes: 0,
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
      updated_at: explicit,
    });

    expect(obj.updated_at).toBe(explicit);
  });

  test("updateObjectTimestamp updates the updated_at column", () => {
    const blob = createBlob({
      bucket: "logs",
      checksum: "",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: null,
      size_bytes: 0,
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
      updated_at: 1000,
    });

    expect(obj.updated_at).toBe(1000);

    updateObjectTimestamp(obj.id);
    const updated = getObject(obj.id);
    expect(updated).toBeDefined();
    expect(updated!.updated_at).toBeGreaterThan(1000);
  });

});

// =============================================================================
// S2.C3: Log object manifest ownership wiring
// =============================================================================

describe("S2.C3: Log object manifest ownership", () => {

  test("log object can be created with manifest ownership", () => {
    const workflow = createTestWorkflow();
    const manifest = createManifest(workflow.id);
    const run = createTestRun(manifest.id);

    const { obj } = createLogObject(run.id, "stdout", manifest.id);

    expect(obj.current_manifest_id).toBe(manifest.id);
  });

  test("object_tags are created for log objects", () => {
    const run = createTestRun();
    const { obj } = createLogObject(run.id, "stdout");

    addObjectTag(obj.id, "run_id", String(run.id));
    addObjectTag(obj.id, "stream", "stdout");

    // Verify tags exist
    const tags = queryMany<{ tag: string }>(
      "SELECT tag FROM object_tags WHERE object_id = ? ORDER BY tag",
      [obj.id]
    );
    expect(tags.length).toBe(2);
    expect(tags.map(t => t.tag)).toContain(`run_id:${run.id}`);
    expect(tags.map(t => t.tag)).toContain("stream:stdout");
  });

  test("log object is registered in manifest_resources", () => {
    const workflow = createTestWorkflow();
    const manifest = createManifest(workflow.id);
    const run = createTestRun(manifest.id);

    const { obj } = createLogObject(run.id, "stdout", manifest.id);
    addResourceToManifest(manifest.id, "object", String(obj.id));

    const resources = queryMany<ManifestResource>(
      "SELECT * FROM manifest_resources WHERE manifest_id = ? AND resource_type = 'object'",
      [manifest.id]
    );
    expect(resources.length).toBe(1);
    expect(resources[0].resource_id).toBe(String(obj.id));
  });

  test("duplicate object_tags are handled idempotently", () => {
    const run = createTestRun();
    const { obj } = createLogObject(run.id, "stdout");

    // Add same tag twice
    addObjectTag(obj.id, "run_id", String(run.id));
    addObjectTag(obj.id, "run_id", String(run.id)); // should not throw

    const tags = queryMany<{ tag: string }>(
      "SELECT tag FROM object_tags WHERE object_id = ?",
      [obj.id]
    );
    expect(tags.length).toBe(1);
  });
});

// =============================================================================
// S2.C2: SSEManager sequence numbering and replay
// =============================================================================

describe("S2.C2: SSEManager log sequence and replay", () => {
  test("broadcastLog assigns monotonically increasing seq IDs", () => {
    const mgr = new SSEManager();
    const runId = "test-run-1";

    // Collect messages via a mock WebSocket
    const received: LogBroadcast[] = [];
    const mockWs = {
      send(data: string) { received.push(JSON.parse(data)); },
      close() {},
      addEventListener() {},
      removeEventListener() {},
    } as unknown as WebSocket;

    mgr.subscribeToLogs(runId, mockWs);

    mgr.broadcastLog(runId, {
      stream: "stdout",
      data: "line 1\n",
      timestamp: 1000,
    });
    mgr.broadcastLog(runId, {
      stream: "stdout",
      data: "line 2\n",
      timestamp: 2000,
    });
    mgr.broadcastLog(runId, {
      stream: "stderr",
      data: "error 1\n",
      timestamp: 3000,
    });

    expect(received.length).toBe(3);
    expect(received[0].seq).toBe(1);
    expect(received[1].seq).toBe(2);
    expect(received[2].seq).toBe(3);

    // Verify monotonic
    for (let i = 1; i < received.length; i++) {
      expect(received[i].seq!).toBeGreaterThan(received[i - 1].seq!);
    }

    mgr.unsubscribeFromLogs(runId, mockWs);
  });

  test("different runs have independent sequence counters", () => {
    const mgr = new SSEManager();

    const received1: LogBroadcast[] = [];
    const received2: LogBroadcast[] = [];

    const mockWs1 = {
      send(data: string) { received1.push(JSON.parse(data)); },
      close() {},
      addEventListener() {},
      removeEventListener() {},
    } as unknown as WebSocket;
    const mockWs2 = {
      send(data: string) { received2.push(JSON.parse(data)); },
      close() {},
      addEventListener() {},
      removeEventListener() {},
    } as unknown as WebSocket;

    mgr.subscribeToLogs("run-A", mockWs1);
    mgr.subscribeToLogs("run-B", mockWs2);

    mgr.broadcastLog("run-A", { stream: "stdout", data: "a1\n", timestamp: 1000 });
    mgr.broadcastLog("run-B", { stream: "stdout", data: "b1\n", timestamp: 1000 });
    mgr.broadcastLog("run-A", { stream: "stdout", data: "a2\n", timestamp: 2000 });

    expect(received1.length).toBe(2);
    expect(received1[0].seq).toBe(1);
    expect(received1[1].seq).toBe(2);

    expect(received2.length).toBe(1);
    expect(received2[0].seq).toBe(1);

    mgr.unsubscribeFromLogs("run-A", mockWs1);
    mgr.unsubscribeFromLogs("run-B", mockWs2);
  });

  test("replayLogsFrom replays messages after given sequence", () => {
    const mgr = new SSEManager();
    const runId = "replay-test";

    // Set up a subscriber to generate buffered messages
    const sink: string[] = [];
    const mockWs = {
      send(data: string) { sink.push(data); },
      close() {},
      addEventListener() {},
      removeEventListener() {},
    } as unknown as WebSocket;

    mgr.subscribeToLogs(runId, mockWs);

    // Send 5 messages
    for (let i = 0; i < 5; i++) {
      mgr.broadcastLog(runId, {
        stream: "stdout",
        data: `line ${i + 1}\n`,
        timestamp: 1000 + i,
      });
    }

    mgr.unsubscribeFromLogs(runId, mockWs);

    // Now replay from seq 3 (should get seq 4 and 5)
    const replayed: LogBroadcast[] = [];
    const replayWs = {
      send(data: string) { replayed.push(JSON.parse(data)); },
      close() {},
      addEventListener() {},
      removeEventListener() {},
    } as unknown as WebSocket;

    const count = mgr.replayLogsFrom(runId, 3, replayWs);

    expect(count).toBe(2);
    expect(replayed.length).toBe(2);
    expect(replayed[0].seq).toBe(4);
    expect(replayed[0].data).toBe("line 4\n");
    expect(replayed[1].seq).toBe(5);
    expect(replayed[1].data).toBe("line 5\n");
  });

  test("replayLogsFrom returns 0 when no messages to replay", () => {
    const mgr = new SSEManager();

    const mockWs = {
      send() {},
      close() {},
      addEventListener() {},
      removeEventListener() {},
    } as unknown as WebSocket;

    const count = mgr.replayLogsFrom("nonexistent", 0, mockWs);
    expect(count).toBe(0);
  });

  test("replayLogsFrom returns 0 when all messages already seen", () => {
    const mgr = new SSEManager();
    const runId = "replay-all-seen";

    const mockWs = {
      send() {},
      close() {},
      addEventListener() {},
      removeEventListener() {},
    } as unknown as WebSocket;

    mgr.subscribeToLogs(runId, mockWs);
    mgr.broadcastLog(runId, { stream: "stdout", data: "x\n", timestamp: 1000 });
    mgr.broadcastLog(runId, { stream: "stdout", data: "y\n", timestamp: 2000 });
    mgr.unsubscribeFromLogs(runId, mockWs);

    const count = mgr.replayLogsFrom(runId, 2, mockWs);
    expect(count).toBe(0);
  });

  test("replay buffer respects max size limit", () => {
    const mgr = new SSEManager();
    const runId = "buffer-limit";

    const mockWs = {
      send() {},
      close() {},
      addEventListener() {},
      removeEventListener() {},
    } as unknown as WebSocket;

    mgr.subscribeToLogs(runId, mockWs);

    // Send more messages than the buffer can hold (buffer size is 500)
    for (let i = 0; i < 600; i++) {
      mgr.broadcastLog(runId, {
        stream: "stdout",
        data: `line ${i}\n`,
        timestamp: 1000 + i,
      });
    }
    mgr.unsubscribeFromLogs(runId, mockWs);

    // Replay from seq 0 should get at most 500 messages
    const replayed: LogBroadcast[] = [];
    const replayWs = {
      send(data: string) { replayed.push(JSON.parse(data)); },
      close() {},
      addEventListener() {},
      removeEventListener() {},
    } as unknown as WebSocket;

    const count = mgr.replayLogsFrom(runId, 0, replayWs);
    expect(count).toBe(500);
    expect(replayed.length).toBe(500);

    // First replayed message should be seq 101 (oldest 100 were evicted)
    expect(replayed[0].seq).toBe(101);
    // Last should be seq 600
    expect(replayed[replayed.length - 1].seq).toBe(600);
  });

  test("cleanupRunSequences removes all tracking for a run", () => {
    const mgr = new SSEManager();
    const runId = "cleanup-test";

    const mockWs = {
      send() {},
      close() {},
      addEventListener() {},
      removeEventListener() {},
    } as unknown as WebSocket;

    mgr.subscribeToLogs(runId, mockWs);
    mgr.broadcastLog(runId, { stream: "stdout", data: "x\n", timestamp: 1000 });
    mgr.unsubscribeFromLogs(runId, mockWs);

    mgr.cleanupRunSequences(runId);

    // Replay should return 0 after cleanup
    const count = mgr.replayLogsFrom(runId, 0, mockWs);
    expect(count).toBe(0);

    // Next broadcast should start at seq 1 again
    mgr.subscribeToLogs(runId, mockWs);
    const received: LogBroadcast[] = [];
    const ws2 = {
      send(data: string) { received.push(JSON.parse(data)); },
      close() {},
      addEventListener() {},
      removeEventListener() {},
    } as unknown as WebSocket;
    mgr.subscribeToLogs(runId, ws2);
    mgr.broadcastLog(runId, { stream: "stdout", data: "new\n", timestamp: 2000 });
    expect(received[0].seq).toBe(1);
    mgr.unsubscribeFromLogs(runId, mockWs);
    mgr.unsubscribeFromLogs(runId, ws2);
  });

  test("broadcastLog still works with no subscribers (buffer only)", () => {
    const mgr = new SSEManager();
    const runId = "no-sub-test";

    // Broadcast without any subscribers
    mgr.broadcastLog(runId, { stream: "stdout", data: "buffered\n", timestamp: 1000 });
    mgr.broadcastLog(runId, { stream: "stdout", data: "also buffered\n", timestamp: 2000 });

    // Replay should still work
    const replayed: LogBroadcast[] = [];
    const ws = {
      send(data: string) { replayed.push(JSON.parse(data)); },
      close() {},
      addEventListener() {},
      removeEventListener() {},
    } as unknown as WebSocket;

    const count = mgr.replayLogsFrom(runId, 0, ws);
    expect(count).toBe(2);
    expect(replayed[0].data).toBe("buffered\n");
    expect(replayed[1].data).toBe("also buffered\n");
  });

  test("getNextLogSequence returns monotonically increasing values", () => {
    const mgr = new SSEManager();

    expect(mgr.getNextLogSequence("run-1")).toBe(1);
    expect(mgr.getNextLogSequence("run-1")).toBe(2);
    expect(mgr.getNextLogSequence("run-1")).toBe(3);
    expect(mgr.getNextLogSequence("run-2")).toBe(1); // independent counter
  });
});

// =============================================================================
// Integration: Full log lifecycle with ownership + sequence
// =============================================================================

describe("Full log lifecycle integration", () => {

  test("log object created with tags, manifest, and correct updated_at", () => {
    const workflow = createTestWorkflow();
    const manifest = createManifest(workflow.id);
    const run = createTestRun(manifest.id);

    // Simulate what agent.ts does when creating a new log object
    const now = Date.now();
    const blob = createBlob({
      bucket: "logs",
      checksum: "",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: null,
      size_bytes: 0,
      last_referenced_at: now,
    });

    const obj = createObject({
      type: "log",
      blob_id: blob.id,
      provider: null,
      provider_object_id: null,
      metadata_json: JSON.stringify({ run_id: run.id, stream: "stdout" }),
      expires_at: null,
      current_manifest_id: manifest.id,
      accessed_at: null,
      updated_at: now,
    });

    addObjectTag(obj.id, "run_id", String(run.id));
    addObjectTag(obj.id, "stream", "stdout");
    addResourceToManifest(manifest.id, "object", String(obj.id));

    // Verify the complete chain
    const loaded = getObject(obj.id);
    expect(loaded).toBeDefined();
    expect(loaded!.current_manifest_id).toBe(manifest.id);
    expect(loaded!.updated_at).toBe(now);

    // Verify tags
    const tags = queryMany<{ tag: string }>(
      "SELECT tag FROM object_tags WHERE object_id = ?",
      [obj.id]
    );
    expect(tags.length).toBe(2);

    // Verify manifest resource
    const resources = queryMany<ManifestResource>(
      "SELECT * FROM manifest_resources WHERE manifest_id = ? AND resource_type = 'object'",
      [manifest.id]
    );
    expect(resources.length).toBe(1);
    expect(resources[0].resource_id).toBe(String(obj.id));
  });

  test("multiple streams for same run each get independent objects and tags", () => {
    const workflow = createTestWorkflow();
    const manifest = createManifest(workflow.id);
    const run = createTestRun(manifest.id);

    const stdout = createLogObject(run.id, "stdout", manifest.id);
    addObjectTag(stdout.obj.id, "run_id", String(run.id));
    addObjectTag(stdout.obj.id, "stream", "stdout");
    addResourceToManifest(manifest.id, "object", String(stdout.obj.id));

    const stderr = createLogObject(run.id, "stderr", manifest.id);
    addObjectTag(stderr.obj.id, "run_id", String(run.id));
    addObjectTag(stderr.obj.id, "stream", "stderr");
    addResourceToManifest(manifest.id, "object", String(stderr.obj.id));

    // Verify both are registered
    const resources = queryMany<ManifestResource>(
      "SELECT * FROM manifest_resources WHERE manifest_id = ? AND resource_type = 'object'",
      [manifest.id]
    );
    expect(resources.length).toBe(2);

    // Verify independent tag lookups
    const stdoutTags = queryMany<{ tag: string }>(
      "SELECT tag FROM object_tags WHERE object_id = ?",
      [stdout.obj.id]
    );
    const stderrTags = queryMany<{ tag: string }>(
      "SELECT tag FROM object_tags WHERE object_id = ?",
      [stderr.obj.id]
    );
    expect(stdoutTags.map(t => t.tag)).toContain("stream:stdout");
    expect(stderrTags.map(t => t.tag)).toContain("stream:stderr");
  });
});
