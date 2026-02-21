// tests/integration/e2e-exercise.test.ts - Comprehensive Exercise Test (Slice 1)
//
// Punishing in-process test suite exercising edge cases and code paths that the
// the basic smoke tests don't cover. No OrbStack required.
//
// Run:
//   bun test impl/tests/integration/e2e-exercise.test.ts

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  initDatabase,
  closeDatabase,
  runMigrations,
  getWorkflow,
  getWorkflowNodes,
  getRun,
  getInstance,
  getManifest,
  queryOne,
  queryMany,
  execute,
  createInstance,
  createRun,
  createAllocation,
  type Workflow,
  type WorkflowNode,
  type Instance,
} from "../../control/src/material/db";
import {
  createWorkflowEngine,
  recoverWorkflows,
  requestEngineShutdown,
  awaitEngineQuiescence,
  resetEngineShutdown,
} from "../../control/src/workflow/engine";
import { registerLaunchRun } from "../../control/src/intent/launch-run";
import { createServer } from "../../control/src/api/routes";
import {
  registerAgentRoutes,
  createRealAgentBridge,
} from "../../control/src/api/agent";
import {
  registerWebSocketRoutes,
  registerSSEWorkflowStream,
} from "../../control/src/api/sse-protocol";
import { setAgentBridge } from "../../control/src/workflow/nodes/start-run";
import { registerProvider, clearProviderCache } from "../../control/src/provider/registry";
import { seedTestApiKey, testFetch } from "./helpers/test-auth";
import type { Server } from "bun";

// =============================================================================
// Test Fixtures
// =============================================================================

let tmpDir: string;
let server: Server<unknown>;
let baseUrl: string;
let wsBaseUrl: string;

beforeAll(async () => {
  resetEngineShutdown();
  tmpDir = await mkdtemp(join(tmpdir(), "skyrepl-exercise-"));
  const dbPath = join(tmpDir, "test.db");

  initDatabase(dbPath);
  runMigrations();
  seedTestApiKey();

  createWorkflowEngine();
  registerLaunchRun();
  setAgentBridge(createRealAgentBridge());

  // Override orbstack provider with a stub that always fails to spawn.
  // This test doesn't need a real provider — it exercises API routes, DB state,
  // and workflow failure paths. Tests that need working workflows use e2e-simulated.
  clearProviderCache();
  await registerProvider({
    provider: {
      name: "orbstack" as any,
      capabilities: {
        snapshots: false,
        spot: false,
        gpu: false,
        multiRegion: false,
        persistentVolumes: false,
        warmVolumes: false,
        hibernation: false,
        costExplorer: false,
        tailscaleNative: false,
        idempotentSpawn: true,
        customNetworking: false,
      },
      async spawn() {
        throw new Error("OrbStack not available in test environment");
      },
      async terminate() {},
      async list() {
        return [];
      },
      async get() {
        return null;
      },
      generateBootstrap() {
        return { content: "#!/bin/bash\n# stub", format: "shell" as const, checksum: "stub" };
      },
    },
  });

  const app = createServer({
    port: 0,
    corsOrigins: ["*"],
    maxBodySize: 10 * 1024 * 1024,
  });
  registerAgentRoutes(app);
  registerWebSocketRoutes(app);
  registerSSEWorkflowStream(app);

  app.listen({ port: 0, idleTimeout: 0 });
  server = app.server!;
  const port = server.port;
  baseUrl = `http://localhost:${port}`;
  wsBaseUrl = `ws://localhost:${port}`;

  console.log(`[e2e-exercise] Test server started on port ${port}`);
});

afterAll(async () => {
  if (server) {
    server.stop(true);
  }
  clearProviderCache();
  requestEngineShutdown();
  await awaitEngineQuiescence(5_000);
  closeDatabase();
  if (tmpDir) {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// =============================================================================
// Helpers
// =============================================================================

/** Launch a run and return the response body. */
async function launchRun(overrides: Record<string, unknown> = {}): Promise<{
  workflow_id: number;
  run_id: number;
  status: string;
  status_url: string;
}> {
  const res = await testFetch(`${baseUrl}/v1/workflows/launch-run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      command: "echo test",
      spec: "cpu-1x",
      max_duration_ms: 60_000,
      provider: "orbstack",
      region: "local",
      workdir: "/workspace",
      ...overrides,
    }),
  });
  expect(res.status).toBe(202);
  return res.json();
}

/** Wait for a workflow to reach a terminal state. Returns the final DB record. */
async function waitForWorkflowTerminal(
  workflowId: number,
  timeoutMs = 8_000
): Promise<Workflow> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const wf = getWorkflow(workflowId);
    if (wf && ["completed", "failed", "cancelled"].includes(wf.status)) {
      return wf;
    }
    await Bun.sleep(100);
  }
  // Return whatever state we have, let the test assertion catch the mismatch
  return getWorkflow(workflowId)!;
}

/** Create a test instance directly in the database. */
function createTestInstance(overrides: Partial<Instance> = {}): Instance {
  return createInstance({
    provider: "orbstack",
    provider_id: `test-vm-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    spec: "cpu-1x",
    region: "local",
    ip: "127.0.0.1",
    workflow_state: "launch-run:provisioning",
    workflow_error: null,
    registration_token_hash: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 0,
    spot_request_id: null,
    init_checksum: null,
    last_heartbeat: Date.now(),
    ...overrides,
  });
}

// =============================================================================
// 1. Concurrent Launches
// =============================================================================

describe("Concurrent launches", () => {
  test("3 simultaneous POST /v1/workflows/launch-run all succeed with unique IDs", async () => {
    const promises = [
      launchRun({ command: "echo concurrent-1" }),
      launchRun({ command: "echo concurrent-2" }),
      launchRun({ command: "echo concurrent-3" }),
    ];

    const results = await Promise.all(promises);

    // All returned 202 (launchRun helper asserts this)
    expect(results.length).toBe(3);

    // All have unique workflow_ids
    const workflowIds = results.map((r) => r.workflow_id);
    const uniqueWorkflowIds = new Set(workflowIds);
    expect(uniqueWorkflowIds.size).toBe(3);

    // All have unique run_ids
    const runIds = results.map((r) => r.run_id);
    const uniqueRunIds = new Set(runIds);
    expect(uniqueRunIds.size).toBe(3);

    // All workflow_ids and run_ids are positive integers
    for (const r of results) {
      expect(r.workflow_id).toBeGreaterThan(0);
      expect(r.run_id).toBeGreaterThan(0);
      expect(r.status).toBeDefined();
      expect(r.status_url).toMatch(/\/v1\/workflows\/\d+\/status/);
    }

    // Verify each workflow has its own distinct set of 9 nodes in the DB
    for (const wfId of workflowIds) {
      const nodes = getWorkflowNodes(wfId);
      expect(nodes.length).toBe(9);
    }
  });
});

// =============================================================================
// 2. Idempotency Deduplication
// =============================================================================

describe("Idempotency deduplication", () => {
  test("second submission with same idempotencyKey returns same workflow_id", async () => {
    const idempotencyKey = `idem-test-${Date.now()}-${Math.random().toString(36).slice(2)}`;

    // First submission
    const res1 = await testFetch(`${baseUrl}/v1/workflows/launch-run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        command: "echo idempotent",
        spec: "cpu-1x",
        provider: "mock",
        region: "local",
        max_duration_ms: 60_000,
        idempotency_key: idempotencyKey,
      }),
    });
    expect(res1.status).toBe(202);
    const body1 = await res1.json();
    expect(body1.workflow_id).toBeGreaterThan(0);
    const firstRunId = body1.run_id;

    // Second submission with same key — different command/spec/duration to prove
    // the workflow is deduplicated (same workflow_id returned).
    const res2 = await testFetch(`${baseUrl}/v1/workflows/launch-run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        command: "echo idempotent-different-command",
        spec: "gpu-a100",
        provider: "mock",
        region: "local",
        max_duration_ms: 120_000,
        idempotency_key: idempotencyKey,
      }),
    });
    expect(res2.status).toBe(202);
    const body2 = await res2.json();

    // Same workflow_id returned (deduplicated at the workflow level)
    expect(body2.workflow_id).toBe(body1.workflow_id);

    // The status comes from the existing workflow's DB record, which is the
    // workflow's current execution status (e.g. "running", "pending", "failed").
    expect(body2.status).toBeDefined();
    expect(typeof body2.status).toBe("string");

    // Verify only ONE workflow exists with this idempotency key in the DB.
    // (The second call did not create a duplicate workflow.)
    const workflows = queryMany<{ id: number }>(
      "SELECT id FROM workflows WHERE idempotency_key = ?",
      [idempotencyKey]
    );
    expect(workflows.length).toBe(1);
    expect(workflows[0].id).toBe(body1.workflow_id);

    // The first run_id should be the one associated with the original workflow,
    // not a run from the second (deduplicated) request.
    expect(body2.run_id).toBe(firstRunId);
  });

  test("different idempotencyKeys produce different workflows", async () => {
    const key1 = `idem-a-${Date.now()}`;
    const key2 = `idem-b-${Date.now()}`;

    const [r1, r2] = await Promise.all([
      launchRun({ idempotencyKey: key1 }),
      launchRun({ idempotencyKey: key2 }),
    ]);

    expect(r1.workflow_id).not.toBe(r2.workflow_id);
    expect(r1.run_id).not.toBe(r2.run_id);
  });
});

// =============================================================================
// 3. Agent Heartbeat
// =============================================================================

describe("Agent heartbeat", () => {
  test("POST /v1/agent/heartbeat updates last_heartbeat in DB and returns ack", async () => {
    // Create a test instance so heartbeat has something to update
    const oldHeartbeat = Date.now() - 60_000;
    const instance = createTestInstance({ last_heartbeat: oldHeartbeat });

    const res = await testFetch(`${baseUrl}/v1/agent/heartbeat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        instance_id: instance.id,
        status: "idle",
        active_allocations: [],
      }),
    });

    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ack).toBe(true);
    expect(body.server_time).toBeGreaterThan(0);
    expect(body.control_plane_id).toBeDefined();
    expect(typeof body.commands_pending).toBe("boolean");

    // Verify DB was updated
    const updated = getInstance(instance.id);
    expect(updated).toBeDefined();
    expect(updated!.last_heartbeat).toBeGreaterThan(oldHeartbeat);
  });

  test("heartbeat for nonexistent instance returns 409 (#8.09)", async () => {
    const res = await fetch(`${baseUrl}/v1/agent/heartbeat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        instance_id: 999999,
        status: "idle",
        active_allocations: [],
      }),
    });
    expect(res.status).toBe(409);
    const body = await res.json();
    expect(body.error.code).toBe("INSTANCE_TERMINATED");
  });

  test("heartbeat for terminated instance returns 409 (#8.09)", async () => {
    const instance = createTestInstance({ workflow_state: "terminate:complete" });
    const res = await fetch(`${baseUrl}/v1/agent/heartbeat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        instance_id: instance.id,
        status: "idle",
        active_allocations: [],
      }),
    });
    expect(res.status).toBe(409);
    const body = await res.json();
    expect(body.error.code).toBe("INSTANCE_TERMINATED");
  });
});

// =============================================================================
// 4. Agent Log Ingestion + Storage
// =============================================================================

describe("Agent log ingestion + storage", () => {
  let testRunId: number;

  test("POST /v1/agent/logs creates log blob and returns ack with bytes_received", async () => {
    // Launch a run to get a valid run_id
    const launch = await launchRun({ command: "echo log-test" });
    testRunId = launch.run_id;

    // Send first log chunk
    const logData = "Hello from the agent! Line 1\n";
    const res = await testFetch(`${baseUrl}/v1/agent/logs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        run_id: testRunId,
        stream: "stdout",
        data: logData,
        timestamp: Date.now(),
        sequence: 1,
        phase: "execution",
      }),
    });

    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ack).toBe(true);
    expect(body.bytes_received).toBe(logData.length);
  });

  test("subsequent log POST appends data and blob grows", async () => {
    // Verify the blob exists from the first POST
    const blobBefore = queryOne<{ id: number; size_bytes: number }>(
      `SELECT b.id, b.size_bytes FROM blobs b
       JOIN objects o ON o.blob_id = b.id
       WHERE o.type = 'log' AND o.metadata_json LIKE ?
       ORDER BY o.id DESC LIMIT 1`,
      [`%"run_id":${testRunId}%"stream":"stdout"%`]
    );
    expect(blobBefore).toBeDefined();
    const sizeBefore = blobBefore!.size_bytes;

    // Send second log chunk
    const logData2 = "Second line of output\nThird line\n";
    const res2 = await testFetch(`${baseUrl}/v1/agent/logs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        run_id: testRunId,
        stream: "stdout",
        data: logData2,
        timestamp: Date.now(),
        sequence: 2,
        phase: "execution",
      }),
    });

    expect(res2.status).toBe(200);
    const body2 = await res2.json();
    expect(body2.ack).toBe(true);
    expect(body2.bytes_received).toBe(logData2.length);

    // Verify blob grew
    const blobAfter = queryOne<{ size_bytes: number }>(
      "SELECT size_bytes FROM blobs WHERE id = ?",
      [blobBefore!.id]
    );
    expect(blobAfter).toBeDefined();
    expect(blobAfter!.size_bytes).toBeGreaterThan(sizeBefore);
  });

  test("separate streams (stdout/stderr) create separate blobs", async () => {
    // Send stderr data
    const stderrData = "WARNING: something went wrong\n";
    const res = await testFetch(`${baseUrl}/v1/agent/logs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        run_id: testRunId,
        stream: "stderr",
        data: stderrData,
        timestamp: Date.now(),
        sequence: 1,
        phase: "execution",
      }),
    });

    expect(res.status).toBe(200);

    // Query both log objects for this run
    const logObjects = queryMany<{ id: number; metadata_json: string }>(
      `SELECT o.id, o.metadata_json FROM objects o
       WHERE o.type = 'log' AND o.metadata_json LIKE ?`,
      [`%"run_id":${testRunId}%`]
    );

    // Should have at least 2 distinct stream entries (stdout and stderr)
    const streams = logObjects.map((o) => JSON.parse(o.metadata_json).stream);
    expect(streams).toContain("stdout");
    expect(streams).toContain("stderr");
  });
});

// =============================================================================
// 5. Agent SSE Command Stream
// =============================================================================

describe("Agent SSE command stream", () => {
  test("GET /v1/agent/commands returns SSE with heartbeat_ack event", async () => {
    const instance = createTestInstance();

    const res = await testFetch(
      `${baseUrl}/v1/agent/commands?instance_id=${instance.id}`
    );
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toBe("text/event-stream");
    expect(res.headers.get("cache-control")).toBe("no-cache");

    // Read the first chunk (should contain heartbeat_ack)
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();

    const readWithTimeout = () =>
      Promise.race([
        reader.read(),
        new Promise<{ done: true; value: undefined }>((resolve) =>
          setTimeout(() => resolve({ done: true, value: undefined }), 500)
        ),
      ]);

    let sseText = "";
    const chunk = await readWithTimeout();
    if (chunk.value) {
      sseText += decoder.decode(chunk.value);
    }
    reader.cancel();

    expect(sseText).toContain("event: heartbeat_ack");
    expect(sseText).toContain("control_plane_id");
  });

  test("pending commands are flushed when agent connects via SSE", async () => {
    const instance = createTestInstance();
    const instanceId = String(instance.id);

    // Import sseManager to queue a command before connection
    const { sseManager } = await import(
      "../../control/src/api/sse-protocol"
    );

    // Queue a command (agent not yet connected)
    await sseManager.sendCommand(instanceId, {
      type: "heartbeat_ack",
      control_plane_id: "test-cp",
    } as any);
    expect(sseManager.hasPendingCommands(instanceId)).toBe(true);

    // Now connect via SSE
    const res = await testFetch(
      `${baseUrl}/v1/agent/commands?instance_id=${instance.id}`
    );
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();

    let sseText = "";
    // Read chunks until we have at least 2 heartbeat_ack events
    const deadline = Date.now() + 500;
    while (Date.now() < deadline) {
      const chunk = await Promise.race([
        reader.read(),
        new Promise<{ done: true; value: undefined }>((resolve) =>
          setTimeout(() => resolve({ done: true, value: undefined }), 500)
        ),
      ]);
      if (chunk.done) break;
      if (chunk.value) sseText += decoder.decode(chunk.value);

      // Check if we have the expected content (2x heartbeat_ack)
      const matches = sseText.match(/event: heartbeat_ack/g);
      if (matches && matches.length >= 2) break;
    }
    reader.cancel();

    // The queued command and the connection heartbeat_ack should both be present
    // Count heartbeat_ack occurrences (at least 2: one queued + one from connection)
    const matches = sseText.match(/event: heartbeat_ack/g);
    expect(matches).toBeDefined();
    expect(matches!.length).toBeGreaterThanOrEqual(2);
  });
});

// =============================================================================
// 6. Multiple WS Subscribers to Same Run Logs
// =============================================================================

describe("Multiple WS subscribers to same run logs", () => {
  test("both subscribers receive broadcast; one closing does not affect the other", async () => {
    // Launch a run
    const launch = await launchRun({ command: "echo ws-multi" });
    const runId = launch.run_id;

    // Open two WebSocket connections
    const wsUrl = `${wsBaseUrl}/v1/runs/${runId}/logs`;
    const ws1 = new WebSocket(wsUrl);
    const ws2 = new WebSocket(wsUrl);

    // Collect messages
    const messages1: string[] = [];
    const messages2: string[] = [];

    const ws1Open = new Promise<void>((resolve) => {
      ws1.onopen = () => resolve();
    });
    const ws2Open = new Promise<void>((resolve) => {
      ws2.onopen = () => resolve();
    });

    ws1.onmessage = (event) => messages1.push(String(event.data));
    ws2.onmessage = (event) => messages2.push(String(event.data));

    // Wait for both to connect
    await Promise.all([ws1Open, ws2Open]);

    // Small delay to ensure subscriptions are registered
    await Bun.sleep(10);

    // Post a log message
    await testFetch(`${baseUrl}/v1/agent/logs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        run_id: runId,
        stream: "stdout",
        data: "broadcast-message-1",
        timestamp: Date.now(),
      }),
    });

    // Poll until both have received the message
    let deadline = Date.now() + 500;
    while (Date.now() < deadline && messages1.length < 1) await Bun.sleep(5);
    deadline = Date.now() + 500;
    while (Date.now() < deadline && messages2.length < 1) await Bun.sleep(5);

    // Both should have received the broadcast
    expect(messages1.length).toBeGreaterThanOrEqual(1);
    expect(messages2.length).toBeGreaterThanOrEqual(1);

    const hasMsg1 = messages1.some((m) => m.includes("broadcast-message-1"));
    const hasMsg2 = messages2.some((m) => m.includes("broadcast-message-1"));
    expect(hasMsg1).toBe(true);
    expect(hasMsg2).toBe(true);

    // Close ws1
    ws1.close();
    await Bun.sleep(10);

    // Post another log message
    await testFetch(`${baseUrl}/v1/agent/logs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        run_id: runId,
        stream: "stdout",
        data: "broadcast-message-2",
        timestamp: Date.now(),
      }),
    });

    // Poll until ws2 has received the second message
    const messages2LenBefore = messages2.length;
    deadline = Date.now() + 500;
    while (Date.now() < deadline && messages2.length <= messages2LenBefore) await Bun.sleep(5);

    // ws2 should still receive the second broadcast
    const hasMsg2Second = messages2.some((m) =>
      m.includes("broadcast-message-2")
    );
    expect(hasMsg2Second).toBe(true);

    // ws1 should NOT have received the second broadcast (it was closed)
    const hasMsg1Second = messages1.some((m) =>
      m.includes("broadcast-message-2")
    );
    expect(hasMsg1Second).toBe(false);

    ws2.close();
  });
});

// =============================================================================
// 7. SSE Workflow Stream Replay
// =============================================================================

describe("SSE workflow stream replay", () => {
  test("replays historical events for a terminal workflow", async () => {
    // Launch a run and wait for it to fail (no OrbStack)
    const launch = await launchRun({ command: "echo sse-replay" });
    const wf = await waitForWorkflowTerminal(launch.workflow_id);
    expect(wf.status).toBe("failed");

    // Now fetch the SSE stream - should replay historical events
    const res = await testFetch(
      `${baseUrl}/v1/workflows/${launch.workflow_id}/stream`
    );
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toBe("text/event-stream");

    // Read the entire stream (it should close after sending final event)
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let fullText = "";

    while (true) {
      const chunk = await Promise.race([
        reader.read(),
        new Promise<{ done: true; value: undefined }>((resolve) =>
          setTimeout(() => resolve({ done: true, value: undefined }), 500)
        ),
      ]);
      if (chunk.done) break;
      if (chunk.value) fullText += decoder.decode(chunk.value);
    }
    reader.cancel();

    // Should contain node_started events
    expect(fullText).toContain("event: node_started");
    // Should contain node_completed for check-budget
    expect(fullText).toContain("event: node_completed");
    expect(fullText).toContain("check-budget");
    // Should contain node_failed for spawn-instance
    expect(fullText).toContain("event: node_failed");
    expect(fullText).toContain("spawn-instance");
    // Should contain workflow_failed
    expect(fullText).toContain("event: workflow_failed");
  });

  test("SSE stream returns 404 for nonexistent workflow", async () => {
    const res = await testFetch(`${baseUrl}/v1/workflows/999999/stream`);
    expect(res.status).toBe(404);
  });
});

// =============================================================================
// 8. Workflow Failure State Integrity
// =============================================================================

describe("Workflow failure state integrity", () => {
  test("failed workflow has correct status, manifest sealed, error populated, timestamps set", async () => {
    const launch = await launchRun({ command: "echo integrity" });
    const wf = await waitForWorkflowTerminal(launch.workflow_id);

    // Workflow status
    expect(wf.status).toBe("failed");
    expect(wf.finished_at).toBeDefined();
    expect(wf.finished_at).toBeGreaterThan(0);
    expect(wf.started_at).toBeDefined();
    expect(wf.started_at).toBeGreaterThan(0);
    expect(wf.error_json).toBeDefined();
    expect(wf.error_json).not.toBeNull();

    // Parse error_json
    const errorData = JSON.parse(wf.error_json!);
    expect(errorData.code).toBeDefined();
    expect(typeof errorData.message).toBe("string");

    // Manifest should be SEALED
    expect(wf.manifest_id).toBeGreaterThan(0);
    const manifest = getManifest(wf.manifest_id!);
    expect(manifest).toBeDefined();
    expect(manifest!.status).toBe("SEALED");
    expect(manifest!.released_at).toBeGreaterThan(0);

    // Node statuses
    const nodes = getWorkflowNodes(launch.workflow_id);
    expect(nodes.length).toBe(9);

    const nodeMap = new Map<string, WorkflowNode>();
    for (const n of nodes) {
      nodeMap.set(n.node_id, n);
    }

    // check-budget should be completed
    const checkBudget = nodeMap.get("check-budget")!;
    expect(checkBudget.status).toBe("completed");
    expect(checkBudget.output_json).toBeDefined();
    const budgetOutput = JSON.parse(checkBudget.output_json!);
    expect(budgetOutput.budgetOk).toBe(true);

    // resolve-instance should be completed
    const resolveInstance = nodeMap.get("resolve-instance")!;
    expect(resolveInstance.status).toBe("completed");

    // spawn-instance should be failed (no OrbStack)
    const spawnInstance = nodeMap.get("spawn-instance")!;
    expect(spawnInstance.status).toBe("failed");
    expect(spawnInstance.error_json).toBeDefined();
    expect(spawnInstance.started_at).toBeGreaterThan(0);
    expect(spawnInstance.finished_at).toBeGreaterThan(0);

    // claim-warm-allocation should be completed or skipped
    // (resolve-instance returns warmAvailable:false, triggering conditional branch skip)
    const claimWarm = nodeMap.get("claim-warm-allocation")!;
    expect(["completed", "skipped", "failed"]).toContain(claimWarm.status);

    // Downstream nodes should be pending (never reached) since spawn-instance failed
    const downstreamNodes = [
      "wait-for-boot",
      "create-allocation",
      "sync-files",
      "await-completion",
      "finalize-run",
    ];
    for (const nodeId of downstreamNodes) {
      const node = nodeMap.get(nodeId)!;
      expect(["pending", "failed", "skipped"]).toContain(node.status);
    }
  });

  test("run record associated with failed workflow has correct state", async () => {
    const launch = await launchRun({ command: "echo run-state" });
    await waitForWorkflowTerminal(launch.workflow_id);

    const run = getRun(launch.run_id);
    expect(run).toBeDefined();
    expect(run!.command).toBe("echo run-state");
    expect(run!.workdir).toBe("/workspace");
    expect(run!.max_duration_ms).toBe(60_000);
    // Run was created but never started on an instance
    expect(run!.exit_code).toBeNull();
  });
});

// =============================================================================
// 9. API Error Handling
// =============================================================================

describe("API error handling", () => {
  test("malformed JSON body returns 400", async () => {
    const res = await testFetch(`${baseUrl}/v1/workflows/launch-run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "this is not json {{{",
    });

    // Elysia should return 400 for parse errors
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toBeDefined();
  });

  test("missing Content-Type with non-JSON body returns error", async () => {
    const res = await testFetch(`${baseUrl}/v1/workflows/launch-run`, {
      method: "POST",
      body: "plain text body",
    });

    // Without Content-Type: application/json, Elysia doesn't parse the body,
    // so body is undefined. The nullish guard in the handler treats it as {}
    // and returns 400 INVALID_INPUT (command is required).
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toBeDefined();
    expect(body.error.code).toBe("INVALID_INPUT");
  });

  test("GET on nonexistent instance returns 404", async () => {
    const res = await testFetch(`${baseUrl}/v1/instances/99999`);
    expect(res.status).toBe(404);
    const body = await res.json();
    expect(body.error).toBeDefined();
    expect(body.error.code).toBe("INSTANCE_NOT_FOUND");
  });

  test("GET on nonexistent run returns 404", async () => {
    const res = await testFetch(`${baseUrl}/v1/runs/99999`);
    expect(res.status).toBe(404);
  });

  test("GET on nonexistent workflow returns 404", async () => {
    const res = await testFetch(`${baseUrl}/v1/workflows/99999`);
    expect(res.status).toBe(404);
  });

  test("GET /v1/workflows/999999/status returns 404", async () => {
    const res = await testFetch(`${baseUrl}/v1/workflows/999999/status`);
    expect(res.status).toBe(404);
  });

  test("POST launch-run with empty body returns 400", async () => {
    const res = await testFetch(`${baseUrl}/v1/workflows/launch-run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toBeDefined();
    expect(body.error.code).toBe("INVALID_INPUT");
  });

  test("POST launch-run with command but no spec returns 400", async () => {
    const res = await testFetch(`${baseUrl}/v1/workflows/launch-run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        command: "echo hello",
        max_duration_ms: 60_000,
      }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error.code).toBe("INVALID_INPUT");
    expect(body.error.message).toContain("spec");
  });

  test("POST launch-run with command and spec but no max_duration_ms returns 400", async () => {
    const res = await testFetch(`${baseUrl}/v1/workflows/launch-run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        command: "echo hello",
        spec: "cpu-1x",
      }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error.code).toBe("INVALID_INPUT");
    expect(body.error.message).toContain("max_duration_ms");
  });

  test("GET /v1/workflows/not-a-number handled gracefully", async () => {
    const res = await testFetch(`${baseUrl}/v1/workflows/not-a-number`);
    // Should be 400 (invalid ID) or handled as a non-match
    const body = await res.json();
    expect(body.error).toBeDefined();
  });

  test("GET /v1/blobs/999/download returns 404", async () => {
    const res = await testFetch(`${baseUrl}/v1/blobs/999/download`);
    expect(res.status).toBe(404);
  });
});

// =============================================================================
// 10. List Endpoints with Filters
// =============================================================================

describe("List endpoints with filters", () => {
  // These tests rely on workflows launched in previous describe blocks
  // Wait for them to settle
  let failedWorkflowId: number;

  test("setup: launch and wait for a workflow to fail", async () => {
    const launch = await launchRun({ command: "echo list-filter-test" });
    failedWorkflowId = launch.workflow_id;
    await waitForWorkflowTerminal(failedWorkflowId);
  });

  test("GET /v1/workflows returns all workflows", async () => {
    const res = await testFetch(`${baseUrl}/v1/workflows`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.data).toBeDefined();
    expect(Array.isArray(body.data)).toBe(true);
    // We've launched many workflows by this point
    expect(body.data.length).toBeGreaterThanOrEqual(5);
  });

  test("GET /v1/workflows?type=launch-run returns only launch-run workflows", async () => {
    const res = await testFetch(`${baseUrl}/v1/workflows?type=launch-run`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.data.length).toBeGreaterThanOrEqual(1);
    for (const wf of body.data) {
      expect(wf.type).toBe("launch-run");
    }
  });

  test("GET /v1/workflows?status=failed returns only failed workflows", async () => {
    const res = await testFetch(`${baseUrl}/v1/workflows?status=failed`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.data.length).toBeGreaterThanOrEqual(1);
    for (const wf of body.data) {
      expect(wf.status).toBe("failed");
    }
  });

  test("GET /v1/workflows?status=completed returns empty when none completed (no OrbStack)", async () => {
    const res = await testFetch(`${baseUrl}/v1/workflows?status=completed`);
    expect(res.status).toBe(200);
    const body = await res.json();
    // In test env without OrbStack, all workflows fail at spawn-instance
    // so completed should be empty
    expect(Array.isArray(body.data)).toBe(true);
    expect(body.data.length).toBe(0);
  });

  test("GET /v1/workflows?type=nonexistent returns empty array", async () => {
    const res = await testFetch(`${baseUrl}/v1/workflows?type=nonexistent`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.data).toEqual([]);
  });

  test("GET /v1/runs returns all runs with field shape", async () => {
    const res = await testFetch(`${baseUrl}/v1/runs`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.data.length).toBeGreaterThanOrEqual(5);
    // Verify field shape on the first item
    const first = body.data[0];
    expect(first).toHaveProperty("id");
    expect(first).toHaveProperty("command");
    expect(first).toHaveProperty("workflow_state");
    expect(typeof first.id).toBe("number");
  });

  test("GET /v1/instances returns created test instances", async () => {
    const res = await testFetch(`${baseUrl}/v1/instances`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.data.length).toBeGreaterThanOrEqual(1);
    // At least some should be our test instances
    const hasOrbstack = body.data.some(
      (i: any) => i.provider === "orbstack"
    );
    expect(hasOrbstack).toBe(true);
  });

  test("GET /v1/instances?provider=nonexistent returns empty array", async () => {
    const res = await testFetch(`${baseUrl}/v1/instances?provider=nonexistent`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.data).toEqual([]);
  });

  test("GET /v1/allocations returns array with field shape", async () => {
    const res = await testFetch(`${baseUrl}/v1/allocations`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(Array.isArray(body.data)).toBe(true);
    // Allocations may be empty at this point — field shape checked only if present
    if (body.data.length > 0) {
      const first = body.data[0];
      expect(first).toHaveProperty("id");
      expect(first).toHaveProperty("instance_id");
      expect(first).toHaveProperty("status");
      expect(["AVAILABLE", "CLAIMED", "ACTIVE", "COMPLETE", "FAILED"]).toContain(first.status);
    }
  });
});

// =============================================================================
// 11. Workflow Node Detail
// =============================================================================

describe("Workflow node detail verification", () => {
  test("workflow?include=nodes returns all node fields", async () => {
    const launch = await launchRun({ command: "echo node-detail" });
    await waitForWorkflowTerminal(launch.workflow_id);

    const res = await testFetch(
      `${baseUrl}/v1/workflows/${launch.workflow_id}?include=nodes`
    );
    expect(res.status).toBe(200);
    const body = await res.json();

    expect(body.data.nodes).toBeDefined();
    expect(body.data.nodes.length).toBe(9);

    for (const node of body.data.nodes) {
      // Every node should have these fields
      expect(node.id).toBeGreaterThan(0);
      expect(node.workflow_id).toBe(launch.workflow_id);
      expect(typeof node.node_id).toBe("string");
      expect(typeof node.node_type).toBe("string");
      expect(typeof node.status).toBe("string");
      expect(
        ["pending", "running", "completed", "failed", "skipped"]
      ).toContain(node.status);
      expect(node.attempt).toBeGreaterThanOrEqual(0);
      expect(node.created_at).toBeGreaterThan(0);
    }
  });

  test("workflow?include=nodes,manifest returns both", async () => {
    const launch = await launchRun({ command: "echo both-include" });
    await waitForWorkflowTerminal(launch.workflow_id);

    const res = await testFetch(
      `${baseUrl}/v1/workflows/${launch.workflow_id}?include=nodes,manifest`
    );
    expect(res.status).toBe(200);
    const body = await res.json();

    expect(body.data.nodes).toBeDefined();
    expect(body.data.manifest).toBeDefined();
    expect(body.data.manifest.status).toBe("SEALED");
    expect(body.data.manifest.workflow_id).toBe(launch.workflow_id);
  });
});

// =============================================================================
// 12. Blob Check Endpoint
// =============================================================================

describe("Blob check endpoint", () => {
  test("POST /v1/blobs/check returns all checksums as missing (Slice 1)", async () => {
    const checksums = [
      "sha256:abc123deadbeef",
      "sha256:fedcba987654",
      "sha256:0000000000000",
    ];
    const res = await testFetch(`${baseUrl}/v1/blobs/check`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ checksums }),
    });

    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.missing).toEqual(checksums);
  });
});

// =============================================================================
// 13. Agent Status Endpoint
// =============================================================================

describe("Agent status endpoint", () => {
  // Helper: create a run + instance + allocation for status endpoint tests
  function createStatusTestFixture() {
    const instance = createTestInstance({ registration_token_hash: null });
    const run = createRun({
      command: "echo status-test",
      workdir: "/workspace",
      max_duration_ms: 60_000,
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
    const allocation = createAllocation({
      run_id: run.id,
      instance_id: instance.id,
      status: "ACTIVE",
      current_manifest_id: null,
      user: "ubuntu",
      workdir: "/workspace",
      debug_hold_until: null,
      completed_at: null,
    });
    return { instance, run, allocation };
  }

  test("POST /v1/agent/status updates run workflow_state", async () => {
    const { run } = createStatusTestFixture();

    const res = await fetch(`${baseUrl}/v1/agent/status`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        run_id: run.id,
        status: "started",
      }),
    });

    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ack).toBe(true);

    const updated = getRun(run.id);
    expect(updated).toBeDefined();
    expect(updated!.workflow_state).toBe("launch-run:running");
  });

  test("POST /v1/agent/status with completed sets finished_at", async () => {
    const { run } = createStatusTestFixture();

    const res = await fetch(`${baseUrl}/v1/agent/status`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        run_id: run.id,
        status: "completed",
        exit_code: 0,
      }),
    });

    expect(res.status).toBe(200);

    const updated = getRun(run.id);
    expect(updated).toBeDefined();
    expect(updated!.workflow_state).toBe("launch-run:complete");
    expect(updated!.exit_code).toBe(0);
    expect(updated!.finished_at).toBeGreaterThan(0);
  });

  test("POST /v1/agent/status with unknown run_id returns 404 (#12.04)", async () => {
    const res = await fetch(`${baseUrl}/v1/agent/status`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        run_id: 999999,
        status: "started",
      }),
    });

    expect(res.status).toBe(404);
    const body = await res.json();
    expect(body.error.code).toBe("ALLOCATION_NOT_FOUND");
  });
});

// =============================================================================
// 14. Panic Diagnostics Endpoint
// =============================================================================

describe("Panic diagnostics endpoint", () => {
  test("POST /v1/instances/:id/panic stores panic record", async () => {
    const instance = createTestInstance();

    const res = await testFetch(`${baseUrl}/v1/instances/${instance.id}/panic`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        instance_id: instance.id,
        reason: "heartbeat_timeout",
        last_state: {
          workflow_state: "launch-run:provisioning",
          run_id: null,
          allocation_id: null,
          uptime_seconds: 300,
          last_heartbeat_ack: Date.now() - 120_000,
          sse_reconnect_count: 5,
        },
        diagnostics: {
          cpu_percent: 95.5,
          memory_percent: 88.2,
          disk_percent: 45.0,
          active_threads: 12,
          log_buffer_size: 1024,
        },
        error_logs: ["Connection refused", "SSE stream broken"],
        timestamp: Date.now(),
      }),
    });

    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ack).toBe(true);
    expect(body.stored).toBe(true);

    // Verify stored in DB
    const panicRecord = queryOne<{ id: number; reason: string }>(
      "SELECT * FROM instance_panic_logs WHERE instance_id = ?",
      [instance.id]
    );
    expect(panicRecord).toBeDefined();
    expect(panicRecord!.reason).toBe("heartbeat_timeout");
  });
});

// =============================================================================
// 15. Spot Interrupt and Heartbeat Panic Stubs
// =============================================================================

describe("Stub endpoints return ack", () => {
  test("POST /v1/agent/spot-interrupt-start returns ack", async () => {
    const instance = createTestInstance({ is_spot: 1, registration_token_hash: null });
    const res = await testFetch(`${baseUrl}/v1/agent/spot-interrupt-start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        instance_id: instance.id,
        action: "terminate",
        action_time: new Date(Date.now() + 120_000).toISOString(),
      }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ack).toBe(true);
  });

  test("POST /v1/agent/spot-interrupt-complete returns ack", async () => {
    const instance = createTestInstance({ is_spot: 1, registration_token_hash: null });
    const res = await testFetch(`${baseUrl}/v1/agent/spot-interrupt-complete`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        instance_id: instance.id,
      }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ack).toBe(true);
  });

  test("POST /v1/agent/heartbeat-panic-start returns ack", async () => {
    const res = await testFetch(`${baseUrl}/v1/agent/heartbeat-panic-start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        run_id: 1,
        time_since_ack_ms: 60_000,
      }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ack).toBe(true);
  });

  test("POST /v1/agent/heartbeat-panic-complete returns ack", async () => {
    const res = await testFetch(`${baseUrl}/v1/agent/heartbeat-panic-complete`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        run_id: 1,
        artifacts_uploaded: 0,
      }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ack).toBe(true);
  });
});

// =============================================================================
// 16. Workflow Timing Invariants
// =============================================================================

describe("Workflow timing invariants", () => {
  test("started_at <= finished_at for failed workflow", async () => {
    const launch = await launchRun({ command: "echo timing" });
    const wf = await waitForWorkflowTerminal(launch.workflow_id);

    expect(wf.started_at).toBeDefined();
    expect(wf.finished_at).toBeDefined();
    expect(wf.started_at!).toBeLessThanOrEqual(wf.finished_at!);
    expect(wf.created_at).toBeLessThanOrEqual(wf.started_at!);
  });

  test("completed nodes have started_at < finished_at", async () => {
    const launch = await launchRun({ command: "echo node-timing" });
    await waitForWorkflowTerminal(launch.workflow_id);

    const nodes = getWorkflowNodes(launch.workflow_id);
    const completedNodes = nodes.filter((n) => n.status === "completed");

    expect(completedNodes.length).toBeGreaterThanOrEqual(1);
    for (const node of completedNodes) {
      expect(node.started_at).toBeDefined();
      expect(node.finished_at).toBeDefined();
      expect(node.started_at!).toBeLessThanOrEqual(node.finished_at!);
    }
  });

  test("failed nodes have started_at and finished_at populated", async () => {
    const launch = await launchRun({ command: "echo failed-timing" });
    await waitForWorkflowTerminal(launch.workflow_id);

    const nodes = getWorkflowNodes(launch.workflow_id);
    const failedNodes = nodes.filter((n) => n.status === "failed");

    expect(failedNodes.length).toBeGreaterThanOrEqual(1);
    for (const node of failedNodes) {
      expect(node.started_at).toBeGreaterThan(0);
      expect(node.finished_at).toBeGreaterThan(0);
    }
  });
});

// =============================================================================
// 17. Agent File Download
// =============================================================================

describe("Agent file download", () => {
  test("GET /v1/agent/download/:filename serves Python agent files", async () => {
    const files = [
      "agent.py",
      "executor.py",
      "heartbeat.py",
      "logs.py",
      "sse.py",
      "http_client.py",
    ];
    for (const f of files) {
      const res = await testFetch(`${baseUrl}/v1/agent/download/${f}`);
      expect(res.status).toBe(200);
      expect(res.headers.get("content-type")).toBe("text/x-python");
      const text = await res.text();
      expect(text.length).toBeGreaterThan(100);
    }
  });

  test("GET /v1/agent/download/malicious.py returns 404", async () => {
    const res = await testFetch(`${baseUrl}/v1/agent/download/malicious.py`);
    expect(res.status).toBe(404);
  });

  test("GET /v1/agent/download/../../../etc/passwd returns 404 (path traversal blocked)", async () => {
    const res = await testFetch(
      `${baseUrl}/v1/agent/download/..%2F..%2F..%2Fetc%2Fpasswd`
    );
    expect(res.status).toBe(404);
  });
});

// =============================================================================
// 18. Workflow Status Endpoint Detail
// =============================================================================

describe("Workflow status endpoint detail", () => {
  test("failed workflow status has error details with nodeId", async () => {
    const launch = await launchRun({ command: "echo status-detail" });
    const wf = await waitForWorkflowTerminal(launch.workflow_id);
    expect(wf.status).toBe("failed");

    const res = await testFetch(
      `${baseUrl}/v1/workflows/${launch.workflow_id}/status`
    );
    expect(res.status).toBe(200);
    const body = await res.json();

    expect(body.status).toBe("failed");
    expect(body.error).toBeDefined();
    expect(body.error.code).toBeDefined();
    expect(body.error.message).toBeDefined();
    expect(body.error.node_id).toBeDefined();
    // The failing node should be spawn-instance
    expect(body.error.node_id).toBe("spawn-instance");
    expect(body.finished_at).toBeGreaterThan(0);
    expect(body.progress.percentage).toBeLessThan(100);
  });
});

// =============================================================================
// 19. Health Check Under Load
// =============================================================================

describe("Health check under load", () => {
  test("GET /v1/health responds correctly even after many workflows", async () => {
    const res = await testFetch(`${baseUrl}/v1/health`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.status).toBe("ok");
    expect(body.timestamp).toBeGreaterThan(0);
  });
});

// =============================================================================
// 20. Standalone Workflow Nodes Endpoint (migrated from e2e-smoke)
// =============================================================================

describe("Standalone workflow nodes endpoint", () => {
  test("GET /v1/workflows/:id/nodes returns all nodes", async () => {
    const launch = await launchRun({ command: "echo nodes-endpoint" });
    // Wait for workflow to progress
    await waitForWorkflowTerminal(launch.workflow_id);

    const res = await testFetch(`${baseUrl}/v1/workflows/${launch.workflow_id}/nodes`);
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body.data).toBeDefined();
    expect(body.data.length).toBe(9);
  });
});
