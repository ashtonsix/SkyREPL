// tests/integration/e2e-smoke.test.ts - Integration Smoke Test (Slice 1 Gate)
//
// Level 1: In-process test (no OrbStack needed)
//   Verifies control plane + API work end-to-end: launch-run creates a workflow,
//   workflow/status endpoints return correct data.
//
// Level 2: Full E2E (requires OrbStack):
//   Terminal 1: cd impl && bun run control/src/main.ts
//   Terminal 2: repl run "echo hello"
//   Expected: "hello" appears in terminal 2, exits with code 0
//
// Run this test:
//   bun test impl/tests/integration/e2e-smoke.test.ts

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
} from "../../control/src/material/db";
import { createWorkflowEngine, recoverWorkflows, requestEngineShutdown, awaitEngineQuiescence, resetEngineShutdown } from "../../control/src/workflow/engine";
import { registerLaunchRun } from "../../control/src/intent/launch-run";
import { createServer } from "../../control/src/api/routes";
import { registerAgentRoutes, createRealAgentBridge } from "../../control/src/api/agent";
import { registerWebSocketRoutes, registerSSEWorkflowStream } from "../../control/src/api/sse-protocol";
import { setAgentBridge } from "../../control/src/workflow/nodes/start-run";
import type { Server } from "bun";

// =============================================================================
// Test Fixtures
// =============================================================================

let tmpDir: string;
let server: Server<unknown>;
let baseUrl: string;

beforeAll(async () => {
  resetEngineShutdown();

  // Create temp directory for DB
  tmpDir = await mkdtemp(join(tmpdir(), "skyrepl-e2e-"));
  const dbPath = join(tmpDir, "test.db");

  // Initialize database
  initDatabase(dbPath);
  runMigrations();

  // Setup workflow engine
  createWorkflowEngine();
  registerLaunchRun();
  setAgentBridge(createRealAgentBridge());

  // Create server with all route groups
  const app = createServer({ port: 0, corsOrigins: ["*"], maxBodySize: 10 * 1024 * 1024 });
  registerAgentRoutes(app);
  registerWebSocketRoutes(app);
  registerSSEWorkflowStream(app);

  // Start on port 0 to get a random available port
  // idleTimeout: 0 is required for SSE connections
  // app.listen() returns the Elysia instance (not the Bun Server).
  // The actual Bun Server is available via app.server after listen().
  app.listen({ port: 0, idleTimeout: 0 });
  server = app.server!;
  const port = server.port;
  baseUrl = `http://localhost:${port}`;

  console.log(`[e2e] Test server started on port ${port}`);
});

afterAll(async () => {
  // Shutdown server
  if (server) {
    server.stop(true);
  }

  // Signal engine shutdown and wait for loops before closing DB
  requestEngineShutdown();
  await awaitEngineQuiescence(5_000);

  // Close database
  closeDatabase();

  // Remove temp directory
  if (tmpDir) {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// =============================================================================
// Tests
// =============================================================================

describe("E2E Smoke: launch-run API flow", () => {
  let workflowId: number;
  let runId: number;

  // ─── Health Check ──────────────────────────────────────────────────────────

  test("GET /v1/health returns ok", async () => {
    const res = await fetch(`${baseUrl}/v1/health`);
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body.status).toBe("ok");
    expect(body.timestamp).toBeGreaterThan(0);
  });

  // ─── Launch Run ────────────────────────────────────────────────────────────

  test("POST /v1/workflows/launch-run returns 202 with workflow_id and run_id", async () => {
    const res = await fetch(`${baseUrl}/v1/workflows/launch-run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        command: "echo hello",
        spec: "cpu-1x",
        maxDurationMs: 60_000,
        provider: "orbstack",
        region: "local",
        workdir: "/workspace",
      }),
    });

    expect(res.status).toBe(202);

    const body = await res.json();
    expect(body.workflow_id).toBeGreaterThan(0);
    expect(body.run_id).toBeGreaterThan(0);
    expect(body.status).toBeDefined();
    expect(body.status_url).toMatch(/\/v1\/workflows\/\d+\/status/);

    workflowId = body.workflow_id;
    runId = body.run_id;
  });

  // ─── Workflow Read ─────────────────────────────────────────────────────────

  test("GET /v1/workflows/:id returns the workflow", async () => {
    const res = await fetch(`${baseUrl}/v1/workflows/${workflowId}`);
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body.data).toBeDefined();
    expect(body.data.id).toBe(workflowId);
    expect(body.data.type).toBe("launch-run");
    expect(body.data.manifest_id).toBeGreaterThan(0);
  });

  test("GET /v1/workflows/:id?include=nodes returns workflow with nodes", async () => {
    const res = await fetch(`${baseUrl}/v1/workflows/${workflowId}?include=nodes`);
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body.data.nodes).toBeDefined();
    expect(Array.isArray(body.data.nodes)).toBe(true);
    // The launch-run blueprint has 9 nodes
    expect(body.data.nodes.length).toBe(9);

    const nodeIds = body.data.nodes.map((n: any) => n.node_id).sort();
    expect(nodeIds).toContain("check-budget");
    expect(nodeIds).toContain("resolve-instance");
    expect(nodeIds).toContain("spawn-instance");
    expect(nodeIds).toContain("create-allocation");
    expect(nodeIds).toContain("sync-files");
    expect(nodeIds).toContain("await-completion");
    expect(nodeIds).toContain("finalize-run");
  });

  // ─── Workflow Status ───────────────────────────────────────────────────────

  test("GET /v1/workflows/:id/status returns progress info", async () => {
    const res = await fetch(`${baseUrl}/v1/workflows/${workflowId}/status`);
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body.workflow_id).toBe(workflowId);
    expect(body.type).toBe("launch-run");
    expect(body.status).toBeDefined();
    expect(body.nodes_total).toBe(9);
    expect(typeof body.nodes_completed).toBe("number");
    expect(typeof body.nodes_failed).toBe("number");
    expect(body.progress).toBeDefined();
    expect(body.progress.total_nodes).toBe(9);
    expect(typeof body.progress.percentage).toBe("number");
  });

  // ─── Workflow Nodes ────────────────────────────────────────────────────────

  test("GET /v1/workflows/:id/nodes returns all nodes", async () => {
    const res = await fetch(`${baseUrl}/v1/workflows/${workflowId}/nodes`);
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body.data).toBeDefined();
    expect(body.data.length).toBe(9);
  });

  // ─── Run Read ──────────────────────────────────────────────────────────────

  test("GET /v1/runs/:id returns the run created by launch-run", async () => {
    const res = await fetch(`${baseUrl}/v1/runs/${runId}`);
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body.data).toBeDefined();
    expect(body.data.id).toBe(runId);
    expect(body.data.command).toBe("echo hello");
    expect(body.data.workdir).toBe("/workspace");
    expect(body.data.max_duration_ms).toBe(60_000);
  });

  // ─── List Endpoints ────────────────────────────────────────────────────────

  test("GET /v1/runs returns at least the run we created", async () => {
    const res = await fetch(`${baseUrl}/v1/runs`);
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body.data).toBeDefined();
    expect(body.data.length).toBeGreaterThanOrEqual(1);
    expect(body.data.some((r: any) => r.id === runId)).toBe(true);
  });

  test("GET /v1/workflows returns at least the workflow we created", async () => {
    const res = await fetch(`${baseUrl}/v1/workflows`);
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body.data).toBeDefined();
    expect(body.data.length).toBeGreaterThanOrEqual(1);
    expect(body.data.some((w: any) => w.id === workflowId)).toBe(true);
  });

  // ─── 404s ──────────────────────────────────────────────────────────────────

  test("GET /v1/workflows/999999 returns 404", async () => {
    const res = await fetch(`${baseUrl}/v1/workflows/999999`);
    expect(res.status).toBe(404);
  });

  test("GET /v1/workflows/999999/status returns 404", async () => {
    const res = await fetch(`${baseUrl}/v1/workflows/999999/status`);
    expect(res.status).toBe(404);
  });

  test("GET /v1/runs/999999 returns 404", async () => {
    const res = await fetch(`${baseUrl}/v1/runs/999999`);
    expect(res.status).toBe(404);
  });

  // ─── Validation ────────────────────────────────────────────────────────────

  test("POST /v1/workflows/launch-run with missing command returns 400", async () => {
    const res = await fetch(`${baseUrl}/v1/workflows/launch-run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        spec: "cpu-1x",
        maxDurationMs: 60_000,
      }),
    });

    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toBeDefined();
    expect(body.error.code).toBe("INVALID_INPUT");
  });

  test("POST /v1/workflows/launch-run with missing spec returns 400", async () => {
    const res = await fetch(`${baseUrl}/v1/workflows/launch-run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        command: "echo hello",
        maxDurationMs: 60_000,
      }),
    });

    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toBeDefined();
    expect(body.error.code).toBe("INVALID_INPUT");
  });

  // ─── Workflow Engine Fires ─────────────────────────────────────────────────

  test("workflow engine begins execution (check-budget node completes)", async () => {
    // Give the engine time to process the fire-and-forget executeLoop
    await Bun.sleep(1000);

    const res = await fetch(`${baseUrl}/v1/workflows/${workflowId}/status`);
    const body = await res.json();

    // At minimum, check-budget (which is a no-op) should have completed.
    // The workflow will fail at resolve-instance or spawn-instance since
    // there is no real OrbStack, but the engine should have started.
    expect(body.nodes_completed).toBeGreaterThanOrEqual(1);

    // Verify via direct DB access: check-budget node should be completed
    const nodes = getWorkflowNodes(workflowId);
    const checkBudget = nodes.find((n) => n.node_id === "check-budget");
    expect(checkBudget).toBeDefined();
    expect(checkBudget!.status).toBe("completed");
    expect(checkBudget!.output_json).toBeDefined();

    const output = JSON.parse(checkBudget!.output_json!);
    expect(output.budgetOk).toBe(true);
  });

  // ─── SSE Workflow Stream ───────────────────────────────────────────────────

  test("GET /v1/workflows/:id/stream returns SSE event-stream", async () => {
    // Wait a bit for the workflow to progress further
    await Bun.sleep(500);

    const res = await fetch(`${baseUrl}/v1/workflows/${workflowId}/stream`);
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toBe("text/event-stream");

    // Read a chunk of the stream
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();

    // Read with a timeout so the test does not hang
    const readWithTimeout = () =>
      Promise.race([
        reader.read(),
        new Promise<{ done: true; value: undefined }>((resolve) =>
          setTimeout(() => resolve({ done: true, value: undefined }), 2000)
        ),
      ]);

    let sseText = "";
    const chunk = await readWithTimeout();
    if (chunk.value) {
      sseText += decoder.decode(chunk.value);
    }

    reader.cancel();

    // Should contain at least one SSE event (node_started, node_completed, or workflow_failed)
    // The stream replays historical events, so there should be data even if workflow already finished
    expect(sseText.length).toBeGreaterThan(0);
    expect(sseText).toMatch(/event: /);
  });

  // ─── Blob Check Endpoint ───────────────────────────────────────────────────

  test("POST /v1/blobs/check returns all checksums as missing (Slice 1)", async () => {
    const res = await fetch(`${baseUrl}/v1/blobs/check`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        checksums: ["abc123", "def456"],
      }),
    });

    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.missing).toEqual(["abc123", "def456"]);
  });
});
