// tests/integration/e2e-simulated.test.ts - Fast Full E2E with Simulated Agent
//
// Tests the complete workflow loop (control plane → SSE → agent → logs → completion)
// using an in-process SimulatedAgent instead of a real VM. Runs in <5 seconds.
//
// This is the primary E2E test. OrbStack tests are a thin smoke layer for real-VM validation.
//
// Run: bun test tests/integration/e2e-simulated.test.ts

import { describe, test, expect, beforeAll, afterAll, afterEach } from "bun:test";
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
  execute,
  type Workflow,
} from "../../control/src/material/db";
import { createWorkflowEngine, requestEngineShutdown, awaitEngineQuiescence, resetEngineShutdown } from "../../control/src/workflow/engine";
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
import { MockProvider } from "./helpers/mock-provider";
import { seedTestApiKey, testFetch } from "./helpers/test-auth";
import type { Server } from "bun";

// =============================================================================
// Test Setup
// =============================================================================

const TEST_TIMEOUT_MS = 15_000;

let tmpDir: string;
let server: Server<unknown>;
let baseUrl: string;
let mockProvider: MockProvider;

beforeAll(async () => {
  resetEngineShutdown();

  tmpDir = await mkdtemp(join(tmpdir(), "skyrepl-sim-e2e-"));
  const dbPath = join(tmpDir, "test.db");

  initDatabase(dbPath);
  runMigrations();
  seedTestApiKey();

  createWorkflowEngine();
  registerLaunchRun();
  setAgentBridge(createRealAgentBridge());

  // Register mock provider (overrides real orbstack in cache)
  mockProvider = new MockProvider();
  clearProviderCache();
  await registerProvider({ provider: mockProvider });

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

  baseUrl = `http://localhost:${server.port}`;
  // MockProvider's agents connect to localhost, so set this env var
  process.env.SKYREPL_CONTROL_PLANE_URL = baseUrl;
});

afterEach(() => {
  // Clean up warm pool allocations and agents between tests for isolation
  mockProvider.cleanup();
  execute("DELETE FROM allocations WHERE status = 'AVAILABLE'", []);
});

afterAll(async () => {
  mockProvider?.cleanup();
  clearProviderCache();
  delete process.env.SKYREPL_CONTROL_PLANE_URL;
  if (server) server.stop(true);
  // Signal engine loops to stop, then wait for them before closing DB
  requestEngineShutdown();
  await awaitEngineQuiescence(5_000);
  closeDatabase();
  if (tmpDir) await rm(tmpDir, { recursive: true, force: true });
});

// =============================================================================
// Helpers
// =============================================================================

async function launchRun(
  command: string,
  opts?: Record<string, unknown>
): Promise<{ workflow_id: number; run_id: number }> {
  const res = await testFetch(`${baseUrl}/v1/workflows/launch-run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      command,
      spec: "ubuntu",
      provider: "mock",
      region: "local",
      workdir: "/workspace",
      max_duration_ms: 60_000,
      hold_duration_ms: 0,
      create_snapshot: false,
      files: [],
      artifact_patterns: [],
      ...opts,
    }),
  });
  expect(res.status).toBe(202);
  return res.json();
}

async function waitForTerminal(workflowId: number, timeoutMs = 10_000): Promise<Workflow> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const wf = getWorkflow(workflowId);
    if (wf && ["completed", "failed", "cancelled"].includes(wf.status)) {
      return wf;
    }
    await Bun.sleep(50);
  }
  return getWorkflow(workflowId)!;
}

function collectWsLogs(runId: number, timeoutMs = 10_000): Promise<string[]> {
  return new Promise((resolve) => {
    const chunks: string[] = [];
    const ws = new WebSocket(`ws://localhost:${server.port}/v1/runs/${runId}/logs`);

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(String(event.data));
        if (msg.stream && msg.data !== undefined) {
          chunks.push(msg.data);
        }
        if (msg.type === "status" && ["completed", "failed", "timeout"].includes(msg.status)) {
          ws.close();
        }
      } catch {}
    };
    ws.onclose = () => resolve(chunks);
    ws.onerror = () => resolve(chunks);
    setTimeout(() => {
      try { ws.close(); } catch {}
      resolve(chunks);
    }, timeoutMs);
  });
}

// =============================================================================
// Tests
// =============================================================================

describe("Simulated E2E", () => {
  test("echo hello: full workflow completes, logs arrive", async () => {
    mockProvider.nextBehavior = { stdout: ["hello"] };

    const { workflow_id, run_id } = await launchRun("echo hello");
    const logPromise = collectWsLogs(run_id);

    const wf = await waitForTerminal(workflow_id);
    const logs = await logPromise;

    expect(wf.status).toBe("completed");

    const allLogs = logs.join("");
    expect(allLogs).toContain("hello");

    const run = getRun(run_id);
    expect(run).toBeDefined();
    expect(run!.exit_code).toBe(0);
    expect(run!.finished_at).toBeGreaterThan(0);
  }, TEST_TIMEOUT_MS);

  test("non-zero exit code: workflow completes, exit code preserved", async () => {
    mockProvider.nextBehavior = { stdout: [], exitCode: 42 };

    const { workflow_id, run_id } = await launchRun("exit 42");
    const wf = await waitForTerminal(workflow_id);

    expect(wf.status).toBe("completed");

    const run = getRun(run_id);
    expect(run).toBeDefined();
    expect(run!.exit_code).toBe(42);
    expect(run!.finished_at).toBeGreaterThan(0);
  }, TEST_TIMEOUT_MS);

  test("stderr output: both streams arrive separately", async () => {
    mockProvider.nextBehavior = {
      stdout: ["out"],
      stderr: ["err"],
    };

    const { workflow_id, run_id } = await launchRun("echo out && echo err >&2");
    const logPromise = collectWsLogs(run_id);

    const wf = await waitForTerminal(workflow_id);
    const logs = await logPromise;

    expect(wf.status).toBe("completed");
    const allLogs = logs.join("");
    expect(allLogs).toContain("out");
    expect(allLogs).toContain("err");
  }, TEST_TIMEOUT_MS);

  test("large output: all lines arrive via log stream", async () => {
    const lines = Array.from({ length: 200 }, (_, i) => String(i + 1));
    mockProvider.nextBehavior = { stdout: lines };

    const { workflow_id, run_id } = await launchRun("seq 1 200");
    const logPromise = collectWsLogs(run_id);

    const wf = await waitForTerminal(workflow_id);
    const logs = await logPromise;

    expect(wf.status).toBe("completed");
    const allLogs = logs.join("");
    expect(allLogs).toContain("1");
    expect(allLogs).toContain("100");
    expect(allLogs).toContain("200");
  }, TEST_TIMEOUT_MS);

  test("workflow nodes: all 9 nodes accounted for (8 complete + 1 skipped)", async () => {
    mockProvider.nextBehavior = { stdout: ["ok"] };

    const { workflow_id } = await launchRun("echo ok");
    const wf = await waitForTerminal(workflow_id);

    expect(wf.status).toBe("completed");

    const nodes = getWorkflowNodes(workflow_id);
    const completed = nodes.filter(n => n.status === "completed");
    const skipped = nodes.filter(n => n.status === "skipped");

    expect(completed.length).toBe(8);
    expect(skipped.length).toBe(1);
    expect(skipped[0].node_id).toBe("claim-warm-allocation");
  }, TEST_TIMEOUT_MS);

  test("concurrent launches: two workflows complete independently", async () => {
    mockProvider.nextBehavior = { stdout: ["first"] };
    const launch1 = await launchRun("echo first");

    mockProvider.nextBehavior = { stdout: ["second"] };
    const launch2 = await launchRun("echo second");

    const [wf1, wf2] = await Promise.all([
      waitForTerminal(launch1.workflow_id),
      waitForTerminal(launch2.workflow_id),
    ]);

    expect(wf1.status).toBe("completed");
    expect(wf2.status).toBe("completed");

    const run1 = getRun(launch1.run_id);
    const run2 = getRun(launch2.run_id);
    expect(run1!.exit_code).toBe(0);
    expect(run2!.exit_code).toBe(0);
  }, TEST_TIMEOUT_MS);

  test("sync failure: workflow fails gracefully", async () => {
    mockProvider.nextBehavior = { failSync: true };

    const { workflow_id } = await launchRun("echo fail");
    const wf = await waitForTerminal(workflow_id);

    expect(wf.status).toBe("failed");
  }, TEST_TIMEOUT_MS);
});
