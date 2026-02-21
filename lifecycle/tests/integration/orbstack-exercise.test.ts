// tests/integration/orbstack-exercise.test.ts - Punishing OrbStack E2E Exercises
//
// Level 3: Requires OrbStack. Spawns REAL VMs for each scenario, testing
// edge cases that orbstack-e2e.test.ts doesn't cover:
//   1. Non-zero exit code
//   2. Stderr output
//   3. Large stdout (500 lines)
//   4. Multi-line ordered output
//
// Auto-skips when orbctl is not available.
// Timeout: 180s (multiple VM spawns, each ~30-40s)
//
// Run: bun test tests/integration/orbstack-exercise.test.ts

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir, hostname } from "node:os";
import {
  initDatabase,
  closeDatabase,
  runMigrations,
  getWorkflowNodes,
  getRun,
  queryOne,
  queryMany,
  type Allocation,
  type Workflow,
} from "../../control/src/material/db";
import { createWorkflowEngine } from "../../control/src/workflow/engine";
import { clearProviderCache } from "../../control/src/provider/registry";
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
import type { Server } from "bun";

// =============================================================================
// OrbStack Detection
// =============================================================================

function detectOrbctl(): boolean {
  try {
    const r = Bun.spawnSync(["orbctl", "version"], {
      stdout: "pipe",
      stderr: "pipe",
    });
    if (r.exitCode === 0) return true;
  } catch {}
  try {
    const r = Bun.spawnSync(["mac", "orbctl", "version"], {
      stdout: "pipe",
      stderr: "pipe",
    });
    if (r.exitCode === 0) return true;
  } catch {}
  return false;
}

const hasOrbctl = detectOrbctl();

function orbctlSync(args: string[]): { stdout: string; exitCode: number } {
  try {
    const r = Bun.spawnSync(["orbctl", ...args], {
      stdout: "pipe",
      stderr: "pipe",
    });
    return {
      stdout: new TextDecoder().decode(r.stdout),
      exitCode: r.exitCode,
    };
  } catch {
    const r = Bun.spawnSync(["mac", "orbctl", ...args], {
      stdout: "pipe",
      stderr: "pipe",
    });
    return {
      stdout: new TextDecoder().decode(r.stdout),
      exitCode: r.exitCode,
    };
  }
}

/**
 * Clean up stray repl-* VMs from previous interrupted test runs.
 * Called at the start of beforeAll so each run inherits a clean state.
 *
 * Note: The provider's terminate() method does a graceful stop-before-delete
 * to help OrbStack's DHCP allocator reclaim IPs. This cleanup uses plain
 * delete -f for speed (to stay within bun's 5s hook timeout).
 */
function cleanupStrayVMs(): void {
  const { stdout, exitCode } = orbctlSync(["list"]);
  if (exitCode !== 0) return;
  for (const line of stdout.split("\n")) {
    const name = line.trim().split(/\s+/)[0];
    if (name?.startsWith("repl-")) {
      console.log(`[exercise] Cleaning up stray VM: ${name}`);
      orbctlSync(["delete", "-f", name]);
    }
  }
}

/**
 * URL reachable from a sibling OrbStack VM.
 * OrbStack VMs resolve <vmname>.orb.local to each other's IPs.
 */
function getReachableUrl(port: number): string {
  return `http://${hostname()}.orb.local:${port}`;
}

// =============================================================================
// Shared Helpers
// =============================================================================

interface LogMessage {
  stream?: "stdout" | "stderr";
  data?: string;
  type?: string;
  status?: string;
  timestamp?: number;
  sequence?: number;
}

/**
 * Launch a run workflow and return workflowId + runId.
 */
async function launchRun(
  baseUrl: string,
  command: string,
  maxDurationMs = 60_000,
): Promise<{ workflowId: number; runId: number }> {
  const res = await fetch(`${baseUrl}/v1/workflows/launch-run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      command,
      spec: "ubuntu",
      provider: "orbstack",
      region: "local",
      workdir: "/workspace",
      max_duration_ms: maxDurationMs,
      hold_duration_ms: 0,
      create_snapshot: false,
      files: [],
      artifact_patterns: [],
    }),
  });

  expect(res.status).toBe(202);
  const body = await res.json();
  expect(body.workflow_id).toBeGreaterThan(0);
  expect(body.run_id).toBeGreaterThan(0);
  return { workflowId: body.workflow_id, runId: body.run_id };
}

/**
 * Collect log messages via WebSocket until a terminal status arrives or timeout.
 */
function collectLogs(
  serverPort: number,
  runId: number,
  timeoutMs: number,
): { promise: Promise<LogMessage[]>; messages: LogMessage[] } {
  const messages: LogMessage[] = [];
  const wsUrl = `ws://localhost:${serverPort}/v1/runs/${runId}/logs`;

  const promise = new Promise<LogMessage[]>((resolve) => {
    const ws = new WebSocket(wsUrl);

    ws.onmessage = (event) => {
      try {
        const msg: LogMessage = JSON.parse(String(event.data));
        messages.push(msg);
        if (
          msg.type === "status" &&
          ["completed", "failed", "timeout"].includes(msg.status!)
        ) {
          ws.close();
        }
      } catch {}
    };

    ws.onclose = () => resolve(messages);
    ws.onerror = () => resolve(messages);

    // Safety timeout
    setTimeout(() => {
      try {
        ws.close();
      } catch {}
      resolve(messages);
    }, timeoutMs - 10_000);
  });

  return { promise, messages };
}

/**
 * Poll workflow status until terminal, returning the final status object.
 */
async function pollUntilDone(
  baseUrl: string,
  workflowId: number,
  label: string,
  timeoutMs: number,
): Promise<{
  status: string;
  nodes_completed: number;
  nodes_total: number;
  error: unknown;
}> {
  const terminalStates = new Set(["completed", "failed", "cancelled"]);

  return new Promise((resolve) => {
    const poll = async () => {
      try {
        const res = await fetch(`${baseUrl}/v1/workflows/${workflowId}/status`);
        const body = await res.json();

        console.log(
          `[exercise:${label}] Poll: status=${body.status} nodes=${body.nodes_completed}/${body.nodes_total}`,
        );

        if (terminalStates.has(body.status)) {
          resolve(body);
          return;
        }
      } catch (err) {
        console.error(`[exercise:${label}] Poll error: ${err}`);
      }
      setTimeout(poll, 3_000);
    };

    // First poll after 2s
    setTimeout(poll, 2_000);

    // Hard timeout safety
    setTimeout(() => {
      resolve({
        status: "timeout",
        nodes_completed: -1,
        nodes_total: -1,
        error: `Poll timeout after ${timeoutMs}ms`,
      });
    }, timeoutMs - 5_000);
  });
}

/**
 * Extract VM name from spawn-instance node output for cleanup.
 */
function extractVmName(workflowId: number): string | null {
  const nodes = getWorkflowNodes(workflowId);
  const spawnNode = nodes.find((n) => n.node_id === "spawn-instance");
  if (spawnNode?.output_json) {
    try {
      const output = JSON.parse(spawnNode.output_json);
      if (output.providerId) return output.providerId;
    } catch {}
  }
  return null;
}

// =============================================================================
// Test Suite
// =============================================================================

const TEST_TIMEOUT_MS = 180_000;
const SINGLE_TEST_TIMEOUT_MS = 120_000;

describe.skipIf(!hasOrbctl || !process.env.ORBSTACK_TESTS)("OrbStack Exercise: punishing E2E scenarios", () => {
  let tmpDir: string;
  let server: Server<unknown>;
  let baseUrl: string;
  let savedControlPlaneUrl: string | undefined;
  const createdVmNames: string[] = [];

  beforeAll(async () => {
    clearProviderCache(); // Ensure no stale MockProvider from other test files
    cleanupStrayVMs();

    tmpDir = await mkdtemp(join(tmpdir(), "skyrepl-orbstack-exercise-"));
    const dbPath = join(tmpDir, "test.db");

    initDatabase(dbPath);
    runMigrations();

    createWorkflowEngine();
    registerLaunchRun();
    setAgentBridge(createRealAgentBridge());

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

    baseUrl = `http://localhost:${server.port!}`;
    const controlPlaneUrl = getReachableUrl(server.port!);

    savedControlPlaneUrl = process.env.SKYREPL_CONTROL_PLANE_URL;
    process.env.SKYREPL_CONTROL_PLANE_URL = controlPlaneUrl;

    console.log(
      `[exercise] Server on port ${server.port!}, reachable at ${controlPlaneUrl}`,
    );
  });

  afterAll(async () => {
    // Restore env
    if (savedControlPlaneUrl !== undefined) {
      process.env.SKYREPL_CONTROL_PLANE_URL = savedControlPlaneUrl;
    } else {
      delete process.env.SKYREPL_CONTROL_PLANE_URL;
    }

    // Terminate all VMs created during tests
    for (const name of createdVmNames) {
      console.log(`[exercise] Cleaning up VM: ${name}`);
      orbctlSync(["delete", "-f", name]);
    }

    if (server) server.stop(true);
    closeDatabase();
    if (tmpDir) await rm(tmpDir, { recursive: true, force: true });
  });

  // ─── Test 1: Non-zero exit code ──────────────────────────────────────────

  test(
    "non-zero exit code: exit 42 completes workflow with exit_code=42",
    async () => {
      const { workflowId, runId } = await launchRun(baseUrl, "exit 42");
      console.log(
        `[exercise:exit42] Launched workflow=${workflowId} run=${runId}`,
      );

      // Collect logs (there may be none since exit 42 produces no output)
      const { promise: logDone } = collectLogs(
        server.port!,
        runId,
        SINGLE_TEST_TIMEOUT_MS,
      );

      // Poll until workflow reaches terminal state
      const finalStatus = await pollUntilDone(
        baseUrl,
        workflowId,
        "exit42",
        SINGLE_TEST_TIMEOUT_MS,
      );

      await logDone;

      // Track VM for cleanup
      const vmName = extractVmName(workflowId);
      if (vmName) createdVmNames.push(vmName);

      // --- Assertions ---

      // Workflow completed (run failure is NOT workflow failure)
      expect(finalStatus.status).toBe("completed");

      // Run exit_code is 42
      const run = getRun(runId);
      expect(run).toBeDefined();
      expect(run!.exit_code).toBe(42);
      expect(run!.finished_at).toBeGreaterThan(0);

      // Allocation reached COMPLETE
      const alloc = queryOne<Allocation>(
        "SELECT * FROM allocations WHERE run_id = ?",
        [runId],
      );
      expect(alloc).toBeDefined();
      expect(alloc!.status).toBe("COMPLETE");

      // Node status: 8 completed, 1 skipped (claim-warm-allocation)
      const nodes = getWorkflowNodes(workflowId);
      const completedNodes = nodes.filter((n) => n.status === "completed");
      const skippedNodes = nodes.filter((n) => n.status === "skipped");
      expect(completedNodes.length).toBe(8);
      expect(skippedNodes.length).toBe(1);
      expect(skippedNodes[0].node_id).toBe("claim-warm-allocation");

      console.log(`[exercise:exit42] PASSED`);
    },
    SINGLE_TEST_TIMEOUT_MS,
  );

  // ─── Test 2: Stderr output ───────────────────────────────────────────────

  test(
    "stderr output: both stdout and stderr logs arrive on WS",
    async () => {
      const { workflowId, runId } = await launchRun(
        baseUrl,
        "echo out && echo err >&2",
      );
      console.log(
        `[exercise:stderr] Launched workflow=${workflowId} run=${runId}`,
      );

      // Collect logs
      const { promise: logDone, messages } = collectLogs(
        server.port!,
        runId,
        SINGLE_TEST_TIMEOUT_MS,
      );

      // Poll until done
      const finalStatus = await pollUntilDone(
        baseUrl,
        workflowId,
        "stderr",
        SINGLE_TEST_TIMEOUT_MS,
      );

      await logDone;

      // Track VM for cleanup
      const vmName = extractVmName(workflowId);
      if (vmName) createdVmNames.push(vmName);

      // --- Assertions ---

      // Workflow completed successfully
      expect(finalStatus.status).toBe("completed");

      // Run exit code is 0
      const run = getRun(runId);
      expect(run).toBeDefined();
      expect(run!.exit_code).toBe(0);

      // Filter log messages (exclude status messages)
      const logMessages = messages.filter(
        (m) => m.stream && m.data !== undefined,
      );

      console.log(
        `[exercise:stderr] Log messages: ${logMessages.length}`,
        logMessages.map((m) => `[${m.stream}] ${m.data?.trim()}`),
      );

      // Stdout contains "out"
      const stdoutChunks = logMessages.filter((m) => m.stream === "stdout");
      const allStdout = stdoutChunks.map((m) => m.data ?? "").join("");
      expect(allStdout).toContain("out");

      // At least one stderr chunk contains "err"
      const stderrChunks = logMessages.filter((m) => m.stream === "stderr");
      const allStderr = stderrChunks.map((m) => m.data ?? "").join("");
      expect(stderrChunks.length).toBeGreaterThan(0);
      expect(allStderr).toContain("err");

      console.log(`[exercise:stderr] PASSED`);
    },
    SINGLE_TEST_TIMEOUT_MS,
  );

  // ─── Test 3: Large stdout (500 lines) ────────────────────────────────────

  test(
    "large stdout: seq 1 500 produces all 500 lines",
    async () => {
      const { workflowId, runId } = await launchRun(baseUrl, "seq 1 500");
      console.log(
        `[exercise:large] Launched workflow=${workflowId} run=${runId}`,
      );

      // Collect logs
      const { promise: logDone, messages } = collectLogs(
        server.port!,
        runId,
        SINGLE_TEST_TIMEOUT_MS,
      );

      // Poll until done
      const finalStatus = await pollUntilDone(
        baseUrl,
        workflowId,
        "large",
        SINGLE_TEST_TIMEOUT_MS,
      );

      await logDone;

      // Track VM for cleanup
      const vmName = extractVmName(workflowId);
      if (vmName) createdVmNames.push(vmName);

      // --- Assertions ---

      // Workflow completed successfully
      expect(finalStatus.status).toBe("completed");

      // Run exit code is 0
      const run = getRun(runId);
      expect(run).toBeDefined();
      expect(run!.exit_code).toBe(0);

      // Concatenate all stdout log chunks
      const logChunks = messages.filter(
        (m) => m.stream && m.data !== undefined,
      );
      const allOutput = logChunks.map((m) => m.data ?? "").join("");
      const lines = allOutput
        .split("\n")
        .map((l) => l.trim())
        .filter((l) => l.length > 0);

      console.log(
        `[exercise:large] Total log chunks: ${logChunks.length}, total lines: ${lines.length}`,
      );

      // Verify all 500 numbers are present
      const numbersFound = new Set<number>();
      for (const line of lines) {
        const num = parseInt(line, 10);
        if (!isNaN(num) && num >= 1 && num <= 500) {
          numbersFound.add(num);
        }
      }

      console.log(
        `[exercise:large] Unique numbers found: ${numbersFound.size}/500`,
      );

      // All 500 numbers must be present
      expect(numbersFound.size).toBe(500);

      // Spot-check specific values
      expect(numbersFound.has(1)).toBe(true);
      expect(numbersFound.has(250)).toBe(true);
      expect(numbersFound.has(500)).toBe(true);

      console.log(`[exercise:large] PASSED`);
    },
    SINGLE_TEST_TIMEOUT_MS,
  );

  // ─── Test 4: Multi-line ordered output ───────────────────────────────────

  test(
    "multi-line ordered output: lines arrive in correct order",
    async () => {
      const { workflowId, runId } = await launchRun(
        baseUrl,
        "echo line1 && sleep 0.1 && echo line2 && sleep 0.1 && echo line3",
      );
      console.log(
        `[exercise:order] Launched workflow=${workflowId} run=${runId}`,
      );

      // Collect logs
      const { promise: logDone, messages } = collectLogs(
        server.port!,
        runId,
        SINGLE_TEST_TIMEOUT_MS,
      );

      // Poll until done
      const finalStatus = await pollUntilDone(
        baseUrl,
        workflowId,
        "order",
        SINGLE_TEST_TIMEOUT_MS,
      );

      await logDone;

      // Track VM for cleanup
      const vmName = extractVmName(workflowId);
      if (vmName) createdVmNames.push(vmName);

      // --- Assertions ---

      // Workflow completed successfully
      expect(finalStatus.status).toBe("completed");

      // Run exit code is 0
      const run = getRun(runId);
      expect(run).toBeDefined();
      expect(run!.exit_code).toBe(0);

      // Concatenate all log data in order received
      const logChunks = messages.filter(
        (m) => m.stream && m.data !== undefined,
      );
      const allOutput = logChunks.map((m) => m.data ?? "").join("");

      console.log(
        `[exercise:order] Output: ${JSON.stringify(allOutput.trim())}`,
      );

      // All three lines must be present
      expect(allOutput).toContain("line1");
      expect(allOutput).toContain("line2");
      expect(allOutput).toContain("line3");

      // They must arrive in order: line1 before line2 before line3
      const idx1 = allOutput.indexOf("line1");
      const idx2 = allOutput.indexOf("line2");
      const idx3 = allOutput.indexOf("line3");

      expect(idx1).toBeGreaterThanOrEqual(0);
      expect(idx2).toBeGreaterThan(idx1);
      expect(idx3).toBeGreaterThan(idx2);

      console.log(`[exercise:order] PASSED`);
    },
    SINGLE_TEST_TIMEOUT_MS,
  );
});
