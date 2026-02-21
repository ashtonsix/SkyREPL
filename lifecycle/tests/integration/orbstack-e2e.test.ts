// tests/integration/orbstack-e2e.test.ts - Full OrbStack End-to-End Test
//
// Level 2: Requires OrbStack. Spawns a real VM, runs "echo hello",
// verifies logs stream back and workflow completes successfully.
//
// Auto-skips when orbctl is not available.
// Timeout: 120s (VM boot ~15s + agent startup ~10s + command + cleanup)
//
// Run: bun test tests/integration/orbstack-e2e.test.ts

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
      console.log(`[orbstack-e2e] Cleaning up stray VM: ${name}`);
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
// Test Suite
// =============================================================================

const TEST_TIMEOUT_MS = 120_000;

describe.skipIf(!hasOrbctl || !process.env.ORBSTACK_TESTS)("OrbStack E2E: repl run 'echo hello'", () => {
  let tmpDir: string;
  let server: Server<unknown>;
  let baseUrl: string;
  let savedControlPlaneUrl: string | undefined;
  const createdVmNames: string[] = [];

  beforeAll(async () => {
    clearProviderCache(); // Ensure no stale MockProvider from other test files
    cleanupStrayVMs();

    tmpDir = await mkdtemp(join(tmpdir(), "skyrepl-orbstack-e2e-"));
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

    // spawn-instance.ts reads this env var for the bootstrap URL
    savedControlPlaneUrl = process.env.SKYREPL_CONTROL_PLANE_URL;
    process.env.SKYREPL_CONTROL_PLANE_URL = controlPlaneUrl;

    console.log(
      `[orbstack-e2e] Server on port ${server.port}, reachable at ${controlPlaneUrl}`,
    );
  });

  afterAll(async () => {
    // Restore env
    if (savedControlPlaneUrl !== undefined) {
      process.env.SKYREPL_CONTROL_PLANE_URL = savedControlPlaneUrl;
    } else {
      delete process.env.SKYREPL_CONTROL_PLANE_URL;
    }

    // Terminate VMs created during this test
    for (const name of createdVmNames) {
      console.log(`[orbstack-e2e] Cleaning up VM: ${name}`);
      orbctlSync(["delete", "-f", name]);
    }

    if (server) server.stop(true);
    closeDatabase();
    if (tmpDir) await rm(tmpDir, { recursive: true, force: true });
  });

  // ─── Agent Download ────────────────────────────────────────────────────

  test("agent download endpoint serves all 6 Python files", async () => {
    const files = [
      "agent.py",
      "executor.py",
      "heartbeat.py",
      "logs.py",
      "sse.py",
      "http_client.py",
    ];
    for (const f of files) {
      const res = await fetch(`${baseUrl}/v1/agent/download/${f}`);
      expect(res.status).toBe(200);
      expect(res.headers.get("content-type")).toBe("text/x-python");
      const text = await res.text();
      expect(text.length).toBeGreaterThan(100);
      expect(text).toContain("import"); // minimal sanity: it's Python
    }
  });

  test("agent download returns 404 for unknown files", async () => {
    const res = await fetch(`${baseUrl}/v1/agent/download/nope.py`);
    expect(res.status).toBe(404);
  });

  // ─── Full E2E ──────────────────────────────────────────────────────────

  test(
    "echo hello: VM spawns, agent runs command, logs stream back, workflow completes",
    async () => {
      // Step 1: Launch run
      const launchRes = await fetch(`${baseUrl}/v1/workflows/launch-run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          command: "echo hello",
          spec: "ubuntu",
          provider: "orbstack",
          region: "local",
          workdir: "/workspace",
          max_duration_ms: 60_000,
          hold_duration_ms: 0,
          create_snapshot: false,
          files: [],
          artifact_patterns: [],
        }),
      });

      expect(launchRes.status).toBe(202);
      const launch = await launchRes.json();
      const workflowId: number = launch.workflow_id;
      const runId: number = launch.run_id;
      expect(workflowId).toBeGreaterThan(0);
      expect(runId).toBeGreaterThan(0);

      console.log(
        `[orbstack-e2e] Launched workflow=${workflowId} run=${runId}`,
      );

      // Step 2: Collect logs via WebSocket
      const logChunks: string[] = [];
      const wsUrl = `ws://localhost:${server.port}/v1/runs/${runId}/logs`;

      const logDone = new Promise<void>((resolve) => {
        const ws = new WebSocket(wsUrl);
        ws.onmessage = (event) => {
          try {
            const msg = JSON.parse(String(event.data));
            if (msg.stream && msg.data !== undefined) {
              logChunks.push(msg.data);
            }
            if (
              msg.type === "status" &&
              ["completed", "failed", "timeout"].includes(msg.status)
            ) {
              ws.close();
            }
          } catch {}
        };
        ws.onclose = () => resolve();
        ws.onerror = () => resolve();
        // Safety timeout: don't hang forever
        setTimeout(() => {
          try {
            ws.close();
          } catch {}
          resolve();
        }, TEST_TIMEOUT_MS - 10_000);
      });

      // Step 3: Poll workflow status until terminal
      const terminalStates = new Set(["completed", "failed", "cancelled"]);
      const finalStatus = await new Promise<{
        status: string;
        nodes_completed: number;
        nodes_total: number;
        error: unknown;
      }>((resolve) => {
        const poll = async () => {
          try {
            const res = await fetch(
              `${baseUrl}/v1/workflows/${workflowId}/status`,
            );
            const body = await res.json();

            console.log(
              `[orbstack-e2e] Poll: status=${body.status} nodes=${body.nodes_completed}/${body.nodes_total}`,
            );

            if (terminalStates.has(body.status)) {
              resolve(body);
              return;
            }
          } catch (err) {
            console.error(`[orbstack-e2e] Poll error: ${err}`);
          }
          setTimeout(poll, 3_000);
        };
        // First poll after 2s to let the engine start
        setTimeout(poll, 2_000);
      });

      await logDone;

      // Step 4: Track spawned VM for cleanup
      const nodes = getWorkflowNodes(workflowId);
      const spawnNode = nodes.find((n) => n.node_id === "spawn-instance");
      if (spawnNode?.output_json) {
        try {
          const output = JSON.parse(spawnNode.output_json);
          if (output.providerId) {
            createdVmNames.push(output.providerId);
          }
        } catch {}
      }

      // Step 5: Assertions
      console.log(`[orbstack-e2e] Final: ${finalStatus.status}`);
      console.log(`[orbstack-e2e] Logs collected: ${logChunks.length} chunks`);
      console.log(`[orbstack-e2e] Log content: ${logChunks.join("").trim()}`);

      // Workflow completed
      expect(finalStatus.status).toBe("completed");
      // 8 completed + 1 skipped (claim-warm-allocation, cold path)
      expect(finalStatus.nodes_completed).toBe(8);

      // Logs contain "hello"
      const allLogs = logChunks.join("");
      expect(allLogs).toContain("hello");

      // Run exit code is 0
      const run = getRun(runId);
      expect(run).toBeDefined();
      expect(run!.exit_code).toBe(0);
      expect(run!.finished_at).toBeGreaterThan(0);

      // 8 completed + 1 skipped = 9 total
      const completedNodes = nodes.filter((n) => n.status === "completed");
      const skippedNodes = nodes.filter((n) => n.status === "skipped");
      expect(completedNodes.length).toBe(8);
      expect(skippedNodes.length).toBe(1);
      expect(skippedNodes[0].node_id).toBe("claim-warm-allocation");

      // Key nodes have correct outputs
      const checkBudget = nodes.find((n) => n.node_id === "check-budget");
      expect(checkBudget!.status).toBe("completed");

      const spawnOutput = spawnNode?.output_json
        ? JSON.parse(spawnNode.output_json)
        : null;
      expect(spawnOutput).toBeDefined();
      expect(spawnOutput.instanceId).toBeGreaterThan(0);
      expect(spawnOutput.providerId).toMatch(/^repl-/);
    },
    TEST_TIMEOUT_MS,
  );
});
