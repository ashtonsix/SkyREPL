// tests/integration/intent-contract.test.ts - Intent Contract: launch-run
//
// Verifies behavioral contracts for the full launch-run intent workflow (all 9 nodes):
//   - Correct terminal state (completed)
//   - All node outputs match TypeBox schemas in NODE_OUTPUT_SCHEMAS
//   - Correct resource emissions tracked in manifest_resources
//   - Correct output shape (finalize node present and completed)
//   - All 9 nodes accounted for (8 completed + 1 skipped on cold path)
//
// Run: bun test lifecycle/tests/integration/intent-contract.test.ts

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
  getManifestResources,
  type Workflow,
} from "../../control/src/material/db";
import {
  createWorkflowEngine,
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
import {
  registerProvider,
  clearProviderCache,
} from "../../control/src/provider/registry";
import { MockProvider } from "./helpers/mock-provider";
import { seedTestApiKey, testFetch } from "./helpers/test-auth";
import {
  checkNodeOutput,
  checkWorkflowInput,
} from "../../control/src/intent/launch-run.schema";
import type { Server } from "bun";

// =============================================================================
// Test Setup
// =============================================================================

const TEST_TIMEOUT_MS = 15_000;

let tmpDir: string;
let server: Server<unknown>;
let baseUrl: string;
let mockProvider: MockProvider;

// Shared workflow result — launched once in beforeAll, reused across all tests
let sharedWorkflowId: number;
let sharedRunId: number;
let sharedWorkflow: Workflow;

beforeAll(async () => {
  resetEngineShutdown();

  tmpDir = await mkdtemp(join(tmpdir(), "skyrepl-intent-contract-"));
  const dbPath = join(tmpDir, "test.db");

  initDatabase(dbPath);
  runMigrations();
  seedTestApiKey();

  createWorkflowEngine();
  registerLaunchRun();
  setAgentBridge(createRealAgentBridge());

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
  process.env.SKYREPL_CONTROL_PLANE_URL = baseUrl;

  // Launch a single workflow and wait for it to reach terminal state.
  // All contract assertions below share this result.
  mockProvider.nextBehavior = { stdout: ["contract-test-output"] };
  const res = await testFetch(`${baseUrl}/v1/workflows/launch-run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      command: "echo contract-test-output",
      spec: "ubuntu",
      provider: "mock",
      region: "local",
      workdir: "/workspace",
      max_duration_ms: 60_000,
      hold_duration_ms: 0,
      create_snapshot: false,
      files: [],
      artifact_patterns: [],
    }),
  });
  expect(res.status).toBe(202);
  const { workflow_id, run_id } = await res.json();
  sharedWorkflowId = workflow_id;
  sharedRunId = run_id;

  // Wait for terminal
  const deadline = Date.now() + 12_000;
  while (Date.now() < deadline) {
    const wf = getWorkflow(sharedWorkflowId);
    if (wf && ["completed", "failed", "cancelled"].includes(wf.status)) {
      sharedWorkflow = wf;
      break;
    }
    await Bun.sleep(50);
  }

  if (!sharedWorkflow!) {
    sharedWorkflow = getWorkflow(sharedWorkflowId)!;
  }
}, TEST_TIMEOUT_MS);

afterAll(async () => {
  mockProvider?.cleanup();
  clearProviderCache();
  delete process.env.SKYREPL_CONTROL_PLANE_URL;
  if (server) server.stop(true);
  requestEngineShutdown();
  await awaitEngineQuiescence(5_000);
  closeDatabase();
  if (tmpDir) await rm(tmpDir, { recursive: true, force: true });
});

// =============================================================================
// Tests
// =============================================================================

describe("Intent Contract: launch-run", () => {
  test("workflow reaches completed terminal state", () => {
    expect(sharedWorkflow).toBeDefined();
    expect(sharedWorkflow.status).toBe("completed");
  });

  test("all node outputs match TypeBox schemas", () => {
    const nodes = getWorkflowNodes(sharedWorkflowId);
    expect(nodes.length).toBeGreaterThan(0);

    const schemaFailures: string[] = [];

    for (const node of nodes) {
      if (node.status !== "completed" || !node.output_json) continue;

      let output: unknown;
      try {
        output = JSON.parse(node.output_json);
      } catch {
        schemaFailures.push(
          `Node ${node.node_id} (${node.node_type}): output_json is not valid JSON`
        );
        continue;
      }

      const errors = checkNodeOutput(node.node_type, output);
      if (errors !== null) {
        schemaFailures.push(
          `Node ${node.node_id} (${node.node_type}) output schema mismatch: ` +
            JSON.stringify(errors.slice(0, 3))
        );
      }
    }

    if (schemaFailures.length > 0) {
      throw new Error(
        `Schema validation failures:\n${schemaFailures.join("\n")}`
      );
    }
  });

  test("resource emissions: instance and allocation tracked in manifest", () => {
    expect(sharedWorkflow.manifest_id).toBeDefined();
    expect(sharedWorkflow.manifest_id).not.toBeNull();

    const resources = getManifestResources(sharedWorkflow.manifest_id!);
    expect(resources.length).toBeGreaterThan(0);

    const instanceRes = resources.find((r) => r.resource_type === "instance");
    expect(instanceRes).toBeDefined();
    expect(instanceRes!.resource_id).toBeTruthy();

    const allocationRes = resources.find(
      (r) => r.resource_type === "allocation"
    );
    expect(allocationRes).toBeDefined();
    expect(allocationRes!.resource_id).toBeTruthy();
  });

  test("finalize node is present and completed", () => {
    const nodes = getWorkflowNodes(sharedWorkflowId);
    const finalizeNode = nodes.find((n) => n.node_type === "finalize");
    expect(finalizeNode).toBeDefined();
    expect(finalizeNode!.status).toBe("completed");
    expect(finalizeNode!.output_json).not.toBeNull();

    // Verify finalize output has expected shape
    const output = JSON.parse(finalizeNode!.output_json!);
    expect(typeof output.allocationStatus).toBe("string");
    expect(typeof output.runStatus).toBe("string");
    expect(typeof output.manifestSealed).toBe("boolean");
  });

  test("9 nodes accounted for: 8 completed + 1 skipped on cold path", () => {
    const nodes = getWorkflowNodes(sharedWorkflowId);
    expect(nodes.length).toBe(9);

    const completed = nodes.filter((n) => n.status === "completed");
    const skipped = nodes.filter((n) => n.status === "skipped");
    expect(completed.length + skipped.length).toBe(9);

    // Cold path: claim-warm-allocation is skipped (no warm pool available)
    const claimWarm = nodes.find((n) => n.node_id === "claim-warm-allocation");
    expect(claimWarm).toBeDefined();
    expect(claimWarm!.status).toBe("skipped");

    expect(completed.length).toBe(8);
    expect(skipped.length).toBe(1);
  });

  test("expected node types all present", () => {
    const nodes = getWorkflowNodes(sharedWorkflowId);
    const nodeTypes = new Set(nodes.map((n) => n.node_type));

    const expectedNodeTypes = [
      "check-budget",
      "resolve-instance",
      "claim-allocation",
      "spawn-instance",
      "wait-for-boot",
      "create-allocation",
      "start-run",
      "wait-completion",
      "finalize",
    ];

    for (const expected of expectedNodeTypes) {
      expect(nodeTypes.has(expected)).toBe(true);
    }
  });

  test("workflow input conforms to LaunchRunWorkflowInputSchema", () => {
    expect(sharedWorkflow.input_json).not.toBeNull();
    const input = JSON.parse(sharedWorkflow.input_json);
    const errors = checkWorkflowInput("launch-run", input);
    if (errors !== null) {
      throw new Error(
        `Workflow input schema mismatch: ${JSON.stringify(errors.slice(0, 3))}`
      );
    }
  });
});
