// api/spot-interrupt.test.ts - Spot Interrupt Endpoint Tests
//
// Tests the /v1/agent/spot-interrupt-start and /v1/agent/spot-interrupt-complete
// endpoints, which the Python SpotMonitor calls when a spot interruption is
// detected on an AWS EC2 spot instance.

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  initDatabase,
  closeDatabase,
  runMigrations,
  createInstance,
  createRun,
  createAllocation,
  queryOne,
  execute,
  type Instance,
} from "../material/db";
import { claimAllocation, activateAllocation } from "../workflow/state-transitions";
import { createServer, registerResourceRoutes, registerOperationRoutes } from "./routes";
import { registerAgentRoutes } from "./agent";
import type { Server } from "bun";

// =============================================================================
// Test Setup
// =============================================================================

let tmpDir: string;
let server: Server<unknown>;
let baseUrl: string;

beforeAll(async () => {
  tmpDir = await mkdtemp(join(tmpdir(), "skyrepl-spot-interrupt-"));
  const dbPath = join(tmpDir, "test.db");

  initDatabase(dbPath);
  runMigrations();

  const app = createServer({
    port: 0,
    corsOrigins: ["*"],
    maxBodySize: 1 * 1024 * 1024,
  });
  registerAgentRoutes(app);

  app.listen({ port: 0, idleTimeout: 0 });
  server = app.server!;
  baseUrl = `http://localhost:${server.port}`;
});

afterAll(async () => {
  if (server) server.stop(true);
  closeDatabase();
  if (tmpDir) await rm(tmpDir, { recursive: true, force: true });
});

// =============================================================================
// Helpers
// =============================================================================

let instanceCounter = 0;

function createSpotInstance(overrides: Partial<Instance> = {}): Instance {
  return createInstance({
    provider: "aws",
    provider_id: `i-spot-${++instanceCounter}`,
    spec: "ml.g4dn.xlarge",
    region: "us-east-1",
    ip: "10.0.0.1",
    workflow_state: "launch-run:provisioning",
    workflow_error: null,
    registration_token_hash: null, // no auth token → unauthenticated access allowed
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 1,
    spot_request_id: `sfr-${instanceCounter}`,
    init_checksum: null,
    last_heartbeat: Date.now(),
    provider_metadata: null,
    display_name: null,
    ...overrides,
  });
}

function post(path: string, body: unknown): Promise<Response> {
  return fetch(`${baseUrl}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

// =============================================================================
// spot-interrupt-start
// =============================================================================

describe("POST /v1/agent/spot-interrupt-start", () => {
  test("returns 400 when instance_id is missing", async () => {
    const res = await post("/v1/agent/spot-interrupt-start", {
      action: "terminate",
    });
    expect(res.status).toBe(400);
    const body = await res.json() as any;
    expect(body.error.code).toBe("INVALID_INPUT");
  });

  test("returns ack for instance with no active allocations", async () => {
    const instance = createSpotInstance();

    const res = await post("/v1/agent/spot-interrupt-start", {
      instance_id: instance.id,
      action: "terminate",
      action_time: new Date(Date.now() + 120_000).toISOString(),
    });

    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.ack).toBe(true);

    // No active allocations on this instance — verify no runs were modified
    // (query via JOIN since runs are linked to instances through allocations)
    const affectedRuns = queryOne<{ count: number }>(
      `SELECT COUNT(*) as count FROM runs r
       JOIN allocations a ON a.run_id = r.id
       WHERE a.instance_id = ? AND r.spot_interrupted = 1`,
      [instance.id]
    );
    expect(affectedRuns!.count).toBe(0);
  });

  test("marks ACTIVE allocations' runs as spot_interrupted", async () => {
    const instance = createSpotInstance();

    // Create a run and active allocation on this instance
    const run = createRun({
      command: "python train.py",
      workdir: "/workspace",
      max_duration_ms: 3_600_000,
      workflow_state: "launch-run:running",
      workflow_error: null,
      current_manifest_id: null,
      exit_code: null,
      init_checksum: null,
      create_snapshot: 0,
      spot_interrupted: 0,
      started_at: Date.now(),
      finished_at: null,
    });

    const alloc = createAllocation({
      instance_id: instance.id,
      run_id: null,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "ubuntu",
      workdir: "/workspace",
      debug_hold_until: null,
      completed_at: null,
    });

    // AVAILABLE → CLAIMED → ACTIVE
    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);

    // Fire spot-interrupt-start
    const res = await post("/v1/agent/spot-interrupt-start", {
      instance_id: instance.id,
      action: "terminate",
      action_time: new Date(Date.now() + 120_000).toISOString(),
    });

    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.ack).toBe(true);

    // Verify the run is marked as spot_interrupted
    const updatedRun = queryOne<{ spot_interrupted: number; workflow_state: string }>(
      "SELECT spot_interrupted, workflow_state FROM runs WHERE id = ?",
      [run.id]
    );
    expect(updatedRun).not.toBeNull();
    expect(updatedRun!.spot_interrupted).toBe(1);
    expect(updatedRun!.workflow_state).toBe("launch-run:spot-interrupted");
  });

  test("marks CLAIMED allocations' runs as spot_interrupted", async () => {
    const instance = createSpotInstance();

    const run = createRun({
      command: "echo test",
      workdir: "/workspace",
      max_duration_ms: 60_000,
      workflow_state: "launch-run:sync",
      workflow_error: null,
      current_manifest_id: null,
      exit_code: null,
      init_checksum: null,
      create_snapshot: 0,
      spot_interrupted: 0,
      started_at: null,
      finished_at: null,
    });

    const alloc = createAllocation({
      instance_id: instance.id,
      run_id: null,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "ubuntu",
      workdir: "/workspace",
      debug_hold_until: null,
      completed_at: null,
    });

    // AVAILABLE → CLAIMED (but not yet ACTIVE — file sync still in progress)
    claimAllocation(alloc.id, run.id);

    const res = await post("/v1/agent/spot-interrupt-start", {
      instance_id: instance.id,
      action: "terminate",
    });

    expect(res.status).toBe(200);

    const updatedRun = queryOne<{ spot_interrupted: number }>(
      "SELECT spot_interrupted FROM runs WHERE id = ?",
      [run.id]
    );
    expect(updatedRun!.spot_interrupted).toBe(1);
  });

  test("is idempotent — second call does not double-write", async () => {
    const instance = createSpotInstance();

    const run = createRun({
      command: "python train.py",
      workdir: "/workspace",
      max_duration_ms: 3_600_000,
      workflow_state: "launch-run:running",
      workflow_error: null,
      current_manifest_id: null,
      exit_code: null,
      init_checksum: null,
      create_snapshot: 0,
      spot_interrupted: 0,
      started_at: Date.now(),
      finished_at: null,
    });

    const alloc = createAllocation({
      instance_id: instance.id,
      run_id: null,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "ubuntu",
      workdir: "/workspace",
      debug_hold_until: null,
      completed_at: null,
    });
    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);

    const payload = { instance_id: instance.id, action: "terminate" };

    // First call
    const res1 = await post("/v1/agent/spot-interrupt-start", payload);
    expect(res1.status).toBe(200);

    // Second call
    const res2 = await post("/v1/agent/spot-interrupt-start", payload);
    expect(res2.status).toBe(200);

    // Still exactly 1 (UPDATE only fires when spot_interrupted = 0)
    const updatedRun = queryOne<{ spot_interrupted: number }>(
      "SELECT spot_interrupted FROM runs WHERE id = ?",
      [run.id]
    );
    expect(updatedRun!.spot_interrupted).toBe(1);
  });

  test("does not affect runs on other instances", async () => {
    const instance1 = createSpotInstance();
    const instance2 = createSpotInstance();

    const run = createRun({
      command: "echo safe",
      workdir: "/workspace",
      max_duration_ms: 60_000,
      workflow_state: "launch-run:running",
      workflow_error: null,
      current_manifest_id: null,
      exit_code: null,
      init_checksum: null,
      create_snapshot: 0,
      spot_interrupted: 0,
      started_at: Date.now(),
      finished_at: null,
    });

    const alloc = createAllocation({
      instance_id: instance2.id, // different instance
      run_id: null,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "ubuntu",
      workdir: "/workspace",
      debug_hold_until: null,
      completed_at: null,
    });
    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);

    // Interrupt instance1 — should not affect run on instance2
    const res = await post("/v1/agent/spot-interrupt-start", {
      instance_id: instance1.id,
      action: "terminate",
    });
    expect(res.status).toBe(200);

    const updatedRun = queryOne<{ spot_interrupted: number }>(
      "SELECT spot_interrupted FROM runs WHERE id = ?",
      [run.id]
    );
    expect(updatedRun!.spot_interrupted).toBe(0); // untouched
  });
});

// =============================================================================
// spot-interrupt-complete
// =============================================================================

describe("POST /v1/agent/spot-interrupt-complete", () => {
  test("returns 400 when instance_id is missing", async () => {
    const res = await post("/v1/agent/spot-interrupt-complete", {});
    expect(res.status).toBe(400);
    const body = await res.json() as any;
    expect(body.error.code).toBe("INVALID_INPUT");
  });

  test("returns ack for valid instance with no active allocations", async () => {
    const instance = createSpotInstance();

    // Count allocations before — should be zero for a fresh instance
    const allocsBefore = queryOne<{ count: number }>(
      "SELECT COUNT(*) as count FROM allocations WHERE instance_id = ?",
      [instance.id]
    );
    expect(allocsBefore!.count).toBe(0);

    const res = await post("/v1/agent/spot-interrupt-complete", {
      instance_id: instance.id,
    });

    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.ack).toBe(true);

    // No side effects: no allocations created or modified
    const allocsAfter = queryOne<{ count: number }>(
      "SELECT COUNT(*) as count FROM allocations WHERE instance_id = ?",
      [instance.id]
    );
    expect(allocsAfter!.count).toBe(0);
  });

  test("is accepted for any valid instance regardless of active runs", async () => {
    const instance = createSpotInstance();

    // Instance with an active run that was already interrupted
    const run = createRun({
      command: "echo done",
      workdir: "/workspace",
      max_duration_ms: 60_000,
      workflow_state: "launch-run:spot-interrupted",
      workflow_error: null,
      current_manifest_id: null,
      exit_code: null,
      init_checksum: null,
      create_snapshot: 0,
      spot_interrupted: 1,
      started_at: Date.now(),
      finished_at: null,
    });

    const alloc = createAllocation({
      instance_id: instance.id,
      run_id: null,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "ubuntu",
      workdir: "/workspace",
      debug_hold_until: null,
      completed_at: null,
    });
    claimAllocation(alloc.id, run.id);

    const res = await post("/v1/agent/spot-interrupt-complete", {
      instance_id: instance.id,
    });

    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.ack).toBe(true);
  });
});
