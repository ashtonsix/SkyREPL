// api/resource-crud.test.ts — Tests for API-01: L1 Resource CRUD routes
//
// Covers: GET/PATCH/DELETE for instances, runs, allocations, manifests, objects.
// Uses in-process Elysia server (no real network) via app.handle().

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../../../tests/harness";
import { createServer } from "./routes";
import { resetRateLimits } from "./middleware/rate-limit";
import {
  createInstance,
  createRun,
  createAllocation,
  createWorkflow,
  createManifest,
  createBlob,
  createObject,
  addObjectTag,
} from "../material/db";
import { createHash } from "node:crypto";
import { getDatabase } from "../material/db/init";

// =============================================================================
// Test Fixtures
// =============================================================================

function makeAdminKey(tenantId: number): string {
  const rawKey = "srk-testadminkey000000000000000a";
  const hash = createHash("sha256").update(rawKey).digest("hex");
  const db = getDatabase();
  db.prepare(
    `INSERT INTO api_keys (key_hash, name, tenant_id, role, permissions, expires_at, created_at)
     VALUES (?, 'admin-key', ?, 'admin', 'all', ?, ?)`
  ).run(hash, tenantId, Date.now() + 86_400_000, Date.now());
  return rawKey;
}

function makeMemberKey(tenantId: number): string {
  const rawKey = "srk-testmemberkey00000000000000a";
  const hash = createHash("sha256").update(rawKey).digest("hex");
  const db = getDatabase();
  db.prepare(
    `INSERT INTO api_keys (key_hash, name, tenant_id, role, permissions, expires_at, created_at)
     VALUES (?, 'member-key', ?, 'member', 'view_resources', ?, ?)`
  ).run(hash, tenantId, Date.now() + 86_400_000, Date.now());
  return rawKey;
}

function makeTenant(name: string): number {
  const db = getDatabase();
  const result = db.prepare(
    `INSERT INTO tenants (name, seat_cap, budget_usd, created_at, updated_at)
     VALUES (?, 5, NULL, ?, ?)`
  ).run(name, Date.now(), Date.now());
  return result.lastInsertRowid as number;
}

function seedWorkflowId(): number {
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

let instanceCounter = 0;
function seedInstance(tenantId: number = 1) {
  return createInstance(
    {
      provider: "orbstack",
      provider_id: `test-vm-${++instanceCounter}`,
      spec: "4vcpu-8gb",
      region: "local",
      ip: null,
      workflow_state: "launch-run:complete",
      workflow_error: null,
      current_manifest_id: null,
      spawn_idempotency_key: null,
      is_spot: 0,
      spot_request_id: null,
      init_checksum: null,
      registration_token_hash: null,
      last_heartbeat: Date.now(),
    },
    tenantId
  );
}

function seedRun(tenantId: number = 1, state: string = "launch-run:completed") {
  return createRun(
    {
      command: "echo hello",
      workdir: "/workspace",
      max_duration_ms: 60000,
      workflow_state: state,
      workflow_error: null,
      current_manifest_id: null,
      exit_code: 0,
      init_checksum: null,
      create_snapshot: 0,
      spot_interrupted: 0,
      started_at: null,
      finished_at: Date.now(),
    },
    tenantId
  );
}

function seedBlob(tenantId: number = 1) {
  return createBlob(
    {
      bucket: "artifacts",
      checksum: `chk-${Math.random().toString(36).slice(2)}`,
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("test"),
      size_bytes: 4,
      last_referenced_at: Date.now(),
    },
    tenantId
  );
}

// =============================================================================
// Test Setup
// =============================================================================

let cleanup: () => Promise<void>;
let app: ReturnType<typeof createServer>;
let tenantId: number;
let adminKey: string;
let memberKey: string;

beforeEach(() => {
  cleanup = setupTest();
  resetRateLimits();
  tenantId = makeTenant("test-team");
  adminKey = makeAdminKey(tenantId);
  memberKey = makeMemberKey(tenantId);
  app = createServer({ port: 0, corsOrigins: [], maxBodySize: 1_048_576 });
});

afterEach(() => cleanup());

// Helper: fire a request against the in-process Elysia app
function req(
  method: string,
  path: string,
  opts: { key?: string; body?: unknown } = {}
): Promise<Response> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (opts.key) headers["Authorization"] = `Bearer ${opts.key}`;
  return app.handle(
    new Request(`http://localhost${path}`, {
      method,
      headers,
      body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
    })
  );
}

// =============================================================================
// GET /v1/manifests
// =============================================================================

describe("GET /v1/manifests", () => {
  test("returns empty list when no manifests", async () => {
    const res = await req("GET", "/v1/manifests", { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data).toEqual([]);
    expect(body.pagination).toBeDefined();
    expect(body.pagination.has_more).toBe(false);
  });

  test("returns tenant-scoped manifests", async () => {
    const wfId = seedWorkflowId();
    createManifest(wfId, { tenant_id: tenantId });
    createManifest(wfId, { tenant_id: tenantId });

    // Manifest for a different tenant
    const otherTenantId = makeTenant("other-team");
    createManifest(wfId, { tenant_id: otherTenantId });

    const res = await req("GET", "/v1/manifests", { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data.length).toBe(2);
    expect(body.data.every((m: any) => m.tenant_id === tenantId)).toBe(true);
  });

  test("paginates with limit: returns has_more when more records exist", async () => {
    const wfId = seedWorkflowId();
    // Use DB to insert manifests with distinct created_at to avoid cursor tie-breaking issues
    const db = getDatabase();
    const now = Date.now();
    for (let i = 0; i < 3; i++) {
      db.prepare(
        `INSERT INTO manifests (workflow_id, tenant_id, status, default_cleanup_priority, retention_ms, created_at, released_at, expires_at, updated_at)
         VALUES (?, ?, 'DRAFT', 50, NULL, ?, NULL, NULL, ?)`
      ).run(wfId, tenantId, now - (3 - i) * 1000, now);
    }

    const res1 = await req("GET", "/v1/manifests?limit=2", { key: adminKey });
    const body1 = await res1.json() as any;
    expect(body1.data.length).toBe(2);
    expect(body1.pagination.has_more).toBe(true);
    expect(body1.pagination.next_cursor).toBeTruthy();

    const res2 = await req("GET", `/v1/manifests?limit=2&cursor=${body1.pagination.next_cursor}`, { key: adminKey });
    const body2 = await res2.json() as any;
    expect(body2.data.length).toBe(1);
    expect(body2.pagination.has_more).toBe(false);
  });
});

// =============================================================================
// GET /v1/manifests/:id
// =============================================================================

describe("GET /v1/manifests/:id", () => {
  test("returns manifest with resources", async () => {
    const wfId = seedWorkflowId();
    const manifest = createManifest(wfId, { tenant_id: tenantId });

    const res = await req("GET", `/v1/manifests/${manifest.id}`, { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data.id).toBe(manifest.id);
    expect(body.data.resources).toBeDefined();
    expect(Array.isArray(body.data.resources)).toBe(true);
  });

  test("404 for unknown manifest", async () => {
    const res = await req("GET", "/v1/manifests/99999", { key: adminKey });
    expect(res.status).toBe(404);
  });

  test("404 for manifest belonging to another tenant", async () => {
    const wfId = seedWorkflowId();
    const otherTenantId = makeTenant("other-team");
    const manifest = createManifest(wfId, { tenant_id: otherTenantId });

    const res = await req("GET", `/v1/manifests/${manifest.id}`, { key: adminKey });
    expect(res.status).toBe(404);
  });
});

// =============================================================================
// GET /v1/objects
// =============================================================================

describe("GET /v1/objects", () => {
  test("returns empty list when no objects", async () => {
    const res = await req("GET", "/v1/objects", { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data).toEqual([]);
  });

  test("returns tenant-scoped objects", async () => {
    const blob = seedBlob(tenantId);
    createObject(
      { type: "artifact", blob_id: blob.id, provider: null, provider_object_id: null, metadata_json: null, expires_at: null, current_manifest_id: null, accessed_at: null, updated_at: null },
      tenantId
    );

    const otherTenantId = makeTenant("other-team");
    const blob2 = seedBlob(otherTenantId);
    createObject(
      { type: "artifact", blob_id: blob2.id, provider: null, provider_object_id: null, metadata_json: null, expires_at: null, current_manifest_id: null, accessed_at: null, updated_at: null },
      otherTenantId
    );

    const res = await req("GET", "/v1/objects", { key: adminKey });
    const body = await res.json() as any;
    expect(body.data.length).toBe(1);
    expect(body.data[0].tenant_id).toBe(tenantId);
  });

  test("filters by type", async () => {
    const blob1 = seedBlob(tenantId);
    const blob2 = seedBlob(tenantId);
    createObject(
      { type: "artifact", blob_id: blob1.id, provider: null, provider_object_id: null, metadata_json: null, expires_at: null, current_manifest_id: null, accessed_at: null, updated_at: null },
      tenantId
    );
    createObject(
      { type: "log", blob_id: blob2.id, provider: null, provider_object_id: null, metadata_json: null, expires_at: null, current_manifest_id: null, accessed_at: null, updated_at: null },
      tenantId
    );

    const res = await req("GET", "/v1/objects?type=artifact", { key: adminKey });
    const body = await res.json() as any;
    expect(body.data.length).toBe(1);
    expect(body.data[0].type).toBe("artifact");
  });
});

// =============================================================================
// GET /v1/objects/:id
// =============================================================================

describe("GET /v1/objects/:id", () => {
  test("returns object detail", async () => {
    const blob = seedBlob(tenantId);
    const obj = createObject(
      { type: "log", blob_id: blob.id, provider: null, provider_object_id: null, metadata_json: null, expires_at: null, current_manifest_id: null, accessed_at: null, updated_at: null },
      tenantId
    );

    const res = await req("GET", `/v1/objects/${obj.id}`, { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data.id).toBe(obj.id);
    expect(body.data.type).toBe("log");
  });

  test("404 for unknown object", async () => {
    const res = await req("GET", "/v1/objects/99999", { key: adminKey });
    expect(res.status).toBe(404);
  });

  test("404 for object belonging to another tenant", async () => {
    const otherTenantId = makeTenant("other-team");
    const blob = seedBlob(otherTenantId);
    const obj = createObject(
      { type: "log", blob_id: blob.id, provider: null, provider_object_id: null, metadata_json: null, expires_at: null, current_manifest_id: null, accessed_at: null, updated_at: null },
      otherTenantId
    );

    const res = await req("GET", `/v1/objects/${obj.id}`, { key: adminKey });
    expect(res.status).toBe(404);
  });
});

// =============================================================================
// PATCH /v1/instances/:id
// =============================================================================

describe("PATCH /v1/instances/:id", () => {
  test("admin can update allowed fields", async () => {
    const inst = seedInstance(tenantId);

    const res = await req("PATCH", `/v1/instances/${inst.id}`, {
      key: adminKey,
      body: { workflow_state: "launch-run:failed", workflow_error: "test-error" },
    });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data.workflow_state).toBe("launch-run:failed");
    expect(body.data.workflow_error).toBe("test-error");
  });

  test("member is forbidden (403)", async () => {
    const inst = seedInstance(tenantId);

    const res = await req("PATCH", `/v1/instances/${inst.id}`, {
      key: memberKey,
      body: { workflow_state: "launch-run:failed" },
    });
    expect(res.status).toBe(403);
  });

  test("404 for instance belonging to another tenant", async () => {
    const otherTenantId = makeTenant("other-team");
    const inst = seedInstance(otherTenantId);

    const res = await req("PATCH", `/v1/instances/${inst.id}`, {
      key: adminKey,
      body: { workflow_state: "launch-run:failed" },
    });
    expect(res.status).toBe(404);
  });

  test("400 when no updatable fields provided", async () => {
    const inst = seedInstance(tenantId);

    const res = await req("PATCH", `/v1/instances/${inst.id}`, {
      key: adminKey,
      body: { provider: "aws" }, // not an allowed field
    });
    expect(res.status).toBe(400);
    const body = await res.json() as any;
    expect(body.error.code).toBe("INVALID_INPUT");
  });

  test("401 without API key", async () => {
    const inst = seedInstance(tenantId);

    const res = await req("PATCH", `/v1/instances/${inst.id}`, {
      body: { workflow_state: "launch-run:failed" },
    });
    expect(res.status).toBe(401);
  });
});

// =============================================================================
// DELETE /v1/instances/:id
// =============================================================================

describe("DELETE /v1/instances/:id", () => {
  test("admin can delete an instance with no active allocations", async () => {
    const inst = seedInstance(tenantId);

    const res = await req("DELETE", `/v1/instances/${inst.id}`, { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data.deleted).toBe(true);
  });

  test("409 when instance has active allocations", async () => {
    const inst = seedInstance(tenantId);
    createAllocation(
      {
        run_id: null,
        instance_id: inst.id,
        status: "AVAILABLE",
        current_manifest_id: null,
        user: "ubuntu",
        workdir: "/workspace",
        debug_hold_until: null,
        completed_at: null,
      },
      tenantId
    );

    const res = await req("DELETE", `/v1/instances/${inst.id}`, { key: adminKey });
    expect(res.status).toBe(409);
    const body = await res.json() as any;
    // ConflictError uses INVALID_STATE_TRANSITION as default code
    expect(body.error.code).toBe("INVALID_STATE_TRANSITION");
  });

  test("member is forbidden (403)", async () => {
    const inst = seedInstance(tenantId);

    const res = await req("DELETE", `/v1/instances/${inst.id}`, { key: memberKey });
    expect(res.status).toBe(403);
  });

  test("404 for instance belonging to another tenant", async () => {
    const otherTenantId = makeTenant("other-team");
    const inst = seedInstance(otherTenantId);

    const res = await req("DELETE", `/v1/instances/${inst.id}`, { key: adminKey });
    expect(res.status).toBe(404);
  });
});

// =============================================================================
// PATCH /v1/runs/:id
// =============================================================================

describe("PATCH /v1/runs/:id", () => {
  test("admin can update allowed fields", async () => {
    const run = seedRun(tenantId, "launch-run:running");
    const now = Date.now();

    const res = await req("PATCH", `/v1/runs/${run.id}`, {
      key: adminKey,
      body: { workflow_state: "launch-run:completed", exit_code: 0, finished_at: now },
    });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data.workflow_state).toBe("launch-run:completed");
    expect(body.data.exit_code).toBe(0);
  });

  test("member is forbidden (403)", async () => {
    const run = seedRun(tenantId);

    const res = await req("PATCH", `/v1/runs/${run.id}`, {
      key: memberKey,
      body: { workflow_state: "launch-run:failed" },
    });
    expect(res.status).toBe(403);
  });

  test("tenant isolation: 404 for run in another tenant", async () => {
    const otherTenantId = makeTenant("other-team");
    const run = seedRun(otherTenantId);

    const res = await req("PATCH", `/v1/runs/${run.id}`, {
      key: adminKey,
      body: { workflow_state: "launch-run:failed" },
    });
    expect(res.status).toBe(404);
  });

  test("400 when no updatable fields provided", async () => {
    const run = seedRun(tenantId);

    const res = await req("PATCH", `/v1/runs/${run.id}`, {
      key: adminKey,
      body: { command: "echo hi" }, // not an allowed field
    });
    expect(res.status).toBe(400);
  });

  test("404 for unknown run", async () => {
    const res = await req("PATCH", "/v1/runs/99999", {
      key: adminKey,
      body: { workflow_state: "launch-run:failed" },
    });
    expect(res.status).toBe(404);
  });
});

// =============================================================================
// DELETE /v1/runs/:id
// =============================================================================

describe("DELETE /v1/runs/:id", () => {
  test("admin can delete a terminal run", async () => {
    const run = seedRun(tenantId, "launch-run:completed");

    const res = await req("DELETE", `/v1/runs/${run.id}`, { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data.deleted).toBe(true);
  });

  test("409 for active (non-terminal) run", async () => {
    const run = seedRun(tenantId, "launch-run:pending");

    const res = await req("DELETE", `/v1/runs/${run.id}`, { key: adminKey });
    expect(res.status).toBe(409);
    const body = await res.json() as any;
    // ConflictError uses INVALID_STATE_TRANSITION as default code
    expect(body.error.code).toBe("INVALID_STATE_TRANSITION");
  });

  test("member is forbidden (403)", async () => {
    const run = seedRun(tenantId, "launch-run:completed");

    const res = await req("DELETE", `/v1/runs/${run.id}`, { key: memberKey });
    expect(res.status).toBe(403);
  });

  test("tenant isolation: 404 for run in another tenant", async () => {
    const otherTenantId = makeTenant("other-team");
    const run = seedRun(otherTenantId, "launch-run:completed");

    const res = await req("DELETE", `/v1/runs/${run.id}`, { key: adminKey });
    expect(res.status).toBe(404);
  });
});

// =============================================================================
// GET /v1/allocations/:id
// =============================================================================

describe("GET /v1/allocations/:id", () => {
  test("returns allocation detail", async () => {
    const inst = seedInstance(tenantId);
    const alloc = createAllocation(
      {
        run_id: null,
        instance_id: inst.id,
        status: "AVAILABLE",
        current_manifest_id: null,
        user: "ubuntu",
        workdir: "/workspace",
        debug_hold_until: null,
        completed_at: null,
      },
      tenantId
    );

    const res = await req("GET", `/v1/allocations/${alloc.id}`, { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data.id).toBe(alloc.id);
    expect(body.data.status).toBe("AVAILABLE");
    expect(body.data.instance_id).toBe(inst.id);
  });

  test("404 for unknown allocation", async () => {
    const res = await req("GET", "/v1/allocations/99999", { key: adminKey });
    expect(res.status).toBe(404);
  });

  test("tenant isolation: 404 for allocation in another tenant", async () => {
    const otherTenantId = makeTenant("other-team");
    const inst = seedInstance(otherTenantId);
    const alloc = createAllocation(
      {
        run_id: null,
        instance_id: inst.id,
        status: "AVAILABLE",
        current_manifest_id: null,
        user: "ubuntu",
        workdir: "/workspace",
        debug_hold_until: null,
        completed_at: null,
      },
      otherTenantId
    );

    const res = await req("GET", `/v1/allocations/${alloc.id}`, { key: adminKey });
    expect(res.status).toBe(404);
  });

  test("member can read allocation detail", async () => {
    const inst = seedInstance(tenantId);
    const alloc = createAllocation(
      {
        run_id: null,
        instance_id: inst.id,
        status: "AVAILABLE",
        current_manifest_id: null,
        user: "ubuntu",
        workdir: "/workspace",
        debug_hold_until: null,
        completed_at: null,
      },
      tenantId
    );

    const res = await req("GET", `/v1/allocations/${alloc.id}`, { key: memberKey });
    expect(res.status).toBe(200);
  });
});

// =============================================================================
// PATCH /v1/allocations/:id
// =============================================================================

describe("PATCH /v1/allocations/:id", () => {
  test("admin can update status", async () => {
    const inst = seedInstance(tenantId);
    const alloc = createAllocation(
      {
        run_id: null,
        instance_id: inst.id,
        status: "AVAILABLE",
        current_manifest_id: null,
        user: "ubuntu",
        workdir: "/workspace",
        debug_hold_until: null,
        completed_at: null,
      },
      tenantId
    );

    const res = await req("PATCH", `/v1/allocations/${alloc.id}`, {
      key: adminKey,
      body: { status: "COMPLETE", completed_at: Date.now() },
    });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data.status).toBe("COMPLETE");
    expect(body.data.completed_at).not.toBeNull();
  });

  test("member is forbidden (403)", async () => {
    const inst = seedInstance(tenantId);
    const alloc = createAllocation(
      {
        run_id: null,
        instance_id: inst.id,
        status: "AVAILABLE",
        current_manifest_id: null,
        user: "ubuntu",
        workdir: "/workspace",
        debug_hold_until: null,
        completed_at: null,
      },
      tenantId
    );

    const res = await req("PATCH", `/v1/allocations/${alloc.id}`, {
      key: memberKey,
      body: { status: "COMPLETE" },
    });
    expect(res.status).toBe(403);
  });

  test("tenant isolation: 404 for allocation in another tenant", async () => {
    const otherTenantId = makeTenant("other-team");
    const inst = seedInstance(otherTenantId);
    const alloc = createAllocation(
      {
        run_id: null,
        instance_id: inst.id,
        status: "AVAILABLE",
        current_manifest_id: null,
        user: "ubuntu",
        workdir: "/workspace",
        debug_hold_until: null,
        completed_at: null,
      },
      otherTenantId
    );

    const res = await req("PATCH", `/v1/allocations/${alloc.id}`, {
      key: adminKey,
      body: { status: "COMPLETE" },
    });
    expect(res.status).toBe(404);
  });

  test("400 when no updatable fields provided", async () => {
    const inst = seedInstance(tenantId);
    const alloc = createAllocation(
      {
        run_id: null,
        instance_id: inst.id,
        status: "AVAILABLE",
        current_manifest_id: null,
        user: "ubuntu",
        workdir: "/workspace",
        debug_hold_until: null,
        completed_at: null,
      },
      tenantId
    );

    const res = await req("PATCH", `/v1/allocations/${alloc.id}`, {
      key: adminKey,
      body: { instance_id: 999 }, // not an allowed field
    });
    expect(res.status).toBe(400);
  });
});
