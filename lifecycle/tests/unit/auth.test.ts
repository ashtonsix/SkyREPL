// tests/unit/auth.test.ts - Authentication Tests

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import crypto from "crypto";
import {
  verifyInstanceToken,
  extractToken,
} from "../../control/src/api/middleware/auth";
import {
  createInstance,
  updateInstance,
} from "../../control/src/material/db";

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest();
});

afterEach(() => cleanup());

describe("Authentication Middleware", () => {

  describe("verifyInstanceToken", () => {
    test("returns false for nonexistent instance", () => {
      const result = verifyInstanceToken(999, "any-token");
      expect(result).toBe(false);
    });

    test("createInstance persists registration_token_hash (FND-08 regression)", () => {
      const rawToken = crypto.randomBytes(16).toString("hex");
      const tokenHash = crypto.createHash("sha256").update(rawToken).digest("hex");

      const instance = createInstance({
        provider: "orbstack",
        provider_id: "test-hash-persist",
        spec: "small",
        region: "local",
        ip: null,
        workflow_state: "launch-run:provisioning",
        workflow_error: null,
        current_manifest_id: null,
        spawn_idempotency_key: null,
        is_spot: 0,
        spot_request_id: null,
        init_checksum: null,
        registration_token_hash: tokenHash,
        last_heartbeat: Date.now(),
        provider_metadata: null,
        display_name: null,
      });

      // Hash should be persisted directly via createInstance (not updateInstance)
      const result = verifyInstanceToken(instance.id, rawToken);
      expect(result).toBe(true);

      // Wrong token should fail
      expect(verifyInstanceToken(instance.id, "wrong")).toBe(false);
    });

    test("returns true for instance with no token hash (backward compatibility)", () => {
      const instance = createInstance({
        provider: "orbstack",
        provider_id: "test-1",
        spec: "small",
        region: "local",
        ip: null,
        workflow_state: "launch-run:provisioning",
        workflow_error: null,
        current_manifest_id: null,
        spawn_idempotency_key: null,
        is_spot: 0,
        spot_request_id: null,
        init_checksum: null,
        registration_token_hash: null,
        last_heartbeat: Date.now(),
        provider_metadata: null,
        display_name: null,
      });

      const result = verifyInstanceToken(instance.id, "any-token");
      expect(result).toBe(true);
    });

    test("returns false for wrong token", () => {
      const rawToken = crypto.randomBytes(16).toString("hex");
      const tokenHash = crypto.createHash("sha256").update(rawToken).digest("hex");

      const instance = createInstance({
        provider: "orbstack",
        provider_id: "test-2",
        spec: "small",
        region: "local",
        ip: null,
        workflow_state: "launch-run:provisioning",
        workflow_error: null,
        current_manifest_id: null,
        spawn_idempotency_key: null,
        is_spot: 0,
        spot_request_id: null,
        init_checksum: null,
        registration_token_hash: null,
        last_heartbeat: Date.now(),
        provider_metadata: null,
        display_name: null,
      });

      updateInstance(instance.id, { registration_token_hash: tokenHash });

      const result = verifyInstanceToken(instance.id, "wrong-token");
      expect(result).toBe(false);
    });

    test("returns true for correct token", () => {
      const rawToken = crypto.randomBytes(16).toString("hex");
      const tokenHash = crypto.createHash("sha256").update(rawToken).digest("hex");

      const instance = createInstance({
        provider: "orbstack",
        provider_id: "test-3",
        spec: "small",
        region: "local",
        ip: null,
        workflow_state: "launch-run:provisioning",
        workflow_error: null,
        current_manifest_id: null,
        spawn_idempotency_key: null,
        is_spot: 0,
        spot_request_id: null,
        init_checksum: null,
        registration_token_hash: null,
        last_heartbeat: Date.now(),
        provider_metadata: null,
        display_name: null,
      });

      updateInstance(instance.id, { registration_token_hash: tokenHash });

      const result = verifyInstanceToken(instance.id, rawToken);
      expect(result).toBe(true);
    });

    test("handles string instance_id", () => {
      const rawToken = crypto.randomBytes(16).toString("hex");
      const tokenHash = crypto.createHash("sha256").update(rawToken).digest("hex");

      const instance = createInstance({
        provider: "orbstack",
        provider_id: "test-4",
        spec: "small",
        region: "local",
        ip: null,
        workflow_state: "launch-run:provisioning",
        workflow_error: null,
        current_manifest_id: null,
        spawn_idempotency_key: null,
        is_spot: 0,
        spot_request_id: null,
        init_checksum: null,
        registration_token_hash: null,
        last_heartbeat: Date.now(),
        provider_metadata: null,
        display_name: null,
      });

      updateInstance(instance.id, { registration_token_hash: tokenHash });

      const result = verifyInstanceToken(String(instance.id), rawToken);
      expect(result).toBe(true);
    });
  });

  describe("extractToken", () => {
    test("extracts token from Bearer header", () => {
      const request = new Request("http://example.com", {
        headers: {
          Authorization: "Bearer test-token-123",
        },
      });

      const token = extractToken(request);
      expect(token).toBe("test-token-123");
    });

    test("extracts token from query param", () => {
      const request = new Request("http://example.com");
      const query = { token: "query-token-456" };

      const token = extractToken(request, query);
      expect(token).toBe("query-token-456");
    });

    test("prefers Authorization header over query param", () => {
      const request = new Request("http://example.com", {
        headers: {
          Authorization: "Bearer header-token",
        },
      });
      const query = { token: "query-token" };

      const token = extractToken(request, query);
      expect(token).toBe("header-token");
    });

    test("returns null when neither present", () => {
      const request = new Request("http://example.com");

      const token = extractToken(request);
      expect(token).toBe(null);
    });

    test("returns null for malformed Authorization header", () => {
      const request = new Request("http://example.com", {
        headers: {
          Authorization: "NotBearer test-token",
        },
      });

      const token = extractToken(request);
      expect(token).toBe(null);
    });
  });
});

// =============================================================================
// API Key Auth Tests (#AUTH-01)
// =============================================================================

import {
  validateApiKey,
  setAuthContext,
  getAuthContext,
  checkPermission,
} from "../../control/src/api/middleware/auth";
import type { Action } from "../../control/src/api/middleware/auth";
import {
  getDatabase,
} from "../../control/src/material/db/init";
import {
  createRun,
  createAllocation,
  createWorkflow,
  queryMany,
  type Run,
  type Instance,
  type Allocation,
  type Workflow,
} from "../../control/src/material/db";
import {
  buildRunFilters,
  buildInstanceFilters,
  buildAllocationFilters,
  buildWorkflowFilters,
} from "../../control/src/api/routes";

function seedKey(rawKey: string, opts: { tenant_id?: number; role?: string; expires_at?: number | null; revoked_at?: number | null } = {}): number {
  const db = getDatabase();
  const hash = crypto.createHash("sha256").update(rawKey).digest("hex");
  const result = db.prepare(
    `INSERT INTO api_keys (key_hash, name, tenant_id, role, permissions, created_at, expires_at, revoked_at)
     VALUES (?, 'test', ?, ?, 'all', ?, ?, ?)`
  ).run(hash, opts.tenant_id ?? 1, opts.role ?? "admin", Date.now(), opts.expires_at ?? null, opts.revoked_at ?? null);
  return Number(result.lastInsertRowid);
}

describe("API Key Authentication", () => {
  test("valid key returns correct AuthContext", () => {
    const rawKey = "srk-valid-test-key-001";
    seedKey(rawKey, { tenant_id: 1, role: "admin" });

    const ctx = validateApiKey(rawKey);
    expect(ctx).not.toBeNull();
    expect(ctx!.tenantId).toBe(1);
    expect(ctx!.role).toBe("admin");
    expect(ctx!.userId).toBeGreaterThan(0);
  });

  test("invalid key returns null", () => {
    const ctx = validateApiKey("srk-does-not-exist");
    expect(ctx).toBeNull();
  });

  test("expired key returns null", () => {
    const rawKey = "srk-expired-key-001";
    seedKey(rawKey, { expires_at: Date.now() - 60_000 }); // expired 1 min ago

    const ctx = validateApiKey(rawKey);
    expect(ctx).toBeNull();
  });

  test("non-expired key returns context", () => {
    const rawKey = "srk-future-expiry-001";
    seedKey(rawKey, { expires_at: Date.now() + 86_400_000 }); // expires tomorrow

    const ctx = validateApiKey(rawKey);
    expect(ctx).not.toBeNull();
    expect(ctx!.tenantId).toBe(1);
  });

  test("revoked key returns null", () => {
    const rawKey = "srk-revoked-key-001";
    seedKey(rawKey, { revoked_at: Date.now() - 60_000 }); // revoked 1 min ago

    const ctx = validateApiKey(rawKey);
    expect(ctx).toBeNull();
  });

  test("updates last_used_at on successful validation", () => {
    const rawKey = "srk-last-used-test-001";
    const keyId = seedKey(rawKey);

    const db = getDatabase();
    const before = db.prepare("SELECT last_used_at FROM api_keys WHERE id = ?").get(keyId) as { last_used_at: number | null };
    expect(before.last_used_at).toBeNull();

    validateApiKey(rawKey);

    const after = db.prepare("SELECT last_used_at FROM api_keys WHERE id = ?").get(keyId) as { last_used_at: number | null };
    expect(after.last_used_at).not.toBeNull();
  });

  test("setAuthContext and getAuthContext round-trip", () => {
    const request = new Request("http://example.com");
    const ctx = { tenantId: 42, userId: 7, keyId: 0, role: "member" as const };

    expect(getAuthContext(request)).toBeNull();
    setAuthContext(request, ctx);
    expect(getAuthContext(request)).toEqual(ctx);
  });
});

// =============================================================================
// Role-Based Permission Tests (#AUTH-03)
// =============================================================================

describe("Role Permissions", () => {
  const actions: Action[] = [
    "launch_run", "terminate_instance", "cancel_workflow",
    "view_resources", "view_usage", "manage_keys", "manage_orphans",
  ];

  test("admin has all permissions", () => {
    const ctx = { tenantId: 1, userId: 1, keyId: 0, role: "admin" as const };
    for (const action of actions) {
      expect(checkPermission(ctx, action)).toBe(true);
    }
  });

  test("member has write + view_resources but not manage_*", () => {
    const ctx = { tenantId: 1, userId: 1, keyId: 0, role: "member" as const };
    expect(checkPermission(ctx, "launch_run")).toBe(true);
    expect(checkPermission(ctx, "terminate_instance")).toBe(true);
    expect(checkPermission(ctx, "cancel_workflow")).toBe(true);
    expect(checkPermission(ctx, "view_resources")).toBe(true);
    expect(checkPermission(ctx, "view_usage")).toBe(false);
    expect(checkPermission(ctx, "manage_keys")).toBe(false);
    expect(checkPermission(ctx, "manage_orphans")).toBe(false);
  });

  test("viewer has view_resources only", () => {
    const ctx = { tenantId: 1, userId: 1, keyId: 0, role: "viewer" as const };
    expect(checkPermission(ctx, "view_resources")).toBe(true);
    expect(checkPermission(ctx, "launch_run")).toBe(false);
    expect(checkPermission(ctx, "terminate_instance")).toBe(false);
    expect(checkPermission(ctx, "manage_keys")).toBe(false);
  });
});

// =============================================================================
// Cross-Tenant Isolation Tests (#AUTH-02)
// =============================================================================

describe("Cross-Tenant Isolation", () => {
  function makeInstance(tenantId: number, suffix: string): Instance {
    return createInstance({
      provider: "mock",
      provider_id: `iso-${tenantId}-${suffix}`,
      spec: "small",
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
      provider_metadata: null,
      display_name: null,
    }, tenantId);
  }

  function makeRun(tenantId: number, suffix: string): Run {
    return createRun({
      command: `echo ${suffix}`,
      workdir: "/workspace",
      max_duration_ms: 60_000,
      workflow_state: "launch-run:complete",
      workflow_error: null,
      current_manifest_id: null,
      exit_code: 0,
      init_checksum: null,
      create_snapshot: 0,
      spot_interrupted: 0,
      started_at: Date.now(),
      finished_at: Date.now(),
    }, tenantId);
  }

  test("filter builders scope runs by tenant_id", () => {
    makeRun(1, "t1-a");
    makeRun(1, "t1-b");
    makeRun(2, "t2-a");

    const f1 = buildRunFilters({}, 1);
    const rows1 = queryMany<Run>(`SELECT * FROM runs WHERE 1=1 ${f1.where}`, f1.values);
    expect(rows1.length).toBe(2);
    expect(rows1.every(r => r.tenant_id === 1)).toBe(true);

    const f2 = buildRunFilters({}, 2);
    const rows2 = queryMany<Run>(`SELECT * FROM runs WHERE 1=1 ${f2.where}`, f2.values);
    expect(rows2.length).toBe(1);
    expect(rows2[0].tenant_id).toBe(2);
  });

  test("filter builders scope instances by tenant_id", () => {
    makeInstance(1, "a");
    makeInstance(1, "b");
    makeInstance(2, "a");

    const f1 = buildInstanceFilters({}, 1);
    const rows1 = queryMany<Instance>(`SELECT * FROM instances WHERE 1=1 ${f1.where}`, f1.values);
    expect(rows1.length).toBe(2);

    const f2 = buildInstanceFilters({}, 2);
    const rows2 = queryMany<Instance>(`SELECT * FROM instances WHERE 1=1 ${f2.where}`, f2.values);
    expect(rows2.length).toBe(1);
  });

  test("filter builders scope allocations by tenant_id", () => {
    const inst1 = makeInstance(1, "alloc-1");
    const inst2 = makeInstance(2, "alloc-2");

    createAllocation({
      run_id: null,
      instance_id: inst1.id,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "default",
      workdir: "work_0",
      debug_hold_until: null,
      completed_at: null,
    }, 1);

    createAllocation({
      run_id: null,
      instance_id: inst2.id,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "default",
      workdir: "work_0",
      debug_hold_until: null,
      completed_at: null,
    }, 2);

    const f1 = buildAllocationFilters({}, 1);
    const rows1 = queryMany<Allocation>(`SELECT a.* FROM allocations a WHERE 1=1 ${f1.where}`, f1.values);
    expect(rows1.length).toBe(1);
    expect(rows1[0].tenant_id).toBe(1);

    const f2 = buildAllocationFilters({}, 2);
    const rows2 = queryMany<Allocation>(`SELECT a.* FROM allocations a WHERE 1=1 ${f2.where}`, f2.values);
    expect(rows2.length).toBe(1);
    expect(rows2[0].tenant_id).toBe(2);
  });

  test("filter builders scope workflows by tenant_id", () => {
    const now = Date.now();
    createWorkflow({
      type: "launch-run",
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
      timeout_ms: 60_000,
      timeout_at: now + 60_000,
      started_at: now,
      finished_at: now,
      updated_at: now,
    }, 1);

    createWorkflow({
      type: "launch-run",
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
      timeout_ms: 60_000,
      timeout_at: now + 60_000,
      started_at: now,
      finished_at: now,
      updated_at: now,
    }, 2);

    const f1 = buildWorkflowFilters({}, 1);
    const rows1 = queryMany<Workflow>(`SELECT * FROM workflows WHERE 1=1 ${f1.where}`, f1.values);
    expect(rows1.length).toBe(1);
    expect(rows1[0].tenant_id).toBe(1);
  });

  test("resources created by tenant A invisible to tenant B", () => {
    // Create resources for tenant 1
    makeRun(1, "exclusive-a");
    makeRun(1, "exclusive-b");
    makeInstance(1, "exclusive");

    // Tenant 2 sees nothing
    const rf = buildRunFilters({}, 2);
    const runs2 = queryMany<Run>(`SELECT * FROM runs WHERE 1=1 ${rf.where}`, rf.values);
    expect(runs2.length).toBe(0);

    const iif = buildInstanceFilters({}, 2);
    const insts2 = queryMany<Instance>(`SELECT * FROM instances WHERE 1=1 ${iif.where}`, iif.values);
    expect(insts2.length).toBe(0);

    // Tenant 1 sees everything
    const rf1 = buildRunFilters({}, 1);
    const runs1 = queryMany<Run>(`SELECT * FROM runs WHERE 1=1 ${rf1.where}`, rf1.values);
    expect(runs1.length).toBe(2);
  });

  test("without tenant filter, all records visible (engine internals)", () => {
    makeRun(1, "no-filter-1");
    makeRun(2, "no-filter-2");

    // No tenantId → no filtering (for background sweeps, etc.)
    const f = buildRunFilters({});
    const rows = queryMany<Run>(`SELECT * FROM runs WHERE 1=1 ${f.where}`, f.values);
    expect(rows.length).toBe(2);
  });
});

// =============================================================================
// HTTP-Level Auth Tests (T1-T6: app.handle, no real server)
// =============================================================================

import { createServer } from "../../control/src/api/routes";

describe("HTTP-Level Auth", () => {
  let app: ReturnType<typeof createServer>;

  beforeEach(() => {
    app = createServer({ port: 0, corsOrigins: [], maxBodySize: 1_000_000 });
  });

  // --- T2: Missing/invalid Bearer token → 401 ---

  test("request without Bearer token returns 401", async () => {
    const res = await app.handle(new Request("http://localhost/v1/runs"));
    expect(res.status).toBe(401);
    const body = await res.json() as { error: { code: string } };
    expect(body.error.code).toBe("UNAUTHORIZED");
  });

  test("request with invalid API key returns 401", async () => {
    const res = await app.handle(new Request("http://localhost/v1/runs", {
      headers: { Authorization: "Bearer srk-does-not-exist" },
    }));
    expect(res.status).toBe(401);
  });

  test("request with expired API key returns 401", async () => {
    seedKey("srk-http-expired", { expires_at: Date.now() - 60_000 });
    const res = await app.handle(new Request("http://localhost/v1/runs", {
      headers: { Authorization: "Bearer srk-http-expired" },
    }));
    expect(res.status).toBe(401);
  });

  // --- T6: Auth middleware exemptions ---

  test("/v1/health is public (no auth required)", async () => {
    const res = await app.handle(new Request("http://localhost/v1/health"));
    expect(res.status).toBe(200);
    const body = await res.json() as { status: string };
    expect(body.status).toBe("ok");
  });

  // --- T5: Role enforcement on actual routes ---

  test("viewer gets 403 on POST /v1/workflows/launch-run", async () => {
    seedKey("srk-http-viewer", { role: "viewer" });
    const res = await app.handle(new Request("http://localhost/v1/workflows/launch-run", {
      method: "POST",
      headers: {
        Authorization: "Bearer srk-http-viewer",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ command: "echo hi", spec: "small", provider: "orbstack", region: "local", max_duration_ms: 60000 }),
    }));
    expect(res.status).toBe(403);
    const body = await res.json() as { error: { code: string } };
    expect(body.error.code).toBe("FORBIDDEN");
  });

  test("viewer gets 403 on POST /v1/workflows/terminate-instance", async () => {
    seedKey("srk-http-viewer-term", { role: "viewer" });
    const res = await app.handle(new Request("http://localhost/v1/workflows/terminate-instance", {
      method: "POST",
      headers: {
        Authorization: "Bearer srk-http-viewer-term",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ instance_id: 999 }),
    }));
    expect(res.status).toBe(403);
  });

  // --- T4: Cross-tenant GET returns 404 (not 403) ---

  test("cross-tenant run access returns 404", async () => {
    seedKey("srk-http-t1", { tenant_id: 1 });
    seedKey("srk-http-t2", { tenant_id: 2 });

    const run = createRun({
      command: "echo cross-tenant",
      workdir: "/workspace",
      max_duration_ms: 60_000,
      workflow_state: "launch-run:complete",
      workflow_error: null,
      current_manifest_id: null,
      exit_code: 0,
      init_checksum: null,
      create_snapshot: 0,
      spot_interrupted: 0,
      started_at: Date.now(),
      finished_at: Date.now(),
    }, 1);

    // Tenant 1 can see it
    const res1 = await app.handle(new Request(`http://localhost/v1/runs/${run.id}`, {
      headers: { Authorization: "Bearer srk-http-t1" },
    }));
    expect(res1.status).toBe(200);

    // Tenant 2 gets 404 (not 403)
    const res2 = await app.handle(new Request(`http://localhost/v1/runs/${run.id}`, {
      headers: { Authorization: "Bearer srk-http-t2" },
    }));
    expect(res2.status).toBe(404);
  });

  test("cross-tenant instance access returns 404", async () => {
    seedKey("srk-http-inst-t1", { tenant_id: 1 });
    seedKey("srk-http-inst-t2", { tenant_id: 2 });

    const instance = createInstance({
      provider: "mock",
      provider_id: "http-cross-tenant-1",
      spec: "small",
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
      provider_metadata: null,
      display_name: null,
    }, 1);

    // Tenant 2 gets 404
    const res = await app.handle(new Request(`http://localhost/v1/instances/${instance.id}`, {
      headers: { Authorization: "Bearer srk-http-inst-t2" },
    }));
    expect(res.status).toBe(404);
  });

  test("cross-tenant workflow access returns 404", async () => {
    seedKey("srk-http-wf-t1", { tenant_id: 1 });
    seedKey("srk-http-wf-t2", { tenant_id: 2 });

    const now = Date.now();
    const wf = createWorkflow({
      type: "launch-run",
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
      timeout_ms: 60_000,
      timeout_at: now + 60_000,
      started_at: now,
      finished_at: now,
      updated_at: now,
    }, 1);

    // Tenant 2 gets 404 on GET
    const res = await app.handle(new Request(`http://localhost/v1/workflows/${wf.id}`, {
      headers: { Authorization: "Bearer srk-http-wf-t2" },
    }));
    expect(res.status).toBe(404);
  });

  // --- T1: HTTP routes properly scope list queries by tenant ---

  test("GET /v1/runs scopes by tenant", async () => {
    seedKey("srk-http-list-t1", { tenant_id: 1 });
    seedKey("srk-http-list-t2", { tenant_id: 2 });

    createRun({ command: "echo t1", workdir: "/w", max_duration_ms: 60000, workflow_state: "launch-run:complete", workflow_error: null, current_manifest_id: null, exit_code: 0, init_checksum: null, create_snapshot: 0, spot_interrupted: 0, started_at: Date.now(), finished_at: Date.now() }, 1);
    createRun({ command: "echo t2", workdir: "/w", max_duration_ms: 60000, workflow_state: "launch-run:complete", workflow_error: null, current_manifest_id: null, exit_code: 0, init_checksum: null, create_snapshot: 0, spot_interrupted: 0, started_at: Date.now(), finished_at: Date.now() }, 2);

    const res1 = await app.handle(new Request("http://localhost/v1/runs", {
      headers: { Authorization: "Bearer srk-http-list-t1" },
    }));
    const body1 = await res1.json() as { data: Run[] };
    expect(body1.data.length).toBe(1);
    expect(body1.data[0].tenant_id).toBe(1);

    const res2 = await app.handle(new Request("http://localhost/v1/runs", {
      headers: { Authorization: "Bearer srk-http-list-t2" },
    }));
    const body2 = await res2.json() as { data: Run[] };
    expect(body2.data.length).toBe(1);
    expect(body2.data[0].tenant_id).toBe(2);
  });

  // --- T3: Cancel-workflow tenant isolation (B1 regression test) ---

  test("cross-tenant workflow cancel returns 404", async () => {
    seedKey("srk-http-cancel-t1", { tenant_id: 1 });
    seedKey("srk-http-cancel-t2", { tenant_id: 2 });

    const now = Date.now();
    const wf = createWorkflow({
      type: "launch-run",
      parent_workflow_id: null,
      depth: 0,
      status: "running",
      current_node: "check-budget",
      input_json: "{}",
      output_json: null,
      error_json: null,
      manifest_id: null,
      trace_id: null,
      idempotency_key: null,
      timeout_ms: 60_000,
      timeout_at: now + 60_000,
      started_at: now,
      finished_at: null,
      updated_at: now,
    }, 1);

    // Tenant 2 tries to cancel tenant 1's workflow → 404
    const res = await app.handle(new Request(`http://localhost/v1/workflows/${wf.id}/cancel`, {
      method: "POST",
      headers: {
        Authorization: "Bearer srk-http-cancel-t2",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({}),
    }));
    expect(res.status).toBe(404);
    const body = await res.json() as { error: { code: string } };
    expect(body.error.code).toBe("WORKFLOW_NOT_FOUND");

    // Tenant 1 can cancel their own workflow
    const res1 = await app.handle(new Request(`http://localhost/v1/workflows/${wf.id}/cancel`, {
      method: "POST",
      headers: {
        Authorization: "Bearer srk-http-cancel-t1",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({}),
    }));
    expect(res1.status).toBe(200);
  });
});

// =============================================================================
// E1: Contract Test Suite for Auth (#AUTH-01)
// Parameterized scenarios covering key states × endpoint matrix
// =============================================================================

describe("Auth Contract: key state × endpoint matrix", () => {
  let app: ReturnType<typeof createServer>;

  beforeEach(() => {
    app = createServer({ port: 0, corsOrigins: [], maxBodySize: 1_000_000 });
  });

  type KeyState = "valid" | "expired" | "revoked" | "wrong-tenant" | "missing";

  interface Scenario {
    description: string;
    key: KeyState;
    expectedStatus: number;
  }

  const scenarios: Scenario[] = [
    { description: "valid key",        key: "valid",        expectedStatus: 200 },
    { description: "expired key",      key: "expired",      expectedStatus: 401 },
    { description: "revoked key",      key: "revoked",      expectedStatus: 401 },
    { description: "wrong-tenant key", key: "wrong-tenant", expectedStatus: 200 },
    { description: "missing key",      key: "missing",      expectedStatus: 401 },
  ];

  function seedScenarioKey(scenario: KeyState, suffix: string): string | null {
    const raw = `srk-contract-${scenario}-${suffix}`;
    switch (scenario) {
      case "valid":
        seedKey(raw, { tenant_id: 1 });
        return raw;
      case "expired":
        seedKey(raw, { tenant_id: 1, expires_at: Date.now() - 60_000 });
        return raw;
      case "revoked":
        seedKey(raw, { tenant_id: 1, revoked_at: Date.now() - 60_000 });
        return raw;
      case "wrong-tenant":
        // A second tenant's key — should still authenticate successfully (200),
        // but will only see their own (empty) data.
        seedKey(raw, { tenant_id: 2 });
        return raw;
      case "missing":
        return null; // do not seed; no key is sent
    }
  }

  function authHeader(rawKey: string | null): Record<string, string> {
    if (rawKey === null) return {};
    return { Authorization: `Bearer ${rawKey}` };
  }

  const endpoints: Array<{ label: string; method: string; path: string; body?: string }> = [
    { label: "GET /v1/runs",       method: "GET",  path: "/v1/runs" },
    { label: "GET /v1/instances",  method: "GET",  path: "/v1/instances" },
    {
      label: "POST /v1/workflows/launch-run",
      method: "POST",
      path: "/v1/workflows/launch-run",
      body: JSON.stringify({ command: "echo hi", spec: "small", max_duration_ms: 60000, provider: "mock", region: "local" }),
    },
  ];

  for (const scenario of scenarios) {
    for (const endpoint of endpoints) {
      // wrong-tenant on launch-run returns 400 (no mock provider) / 202 — skip that cell;
      // the auth contract (200 vs 401) is what we care about for launch-run.
      // We only verify the HTTP auth gate, not full workflow execution.
      const expectAuthGate = scenario.expectedStatus !== 200;

      test(`${scenario.description} → ${endpoint.label}`, async () => {
        const rawKey = seedScenarioKey(scenario.key, `${endpoint.method}-${endpoint.path.replace(/\//g, "_")}`);
        const headers: Record<string, string> = {
          ...authHeader(rawKey),
          ...(endpoint.body ? { "Content-Type": "application/json" } : {}),
        };

        const res = await app.handle(
          new Request(`http://localhost${endpoint.path}`, {
            method: endpoint.method,
            headers,
            ...(endpoint.body ? { body: endpoint.body } : {}),
          })
        );

        if (expectAuthGate) {
          expect(res.status).toBe(scenario.expectedStatus);
          const body = await res.json() as { error: { code: string } };
          expect(body.error.code).toBe("UNAUTHORIZED");
        } else {
          // Valid / wrong-tenant: auth gate passes (not 401/403)
          expect(res.status).not.toBe(401);
          expect(res.status).not.toBe(403);
        }
      });
    }
  }
});

// =============================================================================
// E2: Auth + SSH Integration (#AUTH-02)
// Data flow: auth → tenant-scoped allocation query → SSH-eligible fields
// =============================================================================

describe("Auth + SSH Integration", () => {
  let app: ReturnType<typeof createServer>;

  beforeEach(() => {
    app = createServer({ port: 0, corsOrigins: [], maxBodySize: 1_000_000 });
  });

  test("each tenant sees only their own allocations", async () => {
    const keyT1 = "srk-ssh-tenant1-key";
    const keyT2 = "srk-ssh-tenant2-key";
    seedKey(keyT1, { tenant_id: 10 });
    seedKey(keyT2, { tenant_id: 20 });

    // Create one instance + allocation per tenant
    const inst1 = createInstance({
      provider: "mock", provider_id: "ssh-inst-t10",
      spec: "small", region: "local",
      ip: "10.0.0.1",
      workflow_state: "launch-run:active",
      workflow_error: null, current_manifest_id: null,
      spawn_idempotency_key: null, is_spot: 0, spot_request_id: null,
      init_checksum: null, registration_token_hash: null,
      last_heartbeat: Date.now(),
      provider_metadata: null, display_name: null,
    }, 10);

    const inst2 = createInstance({
      provider: "mock", provider_id: "ssh-inst-t20",
      spec: "small", region: "local",
      ip: "10.0.0.2",
      workflow_state: "launch-run:active",
      workflow_error: null, current_manifest_id: null,
      spawn_idempotency_key: null, is_spot: 0, spot_request_id: null,
      init_checksum: null, registration_token_hash: null,
      last_heartbeat: Date.now(),
      provider_metadata: null, display_name: null,
    }, 20);

    createAllocation({
      run_id: null, instance_id: inst1.id,
      status: "ACTIVE", current_manifest_id: null,
      user: "default", workdir: "work_0",
      debug_hold_until: null, completed_at: null,
    }, 10);

    createAllocation({
      run_id: null, instance_id: inst2.id,
      status: "ACTIVE", current_manifest_id: null,
      user: "default", workdir: "work_0",
      debug_hold_until: null, completed_at: null,
    }, 20);

    // Tenant 10 query
    const res10 = await app.handle(new Request("http://localhost/v1/allocations", {
      headers: { Authorization: `Bearer ${keyT1}` },
    }));
    expect(res10.status).toBe(200);
    const body10 = await res10.json() as { data: Array<{ tenant_id: number; instance_ip: string | null; status: string }> };
    expect(body10.data.length).toBe(1);
    expect(body10.data[0].tenant_id).toBe(10);

    // Tenant 20 query
    const res20 = await app.handle(new Request("http://localhost/v1/allocations", {
      headers: { Authorization: `Bearer ${keyT2}` },
    }));
    expect(res20.status).toBe(200);
    const body20 = await res20.json() as { data: Array<{ tenant_id: number; instance_ip: string | null }> };
    expect(body20.data.length).toBe(1);
    expect(body20.data[0].tenant_id).toBe(20);
  });

  test("allocation response includes SSH-relevant fields (instance_ip, workflow_state)", async () => {
    const rawKey = "srk-ssh-fields-check";
    seedKey(rawKey, { tenant_id: 30 });

    const inst = createInstance({
      provider: "mock", provider_id: "ssh-fields-inst",
      spec: "small", region: "local",
      ip: "192.168.1.5",
      workflow_state: "launch-run:active",
      workflow_error: null, current_manifest_id: null,
      spawn_idempotency_key: null, is_spot: 0, spot_request_id: null,
      init_checksum: null, registration_token_hash: null,
      last_heartbeat: Date.now(),
      provider_metadata: null, display_name: null,
    }, 30);

    createAllocation({
      run_id: null, instance_id: inst.id,
      status: "ACTIVE", current_manifest_id: null,
      user: "default", workdir: "work_0",
      debug_hold_until: null, completed_at: null,
    }, 30);

    const res = await app.handle(new Request("http://localhost/v1/allocations", {
      headers: { Authorization: `Bearer ${rawKey}` },
    }));
    expect(res.status).toBe(200);
    const body = await res.json() as { data: Array<Record<string, unknown>> };
    expect(body.data.length).toBe(1);

    const alloc = body.data[0];
    // SSH config generation requires ip (via JOIN) and allocation status
    expect(alloc).toHaveProperty("instance_ip");
    expect(alloc.instance_ip).toBe("192.168.1.5");
    expect(alloc).toHaveProperty("status");
    expect(alloc.status).toBe("ACTIVE");
  });
});

// =============================================================================
// E4: Tenant POST-check Audit (#AUTH-02)
// All GET-by-ID routes return 404 (not 403) for cross-tenant access
// =============================================================================

describe("Tenant Post-check Audit: cross-tenant GET-by-ID returns 404", () => {
  let app: ReturnType<typeof createServer>;

  beforeEach(() => {
    app = createServer({ port: 0, corsOrigins: [], maxBodySize: 1_000_000 });
  });

  // Seed two tenant keys used across all sub-tests
  function setupTenants() {
    const keyA = "srk-audit-tenant-a";
    const keyB = "srk-audit-tenant-b";
    seedKey(keyA, { tenant_id: 100 });
    seedKey(keyB, { tenant_id: 200 });
    return { keyA, keyB };
  }

  test("/v1/runs/:id returns 404 for cross-tenant access", async () => {
    const { keyA, keyB } = setupTenants();

    const run = createRun({
      command: "echo audit", workdir: "/workspace", max_duration_ms: 60_000,
      workflow_state: "launch-run:complete", workflow_error: null,
      current_manifest_id: null, exit_code: 0, init_checksum: null,
      create_snapshot: 0, spot_interrupted: 0,
      started_at: Date.now(), finished_at: Date.now(),
    }, 100);

    // Owner (tenant A) gets 200
    const resA = await app.handle(new Request(`http://localhost/v1/runs/${run.id}`, {
      headers: { Authorization: `Bearer ${keyA}` },
    }));
    expect(resA.status).toBe(200);

    // Non-owner (tenant B) gets 404, not 403
    const resB = await app.handle(new Request(`http://localhost/v1/runs/${run.id}`, {
      headers: { Authorization: `Bearer ${keyB}` },
    }));
    expect(resB.status).toBe(404);
    const body = await resB.json() as { error: { code: string } };
    expect(body.error.code).toBe("RUN_NOT_FOUND");
  });

  test("/v1/instances/:id returns 404 for cross-tenant access", async () => {
    const { keyA, keyB } = setupTenants();

    const inst = createInstance({
      provider: "mock", provider_id: "audit-inst-100",
      spec: "small", region: "local", ip: null,
      workflow_state: "launch-run:complete", workflow_error: null,
      current_manifest_id: null, spawn_idempotency_key: null,
      is_spot: 0, spot_request_id: null, init_checksum: null,
      registration_token_hash: null, last_heartbeat: Date.now(),
      provider_metadata: null, display_name: null,
    }, 100);

    const resA = await app.handle(new Request(`http://localhost/v1/instances/${inst.id}`, {
      headers: { Authorization: `Bearer ${keyA}` },
    }));
    expect(resA.status).toBe(200);

    const resB = await app.handle(new Request(`http://localhost/v1/instances/${inst.id}`, {
      headers: { Authorization: `Bearer ${keyB}` },
    }));
    expect(resB.status).toBe(404);
    const body = await resB.json() as { error: { code: string } };
    expect(body.error.code).toBe("INSTANCE_NOT_FOUND");
  });

  test("/v1/workflows/:id returns 404 for cross-tenant access", async () => {
    const { keyA, keyB } = setupTenants();

    const now = Date.now();
    const wf = createWorkflow({
      type: "launch-run", parent_workflow_id: null, depth: 0,
      status: "completed", current_node: null,
      input_json: "{}", output_json: null, error_json: null,
      manifest_id: null, trace_id: null, idempotency_key: null,
      timeout_ms: 60_000, timeout_at: now + 60_000,
      started_at: now, finished_at: now, updated_at: now,
    }, 100);

    const resA = await app.handle(new Request(`http://localhost/v1/workflows/${wf.id}`, {
      headers: { Authorization: `Bearer ${keyA}` },
    }));
    expect(resA.status).toBe(200);

    const resB = await app.handle(new Request(`http://localhost/v1/workflows/${wf.id}`, {
      headers: { Authorization: `Bearer ${keyB}` },
    }));
    expect(resB.status).toBe(404);
    const body = await resB.json() as { error: { code: string } };
    expect(body.error.code).toBe("WORKFLOW_NOT_FOUND");
  });

  test("/v1/workflows/:id/nodes returns 404 for cross-tenant access", async () => {
    const { keyA, keyB } = setupTenants();

    const now = Date.now();
    const wf = createWorkflow({
      type: "launch-run", parent_workflow_id: null, depth: 0,
      status: "completed", current_node: null,
      input_json: "{}", output_json: null, error_json: null,
      manifest_id: null, trace_id: null, idempotency_key: null,
      timeout_ms: 60_000, timeout_at: now + 60_000,
      started_at: now, finished_at: now, updated_at: now,
    }, 100);

    const resA = await app.handle(new Request(`http://localhost/v1/workflows/${wf.id}/nodes`, {
      headers: { Authorization: `Bearer ${keyA}` },
    }));
    expect(resA.status).toBe(200);

    const resB = await app.handle(new Request(`http://localhost/v1/workflows/${wf.id}/nodes`, {
      headers: { Authorization: `Bearer ${keyB}` },
    }));
    expect(resB.status).toBe(404);
  });

  test("/v1/workflows/:id/status returns 404 for cross-tenant access", async () => {
    const { keyA, keyB } = setupTenants();

    const now = Date.now();
    const wf = createWorkflow({
      type: "launch-run", parent_workflow_id: null, depth: 0,
      status: "completed", current_node: null,
      input_json: "{}", output_json: null, error_json: null,
      manifest_id: null, trace_id: null, idempotency_key: null,
      timeout_ms: 60_000, timeout_at: now + 60_000,
      started_at: now, finished_at: now, updated_at: now,
    }, 100);

    const resA = await app.handle(new Request(`http://localhost/v1/workflows/${wf.id}/status`, {
      headers: { Authorization: `Bearer ${keyA}` },
    }));
    expect(resA.status).toBe(200);

    const resB = await app.handle(new Request(`http://localhost/v1/workflows/${wf.id}/status`, {
      headers: { Authorization: `Bearer ${keyB}` },
    }));
    expect(resB.status).toBe(404);
  });

  test("/v1/keys returns 404 for cross-tenant revoke", async () => {
    const { keyA, keyB } = setupTenants();

    // Create a key for tenant A
    const db = getDatabase();
    const keyHash = crypto.createHash("sha256").update("srk-audit-target-key").digest("hex");
    const insertResult = db.prepare(
      `INSERT INTO api_keys (key_hash, name, tenant_id, role, permissions, created_at) VALUES (?, 'target', 100, 'member', 'all', ?)`
    ).run(keyHash, Date.now());
    const targetKeyId = Number(insertResult.lastInsertRowid);

    // Tenant B cannot revoke tenant A's key
    const resB = await app.handle(new Request(`http://localhost/v1/keys/${targetKeyId}`, {
      method: "DELETE",
      headers: { Authorization: `Bearer ${keyB}` },
    }));
    expect(resB.status).toBe(404);

    // Tenant A can revoke their own key
    const resA = await app.handle(new Request(`http://localhost/v1/keys/${targetKeyId}`, {
      method: "DELETE",
      headers: { Authorization: `Bearer ${keyA}` },
    }));
    expect(resA.status).toBe(200);
  });

  test("/v1/runs/:id/artifacts returns 404 for cross-tenant access", async () => {
    const { keyA, keyB } = setupTenants();

    const run = createRun({
      command: "echo artifacts", workdir: "/workspace", max_duration_ms: 60_000,
      workflow_state: "launch-run:complete", workflow_error: null,
      current_manifest_id: null, exit_code: 0, init_checksum: null,
      create_snapshot: 0, spot_interrupted: 0,
      started_at: Date.now(), finished_at: Date.now(),
    }, 100);

    const resA = await app.handle(new Request(`http://localhost/v1/runs/${run.id}/artifacts`, {
      headers: { Authorization: `Bearer ${keyA}` },
    }));
    expect(resA.status).toBe(200);

    const resB = await app.handle(new Request(`http://localhost/v1/runs/${run.id}/artifacts`, {
      headers: { Authorization: `Bearer ${keyB}` },
    }));
    expect(resB.status).toBe(404);
    const body = await resB.json() as { error: { code: string } };
    expect(body.error.code).toBe("RUN_NOT_FOUND");
  });
});

// =============================================================================
// Key Management API Tests (#AUTH-04)
// =============================================================================

describe("Key Management API (AUTH-04)", () => {
  let app: ReturnType<typeof createServer>;

  beforeEach(() => {
    app = createServer({ port: 0, corsOrigins: [], maxBodySize: 1_000_000 });
  });

  test("POST /v1/keys creates a new key and returns raw_key", async () => {
    seedKey("srk-keymgmt-admin", { tenant_id: 1, role: "admin" });

    const res = await app.handle(new Request("http://localhost/v1/keys", {
      method: "POST",
      headers: {
        Authorization: "Bearer srk-keymgmt-admin",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ name: "ci-bot", role: "member" }),
    }));

    expect(res.status).toBe(201);
    const body = await res.json() as { data: any };
    expect(body.data.name).toBe("ci-bot");
    expect(body.data.role).toBe("member");
    expect(body.data.raw_key).toMatch(/^srk-/);
    expect(body.data.id).toBeGreaterThan(0);
    expect(body.data.expires_at).toBeGreaterThan(Date.now());
  });

  test("created key is usable for authentication", async () => {
    seedKey("srk-keymgmt-admin-2", { tenant_id: 1, role: "admin" });

    const createRes = await app.handle(new Request("http://localhost/v1/keys", {
      method: "POST",
      headers: {
        Authorization: "Bearer srk-keymgmt-admin-2",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ name: "new-key", role: "member" }),
    }));

    const { data } = await createRes.json() as { data: { raw_key: string } };

    // Use the new key to make a request
    const listRes = await app.handle(new Request("http://localhost/v1/runs", {
      headers: { Authorization: `Bearer ${data.raw_key}` },
    }));
    expect(listRes.status).toBe(200);
  });

  test("GET /v1/keys lists keys for the tenant", async () => {
    seedKey("srk-keymgmt-list", { tenant_id: 1, role: "admin" });

    const res = await app.handle(new Request("http://localhost/v1/keys", {
      headers: { Authorization: "Bearer srk-keymgmt-list" },
    }));

    expect(res.status).toBe(200);
    const body = await res.json() as { data: any[] };
    expect(body.data.length).toBeGreaterThanOrEqual(1);
    // Should NOT include raw_key in list response
    for (const key of body.data) {
      expect(key).not.toHaveProperty("raw_key");
      expect(key).not.toHaveProperty("key_hash");
      expect(key).toHaveProperty("id");
      expect(key).toHaveProperty("name");
      expect(key).toHaveProperty("role");
      expect(key).toHaveProperty("created_at");
    }
  });

  test("DELETE /v1/keys/:id revokes a key", async () => {
    seedKey("srk-keymgmt-revoke-admin", { tenant_id: 1, role: "admin" });
    const targetId = seedKey("srk-keymgmt-revoke-target", { tenant_id: 1, role: "member" });

    // Revoke
    const revokeRes = await app.handle(new Request(`http://localhost/v1/keys/${targetId}`, {
      method: "DELETE",
      headers: { Authorization: "Bearer srk-keymgmt-revoke-admin" },
    }));
    expect(revokeRes.status).toBe(200);
    const revokeBody = await revokeRes.json() as { data: { revoked: boolean } };
    expect(revokeBody.data.revoked).toBe(true);

    // Revoked key no longer authenticates
    const ctx = validateApiKey("srk-keymgmt-revoke-target");
    expect(ctx).toBeNull();
  });

  test("member cannot manage keys (403)", async () => {
    seedKey("srk-keymgmt-member", { tenant_id: 1, role: "member" });

    const listRes = await app.handle(new Request("http://localhost/v1/keys", {
      headers: { Authorization: "Bearer srk-keymgmt-member" },
    }));
    expect(listRes.status).toBe(403);

    const createRes = await app.handle(new Request("http://localhost/v1/keys", {
      method: "POST",
      headers: {
        Authorization: "Bearer srk-keymgmt-member",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ name: "test", role: "viewer" }),
    }));
    expect(createRes.status).toBe(403);
  });

  test("viewer cannot manage keys (403)", async () => {
    seedKey("srk-keymgmt-viewer", { tenant_id: 1, role: "viewer" });

    const res = await app.handle(new Request("http://localhost/v1/keys", {
      headers: { Authorization: "Bearer srk-keymgmt-viewer" },
    }));
    expect(res.status).toBe(403);
  });

  test("POST /v1/keys validates required name field", async () => {
    seedKey("srk-keymgmt-validate", { tenant_id: 1, role: "admin" });

    const res = await app.handle(new Request("http://localhost/v1/keys", {
      method: "POST",
      headers: {
        Authorization: "Bearer srk-keymgmt-validate",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ role: "member" }),
    }));
    expect(res.status).toBe(400);
  });

  test("POST /v1/keys validates role enum", async () => {
    seedKey("srk-keymgmt-badrole", { tenant_id: 1, role: "admin" });

    const res = await app.handle(new Request("http://localhost/v1/keys", {
      method: "POST",
      headers: {
        Authorization: "Bearer srk-keymgmt-badrole",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ name: "test", role: "superadmin" }),
    }));
    expect(res.status).toBe(400);
  });

  test("cross-tenant key list isolation", async () => {
    seedKey("srk-keymgmt-iso-t1", { tenant_id: 50, role: "admin" });
    seedKey("srk-keymgmt-iso-t2", { tenant_id: 60, role: "admin" });

    // Each tenant only sees their own keys
    const res1 = await app.handle(new Request("http://localhost/v1/keys", {
      headers: { Authorization: "Bearer srk-keymgmt-iso-t1" },
    }));
    const body1 = await res1.json() as { data: any[] };
    for (const key of body1.data) {
      // All keys should belong to tenant 50 (the seedKey function always creates with the tenant_id)
      const db = getDatabase();
      const row = db.prepare("SELECT tenant_id FROM api_keys WHERE id = ?").get(key.id) as { tenant_id: number };
      expect(row.tenant_id).toBe(50);
    }

    const res2 = await app.handle(new Request("http://localhost/v1/keys", {
      headers: { Authorization: "Bearer srk-keymgmt-iso-t2" },
    }));
    const body2 = await res2.json() as { data: any[] };
    for (const key of body2.data) {
      const db = getDatabase();
      const row = db.prepare("SELECT tenant_id FROM api_keys WHERE id = ?").get(key.id) as { tenant_id: number };
      expect(row.tenant_id).toBe(60);
    }
  });

  test("DELETE /v1/keys/:id returns 404 for nonexistent key", async () => {
    seedKey("srk-keymgmt-404", { tenant_id: 1, role: "admin" });

    const res = await app.handle(new Request("http://localhost/v1/keys/99999", {
      method: "DELETE",
      headers: { Authorization: "Bearer srk-keymgmt-404" },
    }));
    expect(res.status).toBe(404);
  });

  test("default role is member when not specified", async () => {
    seedKey("srk-keymgmt-defaultrole", { tenant_id: 1, role: "admin" });

    const res = await app.handle(new Request("http://localhost/v1/keys", {
      method: "POST",
      headers: {
        Authorization: "Bearer srk-keymgmt-defaultrole",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ name: "default-role-test" }),
    }));
    expect(res.status).toBe(201);
    const body = await res.json() as { data: { role: string } };
    expect(body.data.role).toBe("member");
  });
});
