// api/routes/admin.ts — tenant/user CRUD, API key management, usage summary, budget enforcement

import { Elysia, t } from "elysia";
import { Type } from "@sinclair/typebox";
import {
  queryOne,
  queryMany,
  getTenant,
  createTenant,
  updateTenant,
  listTenants,
  createUser,
  updateUser,
  removeUser,
  listTenantUsers,
  countTenantUsers,
  getUserByApiKeyId,
  getUser,
  type Tenant,
  type User,
} from "../../material/db";
import { getDatabase } from "../../material/db/init";
import { randomBytes, createHash } from "node:crypto";
import {
  SkyREPLError,
  httpStatusForError,
  errorToApiError,
} from "@skyrepl/contracts";
import { getAuthContext, checkPermission } from "../middleware/auth";

/** Elysia params schema: coerces URL :id param from string to number */
const IdParams = t.Object({ id: t.Numeric() });

// ─── Request Body Schemas ─────────────────────────────────────────────────────

const CreateApiKeySchema = Type.Object({
  name: Type.String({ minLength: 1 }),
  role: Type.Optional(
    Type.Union([Type.Literal("admin"), Type.Literal("member"), Type.Literal("viewer")], { default: "member" })
  ),
});

const CreateTenantSchema = Type.Object({
  name: Type.String({ minLength: 1 }),
  seat_cap: Type.Optional(Type.Integer({ minimum: 1 })),
  budget_usd: Type.Optional(Type.Number({ minimum: 0 })),
});

const PatchTenantSchema = Type.Object({
  name: Type.Optional(Type.String({ minLength: 1 })),
  seat_cap: Type.Optional(Type.Integer({ minimum: 1 })),
  budget_usd: Type.Optional(Type.Union([Type.Number({ minimum: 0 }), Type.Null()])),
});

const CreateUserSchema = Type.Object({
  email: Type.String({ minLength: 1 }),
  role: Type.Optional(
    Type.Union([Type.Literal("admin"), Type.Literal("member"), Type.Literal("viewer")], { default: "member" })
  ),
  display_name: Type.Optional(Type.String()),
  budget_usd: Type.Optional(Type.Number({ minimum: 0 })),
});

const PatchUserSchema = Type.Object({
  display_name: Type.Optional(Type.String()),
  role: Type.Optional(Type.String()),
  budget_usd: Type.Optional(Type.Union([Type.Number({ minimum: 0 }), Type.Null()])),
});

// =============================================================================
// Budget Enforcement (TENANT-03)
// =============================================================================

/**
 * Check budget limits for a tenant and user before allowing a launch.
 * Returns an error message if budget exceeded, null if OK.
 *
 * Design: lifecycle reports usage, shell/proxy enforces budget.
 * For single-machine mode, this runs inline. In managed mode,
 * this check moves to the shell proxy layer.
 */
export function checkBudget(tenantId: number, apiKeyId: number): string | null {
  const tenant = getTenant(tenantId);
  if (!tenant) return null; // No tenant = no budget enforcement

  // Check team budget
  if (tenant.budget_usd !== null) {
    const totalUsage = queryOne<{ total_cost: number | null }>(
      "SELECT SUM(estimated_cost_usd) as total_cost FROM usage_records WHERE tenant_id = ?",
      [tenantId],
    );
    const totalCost = totalUsage?.total_cost ?? 0;
    if (totalCost >= tenant.budget_usd) {
      return `Team budget exceeded: $${totalCost.toFixed(2)} / $${tenant.budget_usd} limit`;
    }
  }

  // Check per-user budget via api_key_id → users.api_key_id
  const user = getUserByApiKeyId(apiKeyId);
  if (user?.budget_usd !== null && user?.budget_usd !== undefined) {
    const userUsage = queryOne<{ total_cost: number | null }>(
      `SELECT SUM(ur.estimated_cost_usd) as total_cost
       FROM usage_records ur
       JOIN allocations a ON ur.allocation_id = a.id
       WHERE ur.tenant_id = ? AND a.claimed_by = ?`,
      [tenantId, apiKeyId],
    );
    const userCost = userUsage?.total_cost ?? 0;
    if (userCost >= user.budget_usd) {
      return `User budget exceeded: $${userCost.toFixed(2)} / $${user.budget_usd} limit`;
    }
  }

  return null;
}

// =============================================================================
// Admin Route Registration
// =============================================================================

export function registerAdminRoutes(app: Elysia<any>): void {
  // ─── API Key Management (AUTH-04) ─────────────────────────────────

  app.get("/v1/keys", ({ request, set }) => {
    const auth = getAuthContext(request);
    if (!auth || !checkPermission(auth, "manage_keys")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }
    const rows = queryMany<{
      id: number; name: string; role: string;
      created_at: number; last_used_at: number | null;
      expires_at: number | null; revoked_at: number | null;
    }>(
      `SELECT id, name, role, created_at, last_used_at, expires_at, revoked_at
       FROM api_keys WHERE tenant_id = ? ORDER BY created_at DESC`,
      [auth.tenantId]
    );
    return { data: rows };
  });

  app.post("/v1/keys", ({ body, set, request }) => {
    const auth = getAuthContext(request);
    if (!auth || !checkPermission(auth, "manage_keys")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }

    const name = body.name.trim();
    const role = body.role ?? "member";

    const BASE62 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    const bytes = randomBytes(32);
    let suffix = "";
    for (let i = 0; i < 32; i++) suffix += BASE62[bytes[i] % 62];
    const rawKey = `srk-${suffix}`;

    const KEY_EXPIRY_MS = 90 * 24 * 60 * 60 * 1000;
    const expiresAt = Date.now() + KEY_EXPIRY_MS;
    const keyHash = createHash("sha256").update(rawKey).digest("hex");
    const now = Date.now();

    const db = getDatabase();
    const result = db.prepare(
      `INSERT INTO api_keys (key_hash, name, tenant_id, role, permissions, expires_at, created_at)
       VALUES (?, ?, ?, ?, 'all', ?, ?)`
    ).run(keyHash, name, auth.tenantId, role, expiresAt, now);

    set.status = 201;
    return {
      data: {
        id: Number(result.lastInsertRowid),
        name,
        role,
        raw_key: rawKey,
        expires_at: expiresAt,
        created_at: now,
      },
    };
  }, { body: CreateApiKeySchema });

  app.delete("/v1/keys/:id", ({ params, set, request }) => {
    const auth = getAuthContext(request);
    if (!auth || !checkPermission(auth, "manage_keys")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }

    const keyId = params.id;
    const row = queryOne<{ id: number; tenant_id: number }>(
      "SELECT id, tenant_id FROM api_keys WHERE id = ?",
      [keyId]
    );
    if (!row || row.tenant_id !== auth.tenantId) {
      set.status = 404;
      return { error: { code: "NOT_FOUND", message: `Key ${keyId} not found`, category: "not_found" } };
    }

    const db = getDatabase();
    db.prepare("UPDATE api_keys SET revoked_at = ? WHERE id = ?").run(Date.now(), keyId);
    return { data: { id: keyId, revoked: true } };
  }, { params: IdParams });

  // ─── Tenant Management (TENANT-01) ──────────────────────────────────

  app.get("/v1/tenants", ({ request, set }) => {
    const auth = getAuthContext(request);
    if (!auth || !checkPermission(auth, "manage_team")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }
    const tenant = getTenant(auth.tenantId);
    return { data: tenant ? [tenant] : [] };
  });

  app.get("/v1/tenants/:id", ({ params, request, set }) => {
    const auth = getAuthContext(request);
    if (!auth) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Missing API key", category: "auth" } };
    }
    const tenantId = params.id;
    // Users can view their own tenant; admins can view any
    if (auth.tenantId !== tenantId && !checkPermission(auth, "manage_team")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Insufficient permissions", category: "auth" } };
    }
    const tenant = getTenant(tenantId);
    if (!tenant) {
      set.status = 404;
      return { error: { code: "RESOURCE_NOT_FOUND", message: `Tenant ${tenantId} not found`, category: "not_found" } };
    }
    const userCount = countTenantUsers(tenantId);
    return { data: { ...tenant, user_count: userCount } };
  }, { params: IdParams });

  app.post("/v1/tenants", ({ body, request, set }) => {
    const auth = getAuthContext(request);
    if (!auth || !checkPermission(auth, "manage_team")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }
    try {
      const tenant = createTenant({
        name: body.name.trim(),
        seatCap: typeof body.seat_cap === "number" ? body.seat_cap : undefined,
        budgetUsd: typeof body.budget_usd === "number" ? body.budget_usd : undefined,
      });
      set.status = 201;
      return { data: tenant };
    } catch (err) {
      if (err instanceof SkyREPLError) {
        set.status = httpStatusForError(err);
        return errorToApiError(err);
      }
      throw err;
    }
  }, { body: CreateTenantSchema });

  app.patch("/v1/tenants/:id", ({ params, body, request, set }) => {
    const auth = getAuthContext(request);
    if (!auth || !checkPermission(auth, "manage_team")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }
    const tenantId = params.id;
    try {
      const tenant = updateTenant(tenantId, {
        name: typeof body.name === "string" ? body.name.trim() : undefined,
        seatCap: typeof body.seat_cap === "number" ? body.seat_cap : undefined,
        budgetUsd: body.budget_usd === null ? null : typeof body.budget_usd === "number" ? body.budget_usd : undefined,
      });
      return { data: tenant };
    } catch (err) {
      if (err instanceof SkyREPLError) {
        set.status = httpStatusForError(err);
        return errorToApiError(err);
      }
      throw err;
    }
  }, { params: IdParams, body: PatchTenantSchema });

  // ─── User Management (TENANT-01) ────────────────────────────────────

  app.get("/v1/tenants/:id/users", ({ params, request, set }) => {
    const auth = getAuthContext(request);
    if (!auth) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Missing API key", category: "auth" } };
    }
    const tenantId = params.id;
    if (auth.tenantId !== tenantId && !checkPermission(auth, "manage_team")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Insufficient permissions", category: "auth" } };
    }
    return { data: listTenantUsers(tenantId) };
  }, { params: IdParams });

  app.post("/v1/tenants/:id/users", ({ params, body, request, set }) => {
    const auth = getAuthContext(request);
    if (!auth || !checkPermission(auth, "manage_team")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }
    const tenantId = params.id;
    try {
      const user = createUser({
        tenantId,
        email: body.email.trim(),
        displayName: typeof body.display_name === "string" ? body.display_name : undefined,
        role: body.role ?? "member",
        budgetUsd: typeof body.budget_usd === "number" ? body.budget_usd : undefined,
      });
      set.status = 201;
      return { data: user };
    } catch (err) {
      if (err instanceof SkyREPLError) {
        set.status = httpStatusForError(err);
        return errorToApiError(err);
      }
      throw err;
    }
  }, { params: IdParams, body: CreateUserSchema });

  app.patch("/v1/users/:id", ({ params, body, request, set }) => {
    const auth = getAuthContext(request);
    if (!auth || !checkPermission(auth, "manage_team")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }
    const userId = params.id;
    const user = getUser(userId);
    if (!user || user.tenant_id !== auth.tenantId) {
      set.status = 404;
      return { error: { code: "RESOURCE_NOT_FOUND", message: `User ${userId} not found`, category: "not_found" } };
    }
    try {
      const updated = updateUser(userId, {
        displayName: typeof body.display_name === "string" ? body.display_name : undefined,
        role: typeof body.role === "string" ? body.role : undefined,
        budgetUsd: body.budget_usd === null ? null : typeof body.budget_usd === "number" ? body.budget_usd : undefined,
      });
      return { data: updated };
    } catch (err) {
      if (err instanceof SkyREPLError) {
        set.status = httpStatusForError(err);
        return errorToApiError(err);
      }
      throw err;
    }
  }, { params: IdParams, body: PatchUserSchema });

  app.delete("/v1/users/:id", ({ params, request, set }) => {
    const auth = getAuthContext(request);
    if (!auth || !checkPermission(auth, "manage_team")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }
    const userId = params.id;
    const user = getUser(userId);
    if (!user || user.tenant_id !== auth.tenantId) {
      set.status = 404;
      return { error: { code: "RESOURCE_NOT_FOUND", message: `User ${userId} not found`, category: "not_found" } };
    }
    try {
      removeUser(userId);
      return { data: { id: userId, removed: true } };
    } catch (err) {
      if (err instanceof SkyREPLError) {
        set.status = httpStatusForError(err);
        return errorToApiError(err);
      }
      throw err;
    }
  }, { params: IdParams });

  // ─── Usage Summary (TENANT-03: budget context) ─────────────────────

  app.get("/v1/usage", ({ query, request, set }) => {
    const auth = getAuthContext(request);
    if (!auth) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Missing API key", category: "auth" } };
    }
    const tenantId = auth.tenantId;
    const tenant = getTenant(tenantId);
    const users = listTenantUsers(tenantId);

    // Aggregate usage from usage_records
    const totalUsage = queryOne<{ total_cost: number | null }>(
      "SELECT SUM(estimated_cost_usd) as total_cost FROM usage_records WHERE tenant_id = ?",
      [tenantId],
    );

    // Per-user usage via api_key_id → allocations.claimed_by
    const userUsage: Record<number, number> = {};
    for (const user of users) {
      if (user.api_key_id) {
        const usage = queryOne<{ total_cost: number | null }>(
          `SELECT SUM(ur.estimated_cost_usd) as total_cost
           FROM usage_records ur
           JOIN allocations a ON ur.allocation_id = a.id
           WHERE ur.tenant_id = ? AND a.claimed_by = ?`,
          [tenantId, user.api_key_id],
        );
        userUsage[user.id] = usage?.total_cost ?? 0;
      }
    }

    return {
      data: {
        tenant_id: tenantId,
        tenant_name: tenant?.name ?? "unknown",
        tenant_budget_usd: tenant?.budget_usd ?? null,
        total_cost_usd: totalUsage?.total_cost ?? 0,
        users: users.map(u => ({
          id: u.id,
          email: u.email,
          role: u.role,
          budget_usd: u.budget_usd,
          used_usd: userUsage[u.id] ?? 0,
        })),
      },
    };
  });
}
