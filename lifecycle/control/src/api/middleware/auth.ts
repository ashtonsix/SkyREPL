// api/middleware/auth.ts — Authentication & Authorization
//
// Consolidated from: api-key-auth.ts, auth.ts, permissions.ts
// All "who is this request and are they allowed?" concerns live here.

// --- Agent Authentication (bearer tokens for agent endpoints) ---
// Validates bearer tokens for agent endpoints via SHA256 hash comparison.

import crypto from "crypto";
import { createHash } from "node:crypto";
import { getDatabase } from "../../material/db/init";
import { getInstanceRecordRaw } from "../../resource/instance";

/**
 * Verify instance auth token from Authorization header or query param.
 * Returns true if the token is valid or if the instance has no token set (backward compatibility).
 */
export function verifyInstanceToken(instanceId: number | string, token: string): boolean {
  const id = typeof instanceId === "string" ? parseInt(instanceId, 10) : instanceId;
  if (isNaN(id)) return false;

  const instance = getInstanceRecordRaw(id);
  if (!instance) return false;

  // Backward compatibility: if no token hash is set, allow any request
  // This ensures existing tests and instances without tokens continue to work
  if (!instance.registration_token_hash) return true;

  const hash = crypto.createHash("sha256").update(token).digest("hex");
  return hash === instance.registration_token_hash;
}

/**
 * Extract token from request: Authorization header or ?token= query param.
 * SSE fallback: EventSource can't set custom headers, so query param is supported.
 */
export function extractToken(request: Request, query?: Record<string, string>): string | null {
  const authHeader = request.headers.get("authorization");
  if (authHeader?.startsWith("Bearer ")) {
    return authHeader.slice(7);
  }
  // SSE fallback: EventSource can't set headers
  if (query?.token) {
    return query.token;
  }
  return null;
}

// --- API Key Authentication (user-facing routes) ---

export interface AuthContext {
  tenantId: number;
  userId: number;  // users.id (resolved via api_keys.user_id)
  keyId: number;   // api_keys.id (for key-specific operations)
  role: "admin" | "member" | "viewer";
}

/**
 * Validates a raw API key (srk-*) and returns the auth context.
 * Returns null if invalid/expired/revoked.
 */
export function validateApiKey(rawKey: string): AuthContext | null {
  const hash = createHash("sha256").update(rawKey).digest("hex");
  const db = getDatabase();
  const row = db.prepare(
    `SELECT ak.id as key_id, ak.user_id, ak.tenant_id, ak.role FROM api_keys ak
     WHERE ak.key_hash = ? AND ak.revoked_at IS NULL
     AND (ak.expires_at IS NULL OR ak.expires_at > ?)`
  ).get(hash, Date.now()) as { key_id: number; user_id: number | null; tenant_id: number; role: string } | undefined;

  if (!row) return null;

  // Update last_used_at (fire-and-forget, don't block the request)
  db.prepare("UPDATE api_keys SET last_used_at = ? WHERE id = ?").run(Date.now(), row.key_id);

  return {
    tenantId: row.tenant_id,
    userId: row.user_id ?? row.key_id, // Fall back to key_id for keys without a linked user
    keyId: row.key_id,
    role: row.role as AuthContext["role"],
  };
}

// WeakMap keyed by Request object for zero-copy auth context storage
const authContextMap = new WeakMap<Request, AuthContext>();

export function setAuthContext(request: Request, ctx: AuthContext): void {
  authContextMap.set(request, ctx);
}

export function getAuthContext(request: Request): AuthContext | null {
  return authContextMap.get(request) ?? null;
}

// --- Role-Based Permissions ---
//
// Thin RBAC layer: checkPermission(ctx, action) returns true/false.
// Current impl is a static role→action map. Future: swap to policy engine.

export type Action =
  | "launch_run"
  | "terminate_instance"
  | "cancel_workflow"
  | "view_resources"
  | "view_usage"
  | "manage_keys"
  | "manage_orphans"
  | "manage_team"
  | "manage_resources";

const PERMISSIONS: Record<string, readonly Action[]> = {
  admin: [
    "launch_run", "terminate_instance", "cancel_workflow",
    "view_resources", "view_usage", "manage_keys", "manage_orphans",
    "manage_team", "manage_resources",
  ],
  member: [
    "launch_run", "terminate_instance", "cancel_workflow",
    "view_resources",
  ],
  viewer: [
    "view_resources",
  ],
};

const ROLE_SETS = Object.fromEntries(
  Object.entries(PERMISSIONS).map(([role, actions]) => [role, new Set(actions)])
) as Record<string, Set<Action>>;

// resource param reserved for future policy engine (see 033 D2). Currently unused — RBAC is role-only.
export function checkPermission(ctx: AuthContext, action: Action, resource?: unknown): boolean {
  return ROLE_SETS[ctx.role]?.has(action) ?? false;
}
