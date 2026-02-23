// db/tenants.ts - Tenant and user CRUD (#TENANT-01)

import { getDatabase, queryOne, queryMany, execute } from "./helpers";
import { SkyREPLError } from "@skyrepl/contracts";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { createHash, randomBytes } from "crypto";
import { homedir } from "os";
import { join } from "path";

// =============================================================================
// Types
// =============================================================================

export interface Tenant {
  id: number;
  name: string;
  seat_cap: number;
  budget_usd: number | null;
  created_at: number;
  updated_at: number;
}

export interface User {
  id: number;
  tenant_id: number;
  email: string;
  display_name: string;
  role: string;
  budget_usd: number | null;
  created_at: number;
  updated_at: number;
}

// =============================================================================
// Tenant CRUD
// =============================================================================

export function getTenant(id: number): Tenant | null {
  return queryOne<Tenant>("SELECT * FROM tenants WHERE id = ?", [id]);
}

export function getTenantByName(name: string): Tenant | null {
  return queryOne<Tenant>("SELECT * FROM tenants WHERE name = ?", [name]);
}

export function createTenant(opts: {
  name: string;
  seatCap?: number;
  budgetUsd?: number | null;
}): Tenant {
  const existing = getTenantByName(opts.name);
  if (existing) {
    throw new SkyREPLError(
      "DUPLICATE_RESOURCE",
      `Tenant "${opts.name}" already exists`,
      "validation",
    );
  }

  const now = Date.now();
  const db = getDatabase();
  const result = db.prepare(
    `INSERT INTO tenants (name, seat_cap, budget_usd, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?)`
  ).run(opts.name, opts.seatCap ?? 5, opts.budgetUsd ?? null, now, now);

  return getTenant(Number(result.lastInsertRowid))!;
}

export function updateTenant(id: number, updates: {
  name?: string;
  seatCap?: number;
  budgetUsd?: number | null;
}): Tenant {
  const tenant = getTenant(id);
  if (!tenant) {
    throw new SkyREPLError("RESOURCE_NOT_FOUND", `Tenant ${id} not found`, "not_found");
  }

  const sets: string[] = ["updated_at = ?"];
  const values: unknown[] = [Date.now()];

  if (updates.name !== undefined) {
    sets.push("name = ?");
    values.push(updates.name);
  }
  if (updates.seatCap !== undefined) {
    sets.push("seat_cap = ?");
    values.push(updates.seatCap);
  }
  if (updates.budgetUsd !== undefined) {
    sets.push("budget_usd = ?");
    values.push(updates.budgetUsd);
  }

  values.push(id);
  execute(`UPDATE tenants SET ${sets.join(", ")} WHERE id = ?`, values);
  return getTenant(id)!;
}

export function listTenants(): Tenant[] {
  return queryMany<Tenant>("SELECT * FROM tenants ORDER BY created_at ASC");
}

// =============================================================================
// User CRUD
// =============================================================================

export function getUser(id: number): User | null {
  return queryOne<User>("SELECT * FROM users WHERE id = ?", [id]);
}

export function getUserByEmail(tenantId: number, email: string): User | null {
  return queryOne<User>(
    "SELECT * FROM users WHERE tenant_id = ? AND email = ?",
    [tenantId, email],
  );
}

export function getUserByApiKeyId(apiKeyId: number): User | null {
  // Reverse lookup: api_keys.user_id → users.id
  const row = queryOne<{ user_id: number | null }>(
    "SELECT user_id FROM api_keys WHERE id = ?",
    [apiKeyId],
  );
  if (!row?.user_id) return null;
  return getUser(row.user_id);
}

export function createUser(opts: {
  tenantId: number;
  email: string;
  displayName?: string;
  role?: string;
  budgetUsd?: number | null;
}): User {
  // Verify tenant exists
  const tenant = getTenant(opts.tenantId);
  if (!tenant) {
    throw new SkyREPLError("RESOURCE_NOT_FOUND", `Tenant ${opts.tenantId} not found`, "not_found");
  }

  // Check seat cap
  const userCount = countTenantUsers(opts.tenantId);
  if (userCount >= tenant.seat_cap) {
    throw new SkyREPLError(
      "SEAT_LIMIT_EXCEEDED",
      `Tenant "${tenant.name}" has reached the ${tenant.seat_cap}-seat limit`,
      "validation",
    );
  }

  // Check for duplicate email within tenant
  const existing = getUserByEmail(opts.tenantId, opts.email);
  if (existing) {
    throw new SkyREPLError(
      "DUPLICATE_RESOURCE",
      `User "${opts.email}" already exists in this tenant`,
      "validation",
    );
  }

  const now = Date.now();
  const db = getDatabase();
  const result = db.prepare(
    `INSERT INTO users (tenant_id, email, display_name, role, budget_usd, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)`
  ).run(
    opts.tenantId,
    opts.email,
    opts.displayName ?? "",
    opts.role ?? "member",
    opts.budgetUsd ?? null,
    now,
    now,
  );

  return getUser(Number(result.lastInsertRowid))!;
}

export function updateUser(id: number, updates: {
  displayName?: string;
  role?: string;
  budgetUsd?: number | null;
}): User {
  const user = getUser(id);
  if (!user) {
    throw new SkyREPLError("RESOURCE_NOT_FOUND", `User ${id} not found`, "not_found");
  }

  const sets: string[] = ["updated_at = ?"];
  const values: unknown[] = [Date.now()];

  if (updates.displayName !== undefined) {
    sets.push("display_name = ?");
    values.push(updates.displayName);
  }
  if (updates.role !== undefined) {
    sets.push("role = ?");
    values.push(updates.role);
  }
  if (updates.budgetUsd !== undefined) {
    sets.push("budget_usd = ?");
    values.push(updates.budgetUsd);
  }

  values.push(id);
  execute(`UPDATE users SET ${sets.join(", ")} WHERE id = ?`, values);
  return getUser(id)!;
}

export function removeUser(id: number): void {
  const user = getUser(id);
  if (!user) {
    throw new SkyREPLError("RESOURCE_NOT_FOUND", `User ${id} not found`, "not_found");
  }

  // Revoke all API keys linked to this user (reverse FK: api_keys.user_id)
  execute(
    "UPDATE api_keys SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL",
    [Date.now(), id],
  );

  execute("DELETE FROM users WHERE id = ?", [id]);
}

export function listTenantUsers(tenantId: number): User[] {
  return queryMany<User>(
    "SELECT * FROM users WHERE tenant_id = ? ORDER BY created_at ASC",
    [tenantId],
  );
}

export function countTenantUsers(tenantId: number): number {
  const row = queryOne<{ count: number }>(
    "SELECT COUNT(*) as count FROM users WHERE tenant_id = ?",
    [tenantId],
  );
  return row?.count ?? 0;
}

// =============================================================================
// Seed Admin API Key
// =============================================================================

/**
 * Ensure an admin API key exists. On first startup, generates srk-<base62-32>,
 * writes raw key to ~/.repl/api-key, and inserts the SHA-256 hash into api_keys.
 * Idempotent — subsequent calls read the existing key file and INSERT OR IGNORE.
 */
export function seedApiKey(): void {
  const keyDir = join(homedir(), ".repl");
  const keyFile = join(keyDir, "api-key");

  const KEY_EXPIRY_MS = 90 * 24 * 60 * 60 * 1000; // 90 days

  let rawKey: string;
  let expiresAt: number;
  if (existsSync(keyFile)) {
    const contents = readFileSync(keyFile, "utf-8").trim().split("\n");
    rawKey = contents[0].trim();
    // Second line is the expiry timestamp (written by newer versions)
    expiresAt = contents[1] ? parseInt(contents[1].trim(), 10) : Date.now() + KEY_EXPIRY_MS;
  } else {
    // Generate srk-<base62-32>
    const BASE62 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    const bytes = randomBytes(32);
    let suffix = "";
    for (let i = 0; i < 32; i++) {
      suffix += BASE62[bytes[i] % 62];
    }
    rawKey = `srk-${suffix}`;
    expiresAt = Date.now() + KEY_EXPIRY_MS;
    mkdirSync(keyDir, { recursive: true });
    writeFileSync(keyFile, `${rawKey}\n${expiresAt}\n`, { mode: 0o600 });
    console.log("[control] Generated API key → ~/.repl/api-key");
  }

  const hash = createHash("sha256").update(rawKey).digest("hex");
  const db = getDatabase();
  db.prepare(
    `INSERT OR IGNORE INTO api_keys (key_hash, name, tenant_id, role, permissions, expires_at, created_at)
     VALUES (?, 'admin', 1, 'admin', 'all', ?, ?)`
  ).run(hash, expiresAt, Date.now());
}
