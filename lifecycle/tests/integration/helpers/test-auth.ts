// test-auth.ts — Shared test authentication helper
//
// Seeds an API key in the test DB and provides helpers for authenticated requests.
// Call seedTestApiKey() in beforeAll after runMigrations().

import { createHash } from "node:crypto";
import { getDatabase } from "../../../control/src/material/db/init";

const TEST_RAW_KEY = "srk-test-key-for-integration-tests-000";

/**
 * Seeds a test API key into the database. Call after runMigrations().
 * Returns the raw key for use in Authorization headers.
 */
export function seedTestApiKey(): string {
  const db = getDatabase();
  const hash = createHash("sha256").update(TEST_RAW_KEY).digest("hex");
  db.prepare(
    `INSERT OR IGNORE INTO api_keys (key_hash, name, tenant_id, role, permissions, created_at)
     VALUES (?, 'test-admin', 1, 'admin', 'all', ?)`
  ).run(hash, Date.now());
  return TEST_RAW_KEY;
}

/** Authorization headers for test requests. Call seedTestApiKey() first. */
export const testAuthHeaders = {
  Authorization: `Bearer ${TEST_RAW_KEY}`,
};

/**
 * Seeds an API key for a specific tenant/role. Returns the raw key.
 */
export function seedTestApiKeyForTenant(
  tenantId: number,
  role: "admin" | "member" | "viewer" = "admin",
  suffix?: string,
): string {
  const rawKey = `srk-test-tenant${tenantId}-${role}-${suffix ?? "000"}`;
  const db = getDatabase();
  const hash = createHash("sha256").update(rawKey).digest("hex");
  db.prepare(
    `INSERT OR IGNORE INTO api_keys (key_hash, name, tenant_id, role, permissions, created_at)
     VALUES (?, ?, ?, ?, 'all', ?)`
  ).run(hash, `test-${role}-t${tenantId}`, tenantId, role, Date.now());
  return rawKey;
}

/**
 * Wrapper around fetch that automatically includes auth headers.
 * Drop-in replacement: testFetch(url, opts) instead of fetch(url, opts).
 */
export function testFetch(url: string | URL, init?: RequestInit): Promise<Response> {
  const headers = new Headers(init?.headers);
  if (!headers.has("Authorization")) {
    headers.set("Authorization", `Bearer ${TEST_RAW_KEY}`);
  }
  return fetch(url, { ...init, headers });
}
