// api/routes/preflight.ts — GET /v1/preflight: server-side preflight checks
//
// Aggregates: orphan count (from last scan), budget check, credential expiry,
// stale allocations. All checks are non-blocking warnings by default; budget
// exhaustion is the only blocking error.

import { Elysia } from "elysia";
import { getAuthContext } from "../middleware/auth";
import { getRecentScans } from "../../material/db/orphans";
import { findStaleClaimed } from "../../material/db/allocations";
import {
  queryOne,
  getTenant,
  getUser,
} from "../../material/db"; // raw-db: api_keys expiry check (Bucket D), see WL-057
import { TIMING } from "@skyrepl/contracts";
import { projectBudget } from "../../billing/budget";

// =============================================================================
// Types
// =============================================================================

interface PreflightWarning {
  type: "orphaned_resources" | "budget_limit" | "credential_expiry" | "stale_allocations";
  message: string;
  severity: "info" | "warning" | "critical";
  details?: Record<string, unknown>;
}

interface PreflightError {
  code: string;
  message: string;
  details?: Record<string, unknown>;
}

interface PreflightResponse {
  ok: boolean;
  warnings: PreflightWarning[];
  errors?: PreflightError[];
}

// =============================================================================
// Route Registration
// =============================================================================

export function registerPreflightRoutes(app: Elysia<any>): void {
  // GET /v1/preflight
  //
  // Query params:
  //   operation: 'launch-run' | 'terminate-instance' | 'create-snapshot'
  //   spec?:     instance spec string (e.g. "ubuntu")
  //   provider?: cloud provider name
  //   region?:   cloud region
  //
  // All checks are scoped to the caller's tenant via API key auth (which the
  // global middleware in server.ts already enforces for all /v1/* routes).

  app.get("/v1/preflight", async ({ query, request, set }) => {
    const auth = getAuthContext(request);
    if (!auth) {
      // Should not reach here — global middleware rejects unauthenticated /v1/* requests.
      // Kept as a safety net in case the bypass list is ever widened.
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Missing API key", category: "auth" } };
    }

    const { tenantId, userId, keyId } = auth;

    const warnings: PreflightWarning[] = [];
    const errors: PreflightError[] = [];

    // ── 1. Orphan count from most recent scan ──────────────────────────────
    //
    // We read from the orphan_scans table rather than running a live cloud
    // scan (which would be slow and rate-limited). The periodic background
    // task (A3) keeps this fresh.

    try {
      const recentScans = getRecentScans(undefined, 5);
      if (recentScans.length > 0) {
        // Sum orphans across the most recent scan per provider
        const latestByProvider = new Map<string, typeof recentScans[0]>();
        for (const scan of recentScans) {
          if (!latestByProvider.has(scan.provider)) {
            latestByProvider.set(scan.provider, scan);
          }
        }
        const totalOrphans = [...latestByProvider.values()].reduce(
          (sum, s) => sum + s.orphans_found,
          0
        );
        if (totalOrphans > 0) {
          warnings.push({
            type: "orphaned_resources",
            message: `${totalOrphans} orphaned cloud resource${totalOrphans === 1 ? "" : "s"} detected`,
            severity: totalOrphans >= 5 ? "warning" : "info",
            details: {
              total: totalOrphans,
              providers: Object.fromEntries(
                [...latestByProvider.entries()].map(([p, s]) => [p, s.orphans_found])
              ),
            },
          });
        }
      }
    } catch {
      // Non-blocking — orphan check failure does not prevent launch
    }

    // ── 2. Budget check ────────────────────────────────────────────────────

    try {
      const tenant = getTenant(tenantId);
      const now = Date.now();
      const monthStart = new Date();
      monthStart.setDate(1);
      monthStart.setHours(0, 0, 0, 0);
      const periodStartMs = monthStart.getTime();

      if (tenant?.budget_usd !== null && tenant?.budget_usd !== undefined) {
        const projection = await projectBudget({
          tenant_id: tenantId,
          period_start_ms: periodStartMs,
          period_end_ms: now,
        });
        const totalCost = projection.total_cents / 100;
        const budget = tenant.budget_usd;
        const remaining = budget - totalCost;

        if (remaining <= 0) {
          errors.push({
            code: "BUDGET_EXCEEDED",
            message: `Team budget exhausted ($${totalCost.toFixed(2)} / $${budget.toFixed(2)})`,
            details: { used_usd: totalCost, budget_usd: budget },
          });
        } else if (remaining < budget * 0.1) {
          warnings.push({
            type: "budget_limit",
            message: `Team budget nearly exhausted ($${totalCost.toFixed(2)} / $${budget.toFixed(2)})`,
            severity: "warning",
            details: { used_usd: totalCost, budget_usd: budget, remaining_usd: remaining },
          });
        }
      }

      // Per-user budget
      const user = getUser(userId);
      if (user?.budget_usd !== null && user?.budget_usd !== undefined) {
        const userProjection = await projectBudget({
          tenant_id: tenantId,
          user_id: userId,
          period_start_ms: periodStartMs,
          period_end_ms: now,
        });
        const userCost = userProjection.total_cents / 100;
        const userBudget = user.budget_usd;

        if (userCost >= userBudget) {
          errors.push({
            code: "USER_BUDGET_EXCEEDED",
            message: `Your budget exhausted ($${userCost.toFixed(2)} / $${userBudget.toFixed(2)})`,
            details: { used_usd: userCost, budget_usd: userBudget },
          });
        }
      }
    } catch {
      // Non-blocking
    }

    // ── 3. Credential expiry ───────────────────────────────────────────────
    //
    // Check the caller's own API key for upcoming expiry.

    try {
      const keyRow = queryOne<{ expires_at: number | null }>(
        "SELECT expires_at FROM api_keys WHERE id = ? AND revoked_at IS NULL",
        [keyId]
      );
      if (keyRow?.expires_at !== null && keyRow?.expires_at !== undefined) {
        const expiresAt = keyRow.expires_at;
        const now = Date.now();
        const remaining = expiresAt - now;
        const WARN_WITHIN_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

        if (remaining <= 0) {
          // Should never reach here (middleware rejects expired keys), but belt-and-suspenders
          errors.push({
            code: "CREDENTIAL_EXPIRED",
            message: "Your API key has expired",
            details: { expires_at: expiresAt },
          });
        } else if (remaining < WARN_WITHIN_MS) {
          const daysLeft = Math.ceil(remaining / (24 * 60 * 60 * 1000));
          warnings.push({
            type: "credential_expiry",
            message: `Your API key expires in ${daysLeft} day${daysLeft === 1 ? "" : "s"}`,
            severity: daysLeft <= 1 ? "warning" : "info",
            details: { expires_at: expiresAt, days_remaining: daysLeft },
          });
        }
      }
    } catch {
      // Non-blocking
    }

    // ── 4. Stale allocations ───────────────────────────────────────────────
    //
    // Stale CLAIMED allocations indicate stuck runs that haven't progressed.
    // Scoped to the caller's tenant.

    try {
      const cutoff = Date.now() - TIMING.STALE_DETECTION_MS;
      const stale = findStaleClaimed(cutoff).filter(a => a.tenant_id === tenantId);
      if (stale.length > 0) {
        warnings.push({
          type: "stale_allocations",
          message: `${stale.length} stale allocation${stale.length === 1 ? "" : "s"} detected (stuck CLAIMED)`,
          severity: "warning",
          details: {
            count: stale.length,
            allocation_ids: stale.map(a => a.id),
          },
        });
      }
    } catch {
      // Non-blocking
    }

    const ok = errors.length === 0;
    const response: PreflightResponse = { ok, warnings };
    if (errors.length > 0) {
      response.errors = errors;
    }

    return response;
  });
}
