// api/routes/orphans.ts — registerOrphanRoutes(): whitelist CRUD + scan results
//
// POST   /v1/orphans/whitelist          — add to whitelist
// DELETE /v1/orphans/whitelist/:id      — remove from whitelist by provider+provider_id
// GET    /v1/orphans/whitelist          — list whitelist entries
// GET    /v1/orphans/scans              — recent scan results

import { Elysia } from "elysia";
import { Type } from "@sinclair/typebox";
import {
  addToWhitelist,
  removeFromWhitelist,
  getWhitelist,
  getRecentScans,
} from "../../material/db";
import { getAuthContext, checkPermission } from "../middleware/auth";

// =============================================================================
// Request Body Schemas
// =============================================================================

const AddWhitelistSchema = Type.Object({
  provider: Type.String({ minLength: 1 }),
  provider_id: Type.String({ minLength: 1 }),
  resource_type: Type.Optional(Type.String({ default: "instance" })),
  reason: Type.String({ minLength: 1 }),
  acknowledged_by: Type.Optional(Type.String()),
});

// =============================================================================
// Route Registration
// =============================================================================

export function registerOrphanRoutes(app: Elysia<any>): void {
  // ─── GET /v1/orphans/whitelist ─────────────────────────────────────────
  // List all whitelist entries, optionally filtered by provider.

  app.get("/v1/orphans/whitelist", ({ query, request, set }) => {
    const auth = getAuthContext(request);
    if (!auth) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Missing API key", category: "auth" } };
    }

    const provider = typeof query.provider === "string" ? query.provider : undefined;
    const entries = getWhitelist(provider);
    return { data: entries };
  });

  // ─── POST /v1/orphans/whitelist ────────────────────────────────────────
  // Add a provider resource to the whitelist.

  app.post("/v1/orphans/whitelist", ({ body, request, set }) => {
    const auth = getAuthContext(request);
    if (!auth) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Missing API key", category: "auth" } };
    }
    if (!checkPermission(auth, "manage_resources")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }

    const resourceType = body.resource_type ?? "instance";
    const acknowledgedBy = body.acknowledged_by ?? `user:${auth.userId}`;

    addToWhitelist(
      body.provider,
      body.provider_id,
      resourceType,
      body.reason,
      acknowledgedBy
    );

    const entries = getWhitelist(body.provider);
    const entry = entries.find(e => e.provider_id === body.provider_id);

    set.status = 201;
    return { data: entry };
  }, { body: AddWhitelistSchema });

  // ─── DELETE /v1/orphans/whitelist/:provider/:provider_id ──────────────
  // Remove a resource from the whitelist.
  // provider and provider_id are path params (provider_id may contain dashes).

  app.delete("/v1/orphans/whitelist/:provider/:provider_id", ({ params, set, request }) => {
    const auth = getAuthContext(request);
    if (!auth) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Missing API key", category: "auth" } };
    }
    if (!checkPermission(auth, "manage_resources")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }

    const provider = (params as Record<string, string>).provider;
    const providerId = (params as Record<string, string>).provider_id;

    const removed = removeFromWhitelist(provider, providerId);
    if (!removed) {
      set.status = 404;
      return { error: { code: "NOT_FOUND", message: `Whitelist entry for ${provider}/${providerId} not found`, category: "not_found" } };
    }

    return { data: { provider, provider_id: providerId, removed: true } };
  });

  // ─── GET /v1/orphans/scans ─────────────────────────────────────────────
  // List recent orphan scan results.

  app.get("/v1/orphans/scans", ({ query, request, set }) => {
    const auth = getAuthContext(request);
    if (!auth) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Missing API key", category: "auth" } };
    }

    const provider = typeof query.provider === "string" ? query.provider : undefined;
    const limit = Math.min(Number(query.limit) || 20, 100);
    const scans = getRecentScans(provider, limit);
    return { data: scans };
  });
}
