// api/routes/blobs.ts — blob/artifact download, blob check endpoints

import { Elysia, t } from "elysia";
import { Type } from "@sinclair/typebox";
import { createHash } from "node:crypto";
import {
  queryOne,
  queryMany,
  type Run,
} from "../../material/db";
import { getAuthContext, extractToken } from "../middleware/auth";

/** Elysia params schema: coerces URL :id param from string to number */
const IdParams = t.Object({ id: t.Numeric() });

const BlobCheckSchema = Type.Object({
  checksums: Type.Array(Type.String()),
});

export function registerBlobRoutes(app: Elysia<any>): void {
  // ─── Artifacts (SNAP-04) ────────────────────────────────────────────

  app.get("/v1/runs/:id/artifacts", ({ params, set, request }) => {
    const runId = params.id;
    const auth = getAuthContext(request);
    const run = queryOne<Run>("SELECT * FROM runs WHERE id = ?", [runId]);
    // (the existing !run check is fine — Run type includes tenant_id)
    if (!run || (auth && run.tenant_id !== auth.tenantId)) {
      set.status = 404;
      return { error: { code: "RUN_NOT_FOUND", message: `Run ${runId} not found`, category: "not_found" } };
    }

    // Find all artifact objects tagged with this run_id
    const artifacts = queryMany<{
      id: number;
      blob_id: number;
      metadata_json: string | null;
      created_at: number;
    }>(
      `SELECT o.id, o.blob_id, o.metadata_json, o.created_at
       FROM objects o
       JOIN object_tags t ON o.id = t.object_id
       WHERE o.type = 'artifact' AND t.tag = ?${auth ? ' AND o.tenant_id = ?' : ''}
       ORDER BY o.created_at ASC`,
      auth ? [`run_id:${runId}`, auth.tenantId] : [`run_id:${runId}`]
    );

    const data = artifacts.map((a) => {
      const meta = a.metadata_json ? JSON.parse(a.metadata_json) : {};
      return {
        id: a.id,
        run_id: runId,
        path: meta.path ?? null,
        checksum: meta.checksum ?? null,
        size_bytes: meta.size_bytes ?? 0,
        created_at: a.created_at,
      };
    });

    return { data };
  }, { params: IdParams });

  app.get("/v1/artifacts/:id/download", ({ params, set, request }) => {
    const artifactId = params.id;
    const auth = getAuthContext(request);
    const obj = queryOne<{ id: number; blob_id: number; metadata_json: string | null }>(
      `SELECT id, blob_id, metadata_json FROM objects WHERE id = ? AND type = 'artifact'${auth ? ' AND tenant_id = ?' : ''}`,
      auth ? [artifactId, auth.tenantId] : [artifactId]
    );
    if (!obj) {
      set.status = 404;
      return { error: { code: "RESOURCE_NOT_FOUND", message: `Artifact ${artifactId} not found`, category: "not_found" } };
    }

    const blob = queryOne<{ id: number; payload: Buffer | null; size_bytes: number }>(
      "SELECT id, payload, size_bytes FROM blobs WHERE id = ?",
      [obj.blob_id]
    );
    if (!blob || !blob.payload) {
      set.status = 404;
      return { error: { code: "RESOURCE_NOT_FOUND", message: "Artifact data not available", category: "not_found" } };
    }

    const meta = obj.metadata_json ? JSON.parse(obj.metadata_json) : {};
    const rawFilename = meta.path ? meta.path.split("/").pop() : `artifact-${artifactId}`;
    // Sanitize filename: keep alphanumeric, dots, hyphens, underscores only
    const filename = rawFilename.replace(/[^a-zA-Z0-9._-]/g, "_") || `artifact-${artifactId}`;

    return new Response(new Uint8Array(blob.payload), {
      headers: {
        "Content-Type": "application/octet-stream",
        "Content-Length": String(blob.size_bytes),
        "Content-Disposition": `attachment; filename="${filename}"`,
      },
    });
  }, { params: IdParams });

  // ─── Blob Check ───────────────────────────────────────────────────────

  app.post("/v1/blobs/check", ({ body, request }) => {
    const checksums = body.checksums;
    const auth = getAuthContext(request);

    if (checksums.length === 0) {
      return { missing: [], urls: {} as Record<string, string> };
    }

    // Query blobs table for matching checksums, scoped to tenant when auth present
    const placeholders = checksums.map(() => "?").join(",");
    const rows = auth
      ? queryMany<{ checksum: string }>(
          `SELECT checksum FROM blobs WHERE checksum IN (${placeholders}) AND tenant_id = ?`,
          [...checksums, auth.tenantId]
        )
      : queryMany<{ checksum: string }>(
          `SELECT checksum FROM blobs WHERE checksum IN (${placeholders})`,
          checksums
        );

    const found = new Set(rows.map(r => r.checksum));
    const missing: string[] = [];
    const urls: Record<string, string> = {};

    for (const cs of checksums) {
      if (found.has(cs)) {
        // Inline blob: point to by-checksum endpoint; S3 blobs deferred
        urls[cs] = `/v1/blobs/by-checksum/${encodeURIComponent(cs)}`;
      } else {
        missing.push(cs);
      }
    }

    return { missing, urls };
  }, { body: BlobCheckSchema });

  // ─── Blob Download (by ID) ──────────────────────────────────────────

  app.get("/v1/blobs/:id/download", ({ params, set, request }) => {
    const id = params.id;
    const auth = getAuthContext(request);
    const blob = queryOne<{ id: number; payload: Buffer | null; size_bytes: number }>(
      `SELECT id, payload, size_bytes FROM blobs WHERE id = ?${auth ? ' AND tenant_id = ?' : ''}`,
      auth ? [id, auth.tenantId] : [id]
    );
    if (!blob) {
      set.status = 404;
      return { error: { code: "OBJECT_NOT_FOUND", message: `Blob ${id} not found`, category: "not_found" } };
    }
    if (!blob.payload) {
      set.status = 400;
      return { error: { code: "INVALID_INPUT", message: "Blob not stored inline (S3 not available in Slice 1)", category: "validation" } };
    }
    return new Response(new Uint8Array(blob.payload), {
      headers: {
        "Content-Type": "application/octet-stream",
        "Content-Length": String(blob.size_bytes),
      },
    });
  }, { params: IdParams });

  // ─── Blob Download (by checksum) ──────────────────────────────────
  // Agent bridge sends /v1/blobs/<checksum> as file URLs in start_run messages

  app.get("/v1/blobs/by-checksum/:id", ({ params, set, request, query }: { params: { id: string }; set: { status?: number | string }; request: Request; query: Record<string, string> }) => {
    const checksum = decodeURIComponent(params.id);

    // Auth: accept instance registration token (Bearer header or ?token= query param).
    // Derive tenant_id from the instance record so blobs are scoped to the right tenant.
    const token = extractToken(request, query);
    if (!token) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Missing authentication token", category: "auth" } };
    }

    // Find the instance that owns this token by matching its SHA256 hash.
    // This is the same hash algorithm used by verifyInstanceToken() in auth.ts.
    const tokenHash = createHash("sha256").update(token).digest("hex");
    const instanceRow = queryOne<{ id: number; tenant_id: number }>(
      "SELECT id, tenant_id FROM instances WHERE registration_token_hash = ?",
      [tokenHash]
    );
    if (!instanceRow) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Invalid authentication token", category: "auth" } };
    }

    const tenantId = instanceRow.tenant_id;
    const blob = queryOne<{ id: number; payload: Buffer | null; size_bytes: number }>(
      "SELECT id, payload, size_bytes FROM blobs WHERE checksum = ? AND tenant_id = ?",
      [checksum, tenantId]
    );
    if (!blob) {
      set.status = 404;
      return { error: { code: "OBJECT_NOT_FOUND", message: `Blob with checksum ${checksum} not found`, category: "not_found" } };
    }
    if (!blob.payload) {
      set.status = 400;
      return { error: { code: "INVALID_INPUT", message: "Blob not stored inline (S3 not available in Slice 1)", category: "validation" } };
    }
    return new Response(new Uint8Array(blob.payload), {
      headers: {
        "Content-Type": "application/octet-stream",
        "Content-Length": String(blob.size_bytes),
      },
    });
  });
}
