// api/routes/blobs.ts — blob/artifact download, blob check, presigned URL endpoints

import { Elysia, t } from "elysia";
import { Type } from "@sinclair/typebox";
import { createHash } from "node:crypto";
import {
  queryOne,
  queryMany,
  createBlob,
  getBlob,
  getDatabase,
  getRun,
  getAllocationByRunId,
  getInstanceByTokenHash,
  updateBlobLastReferenced,
  updateBlobStorageKey,
  updateBlobSize,
  type Run,
  type Allocation,
} from "../../material/db"; // raw-db: boutique queries (partial-column instance selects, checksum IN queries), see WL-057
import { getControlId } from "../../material/control-id";
import { storeInline, getSqlStorageSizeCache, SQL_STORAGE_ADVISORY_BYTES, SQL_STORAGE_STRONG_BYTES } from "../../material/storage";
import { downloadLogs } from "../../background/log-streaming";
import { getAuthContext, extractToken, verifyInstanceToken } from "../middleware/auth";
import { getBlobProvider, getDefaultBlobProvider } from "../../provider/storage/registry";
import { SBO_THRESHOLD, PRESIGNED_PUT_TTL_MS, PRESIGNED_GET_TTL_MS } from "../../provider/storage/types";
import { SqlBlobProvider } from "../../provider/storage/sql-blob";

/** Elysia params schema: coerces URL :id param from string to number */
const IdParams = t.Object({ id: t.Numeric() });

// =============================================================================
// SQL storage upgrade encouragement
// =============================================================================

/**
 * Return a warning message if the default blob provider is SQL and its total
 * payload size exceeds advisory thresholds; otherwise return null.
 * Uses the module-level cache refreshed during GC — no per-request DB query.
 */
function getSqlStorageWarning(): string | null {
  const provider = getDefaultBlobProvider();
  if (!(provider instanceof SqlBlobProvider)) return null;

  const bytes = getSqlStorageSizeCache();
  if (bytes === 0) return null;

  const gb = (bytes / (1024 * 1024 * 1024)).toFixed(1);
  if (bytes >= SQL_STORAGE_STRONG_BYTES) {
    return `SQL blob storage at ${gb}GB. Strongly recommended to upgrade to S3/MinIO for reliability and performance.`;
  }
  if (bytes >= SQL_STORAGE_ADVISORY_BYTES) {
    return `SQL blob storage at ${gb}GB. Consider upgrading to S3/MinIO for better performance.`;
  }
  return null;
}

const BlobCheckSchema = Type.Object({
  checksums: Type.Array(Type.String()),
});

const UploadUrlSchema = Type.Object({
  checksum: Type.String(),
  size_bytes: Type.Number(),
  content_type: Type.Optional(Type.String()),
  run_id: Type.Optional(Type.Number()),
});

const ConfirmSchema = Type.Object({
  blob_id: Type.Number(),
  checksum: Type.String(),
});

// =============================================================================
// Auth helper for agent blob endpoints
// =============================================================================

/**
 * Resolve tenant_id from a run_id via its allocation's instance.
 * Returns { tenantId, instanceId } or null if not found / auth fails.
 */
function resolveAgentAuth(
  runId: number | undefined,
  request: Request
): { tenantId: number; instanceId: number } | null {
  if (!runId) return null;

  const run = getRun(runId);
  if (!run) return null;

  const allocation = getAllocationByRunId(runId);
  if (!allocation) return null;

  // Verify instance token
  const token = extractToken(request);
  if (token) {
    if (!verifyInstanceToken(allocation.instance_id, token)) return null;
  } else {
    // Check if instance requires auth
    const instance = queryOne<{ id: number; registration_token_hash: string | null }>(
      "SELECT id, registration_token_hash FROM instances WHERE id = ?",
      [allocation.instance_id]
    );
    if (instance?.registration_token_hash) return null;
  }

  const instance = queryOne<{ id: number; tenant_id: number }>(
    "SELECT id, tenant_id FROM instances WHERE id = ?",
    [allocation.instance_id]
  );
  if (!instance) return null;

  return { tenantId: instance.tenant_id, instanceId: instance.id };
}

/**
 * Resolve tenant from a blob_id's tenant_id column, verifying agent auth via
 * the Bearer token matching an instance registration token.
 */
function resolveAgentAuthFromToken(
  request: Request
): { tenantId: number; instanceId: number } | null {
  const token = extractToken(request);
  if (!token) return null;

  const tokenHash = createHash("sha256").update(token).digest("hex");
  const instance = getInstanceByTokenHash(tokenHash);
  if (!instance) return null;

  return { tenantId: instance.tenant_id, instanceId: instance.id };
}

export function registerBlobRoutes(app: Elysia<any>): void {
  // ─── Storage warning header on blob endpoints ────────────────────────
  // Attach X-SkyREPL-Storage-Warning to responses from /v1/blobs/* and
  // /v1/agent/blobs/* when the SQL provider total size exceeds thresholds.
  app.onAfterHandle(({ request, set }) => {
    const pathname = new URL(request.url).pathname;
    if (
      pathname.startsWith("/v1/blobs/") ||
      pathname.startsWith("/v1/agent/blobs/")
    ) {
      const warning = getSqlStorageWarning();
      if (warning) {
        set.headers["X-SkyREPL-Storage-Warning"] = warning;
      }
    }
  });

  // ─── Artifacts (SNAP-04) ────────────────────────────────────────────

  app.get("/v1/runs/:id/artifacts", ({ params, set, request }) => {
    const runId = params.id;
    const auth = getAuthContext(request);
    const run = getRun(runId);
    // (the existing !run check is fine -- Run type includes tenant_id)
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

  app.get("/v1/artifacts/:id/download", async ({ params, set, request }) => {
    const artifactId = params.id;
    const auth = getAuthContext(request);
    const obj = queryOne<{ id: number; blob_id: number; metadata_json: string | null; tenant_id: number }>(
      `SELECT id, blob_id, metadata_json, tenant_id FROM objects WHERE id = ? AND type = 'artifact'${auth ? ' AND tenant_id = ?' : ''}`,
      auth ? [artifactId, auth.tenantId] : [artifactId]
    );
    if (!obj) {
      set.status = 404;
      return { error: { code: "RESOURCE_NOT_FOUND", message: `Artifact ${artifactId} not found`, category: "not_found" } };
    }

    const blob = queryOne<{ id: number; payload: Buffer | null; s3_key: string | null; size_bytes: number; tenant_id: number }>(
      "SELECT id, payload, s3_key, size_bytes, tenant_id FROM blobs WHERE id = ?",
      [obj.blob_id]
    );
    if (!blob) {
      set.status = 404;
      return { error: { code: "RESOURCE_NOT_FOUND", message: "Artifact data not available", category: "not_found" } };
    }

    const meta = obj.metadata_json ? JSON.parse(obj.metadata_json) : {};
    const rawFilename = meta.path ? meta.path.split("/").pop() : `artifact-${artifactId}`;
    // Sanitize filename: keep alphanumeric, dots, hyphens, underscores only
    const filename = rawFilename.replace(/[^a-zA-Z0-9._-]/g, "_") || `artifact-${artifactId}`;

    // Inline blob: stream payload directly
    if (blob.payload !== null) {
      return new Response(new Uint8Array(blob.payload), {
        headers: {
          "Content-Type": "application/octet-stream",
          "Content-Length": String(blob.size_bytes),
          "Content-Disposition": `attachment; filename="${filename}"`,
        },
      });
    }

    // External blob: use provider
    if (blob.s3_key) {
      const provider = getBlobProvider(blob.tenant_id);
      if (provider.capabilities.supportsPresignedUrls) {
        // 302 redirect to presigned GET URL
        const url = await provider.generatePresignedUrl(blob.s3_key, "GET", { ttlMs: PRESIGNED_GET_TTL_MS });
        set.status = 302;
        set.headers["Location"] = url;
        set.headers["Content-Disposition"] = `attachment; filename="${filename}"`;
        return;
      } else {
        // SQL provider proxy: download and stream
        const data = await provider.download(blob.s3_key);
        return new Response(new Uint8Array(data), {
          headers: {
            "Content-Type": "application/octet-stream",
            "Content-Length": String(data.length),
            "Content-Disposition": `attachment; filename="${filename}"`,
          },
        });
      }
    }

    set.status = 404;
    return { error: { code: "RESOURCE_NOT_FOUND", message: "Artifact data not available", category: "not_found" } };
  }, { params: IdParams });

  // ─── Log Download ───────────────────────────────────────────────────

  app.get("/v1/runs/:id/logs", async ({ params, query, set, request }) => {
    const runId = params.id;

    // Look up run (no API key required — auth bypass in server.ts)
    const run = getRun(runId);
    if (!run) {
      set.status = 404;
      return { error: { code: "RUN_NOT_FOUND", message: `Run ${runId} not found`, category: "not_found" } };
    }

    const stream = (query as any).stream ?? "stdout";
    const fromMs = (query as any).from ? parseInt((query as any).from) : undefined;
    const toMs = (query as any).to ? parseInt((query as any).to) : undefined;
    const maxBytes = (query as any).max_bytes ? parseInt((query as any).max_bytes) : undefined;

    const data = await downloadLogs(runId, { stream, fromMs, toMs, maxBytes });

    return new Response(new Uint8Array(data), {
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
        "Content-Length": String(data.length),
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

  app.get("/v1/blobs/:id/download", async ({ params, set, request }) => {
    const id = params.id;
    const auth = getAuthContext(request);
    const blob = queryOne<{ id: number; payload: Buffer | null; s3_key: string | null; size_bytes: number; tenant_id: number }>(
      `SELECT id, payload, s3_key, size_bytes, tenant_id FROM blobs WHERE id = ?${auth ? ' AND tenant_id = ?' : ''}`,
      auth ? [id, auth.tenantId] : [id]
    );
    if (!blob) {
      set.status = 404;
      return { error: { code: "OBJECT_NOT_FOUND", message: `Blob ${id} not found`, category: "not_found" } };
    }

    // Inline blob
    if (blob.payload !== null) {
      return new Response(new Uint8Array(blob.payload), {
        headers: {
          "Content-Type": "application/octet-stream",
          "Content-Length": String(blob.size_bytes),
        },
      });
    }

    // External blob: use provider
    if (blob.s3_key) {
      const provider = getBlobProvider(blob.tenant_id);
      if (provider.capabilities.supportsPresignedUrls) {
        const url = await provider.generatePresignedUrl(blob.s3_key, "GET", { ttlMs: PRESIGNED_GET_TTL_MS });
        set.status = 302;
        set.headers["Location"] = url;
        return;
      } else {
        const data = await provider.download(blob.s3_key);
        return new Response(new Uint8Array(data), {
          headers: {
            "Content-Type": "application/octet-stream",
            "Content-Length": String(data.length),
          },
        });
      }
    }

    set.status = 400;
    return { error: { code: "INVALID_INPUT", message: "Blob has no inline payload or external storage key", category: "validation" } };
  }, { params: IdParams });

  // ─── Blob Download (by checksum) ──────────────────────────────────
  // Agent bridge sends /v1/blobs/<checksum> as file URLs in start_run messages

  app.get("/v1/blobs/by-checksum/:id", async ({ params, set, request, query }: any) => {
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
    const instance = getInstanceByTokenHash(tokenHash);
    if (!instance) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Invalid authentication token", category: "auth" } };
    }

    const tenantId = instance.tenant_id;
    const blob = queryOne<{ id: number; payload: Buffer | null; s3_key: string | null; size_bytes: number }>(
      "SELECT id, payload, s3_key, size_bytes FROM blobs WHERE checksum = ? AND tenant_id = ?",
      [checksum, tenantId]
    );
    if (!blob) {
      set.status = 404;
      return { error: { code: "OBJECT_NOT_FOUND", message: `Blob with checksum ${checksum} not found`, category: "not_found" } };
    }

    // Inline blob
    if (blob.payload !== null) {
      return new Response(new Uint8Array(blob.payload), {
        headers: {
          "Content-Type": "application/octet-stream",
          "Content-Length": String(blob.size_bytes),
        },
      });
    }

    // External blob: use provider
    if (blob.s3_key) {
      const provider = getBlobProvider(tenantId);
      if (provider.capabilities.supportsPresignedUrls) {
        const url = await provider.generatePresignedUrl(blob.s3_key, "GET", { ttlMs: PRESIGNED_GET_TTL_MS });
        set.status = 302;
        set.headers["Location"] = url;
        return;
      } else {
        const data = await provider.download(blob.s3_key);
        return new Response(new Uint8Array(data), {
          headers: {
            "Content-Type": "application/octet-stream",
            "Content-Length": String(data.length),
          },
        });
      }
    }

    set.status = 400;
    return { error: { code: "INVALID_INPUT", message: "Blob has no inline payload or external storage key", category: "validation" } };
  });

  // ─── Agent Blob Upload URL (presigned URL flow) ─────────────────────

  app.post("/v1/agent/blobs/upload-url", async ({ body, set, request }) => {
    const req = body as {
      checksum: string;
      size_bytes: number;
      content_type?: string;
      run_id?: number;
    };

    // Auth: resolve tenant from run -> allocation -> instance
    const agentAuth = req.run_id
      ? resolveAgentAuth(req.run_id, request)
      : resolveAgentAuthFromToken(request);
    if (!agentAuth) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Unable to verify agent identity", category: "auth" } };
    }

    const { tenantId } = agentAuth;
    const provider = getBlobProvider(tenantId);
    const now = Date.now();

    if (req.size_bytes < SBO_THRESHOLD) {
      // Small blob: inline path
      const blob = createBlob({
        bucket: "artifacts",
        checksum: req.checksum,
        checksum_bytes: null,
        s3_key: null,
        s3_bucket: null,
        payload: null,
        size_bytes: req.size_bytes,
        last_referenced_at: now,
      }, tenantId);

      return {
        blob_id: blob.id,
        method: "PUT",
        url: `/v1/agent/blobs/${blob.id}/data`,
        inline: true,
      };
    }

    // Large blob: external storage
    // Key embeds controlId for orphan recovery across provider migrations.
    const controlId = getControlId(getDatabase());
    const s3Key = `blobs/${controlId}/${tenantId}/${Date.now()}-${req.checksum.slice(0, 16)}`;

    const blob = createBlob({
      bucket: "artifacts",
      checksum: req.checksum,
      checksum_bytes: null,
      s3_key: s3Key,
      s3_bucket: null,
      payload: null,
      size_bytes: req.size_bytes,
      last_referenced_at: now,
    }, tenantId);

    if (provider.capabilities.supportsPresignedUrls) {
      // Generate presigned PUT URL for direct upload
      const presignedUrl = await provider.generatePresignedUrl(s3Key, "PUT", {
        ttlMs: PRESIGNED_PUT_TTL_MS,
        contentType: req.content_type,
      });
      return {
        blob_id: blob.id,
        method: "PUT",
        url: presignedUrl,
        inline: false,
      };
    } else {
      // SQL provider: proxy through control plane
      return {
        blob_id: blob.id,
        method: "PUT",
        url: `/v1/agent/blobs/${blob.id}/upload`,
        inline: false,
      };
    }
  }, { body: UploadUrlSchema });

  // ─── Agent Blob Data Upload ─────────────────────────────────────────

  app.put("/v1/agent/blobs/:id/data", async ({ params, set, request }) => {
    const blobId = Number(params.id);

    // Auth: verify via token -> instance
    const agentAuth = resolveAgentAuthFromToken(request);
    if (!agentAuth) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Unable to verify agent identity", category: "auth" } };
    }

    const blob = getBlob(blobId);
    if (!blob) {
      set.status = 404;
      return { error: { code: "OBJECT_NOT_FOUND", message: `Blob ${blobId} not found`, category: "not_found" } };
    }
    if (blob.tenant_id !== agentAuth.tenantId) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Blob does not belong to this tenant", category: "auth" } };
    }

    const rawBody = await request.arrayBuffer();
    const data = Buffer.from(rawBody);

    storeInline(blobId, data);

    return { ack: true, blob_id: blobId, size_bytes: data.length };
  }, { params: IdParams });

  // ─── Agent Blob Upload (SQL provider proxy) ─────────────────────────

  app.put("/v1/agent/blobs/:id/upload", async ({ params, set, request }) => {
    const blobId = Number(params.id);

    // Auth: verify via token -> instance
    const agentAuth = resolveAgentAuthFromToken(request);
    if (!agentAuth) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Unable to verify agent identity", category: "auth" } };
    }

    const blob = getBlob(blobId);
    if (!blob) {
      set.status = 404;
      return { error: { code: "OBJECT_NOT_FOUND", message: `Blob ${blobId} not found`, category: "not_found" } };
    }
    if (blob.tenant_id !== agentAuth.tenantId) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Blob does not belong to this tenant", category: "auth" } };
    }

    const rawBody = await request.arrayBuffer();
    const data = Buffer.from(rawBody);

    const provider = getBlobProvider(agentAuth.tenantId);
    const controlId = getControlId(getDatabase());
    const s3Key = blob.s3_key || `blobs/${controlId}/${agentAuth.tenantId}/${blobId}`;

    await provider.upload(s3Key, data);

    // Update blob record with s3_key if not already set, and size
    if (!blob.s3_key) {
      updateBlobStorageKey(blobId, s3Key, data.length);
    } else {
      updateBlobSize(blobId, data.length);
    }

    return { ack: true, blob_id: blobId, size_bytes: data.length };
  }, { params: IdParams });

  // ─── Agent Blob Confirm ─────────────────────────────────────────────

  app.post("/v1/agent/blobs/confirm", async ({ body, set, request }) => {
    const req = body as { blob_id: number; checksum: string };

    // Auth: verify via token -> instance
    const agentAuth = resolveAgentAuthFromToken(request);
    if (!agentAuth) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Unable to verify agent identity", category: "auth" } };
    }

    const blob = getBlob(req.blob_id);
    if (!blob) {
      set.status = 404;
      return { error: { code: "OBJECT_NOT_FOUND", message: `Blob ${req.blob_id} not found`, category: "not_found" } };
    }
    if (blob.tenant_id !== agentAuth.tenantId) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Blob does not belong to this tenant", category: "auth" } };
    }

    // Verify checksum matches
    if (blob.checksum !== req.checksum) {
      set.status = 400;
      return { error: { code: "CHECKSUM_MISMATCH", message: `Expected checksum ${blob.checksum}, got ${req.checksum}`, category: "validation" } };
    }

    // For inline blobs: verify payload exists
    if (!blob.s3_key && blob.payload === null) {
      // Re-check in case payload was stored after initial blob creation
      const freshBlob = getBlob(req.blob_id);
      if (!freshBlob || freshBlob.payload === null) {
        set.status = 400;
        return { error: { code: "BLOB_NOT_UPLOADED", message: "Inline blob payload not yet uploaded", category: "validation" } };
      }
    }

    // For external blobs: verify object exists in provider
    if (blob.s3_key) {
      const provider = getBlobProvider(agentAuth.tenantId);
      const exists = await provider.exists(blob.s3_key);
      if (!exists) {
        set.status = 400;
        return { error: { code: "BLOB_NOT_UPLOADED", message: "External blob not found in storage provider", category: "validation" } };
      }
    }

    // Mark as confirmed by updating last_referenced_at
    updateBlobLastReferenced(req.blob_id);

    return { confirmed: true, blob_id: req.blob_id };
  }, { body: ConfirmSchema });
}
