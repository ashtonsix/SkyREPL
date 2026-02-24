// api/routes/resources.ts — registerResourceRoutes(): GET list/detail + PATCH/DELETE for resources

import { Elysia, t } from "elysia";
import { Type } from "@sinclair/typebox";
import { getRunRecordRaw, materializeRun } from "../../resource/run";
import { getInstanceRecordRaw, materializeInstance } from "../../resource/instance";
import { materializeAllocation } from "../../resource/allocation";
import { materializeManifest } from "../../resource/manifest";
import { materializeWorkflow } from "../../resource/workflow";
import { stampMaterialized } from "../../resource/materializer";
import { materializeInstanceBatch } from "../../resource/instance";
import {
  queryMany,
  getWorkflow,
  getWorkflowNodes,
  updateInstance,
  deleteInstance,
  updateRun,
  deleteRun,
  getAllocation,
  updateAllocation,
  getManifest,
  getManifestResources,
  getObject,
  type Workflow,
  type Instance,
  type Allocation,
  type Manifest,
  type StorageObject,
  type Run,
} from "../../material/db"; // raw-db: boutique queries (allocations JOIN instances, workflow manifest inline), see WL-057
import {
  SkyREPLError,
  httpStatusForError,
  errorToApiError,
} from "@skyrepl/contracts";
import { getAuthContext, checkPermission } from "../middleware/auth";
import {
  paginatedList,
  encodeCursor,
  decodeCursor,
  buildRunFilters,
  buildInstanceFilters,
  buildAllocationFilters,
  buildManifestFilters,
  buildObjectFilters,
  buildWorkflowFilters,
} from "./pagination";

// =============================================================================
// Route Registration
// =============================================================================

/** Elysia params schema: coerces URL :id param from string to number */
const IdParams = t.Object({ id: t.Numeric() });

// ─── PATCH Request Body Schemas ───────────────────────────────────────────────

const PatchInstanceSchema = Type.Object({
  workflow_state: Type.Optional(Type.String()),
  workflow_error: Type.Optional(Type.Union([Type.String(), Type.Null()])),
  ip: Type.Optional(Type.Union([Type.String(), Type.Null()])),
  last_heartbeat: Type.Optional(Type.Number()),
});

const PatchRunSchema = Type.Object({
  workflow_state: Type.Optional(Type.String()),
  workflow_error: Type.Optional(Type.Union([Type.String(), Type.Null()])),
  exit_code: Type.Optional(Type.Union([Type.Integer(), Type.Null()])),
  started_at: Type.Optional(Type.Union([Type.Number(), Type.Null()])),
  finished_at: Type.Optional(Type.Union([Type.Number(), Type.Null()])),
});

const PatchAllocationSchema = Type.Object({
  status: Type.Optional(Type.String()),
  debug_hold_until: Type.Optional(Type.Union([Type.Number(), Type.Null()])),
  completed_at: Type.Optional(Type.Union([Type.Number(), Type.Null()])),
});

export function registerResourceRoutes(app: Elysia<any>): void {
  // ─── Runs ────────────────────────────────────────────────────────────

  app.get("/v1/runs", ({ query, request }: { query: Record<string, unknown>; request: Request }) => {
    const auth = getAuthContext(request);
    const filters = buildRunFilters(query, auth?.tenantId);
    const result = paginatedList<Run>("runs", filters, query);
    return { ...result, data: result.data.map(stampMaterialized) };
  });

  app.get("/v1/runs/:id", ({ params, set, request }) => {
    const auth = getAuthContext(request);
    const id = params.id;
    const run = materializeRun(id);
    if (!run || (auth && run.tenant_id !== auth.tenantId)) {
      return new Response(
        JSON.stringify({ error: { code: "RUN_NOT_FOUND", message: `Run ${id} not found`, category: "not_found" } }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      );
    }
    return { data: run };
  }, { params: IdParams });

  // ─── Instances ───────────────────────────────────────────────────────

  app.get("/v1/instances", async ({ query, request }: { query: Record<string, unknown>; request: Request }) => {
    const auth = getAuthContext(request);
    const filters = buildInstanceFilters(query, auth?.tenantId);
    const result = paginatedList<Instance>("instances", filters, query);
    const ids = result.data.map(r => r.id);
    if (ids.length === 0) return { ...result, data: [] };
    const materialized = await materializeInstanceBatch(ids, { tier: "display" });
    // Preserve pagination order (materializeInstanceBatch may reorder)
    const byId = new Map(materialized.map(m => [m.id, m]));
    const ordered = ids.map(id => byId.get(id)).filter(Boolean);
    return { ...result, data: ordered };
  });

  app.get("/v1/instances/:id", async ({ params, set, request }) => {
    const auth = getAuthContext(request);
    const id = params.id;
    const instance = await materializeInstance(id);
    if (!instance || (auth && instance.tenant_id !== auth.tenantId)) {
      return new Response(
        JSON.stringify({ error: { code: "INSTANCE_NOT_FOUND", message: `Instance ${id} not found`, category: "not_found" } }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      );
    }
    return { data: instance };
  }, { params: IdParams });

  // ─── Instances: PATCH + DELETE ───────────────────────────────────────

  app.patch("/v1/instances/:id", ({ params, body, set, request }) => {
    const auth = getAuthContext(request);
    if (!auth || !checkPermission(auth, "manage_resources")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }
    const id = Number(params.id);
    const instance = getInstanceRecordRaw(id);
    if (!instance || instance.tenant_id !== auth.tenantId) {
      set.status = 404;
      return { error: { code: "INSTANCE_NOT_FOUND", message: `Instance ${id} not found`, category: "not_found" } };
    }
    const allowed = ["workflow_state", "workflow_error", "ip", "last_heartbeat"] as const;
    const updates: Record<string, unknown> = {};
    for (const field of allowed) {
      if (body !== null && body !== undefined && field in body) updates[field] = (body as Record<string, unknown>)[field];
    }
    if (Object.keys(updates).length === 0) {
      set.status = 400;
      return { error: { code: "INVALID_INPUT", message: "No updatable fields provided", category: "validation" } };
    }
    const updated = updateInstance(id, updates);
    return { data: updated };
  }, { params: IdParams, body: PatchInstanceSchema });

  app.delete("/v1/instances/:id", ({ params, set, request }) => {
    const auth = getAuthContext(request);
    if (!auth || !checkPermission(auth, "manage_resources")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }
    const id = Number(params.id);
    const instance = getInstanceRecordRaw(id);
    if (!instance || instance.tenant_id !== auth.tenantId) {
      set.status = 404;
      return { error: { code: "INSTANCE_NOT_FOUND", message: `Instance ${id} not found`, category: "not_found" } };
    }
    try {
      deleteInstance(id);
    } catch (err) {
      if (err instanceof SkyREPLError) {
        set.status = httpStatusForError(err);
        return errorToApiError(err);
      }
      // ConflictError from deleteInstance (active allocations)
      set.status = 409;
      return { error: { code: "CONFLICT", message: err instanceof Error ? err.message : "Cannot delete instance", category: "conflict" } };
    }
    return { data: { id, deleted: true } };
  }, { params: IdParams });

  // ─── Runs: PATCH + DELETE ────────────────────────────────────────────

  app.patch("/v1/runs/:id", ({ params, body, set, request }) => {
    const auth = getAuthContext(request);
    if (!auth || !checkPermission(auth, "manage_resources")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }
    const id = Number(params.id);
    const run = getRunRecordRaw(id);
    if (!run || run.tenant_id !== auth.tenantId) {
      set.status = 404;
      return { error: { code: "RUN_NOT_FOUND", message: `Run ${id} not found`, category: "not_found" } };
    }
    const allowed = ["workflow_state", "workflow_error", "exit_code", "started_at", "finished_at"] as const;
    const updates: Record<string, unknown> = {};
    for (const field of allowed) {
      if (body !== null && body !== undefined && field in body) updates[field] = (body as Record<string, unknown>)[field];
    }
    if (Object.keys(updates).length === 0) {
      set.status = 400;
      return { error: { code: "INVALID_INPUT", message: "No updatable fields provided", category: "validation" } };
    }
    const updated = updateRun(id, updates);
    return { data: updated };
  }, { params: IdParams, body: PatchRunSchema });

  app.delete("/v1/runs/:id", ({ params, set, request }) => {
    const auth = getAuthContext(request);
    if (!auth || !checkPermission(auth, "manage_resources")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }
    const id = Number(params.id);
    const run = getRunRecordRaw(id);
    if (!run || run.tenant_id !== auth.tenantId) {
      set.status = 404;
      return { error: { code: "RUN_NOT_FOUND", message: `Run ${id} not found`, category: "not_found" } };
    }
    try {
      deleteRun(id);
    } catch (err) {
      if (err instanceof SkyREPLError) {
        set.status = httpStatusForError(err);
        return errorToApiError(err);
      }
      // ConflictError from deleteRun (active run)
      set.status = 409;
      return { error: { code: "CONFLICT", message: err instanceof Error ? err.message : "Cannot delete run", category: "conflict" } };
    }
    return { data: { id, deleted: true } };
  }, { params: IdParams });

  // ─── Allocations ─────────────────────────────────────────────────────

  app.get("/v1/allocations", ({ query, request }: { query: Record<string, unknown>; request: Request }) => {
    const auth = getAuthContext(request);
    const filters = buildAllocationFilters(query, auth?.tenantId);
    // JOIN instances to include IP for SSH config generation.
    const limit = Math.min(Number(query.limit) || 50, 500);
    const cursor = typeof query.cursor === "string" ? query.cursor : undefined;
    let cursorClause = "";
    const cursorValues: unknown[] = [];
    if (cursor) {
      const decoded = decodeCursor(cursor);
      cursorClause = " AND (a.created_at, a.id) < (?, ?)";
      cursorValues.push(decoded.value, decoded.id);
    }
    const sql = `
      SELECT a.*, i.ip as instance_ip, i.provider as instance_provider
      FROM allocations a
      LEFT JOIN instances i ON a.instance_id = i.id
      WHERE 1=1 ${filters.where}${cursorClause}
      ORDER BY a.created_at DESC
      LIMIT ${limit + 1}
    `;
    const rows = queryMany<Allocation & { instance_ip: string | null; instance_provider: string | null }>(
      sql, [...filters.values, ...cursorValues]
    );
    const hasMore = rows.length > limit;
    const data = (hasMore ? rows.slice(0, limit) : rows).map(stampMaterialized);
    const lastRow = data[data.length - 1];
    return {
      data,
      pagination: {
        limit,
        has_more: hasMore,
        next_cursor: hasMore && lastRow ? encodeCursor(lastRow.created_at, lastRow.id) : undefined,
      },
    };
  });

  // ─── Allocations: Detail + PATCH ─────────────────────────────────────

  app.get("/v1/allocations/:id", ({ params, set, request }) => {
    const auth = getAuthContext(request);
    const id = Number(params.id);
    const allocation = materializeAllocation(id);
    if (!allocation || (auth && allocation.tenant_id !== auth.tenantId)) {
      set.status = 404;
      return { error: { code: "ALLOCATION_NOT_FOUND", message: `Allocation ${id} not found`, category: "not_found" } };
    }
    return { data: allocation };
  }, { params: IdParams });

  app.patch("/v1/allocations/:id", ({ params, body, set, request }) => {
    const auth = getAuthContext(request);
    if (!auth || !checkPermission(auth, "manage_resources")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Admin only", category: "auth" } };
    }
    const id = Number(params.id);
    const allocation = getAllocation(id);
    if (!allocation || allocation.tenant_id !== auth.tenantId) {
      set.status = 404;
      return { error: { code: "ALLOCATION_NOT_FOUND", message: `Allocation ${id} not found`, category: "not_found" } };
    }
    const allowed = ["status", "debug_hold_until", "completed_at"] as const;
    const updates: Record<string, unknown> = {};
    for (const field of allowed) {
      if (body !== null && body !== undefined && field in body) updates[field] = (body as Record<string, unknown>)[field];
    }
    if (Object.keys(updates).length === 0) {
      set.status = 400;
      return { error: { code: "INVALID_INPUT", message: "No updatable fields provided", category: "validation" } };
    }
    const updated = updateAllocation(id, updates as any);
    return { data: updated };
  }, { params: IdParams, body: PatchAllocationSchema });

  // ─── Manifests (read-only) ────────────────────────────────────────────

  app.get("/v1/manifests", ({ query, request }: { query: Record<string, unknown>; request: Request }) => {
    const auth = getAuthContext(request);
    const filters = buildManifestFilters(query, auth?.tenantId);
    const result = paginatedList<Manifest>("manifests", filters, query);
    return { ...result, data: result.data.map(stampMaterialized) };
  });

  app.get("/v1/manifests/:id", ({ params, set, request }) => {
    const auth = getAuthContext(request);
    const id = Number(params.id);
    const manifest = materializeManifest(id);
    if (!manifest || (auth && manifest.tenant_id !== auth.tenantId)) {
      set.status = 404;
      return { error: { code: "MANIFEST_NOT_FOUND", message: `Manifest ${id} not found`, category: "not_found" } };
    }
    const resources = getManifestResources(id);
    return { data: { ...manifest, resources } };
  }, { params: IdParams });

  // ─── Objects (read-only) ──────────────────────────────────────────────

  app.get("/v1/objects", ({ query, request }: { query: Record<string, unknown>; request: Request }) => {
    const auth = getAuthContext(request);
    const filters = buildObjectFilters(query, auth?.tenantId);
    // Support ?type= filter on top of tenant scoping
    if (query.type) {
      filters.where += " AND type = ?";
      filters.values.push(query.type);
    }
    return paginatedList<StorageObject>("objects", filters, query);
  });

  app.get("/v1/objects/:id", ({ params, set, request }) => {
    const auth = getAuthContext(request);
    const id = Number(params.id);
    const obj = getObject(id);
    if (!obj || (auth && obj.tenant_id !== auth.tenantId)) {
      set.status = 404;
      return { error: { code: "OBJECT_NOT_FOUND", message: `Object ${id} not found`, category: "not_found" } };
    }
    return { data: obj };
  }, { params: IdParams });

  // ─── Workflows ───────────────────────────────────────────────────────

  app.get("/v1/workflows", ({ query, request }: { query: Record<string, unknown>; request: Request }) => {
    const auth = getAuthContext(request);
    const filters = buildWorkflowFilters(query, auth?.tenantId);
    const result = paginatedList<Workflow>("workflows", filters, query);
    return { ...result, data: result.data.map(stampMaterialized) };
  });

  app.get("/v1/workflows/:id", ({ params, query, set, request }) => {
    const auth = getAuthContext(request);
    const id = params.id;
    const workflow = getWorkflow(id);
    if (!workflow || (auth && workflow.tenant_id !== auth.tenantId)) {
      return new Response(
        JSON.stringify({ error: { code: "WORKFLOW_NOT_FOUND", message: `Workflow ${id} not found`, category: "not_found" } }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      );
    }

    const result: Record<string, unknown> = { data: { ...materializeWorkflow(id)! } };
    const include = typeof query.include === "string" ? query.include.split(",") : [];

    if (include.includes("nodes")) {
      (result.data as Record<string, unknown>).nodes = getWorkflowNodes(id);
    }
    if (include.includes("manifest") && workflow.manifest_id) {
      (result.data as Record<string, unknown>).manifest = getManifest(workflow.manifest_id);
    }

    return result;
  }, { params: IdParams });

  app.get("/v1/workflows/:id/nodes", ({ params, set, request }) => {
    const auth = getAuthContext(request);
    const id = params.id;
    const workflow = getWorkflow(id);
    if (!workflow || (auth && workflow.tenant_id !== auth.tenantId)) {
      return new Response(
        JSON.stringify({ error: { code: "WORKFLOW_NOT_FOUND", message: `Workflow ${id} not found`, category: "not_found" } }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      );
    }

    const nodes = getWorkflowNodes(id);
    return { data: nodes };
  }, { params: IdParams });
}
