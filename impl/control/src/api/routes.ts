// api/routes.ts - Route Registration and Resource Handlers

import { Elysia } from "elysia";
import {
  queryOne,
  queryMany,
  getWorkflow,
  getWorkflowNodes,
  getRun,
  getInstance,
  type Workflow,
  type WorkflowNode,
  type Run,
  type Instance,
  type Allocation,
} from "../material/db";
import { launchRun } from "../intent/launch-run";
import type { LaunchRunInput } from "../intent/launch-run.types";
import {
  SkyREPLError,
  httpStatusForError,
  errorToApiError,
} from "@skyrepl/shared";

// =============================================================================
// Types
// =============================================================================

export interface ServerConfig {
  port: number;
  corsOrigins: string[];
  maxBodySize: number;
}

export interface PaginationMeta {
  total: number;
  limit: number;
  offset: number;
  hasMore: boolean;
  nextCursor?: string;
}

export interface ListRequest {
  limit?: number;
  cursor?: string;
  sortBy?: string;
  sortOrder?: "asc" | "desc";
}

// =============================================================================
// Server Setup
// =============================================================================

export function createServer(config: ServerConfig): Elysia {
  const app = new Elysia();

  // Global error handler: catch errors and return structured JSON
  app.onError(({ error, set }) => {
    // If it's a SkyREPLError, use its category for HTTP status
    if (error instanceof SkyREPLError) {
      set.status = httpStatusForError(error);
      return errorToApiError(error);
    }

    // Elysia validation errors (from TypeBox)
    if (error && typeof error === "object" && "code" in error) {
      const e = error as Record<string, unknown>;
      if (e.code === "VALIDATION" || e.code === "PARSE") {
        set.status = 400;
        return {
          error: {
            code: "INVALID_INPUT",
            message: String(e.message ?? "Validation error"),
            category: "validation",
          },
        };
      }
      if (e.code === "NOT_FOUND") {
        set.status = 404;
        return {
          error: {
            code: "NOT_FOUND",
            message: String(e.message ?? "Not found"),
            category: "not_found",
          },
        };
      }
    }

    // Catch-all: internal error
    console.error("[routes] Unhandled error:", error);
    set.status = 500;
    return {
      error: {
        code: "INTERNAL_ERROR",
        message: error instanceof Error ? error.message : "Internal server error",
        category: "internal",
      },
    };
  });

  // Health check
  app.get("/v1/health", () => ({
    status: "ok",
    timestamp: Date.now(),
  }));

  // Register route groups
  registerResourceRoutes(app);
  registerOperationRoutes(app);

  return app;
}

// =============================================================================
// Route Registration
// =============================================================================

export function registerResourceRoutes(app: Elysia<any>): void {
  // ─── Runs ────────────────────────────────────────────────────────────

  app.get("/v1/runs", ({ query }: { query: Record<string, unknown> }) => {
    const filters = buildRunFilters(query);
    let sql = `SELECT * FROM runs WHERE 1=1 ${filters.where}`;
    sql += " ORDER BY created_at DESC";
    const rows = queryMany<Run>(sql, filters.values);
    return { data: rows };
  });

  app.get("/v1/runs/:id", ({ params }: { params: { id: string } }) => {
    const id = parseInt(params.id, 10);
    if (isNaN(id)) {
      return { error: { code: "INVALID_INPUT", message: "Invalid run ID", category: "validation" } };
    }
    const run = getRun(id);
    if (!run) {
      return new Response(
        JSON.stringify({ error: { code: "RUN_NOT_FOUND", message: `Run ${id} not found`, category: "not_found" } }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      );
    }
    return { data: run };
  });

  // ─── Instances ───────────────────────────────────────────────────────

  app.get("/v1/instances", ({ query }: { query: Record<string, unknown> }) => {
    const filters = buildInstanceFilters(query);
    let sql = `SELECT * FROM instances WHERE 1=1 ${filters.where}`;
    sql += " ORDER BY created_at DESC";
    const rows = queryMany<Instance>(sql, filters.values);
    return { data: rows };
  });

  app.get("/v1/instances/:id", ({ params }: { params: { id: string } }) => {
    const id = parseInt(params.id, 10);
    if (isNaN(id)) {
      return { error: { code: "INVALID_INPUT", message: "Invalid instance ID", category: "validation" } };
    }
    const instance = getInstance(id);
    if (!instance) {
      return new Response(
        JSON.stringify({ error: { code: "INSTANCE_NOT_FOUND", message: `Instance ${id} not found`, category: "not_found" } }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      );
    }
    return { data: instance };
  });

  // ─── Allocations ─────────────────────────────────────────────────────

  app.get("/v1/allocations", ({ query }: { query: Record<string, unknown> }) => {
    const filters = buildAllocationFilters(query);
    let sql = `SELECT * FROM allocations WHERE 1=1 ${filters.where}`;
    sql += " ORDER BY created_at DESC";
    const rows = queryMany<Allocation>(sql, filters.values);
    return { data: rows };
  });

  // ─── Workflows ───────────────────────────────────────────────────────

  app.get("/v1/workflows", ({ query }: { query: Record<string, unknown> }) => {
    const filters = buildWorkflowFilters(query);
    let sql = `SELECT * FROM workflows WHERE 1=1 ${filters.where}`;
    sql += " ORDER BY created_at DESC";
    const rows = queryMany<Workflow>(sql, filters.values);
    return { data: rows };
  });

  app.get("/v1/workflows/:id", ({ params, query }: { params: { id: string }; query: Record<string, unknown> }) => {
    const id = parseInt(params.id, 10);
    if (isNaN(id)) {
      return { error: { code: "INVALID_INPUT", message: "Invalid workflow ID", category: "validation" } };
    }
    const workflow = getWorkflow(id);
    if (!workflow) {
      return new Response(
        JSON.stringify({ error: { code: "WORKFLOW_NOT_FOUND", message: `Workflow ${id} not found`, category: "not_found" } }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      );
    }

    const result: Record<string, unknown> = { data: { ...workflow } };
    const include = typeof query.include === "string" ? query.include.split(",") : [];

    if (include.includes("nodes")) {
      (result.data as Record<string, unknown>).nodes = getWorkflowNodes(id);
    }
    if (include.includes("manifest") && workflow.manifest_id) {
      (result.data as Record<string, unknown>).manifest = queryOne(
        "SELECT * FROM manifests WHERE id = ?",
        [workflow.manifest_id]
      );
    }

    return result;
  });

  app.get("/v1/workflows/:id/nodes", ({ params }: { params: { id: string } }) => {
    const id = parseInt(params.id, 10);
    if (isNaN(id)) {
      return { error: { code: "INVALID_INPUT", message: "Invalid workflow ID", category: "validation" } };
    }
    const workflow = getWorkflow(id);
    if (!workflow) {
      return new Response(
        JSON.stringify({ error: { code: "WORKFLOW_NOT_FOUND", message: `Workflow ${id} not found`, category: "not_found" } }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      );
    }

    const nodes = getWorkflowNodes(id);
    return { data: nodes };
  });
}

export function registerOperationRoutes(app: Elysia<any>): void {
  // ─── Launch Run ──────────────────────────────────────────────────────

  app.post("/v1/workflows/launch-run", async ({ body, set }: { body: unknown; set: { status?: number | string } }) => {
    const b = body as Record<string, unknown>;

    // Validate required fields
    if (!b.command || typeof b.command !== "string") {
      set.status = 400;
      return {
        error: { code: "INVALID_INPUT", message: "command is required and must be a string", category: "validation" },
      };
    }
    if (!b.spec || typeof b.spec !== "string") {
      set.status = 400;
      return {
        error: { code: "INVALID_INPUT", message: "spec is required and must be a string", category: "validation" },
      };
    }
    if (!b.maxDurationMs && !b.max_duration_ms) {
      set.status = 400;
      return {
        error: { code: "INVALID_INPUT", message: "maxDurationMs is required", category: "validation" },
      };
    }

    // Build LaunchRunInput (runId will be assigned by launchRun after creating the Run record)
    const input: Omit<LaunchRunInput, "runId"> = {
      command: b.command as string,
      spec: b.spec as string,
      workdir: (b.workdir as string) ?? "/workspace",
      provider: (b.provider as string) ?? "orbstack",
      region: (b.region as string) ?? "local",
      env: (b.env as Record<string, string>) ?? undefined,
      maxDurationMs: (b.maxDurationMs ?? b.max_duration_ms) as number,
      holdDurationMs: (b.holdDurationMs ?? b.hold_duration_ms ?? 300_000) as number,
      createSnapshot: (b.createSnapshot ?? b.create_snapshot ?? false) as boolean,
      initChecksum: (b.initChecksum ?? b.init_checksum ?? undefined) as string | undefined,
      files: (b.files as LaunchRunInput["files"]) ?? [],
      artifactPatterns: (b.artifactPatterns ?? b.artifact_patterns ?? []) as string[],
      idempotencyKey: (b.idempotencyKey ?? b.idempotency_key) as string | undefined,
    };

    try {
      // launchRun creates the Run record internally, so it doesn't need runId
      const workflow = await launchRun(input as LaunchRunInput);
      const workflowInput = workflow.input_json ? JSON.parse(workflow.input_json) : {};
      set.status = 202;
      return {
        workflow_id: workflow.id,
        run_id: workflowInput.runId ?? null,
        status: workflow.status,
        status_url: `/v1/workflows/${workflow.id}/status`,
      };
    } catch (err) {
      if (err instanceof SkyREPLError) {
        set.status = httpStatusForError(err);
        return errorToApiError(err);
      }
      console.error("[routes] launch-run error:", err);
      set.status = 500;
      return {
        error: {
          code: "INTERNAL_ERROR",
          message: err instanceof Error ? err.message : "Failed to launch run",
          category: "internal",
        },
      };
    }
  });

  // ─── Workflow Status ──────────────────────────────────────────────────

  app.get("/v1/workflows/:id/status", ({ params }: { params: { id: string } }) => {
    const id = parseInt(params.id, 10);
    if (isNaN(id)) {
      return { error: { code: "INVALID_INPUT", message: "Invalid workflow ID", category: "validation" } };
    }

    const workflow = getWorkflow(id);
    if (!workflow) {
      return new Response(
        JSON.stringify({ error: { code: "WORKFLOW_NOT_FOUND", message: `Workflow ${id} not found`, category: "not_found" } }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      );
    }

    const nodes = getWorkflowNodes(id);
    const completedNodes = nodes.filter((n) => n.status === "completed").length;
    const failedNodes = nodes.filter((n) => n.status === "failed").length;
    const totalNodes = nodes.length;

    const result: Record<string, unknown> = {
      workflowId: workflow.id,
      type: workflow.type,
      status: workflow.status,
      currentNode: workflow.current_node,
      nodesTotal: totalNodes,
      nodesCompleted: completedNodes,
      nodesFailed: failedNodes,
      startedAt: workflow.started_at,
      finishedAt: workflow.finished_at,
      progress: {
        completed_nodes: completedNodes,
        total_nodes: totalNodes,
        percentage: totalNodes > 0 ? Math.round((completedNodes / totalNodes) * 100) : 0,
      },
      output: workflow.output_json ? JSON.parse(workflow.output_json) : null,
      error: null as unknown,
    };

    if (workflow.status === "failed") {
      const failedNode = nodes.find((n) => n.status === "failed");
      if (failedNode?.error_json) {
        try {
          const errorData = JSON.parse(failedNode.error_json);
          result.error = {
            code: errorData.code ?? "INTERNAL_ERROR",
            message: errorData.message ?? "Workflow failed",
            category: errorData.category ?? "internal",
            nodeId: failedNode.node_id,
            details: errorData.details,
          };
        } catch {
          result.error = {
            code: "INTERNAL_ERROR",
            message: "Workflow failed",
            category: "internal",
            nodeId: failedNode.node_id,
          };
        }
      } else if (workflow.error_json) {
        try {
          result.error = JSON.parse(workflow.error_json);
        } catch {
          result.error = {
            code: "INTERNAL_ERROR",
            message: "Workflow failed",
            category: "internal",
          };
        }
      }
    }

    return result;
  });

  // ─── Blob Check ───────────────────────────────────────────────────────

  app.post("/v1/blobs/check", ({ body }: { body: unknown }) => {
    const b = body as Record<string, unknown>;
    const checksums = (b.checksums as string[]) ?? [];

    // For Slice 1: no S3, return all as missing with empty URLs
    return {
      missing: checksums,
      urls: {} as Record<string, string>,
    };
  });

  // ─── Blob Download (by ID) ──────────────────────────────────────────

  app.get("/v1/blobs/:id/download", ({ params, set }: { params: { id: string }; set: { status?: number | string } }) => {
    const id = parseInt(params.id, 10);
    if (isNaN(id)) {
      set.status = 400;
      return { error: { code: "INVALID_INPUT", message: "Invalid blob ID", category: "validation" } };
    }
    const blob = queryOne<{ id: number; payload: Buffer | null; size_bytes: number }>(
      "SELECT id, payload, size_bytes FROM blobs WHERE id = ?",
      [id]
    );
    if (!blob) {
      set.status = 404;
      return { error: { code: "BLOB_NOT_FOUND", message: `Blob ${id} not found`, category: "not_found" } };
    }
    if (!blob.payload) {
      set.status = 404;
      return { error: { code: "BLOB_NOT_INLINE", message: "Blob not stored inline (S3 not available in Slice 1)", category: "not_found" } };
    }
    return new Response(new Uint8Array(blob.payload), {
      headers: {
        "Content-Type": "application/octet-stream",
        "Content-Length": String(blob.size_bytes),
      },
    });
  });

  // ─── Blob Download (by checksum) ──────────────────────────────────
  // Agent bridge sends /v1/blobs/<checksum> as file URLs in start_run messages

  app.get("/v1/blobs/by-checksum/:id", ({ params, set }: { params: { id: string }; set: { status?: number | string } }) => {
    const checksum = decodeURIComponent(params.id);
    const blob = queryOne<{ id: number; payload: Buffer | null; size_bytes: number }>(
      "SELECT id, payload, size_bytes FROM blobs WHERE checksum = ?",
      [checksum]
    );
    if (!blob) {
      set.status = 404;
      return { error: { code: "BLOB_NOT_FOUND", message: `Blob with checksum ${checksum} not found`, category: "not_found" } };
    }
    if (!blob.payload) {
      set.status = 404;
      return { error: { code: "BLOB_NOT_INLINE", message: "Blob not stored inline (S3 not available in Slice 1)", category: "not_found" } };
    }
    return new Response(new Uint8Array(blob.payload), {
      headers: {
        "Content-Type": "application/octet-stream",
        "Content-Length": String(blob.size_bytes),
      },
    });
  });
}

// =============================================================================
// Pagination Helpers
// =============================================================================

export function buildPaginatedQuery(
  baseQuery: string,
  params: ListRequest,
  allowedSortFields: string[],
): { sql: string; values: unknown[]; countSql: string } {
  const values: unknown[] = [];

  // Sort
  const sort =
    params.sortBy && allowedSortFields.includes(params.sortBy)
      ? params.sortBy
      : "created_at";
  const order = params.sortOrder === "asc" ? "ASC" : "DESC";

  let where = "";

  // Cursor-based pagination
  if (params.cursor) {
    const decoded = decodeCursor(params.cursor);
    where += ` AND (${sort}, id) < (?, ?)`;
    values.push(decoded.value, decoded.id);
  }

  const limit = Math.min(params.limit ?? 50, 500);

  const sql = `${baseQuery} ${where} ORDER BY ${sort} ${order} LIMIT ${limit + 1}`;
  const countSql = `SELECT COUNT(*) as count FROM (${baseQuery} ${where})`;

  return { sql, values, countSql };
}

export function encodeCursor(sortValue: unknown, id: number): string {
  return Buffer.from(JSON.stringify({ value: sortValue, id })).toString("base64url");
}

export function decodeCursor(cursor: string): { value: unknown; id: number } {
  return JSON.parse(Buffer.from(cursor, "base64url").toString());
}

export function buildPaginationMeta<T extends { id: number }>(
  rows: T[],
  limit: number,
  sortField: string,
  countTotal?: boolean,
): { data: T[]; pagination: PaginationMeta } {
  const hasMore = rows.length > limit;
  const data = hasMore ? rows.slice(0, limit) : rows;
  const lastRow = data[data.length - 1];

  return {
    data,
    pagination: {
      total: 0, // Only populated if count=true requested
      limit,
      offset: 0,
      hasMore,
      nextCursor:
        hasMore && lastRow
          ? encodeCursor((lastRow as Record<string, unknown>)[sortField], lastRow.id)
          : undefined,
    },
  };
}

// =============================================================================
// Filter Builders
// =============================================================================

export function buildRunFilters(query: Record<string, unknown>): {
  where: string;
  values: unknown[];
} {
  const clauses: string[] = [];
  const values: unknown[] = [];

  if (query.status) {
    const statuses = String(query.status).split(",");
    clauses.push(`workflow_state IN (${statuses.map(() => "?").join(",")})`);
    values.push(...statuses);
  }
  if (query.created_after) {
    clauses.push("created_at >= ?");
    values.push(query.created_after);
  }
  if (query.created_before) {
    clauses.push("created_at <= ?");
    values.push(query.created_before);
  }

  return {
    where: clauses.length > 0 ? "AND " + clauses.join(" AND ") : "",
    values,
  };
}

export function buildInstanceFilters(query: Record<string, unknown>): {
  where: string;
  values: unknown[];
} {
  const clauses: string[] = [];
  const values: unknown[] = [];

  if (query.provider) {
    clauses.push("provider = ?");
    values.push(query.provider);
  }
  if (query.status) {
    const statuses = String(query.status).split(",");
    clauses.push(`workflow_state IN (${statuses.map(() => "?").join(",")})`);
    values.push(...statuses);
  }
  if (query.spec) {
    clauses.push("spec = ?");
    values.push(query.spec);
  }

  return {
    where: clauses.length > 0 ? "AND " + clauses.join(" AND ") : "",
    values,
  };
}

export function buildAllocationFilters(query: Record<string, unknown>): {
  where: string;
  values: unknown[];
} {
  const clauses: string[] = [];
  const values: unknown[] = [];

  if (query.status) {
    clauses.push("status = ?");
    values.push(query.status);
  }
  if (query.instance_id) {
    clauses.push("instance_id = ?");
    values.push(query.instance_id);
  }
  if (query.run_id) {
    clauses.push("run_id = ?");
    values.push(query.run_id);
  }

  return {
    where: clauses.length > 0 ? "AND " + clauses.join(" AND ") : "",
    values,
  };
}

export function buildWorkflowFilters(query: Record<string, unknown>): {
  where: string;
  values: unknown[];
} {
  const clauses: string[] = [];
  const values: unknown[] = [];

  if (query.type) {
    clauses.push("type = ?");
    values.push(query.type);
  }
  if (query.status) {
    const statuses = String(query.status).split(",");
    clauses.push(`status IN (${statuses.map(() => "?").join(",")})`);
    values.push(...statuses);
  }

  return {
    where: clauses.length > 0 ? "AND " + clauses.join(" AND ") : "",
    values,
  };
}

export function buildManifestFilters(query: Record<string, unknown>): {
  where: string;
  values: unknown[];
} {
  // Stub for Slice 1
  return { where: "", values: [] };
}

export function buildObjectFilters(query: Record<string, unknown>): {
  where: string;
  values: unknown[];
} {
  // Stub for Slice 1
  return { where: "", values: [] };
}
