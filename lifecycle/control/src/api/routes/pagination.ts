// api/routes/pagination.ts — Pagination helpers and filter builders

import { queryMany } from "../../material/db";
import type { PaginationMeta, ListRequest } from "./server";

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
// Paginated List Helper (API-02)
// =============================================================================

/**
 * Generic paginated list for simple table queries (no JOINs).
 * Supports cursor-based pagination via ?cursor=<token>&limit=N.
 */
export const VALID_TABLES = new Set(["runs", "instances", "allocations", "manifests", "objects", "workflows"]);

export function paginatedList<T extends { id: number; created_at: number }>(
  table: string,
  filters: { where: string; values: unknown[] },
  query: Record<string, unknown>,
): { data: T[]; pagination: { limit: number; has_more: boolean; next_cursor?: string } } {
  if (!VALID_TABLES.has(table)) {
    throw new Error(`Invalid table name: ${table}`);
  }
  const limit = Math.min(Number(query.limit) || 50, 500);
  const cursor = typeof query.cursor === "string" ? query.cursor : undefined;

  let cursorClause = "";
  const cursorValues: unknown[] = [];
  if (cursor) {
    const decoded = decodeCursor(cursor);
    cursorClause = " AND (created_at, id) < (?, ?)";
    cursorValues.push(decoded.value, decoded.id);
  }

  const sql = `SELECT * FROM ${table} WHERE 1=1 ${filters.where}${cursorClause} ORDER BY created_at DESC LIMIT ${limit + 1}`;
  const rows = queryMany<T>(sql, [...filters.values, ...cursorValues]);

  const hasMore = rows.length > limit;
  const data = hasMore ? rows.slice(0, limit) : rows;
  const lastRow = data[data.length - 1];

  return {
    data,
    pagination: {
      limit,
      has_more: hasMore,
      next_cursor: hasMore && lastRow ? encodeCursor(lastRow.created_at, lastRow.id) : undefined,
    },
  };
}

// =============================================================================
// Filter Builders
// =============================================================================

export function buildRunFilters(query: Record<string, unknown>, tenantId?: number): {
  where: string;
  values: unknown[];
} {
  const clauses: string[] = [];
  const values: unknown[] = [];
  if (tenantId !== undefined) {
    clauses.push("tenant_id = ?");
    values.push(tenantId);
  }

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

export function buildInstanceFilters(query: Record<string, unknown>, tenantId?: number): {
  where: string;
  values: unknown[];
} {
  const clauses: string[] = [];
  const values: unknown[] = [];
  if (tenantId !== undefined) {
    clauses.push("tenant_id = ?");
    values.push(tenantId);
  }

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

export function buildAllocationFilters(query: Record<string, unknown>, tenantId?: number): {
  where: string;
  values: unknown[];
} {
  const clauses: string[] = [];
  const values: unknown[] = [];
  if (tenantId !== undefined) {
    clauses.push("a.tenant_id = ?");
    values.push(tenantId);
  }

  if (query.status) {
    clauses.push("a.status = ?");
    values.push(query.status);
  }
  if (query.instance_id) {
    clauses.push("a.instance_id = ?");
    values.push(query.instance_id);
  }
  if (query.run_id) {
    clauses.push("a.run_id = ?");
    values.push(query.run_id);
  }
  // slug: base36 allocation ID for SSH fallback lookup
  if (query.slug) {
    const slugId = parseInt(String(query.slug), 36);
    if (Number.isFinite(slugId) && slugId > 0) {
      clauses.push("a.id = ?");
      values.push(slugId);
    } else {
      // Invalid slug — force no results
      clauses.push("1 = 0");
    }
  }

  return {
    where: clauses.length > 0 ? "AND " + clauses.join(" AND ") : "",
    values,
  };
}

export function buildWorkflowFilters(query: Record<string, unknown>, tenantId?: number): {
  where: string;
  values: unknown[];
} {
  const clauses: string[] = [];
  const values: unknown[] = [];
  if (tenantId !== undefined) {
    clauses.push("tenant_id = ?");
    values.push(tenantId);
  }

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

export function buildManifestFilters(query: Record<string, unknown>, tenantId?: number): {
  where: string;
  values: unknown[];
} {
  const clauses: string[] = [];
  const values: unknown[] = [];
  if (tenantId !== undefined) {
    clauses.push("tenant_id = ?");
    values.push(tenantId);
  }
  return {
    where: clauses.length > 0 ? "AND " + clauses.join(" AND ") : "",
    values,
  };
}

export function buildObjectFilters(query: Record<string, unknown>, tenantId?: number): {
  where: string;
  values: unknown[];
} {
  const clauses: string[] = [];
  const values: unknown[] = [];
  if (tenantId !== undefined) {
    clauses.push("tenant_id = ?");
    values.push(tenantId);
  }
  return {
    where: clauses.length > 0 ? "AND " + clauses.join(" AND ") : "",
    values,
  };
}
