// ─────────────────────────────────────────────────────────────────────────────
// RAW DB LAYER — runs table
// Business code should use materializeRun() from resource/run.ts,
// not these functions directly. @see resource/run.ts
// DB operations below — add new queries here, not at call sites.
// ─────────────────────────────────────────────────────────────────────────────
// db/runs.ts - Run CRUD

import { type SQLQueryBindings } from "bun:sqlite";
import { ConflictError } from "@skyrepl/contracts";
import { getDatabase, queryOne, queryMany, execute } from "./helpers";

export interface Run {
  id: number;
  tenant_id: number;
  command: string;
  workdir: string;
  max_duration_ms: number;
  workflow_state: string;
  workflow_error: string | null;
  current_manifest_id: number | null;
  exit_code: number | null;
  init_checksum: string | null;
  create_snapshot: number;
  spot_interrupted: number;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
}

/** @see resource/run.ts — use materializeRun() for business reads */
export function getRun(id: number): Run | null {
  return queryOne<Run>("SELECT * FROM runs WHERE id = ?", [id]);
}

export function createRun(data: Omit<Run, "id" | "created_at" | "tenant_id">, tenantId: number = 1): Run {
  const now = Date.now();
  const db = getDatabase();

  const stmt = db.prepare(`
    INSERT INTO runs (tenant_id, command, workdir, max_duration_ms, workflow_state, workflow_error, current_manifest_id, exit_code, init_checksum, create_snapshot, spot_interrupted, created_at, started_at, finished_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const result = stmt.run(
    tenantId,
    data.command,
    data.workdir,
    data.max_duration_ms,
    data.workflow_state,
    data.workflow_error,
    data.current_manifest_id,
    data.exit_code,
    data.init_checksum,
    data.create_snapshot,
    data.spot_interrupted,
    now,
    data.started_at,
    data.finished_at
  );

  return getRun(result.lastInsertRowid as number)!;
}

export function updateRun(id: number, updates: Partial<Run>): Run {
  const db = getDatabase();

  const fields = Object.keys(updates).filter(k => k !== "id");
  const setClause = fields.map(f => `${f} = ?`).join(", ");
  const values = fields.map(f => updates[f as keyof Run]);

  db.prepare(`UPDATE runs SET ${setClause} WHERE id = ?`)
    .run(...([...values, id] as SQLQueryBindings[]));

  return getRun(id)!;
}

/** @see resource/run.ts — use materializeRun() for business reads */
export function listRuns(filter?: {
  current_manifest_id?: number;
  workflow_state?: string;
}): Run[] {
  let sql = "SELECT * FROM runs WHERE 1=1";
  const params: unknown[] = [];

  if (filter?.current_manifest_id) {
    sql += " AND current_manifest_id = ?";
    params.push(filter.current_manifest_id);
  }
  if (filter?.workflow_state) {
    sql += " AND workflow_state = ?";
    params.push(filter.workflow_state);
  }

  return queryMany<Run>(sql, params);
}

export function deleteRun(id: number): void {
  const run = getRun(id);
  if (run && !["completed", "failed"].includes(run.workflow_state.split(":")[1] ?? "")) {
    throw new ConflictError("Cannot delete active run");
  }

  execute("DELETE FROM runs WHERE id = ?", [id]);
}
