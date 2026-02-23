// db/instances.ts - Instance CRUD

import { type SQLQueryBindings } from "bun:sqlite";
import { ConflictError } from "@skyrepl/contracts";
import { getDatabase, queryOne, queryMany } from "./helpers";

export interface Instance {
  id: number;
  tenant_id: number;
  provider: string;
  provider_id: string;
  spec: string;
  region: string;
  ip: string | null;
  workflow_state: string;
  workflow_error: string | null;
  current_manifest_id: number | null;
  spawn_idempotency_key: string | null;
  is_spot: number;
  spot_request_id: string | null;
  init_checksum: string | null;
  registration_token_hash: string | null;
  provider_metadata: string | null;
  created_at: number;
  last_heartbeat: number;
}

export function getInstance(id: number): Instance | null {
  return queryOne<Instance>("SELECT * FROM instances WHERE id = ?", [id]);
}

export function getInstanceByProviderId(
  provider: string,
  providerId: string
): Instance | null {
  return queryOne<Instance>(
    "SELECT * FROM instances WHERE provider = ? AND provider_id = ?",
    [provider, providerId]
  );
}

export function createInstance(
  data: Omit<Instance, "id" | "created_at" | "tenant_id">,
  tenantId: number = 1
): Instance {
  const now = Date.now();
  const db = getDatabase();

  const stmt = db.prepare(`
    INSERT INTO instances (tenant_id, provider, provider_id, spec, region, ip, workflow_state, workflow_error, current_manifest_id, spawn_idempotency_key, is_spot, spot_request_id, init_checksum, registration_token_hash, provider_metadata, created_at, last_heartbeat)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const result = stmt.run(
    tenantId,
    data.provider,
    data.provider_id,
    data.spec,
    data.region,
    data.ip,
    data.workflow_state,
    data.workflow_error,
    data.current_manifest_id,
    data.spawn_idempotency_key,
    data.is_spot,
    data.spot_request_id,
    data.init_checksum,
    data.registration_token_hash,
    data.provider_metadata,
    now,
    data.last_heartbeat
  );

  return getInstance(result.lastInsertRowid as number)!;
}

export function updateInstance(
  id: number,
  updates: Partial<Instance>
): Instance {
  const db = getDatabase();

  const fields = Object.keys(updates).filter(k => k !== "id");
  const setClause = fields.map(f => `${f} = ?`).join(", ");
  const values = fields.map(f => updates[f as keyof Instance]);

  db.prepare(`UPDATE instances SET ${setClause} WHERE id = ?`)
    .run(...([...values, id] as SQLQueryBindings[]));

  return getInstance(id)!;
}

export function listInstances(filter?: {
  provider?: string;
  workflow_state?: string;
}): Instance[] {
  let sql = "SELECT * FROM instances WHERE 1=1";
  const params: unknown[] = [];

  if (filter?.provider) {
    sql += " AND provider = ?";
    params.push(filter.provider);
  }
  if (filter?.workflow_state) {
    sql += " AND workflow_state = ?";
    params.push(filter.workflow_state);
  }

  return queryMany<Instance>(sql, params);
}

export function deleteInstance(id: number): void {
  const db = getDatabase();

  // Check no active allocations
  const active = queryOne<{ id: number }>(
    `SELECT id FROM allocations WHERE instance_id = ? AND status NOT IN ('COMPLETE', 'FAILED')`,
    [id]
  );

  if (active) {
    throw new ConflictError("Cannot delete instance with active allocations");
  }

  db.prepare("DELETE FROM instances WHERE id = ?").run(id);
}
