// ─────────────────────────────────────────────────────────────────────────────
// RAW DB LAYER — manifests table
// Business code should use materializeManifest() from resource/manifest.ts,
// not these functions directly. @see resource/manifest.ts
// DB operations below — add new queries here, not at call sites.
// ─────────────────────────────────────────────────────────────────────────────
// db/manifests.ts - Manifest and ManifestResource operations

import { ConflictError } from "@skyrepl/contracts";
import { getDatabase, queryOne, queryMany, execute } from "./helpers";
import { generateUniqueName } from "../../naming/wordlist";

export interface Manifest {
  id: number;
  tenant_id: number;
  workflow_id: number;
  status: "DRAFT" | "SEALED";
  default_cleanup_priority: number;
  retention_ms: number | null;
  created_by: number | null;
  display_name: string | null;
  created_at: number;
  released_at: number | null;
  expires_at: number | null;
  updated_at: number;
}

export interface ManifestResource {
  id: number;
  manifest_id: number;
  resource_type: string;
  resource_id: string;
  cleanup_priority: number | null;
  added_at: number;
  owner_type: string;
  cleanup_processed_at: number | null;
}

export interface ManifestCleanupState {
  manifest_id: number;
  last_completed_priority: number;
  last_completed_resource_id: number;
  started_at: number;
  updated_at: number;
}

interface AddResourceOptions {
  cleanupPriority?: number;
  allowRecovery?: boolean;
  ownerType?: string;
}

/** @see resource/manifest.ts — use materializeManifest() for business reads */
export function getManifest(id: number): Manifest | null {
  return queryOne<Manifest>("SELECT * FROM manifests WHERE id = ?", [id]);
}

export function createManifest(
  workflowId: number,
  options?: { default_cleanup_priority?: number; retention_ms?: number; tenant_id?: number; created_by?: number | null; display_name?: string | null }
): Manifest {
  const now = Date.now();
  const db = getDatabase();
  const tenantId = options?.tenant_id ?? 1;

  const displayName = options?.display_name !== undefined
    ? options.display_name
    : generateUniqueName(tenantId, "manifest");

  const stmt = db.prepare(`
    INSERT INTO manifests (workflow_id, tenant_id, status, default_cleanup_priority, retention_ms, display_name, created_at, released_at, expires_at, updated_at)
    VALUES (?, ?, 'DRAFT', ?, ?, ?, ?, NULL, NULL, ?)
  `);

  const result = stmt.run(
    workflowId,
    tenantId,
    options?.default_cleanup_priority ?? 50,
    options?.retention_ms ?? null,
    displayName,
    now,
    now
  );

  return getManifest(result.lastInsertRowid as number)!;
}


export function addResourceToManifest(
  manifestId: number,
  resourceType: string,
  resourceId: string,
  options?: AddResourceOptions
): void {
  const now = Date.now();

  const manifest = getManifest(manifestId);
  if (!manifest) {
    throw new ConflictError("Manifest not found");
  }

  // Enforce immutability: SEALED manifests reject additions
  // Exception: A13 recovery path can update provider_id for orphan matching
  if (manifest.status !== "DRAFT" && !options?.allowRecovery) {
    throw new ConflictError("Cannot add resources to sealed manifest");
  }

  execute(
    `INSERT INTO manifest_resources (manifest_id, resource_type, resource_id, cleanup_priority, added_at, owner_type)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [manifestId, resourceType, resourceId, options?.cleanupPriority ?? null, now, options?.ownerType ?? 'manifest']
  );
}

export function getManifestResources(manifestId: number): ManifestResource[] {
  return queryMany<ManifestResource>(
    "SELECT * FROM manifest_resources WHERE manifest_id = ? ORDER BY cleanup_priority DESC",
    [manifestId]
  );
}

export function deleteManifest(id: number): void {
  const db = getDatabase();

  db.transaction(() => {
    // Cascade delete manifest_resources
    db.prepare("DELETE FROM manifest_resources WHERE manifest_id = ?").run(id);
    db.prepare("DELETE FROM manifests WHERE id = ?").run(id);
  })();
}

export function listExpiredManifests(cutoffTime: number): Manifest[] {
  return queryMany<Manifest>(
    `SELECT * FROM manifests
     WHERE status = ? AND expires_at < ?
     AND NOT EXISTS (
       SELECT 1 FROM manifest_resources
       WHERE manifest_resources.manifest_id = manifests.id
       AND manifest_resources.owner_type = 'manifest'
     )`,
    ["SEALED", cutoffTime]
  );
}

export function getManifestObjectIds(manifestId: number): string[] {
  const rows = queryMany<{ resource_id: string }>(
    `SELECT resource_id FROM manifest_resources
     WHERE manifest_id = ? AND resource_type = 'object'`,
    [manifestId]
  );

  return rows.map(r => r.resource_id);
}
