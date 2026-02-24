// resource/manifest.ts - Manifest Resource Operations (Ownership Boundaries)
// Resource claiming protocol: cross-manifest resource transfer, warm instance
// claiming, and manifest lifecycle helpers.
//
// MATERIALIZATION BOUNDARY — manifest resources
// READS: use materializeManifest() / materializeManifestBatch() (DB-authoritative, stamps materialized_at)
// WRITES: use createManifest(), sealManifest() from workflow/state-transitions.ts
// Manifests are DB-authoritative — no provider enrichment needed.

import { TIMING } from "@skyrepl/contracts";
import type { Materialized, MaterializeOptions } from "@skyrepl/contracts";
import {
  getManifest,
  addResourceToManifest,
  queryOne,
  queryMany,
  execute,
  transaction,
  type Manifest,
  type ManifestResource,
  type Workflow,
} from "../material/db";
import { sealManifest } from "../workflow/state-transitions";
import { stampMaterialized } from "./materializer";
import type { TransitionResult } from "../workflow/state-transitions";

// =============================================================================
// Materializer (DB-authoritative — trivially thin)
// =============================================================================

export function materializeManifest(id: number, _opts?: MaterializeOptions): Materialized<Manifest> | null {
  const record = getManifest(id);
  if (!record) return null;
  return stampMaterialized(record);
}

export function materializeManifestBatch(ids: number[], _opts?: MaterializeOptions): Materialized<Manifest>[] {
  if (ids.length === 0) return [];
  const placeholders = ids.map(() => "?").join(", ");
  const records = queryMany<Manifest>(`SELECT * FROM manifests WHERE id IN (${placeholders})`, ids);
  return records.map(stampMaterialized);
}

// =============================================================================
// Retention Policies
// =============================================================================

const RETENTION_POLICIES: Record<string, number> = {
  "launch-run": 30 * 60_000, // 30 minutes
  "create-snapshot": Infinity, // indefinite (use null in DB)
  "install-feature": 60 * 60_000, // 60 minutes
  "terminate-instance": 5 * 60_000, // 5 minutes
  "cleanup-manifest": 0, // immediate (manifest cleaned up by workflow)
};

// =============================================================================
// Seal with Policy
// =============================================================================

/**
 * Seal a manifest with retention policy based on workflow type.
 * Maps workflow types to retention durations, then delegates to the
 * state-transition sealManifest for the actual DRAFT -> SEALED transition.
 */
export function sealManifestWithPolicy(
  manifestId: number,
  workflowType: string
): TransitionResult<Manifest> {
  const retentionMs =
    RETENTION_POLICIES[workflowType] ?? TIMING.DEFAULT_MANIFEST_RETENTION_MS;
  const now = Date.now();
  // Infinity maps to 1 year in the DB (effectively indefinite)
  const expiresAt =
    retentionMs === Infinity
      ? now + 365 * 86_400_000
      : retentionMs === 0
        ? now
        : now + retentionMs;
  return sealManifest(manifestId, expiresAt);
}

// =============================================================================
// Atomic Resource Claiming
// =============================================================================

/**
 * Atomically transfer a resource from a SEALED manifest to a DRAFT manifest.
 * Removes the resource from its current manifest and adds it to the new one
 * with owner_type='workflow'. Both operations happen in a single transaction.
 */
export function claimResourceAtomic(
  newManifestId: number,
  resourceType: string,
  resourceId: string
): boolean {
  return transaction(() => {
    // Find the resource in any SEALED manifest
    const existing = queryOne<ManifestResource>(
      `SELECT mr.* FROM manifest_resources mr
       JOIN manifests m ON m.id = mr.manifest_id
       WHERE mr.resource_type = ? AND mr.resource_id = ?
       AND m.status = 'SEALED'`,
      [resourceType, resourceId]
    );

    if (!existing) return false;

    // Verify the new manifest is DRAFT
    const newManifest = getManifest(newManifestId);
    if (!newManifest || newManifest.status !== "DRAFT") return false;

    // Transfer: remove from old manifest, add to new
    const now = Date.now();
    execute(`DELETE FROM manifest_resources WHERE id = ?`, [existing.id]);

    execute(
      `INSERT INTO manifest_resources (manifest_id, resource_type, resource_id, cleanup_priority, added_at, owner_type)
       VALUES (?, ?, ?, ?, ?, 'workflow')`,
      [
        newManifestId,
        resourceType,
        resourceId,
        existing.cleanup_priority,
        now,
      ]
    );

    return true;
  });
}

// =============================================================================
// Warm Instance Claiming
// =============================================================================

export interface WarmInstanceQuery {
  spec: string;
  region?: string;
  initChecksum?: string;
  tenantId?: number;
  excludeInstanceIds?: number[];
}

/**
 * Query SEALED manifests for available instances matching spec, claim one
 * atomically into the target manifest. Prefers checksum-matching instances.
 */
export function claimWarmInstance(
  manifestId: number,
  query: WarmInstanceQuery
): { success: boolean; instanceId?: number } {
  return transaction(() => {
    // Find instances in SEALED manifests that match the spec
    let sql = `
      SELECT mr.resource_id, mr.manifest_id, i.id as instance_id, i.spec, i.init_checksum
      FROM manifest_resources mr
      JOIN manifests m ON m.id = mr.manifest_id
      JOIN instances i ON CAST(mr.resource_id AS INTEGER) = i.id
      WHERE mr.resource_type = 'instance'
        AND m.status = 'SEALED'
        AND i.spec = ?
        AND (i.workflow_state LIKE '%:complete' OR i.workflow_state = 'launch-run:provisioning')
    `;
    const params: unknown[] = [query.spec];

    if (query.region) {
      sql += " AND i.region = ?";
      params.push(query.region);
    }

    if (query.excludeInstanceIds?.length) {
      sql += ` AND i.id NOT IN (${query.excludeInstanceIds.map(() => "?").join(",")})`;
      params.push(...query.excludeInstanceIds);
    }

    // Prefer checksum match
    if (query.initChecksum) {
      sql +=
        " ORDER BY CASE WHEN i.init_checksum = ? THEN 0 ELSE 1 END, i.created_at ASC";
      params.push(query.initChecksum);
    } else {
      sql += " ORDER BY i.created_at ASC";
    }

    sql += " LIMIT 1";

    const match = queryOne<{
      resource_id: string;
      manifest_id: number;
      instance_id: number;
    }>(sql, params);
    if (!match) return { success: false };

    // Claim it
    const claimed = claimResourceAtomic(manifestId, "instance", match.resource_id);
    if (!claimed) return { success: false };

    return { success: true, instanceId: match.instance_id };
  });
}

// =============================================================================
// Subworkflow Resource Claiming
// =============================================================================

/**
 * Parent claims a resource from a child subworkflow's SEALED manifest.
 * Finds released resources (owner_type='released') in the child's manifest
 * and transfers them to the parent's manifest.
 */
export function parentClaimSubworkflowResource(
  parentManifestId: number,
  subworkflowId: number,
  resourceType: string
): { success: boolean; resourceId?: string } {
  return transaction(() => {
    // Find the child workflow's manifest
    const childWorkflow = queryOne<Workflow>(
      "SELECT * FROM workflows WHERE id = ?",
      [subworkflowId]
    );
    if (!childWorkflow?.manifest_id) return { success: false };

    const childManifest = getManifest(childWorkflow.manifest_id);
    if (!childManifest || childManifest.status !== "SEALED")
      return { success: false };

    // Find a released resource of the requested type in the child manifest.
    const resource = queryOne<ManifestResource>(
      `SELECT * FROM manifest_resources
       WHERE manifest_id = ? AND resource_type = ? AND owner_type = 'released'
       LIMIT 1`,
      [childManifest.id, resourceType]
    );
    if (!resource) return { success: false };

    // Claim it
    const claimed = claimResourceAtomic(
      parentManifestId,
      resourceType,
      resource.resource_id
    );
    if (!claimed) return { success: false };

    return { success: true, resourceId: resource.resource_id };
  });
}

// =============================================================================
// Manifest Expiry
// =============================================================================

/**
 * Check whether a manifest has expired based on its expires_at timestamp.
 * Only SEALED manifests with a non-null expires_at can be expired.
 * Per §4.1: manifest is NOT expired if it still has unclaimed resources
 * (owner_type='manifest'). Resources with owner_type='workflow'
 * have been transferred and don't block expiry.
 */
export function isManifestExpired(manifestId: number): boolean {
  const manifest = getManifest(manifestId);
  if (!manifest) return false;
  if (manifest.status !== "SEALED") return false;
  if (!manifest.expires_at) return false;
  if (Date.now() <= manifest.expires_at) return false;

  // Check for unclaimed resources still owned by this manifest
  const unclaimed = queryOne<{ cnt: number }>(
    `SELECT COUNT(*) as cnt FROM manifest_resources
     WHERE manifest_id = ? AND owner_type = 'manifest'`,
    [manifestId]
  );
  if (unclaimed && unclaimed.cnt > 0) return false;

  return true;
}

// =============================================================================
// Resource Summary
// =============================================================================

/**
 * Get resource counts grouped by type for a manifest.
 */
export function getManifestResourceSummary(
  manifestId: number
): Record<string, number> {
  const rows = queryMany<{ resource_type: string; count: number }>(
    `SELECT resource_type, COUNT(*) as count FROM manifest_resources
     WHERE manifest_id = ? GROUP BY resource_type`,
    [manifestId]
  );
  const summary: Record<string, number> = {};
  for (const row of rows) {
    summary[row.resource_type] = row.count;
  }
  return summary;
}
