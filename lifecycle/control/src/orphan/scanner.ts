// orphan/scanner.ts - Orphan scanner and classifier
//
// Compares cloud-side instances against DB-tracked instances and classifies
// any discrepancies. Advisory only — never takes destructive action.

import { getControlId, parseResourceName } from "../material/control-id";
import { getDatabase } from "../material/db/helpers";
import { queryMany, updateInstance, getInstance, isWhitelisted, recordOrphanScan, execute, type Instance } from "../material/db"; // raw-db: boutique queries (partial-column tracked instances, provider_id status filter), see WL-057
import { getProvider, getAllProviders } from "../provider/registry";
import type { ProviderName } from "../provider/types";
import { cacheSet } from "../resource/cache";
import { stampMaterialized, getTtlForTier } from "../resource/materializer";
import type { ProviderOperationError } from "../provider/errors";

// =============================================================================
// Types
// =============================================================================

export interface OrphanRecord {
  provider: string;
  providerId: string;
  resourceName: string;
  classification: "known_orphan" | "foreign_orphan" | "unknown" | "whitelisted";
  parsedName: { controlId: string; manifestId: number | null; resourceId: number } | null;
  providerCreatedAt: number;
  ageMs: number;
  isSpot: boolean;
}

export interface ScanResult {
  provider: string;
  scannedAt: number;
  cloudCount: number;
  trackedCount: number;
  orphans: OrphanRecord[];
  /** provider_ids in DB but not in cloud (informational only) */
  missing: string[];
}

// =============================================================================
// Spawn Error Classification (§12.8)
// =============================================================================

/**
 * Classification of a spawn failure by orphan risk.
 *
 * orphanRisk: true if the provider may have created a resource despite the error.
 * confidenceNoResource: confidence that NO resource was created.
 * action: what to do in response.
 */
export interface SpawnErrorClassification {
  orphanRisk: boolean;
  confidenceNoResource: "high" | "medium" | "low";
  action: "none" | "scan_immediately";
}

/**
 * Classify a spawn failure by orphan risk per spec §12.8.
 *
 * Maps provider error codes to risk level. Errors where the provider
 * definitively rejected the request (auth, invalid spec, capacity) have
 * no orphan risk. Errors that may have partially succeeded (timeout,
 * network, internal) carry orphan risk.
 */
export function classifySpawnError(err: unknown): SpawnErrorClassification {
  if (err && typeof err === "object" && "code" in err) {
    const code = (err as { code: string }).code;

    switch (code) {
      case "INVALID_SPEC":
      case "AUTH_ERROR":
      case "BUDGET_EXCEEDED":
      case "CAPACITY_ERROR":
      case "SPOT_INTERRUPTED":
      case "QUOTA_EXCEEDED":
        return { orphanRisk: false, confidenceNoResource: "high", action: "none" };

      case "TIMEOUT_ERROR":
      case "OPERATION_TIMEOUT": // thrown by wait-for-boot on boot timeout (spec WAIT_FOR_BOOT_TIMEOUT)
      case "PROVIDER_INTERNAL":
        return { orphanRisk: true, confidenceNoResource: "low", action: "scan_immediately" };

      case "NETWORK_ERROR":
      case "RATE_LIMIT_ERROR": // provider may have partially processed the request
        return { orphanRisk: true, confidenceNoResource: "medium", action: "scan_immediately" };

      case "AGENT_REGISTRATION_FAILED": // agent auth/registration failed after spawn
        return { orphanRisk: true, confidenceNoResource: "low", action: "scan_immediately" };
    }
  }

  // Unknown/generic error — treat as potentially risky
  const msg = err instanceof Error ? err.message.toLowerCase() : String(err).toLowerCase();

  if (msg.includes("timeout") || msg.includes("timed out")) {
    return { orphanRisk: true, confidenceNoResource: "low", action: "scan_immediately" };
  }
  if (msg.includes("network") || msg.includes("connection") || msg.includes("econnrefused")) {
    return { orphanRisk: true, confidenceNoResource: "medium", action: "scan_immediately" };
  }
  if (msg.includes("auth") || msg.includes("credential") || msg.includes("unauthorized")) {
    return { orphanRisk: false, confidenceNoResource: "high", action: "none" };
  }
  if (msg.includes("capacity") || msg.includes("quota") || msg.includes("insufficient")) {
    return { orphanRisk: false, confidenceNoResource: "high", action: "none" };
  }

  // Conservative default: treat as orphan risk
  return { orphanRisk: true, confidenceNoResource: "low", action: "scan_immediately" };
}

// =============================================================================
// Helpers
// =============================================================================

interface TrackedInstance {
  id: number;
  provider_id: string;
  ip: string | null;
}

/**
 * Query DB for active (non-terminating) instances for a given provider.
 */
function getActiveTrackedInstances(provider: string): TrackedInstance[] {
  return queryMany<TrackedInstance>(
    `SELECT id, provider_id, ip FROM instances
     WHERE provider = ? AND workflow_state NOT LIKE 'terminate:%'`,
    [provider]
  );
}

/**
 * A13 (BL-60): Attempt to adopt an orphaned cloud resource into tracking.
 *
 * Looks up the DB instance record by instanceDbId. If the record exists,
 * belongs to this provider, and has no provider_id (spawn was interrupted
 * before Phase 3 completed), adopt it by updating provider_id and advancing
 * workflow state to launch-run:provisioning.
 *
 * Returns true if adoption succeeded, false otherwise.
 */
function tryAdoptOrphan(
  providerName: string,
  cloudProviderId: string,
  instanceDbId: number,
  ip: string | null
): boolean {
  const dbInst = getInstance(instanceDbId);
  if (!dbInst) return false;
  if (dbInst.provider !== providerName) return false;
  // Only adopt if provider_id is empty (spawn never completed Phase 3)
  if (dbInst.provider_id && dbInst.provider_id !== "") return false;
  // Only adopt if not in a terminal state
  if (
    dbInst.workflow_state.startsWith("terminate:") ||
    dbInst.workflow_state === "spawn:error" ||
    dbInst.workflow_state === "launch-run:error" ||
    dbInst.workflow_state === "launch-run:compensated"
  ) {
    return false;
  }

  // B12: atomic CAS — only update if workflow_state is still a pre-spawn state.
  // If compensate() has already written spawn:error between the fire-and-forget
  // scanProviderTargeted() call and this point, the UPDATE matches 0 rows and
  // adoption is a no-op (correct). If adoption runs first, compensate will
  // subsequently be blocked by the terminal-state guard on workflow_state above.
  const db = getDatabase();
  const result = db.prepare(
    `UPDATE instances
     SET provider_id = ?, ip = ?, workflow_state = 'launch-run:provisioning', last_heartbeat = ?
     WHERE id = ? AND (workflow_state IS NULL OR workflow_state = '' OR workflow_state = 'launch-run:spawning' OR workflow_state = 'spawn:pending')`
  ).run(cloudProviderId, ip ?? dbInst.ip, Date.now(), instanceDbId);

  return (result.changes as number) > 0;
}

// =============================================================================
// Scanner
// =============================================================================

/**
 * Scan a single provider for orphaned cloud resources.
 */
export async function scanProvider(providerName: string): Promise<ScanResult> {
  const scannedAt = Date.now();
  const db = getDatabase();
  const controlId = getControlId(db);

  const provider = await getProvider(providerName as ProviderName);

  // 1. Fetch cloud-side instances (active only)
  const cloudInstances = await provider.list({ includeTerminated: false });

  // 2. Fetch DB-tracked instances for this provider
  const tracked = getActiveTrackedInstances(providerName);
  const trackedIdSet = new Set(tracked.map(t => t.provider_id));

  // 3. Build set of cloud provider IDs
  const cloudIdSet = new Set(cloudInstances.map(i => i.id));

  // 4. Find missing (in DB but not in cloud)
  const missing = tracked
    .filter(t => t.provider_id !== "" && !cloudIdSet.has(t.provider_id))
    .map(t => t.provider_id);

  // 5. Write-back: update tracked instances with cloud-discovered state.
  //    The scanner already pays for provider.list() — writing back discovered
  //    state (IP changes) completes the materialization contract.
  const trackedById = new Map(tracked.map(t => [t.provider_id, t]));
  let writeBackCount = 0;
  for (const cloudInst of cloudInstances) {
    const dbInst = trackedById.get(cloudInst.id);
    if (!dbInst) continue;

    const updates: Record<string, unknown> = {};
    if (cloudInst.ip && cloudInst.ip !== dbInst.ip) {
      updates.ip = cloudInst.ip;
    }

    if (Object.keys(updates).length > 0) {
      updateInstance(dbInst.id, updates);
      writeBackCount++;
    }

    // Populate materializer cache with this fresh observation (batch-tier TTL)
    const freshRecord = getInstance(dbInst.id);
    if (freshRecord) {
      cacheSet(`instance:${dbInst.id}`, stampMaterialized(freshRecord), getTtlForTier('batch', providerName));
    }
  }

  if (writeBackCount > 0) {
    console.log(`[orphan] Write-back: updated ${writeBackCount} tracked instance(s) for ${providerName}`);
  }

  // 6. Find orphan candidates (in cloud but not tracked in DB)
  //    A13 (BL-60): when a known_orphan's resource name encodes an instance ID
  //    that matches a DB record waiting for a provider_id (spawn:pending or
  //    empty provider_id), adopt it into tracking rather than flagging it.
  const now = Date.now();
  const orphans: OrphanRecord[] = [];
  let adoptionCount = 0;

  for (const cloudInst of cloudInstances) {
    if (trackedIdSet.has(cloudInst.id)) {
      continue; // tracked — not an orphan
    }

    // Get name from metadata or use id as fallback
    const resourceName = (cloudInst.metadata?.name as string) ?? cloudInst.id;
    const whitelisted = isWhitelisted(providerName, cloudInst.id);
    const parsed = parseResourceName(resourceName);

    let classification: OrphanRecord["classification"];

    if (whitelisted) {
      classification = "whitelisted";
    } else if (parsed === null) {
      classification = "unknown";
    } else if (parsed.controlId === controlId) {
      // A13: attempt tag-based adoption before flagging as known_orphan.
      // The resource name encodes the instance DB id (resourceId). If that
      // instance exists in DB without a provider_id, adopt it now.
      const adopted = tryAdoptOrphan(providerName, cloudInst.id, parsed.resourceId, cloudInst.ip ?? null);
      if (adopted) {
        // Adopted — add to tracked set so it doesn't appear in missing list.
        trackedIdSet.add(cloudInst.id);
        adoptionCount++;
        continue; // Skip adding to orphans list
      }
      classification = "known_orphan";
    } else {
      classification = "foreign_orphan";
    }

    orphans.push({
      provider: providerName,
      providerId: cloudInst.id,
      resourceName,
      classification,
      parsedName: parsed,
      providerCreatedAt: cloudInst.createdAt,
      ageMs: now - cloudInst.createdAt,
      isSpot: cloudInst.isSpot,
    });
  }

  if (adoptionCount > 0) {
    console.log(`[orphan] A13: adopted ${adoptionCount} orphaned instance(s) into tracking for ${providerName}`);
  }

  // 7. Record the scan
  recordOrphanScan({
    provider: providerName,
    scanned_at: scannedAt,
    orphans_found: orphans.filter(o => o.classification !== "whitelisted").length,
    orphan_ids: orphans
      .filter(o => o.classification !== "whitelisted")
      .map(o => o.providerId),
  });

  return {
    provider: providerName,
    scannedAt,
    cloudCount: cloudInstances.length,
    trackedCount: tracked.length,
    orphans,
    missing,
  };
}

/**
 * Targeted scan: scan a single provider looking only at resources whose name
 * starts with the given prefix. Used for post-spawn-failure checks where we
 * know the expected resource name prefix.
 *
 * Returns only the orphan records that match the prefix. Does NOT persist
 * a full scan record — this is a quick targeted lookup, not a scheduled sweep.
 */
export async function scanProviderTargeted(
  providerName: string,
  namePrefix: string
): Promise<OrphanRecord[]> {
  const scannedAt = Date.now();
  const db = getDatabase();
  const controlId = getControlId(db);

  const provider = await getProvider(providerName as ProviderName);
  const cloudInstances = await provider.list({ includeTerminated: false });

  const tracked = getActiveTrackedInstances(providerName);
  const trackedIdSet = new Set(tracked.map(t => t.provider_id));

  const now = Date.now();
  const orphans: OrphanRecord[] = [];

  for (const cloudInst of cloudInstances) {
    if (trackedIdSet.has(cloudInst.id)) {
      continue; // tracked — not an orphan
    }

    const resourceName = (cloudInst.metadata?.name as string) ?? cloudInst.id;

    // Only include resources matching the prefix
    if (!resourceName.startsWith(namePrefix)) {
      continue;
    }

    const whitelisted = isWhitelisted(providerName, cloudInst.id);
    const parsed = parseResourceName(resourceName);

    let classification: OrphanRecord["classification"];
    if (whitelisted) {
      classification = "whitelisted";
    } else if (parsed === null) {
      classification = "unknown";
    } else if (parsed.controlId === controlId) {
      // A13: attempt tag-based adoption before flagging as known_orphan.
      // The resource name encodes the instance DB id (resourceId). If that
      // instance exists in DB without a provider_id, adopt it now.
      const adopted = tryAdoptOrphan(providerName, cloudInst.id, parsed.resourceId, cloudInst.ip ?? null);
      if (adopted) {
        // Adopted — skip adding to orphans list
        continue;
      }
      classification = "known_orphan";
    } else {
      classification = "foreign_orphan";
    }

    orphans.push({
      provider: providerName,
      providerId: cloudInst.id,
      resourceName,
      classification,
      parsedName: parsed,
      providerCreatedAt: cloudInst.createdAt,
      ageMs: now - cloudInst.createdAt,
      isSpot: cloudInst.isSpot,
    });
  }

  // B13: record targeted scans in the audit trail, mirroring scanProvider.
  recordOrphanScan({
    provider: providerName,
    scanned_at: scannedAt,
    orphans_found: orphans.filter(o => o.classification !== "whitelisted").length,
    orphan_ids: orphans
      .filter(o => o.classification !== "whitelisted")
      .map(o => o.providerId),
  });

  return orphans;
}

// B5: Module-level flag prevents concurrent scanAll invocations.
let scanning = false;

/**
 * Scan all registered providers.
 *
 * B5: Returns early if a previous scan is still running (concurrent-scan guard).
 * B6: Per-provider errors are caught and logged; remaining providers still run.
 */
export async function scanAll(): Promise<ScanResult[]> {
  if (scanning) {
    console.warn("[orphan] scanAll: previous scan still running, skipping");
    return [];
  }
  scanning = true;
  const results: ScanResult[] = [];
  try {
    const providerNames = getAllProviders();
    for (const name of providerNames) {
      try {
        const result = await scanProvider(name);
        results.push(result);
      } catch (err) {
        console.error(`[orphan] scanAll: error scanning provider ${name}:`, err);
      }
    }
  } finally {
    scanning = false;
  }
  return results;
}

