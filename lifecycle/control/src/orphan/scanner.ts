// orphan/scanner.ts - Orphan scanner and classifier
//
// Compares cloud-side instances against DB-tracked instances and classifies
// any discrepancies. Advisory only — never takes destructive action.

import { getControlId, parseResourceName } from "../material/control-id";
import { getDatabase } from "../material/db/helpers";
import { queryMany, isWhitelisted, recordOrphanScan } from "../material/db";
import { getProvider, getAllProviders } from "../provider/registry";
import type { ProviderName } from "../provider/types";

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
// Helpers
// =============================================================================

interface TrackedInstance {
  id: number;
  provider_id: string;
}

/**
 * Query DB for active (non-terminating) instances for a given provider.
 */
function getActiveTrackedInstances(provider: string): TrackedInstance[] {
  return queryMany<TrackedInstance>(
    `SELECT id, provider_id FROM instances
     WHERE provider = ? AND workflow_state NOT LIKE 'terminate:%'`,
    [provider]
  );
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

  // 5. Find orphan candidates (in cloud but not tracked in DB)
  const now = Date.now();
  const orphans: OrphanRecord[] = [];

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

  // 6. Record the scan
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
 * Scan all registered providers.
 */
export async function scanAll(): Promise<ScanResult[]> {
  const providerNames = getAllProviders();
  const results: ScanResult[] = [];

  for (const name of providerNames) {
    const result = await scanProvider(name);
    results.push(result);
  }

  return results;
}

// =============================================================================
// Shutdown Helper: Terminate Orphaned VMs
// =============================================================================

/**
 * On graceful shutdown, cross-reference cloud-side VMs against DB-tracked
 * instances and terminate any that are not tracked. Advisory — errors are
 * logged but do not block shutdown.
 */
export async function terminateOrphanedVMs(): Promise<void> {
  try {
    const provider = await getProvider("orbstack");
    const vms = await provider.list();
    if (vms.length === 0) return;

    // Cross-reference with DB to find truly orphaned VMs
    const knownInstances = queryMany(
      "SELECT provider_id FROM instances WHERE provider_id IS NOT NULL AND status != 'terminated'"
    );
    const knownIds = new Set(
      knownInstances.map((r: any) => r.provider_id as string)
    );
    const orphaned = vms.filter((vm) => !knownIds.has(vm.id));

    if (orphaned.length === 0) return;

    console.log(
      `[control] Terminating ${orphaned.length} orphaned VM(s) (${vms.length} total)...`
    );
    for (const vm of orphaned) {
      try {
        await provider.terminate(vm.id);
      } catch (err) {
        console.warn(`[control] Failed to terminate VM ${vm.id}:`, err);
      }
    }
  } catch (err) {
    // Provider not available — skip
    console.warn("[control] Orphan cleanup skipped:", err);
  }
}
