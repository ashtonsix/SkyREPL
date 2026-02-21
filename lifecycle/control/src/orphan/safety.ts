// orphan/safety.ts - Safety constraints for orphan termination
//
// All terminations require explicit caller request.
// Default protections: age threshold (1 hour), whitelist, dry-run.

import { isWhitelisted } from "../material/db";
import { getProvider } from "../provider/registry";
import type { ProviderName } from "../provider/types";

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_MIN_AGE_MS = 60 * 60 * 1000; // 1 hour

// =============================================================================
// Types
// =============================================================================

export interface TerminateRequest {
  provider: string;
  providerId: string;
  dryRun: boolean;
  force: boolean;
  /** Minimum age in ms before termination is allowed. Default: 1 hour. */
  minAgeMs?: number;
}

export interface TerminateResult {
  providerId: string;
  action:
    | "terminated"
    | "skipped_dry_run"
    | "skipped_too_young"
    | "skipped_whitelisted"
    | "failed";
  reason?: string;
}

// =============================================================================
// Safety checks
// =============================================================================

/**
 * Safely terminate a single orphaned cloud resource.
 *
 * Safety rules:
 * - dryRun: return what WOULD happen, do nothing.
 * - Whitelisted resources are skipped unless force: true.
 * - Resources younger than minAgeMs are skipped unless force: true.
 * - Every action (including skips) is logged.
 */
export async function safeTerminateOrphan(
  req: TerminateRequest
): Promise<TerminateResult> {
  const { provider: providerName, providerId, dryRun, force } = req;
  const minAgeMs = req.minAgeMs ?? DEFAULT_MIN_AGE_MS;

  // Check whitelist (unless forced)
  if (!force && isWhitelisted(providerName, providerId)) {
    console.log(
      `[orphan] skipped_whitelisted provider=${providerName} id=${providerId} dryRun=${dryRun}`
    );
    return { providerId, action: "skipped_whitelisted", reason: "resource is whitelisted" };
  }

  // Fetch current state from cloud to determine age
  let createdAt: number | null = null;
  try {
    const provider = await getProvider(providerName as ProviderName);
    const inst = await provider.get(providerId);
    if (inst) {
      createdAt = inst.createdAt;
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(
      `[orphan] failed to fetch provider=${providerName} id=${providerId}: ${msg}`
    );
    return { providerId, action: "failed", reason: `provider.get failed: ${msg}` };
  }

  // Age check (unless forced)
  if (!force && createdAt !== null) {
    const ageMs = Date.now() - createdAt;
    if (ageMs < minAgeMs) {
      console.log(
        `[orphan] skipped_too_young provider=${providerName} id=${providerId} ageMs=${ageMs} minAgeMs=${minAgeMs} dryRun=${dryRun}`
      );
      return {
        providerId,
        action: "skipped_too_young",
        reason: `age ${ageMs}ms < minimum ${minAgeMs}ms`,
      };
    }
  }

  // Dry-run gate
  if (dryRun) {
    console.log(
      `[orphan] skipped_dry_run provider=${providerName} id=${providerId} (would terminate)`
    );
    return { providerId, action: "skipped_dry_run", reason: "dry-run mode" };
  }

  // Terminate
  try {
    const provider = await getProvider(providerName as ProviderName);
    await provider.terminate(providerId);
    console.log(
      `[orphan] terminated provider=${providerName} id=${providerId} force=${force}`
    );
    return { providerId, action: "terminated" };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(
      `[orphan] terminate failed provider=${providerName} id=${providerId}: ${msg}`
    );
    return { providerId, action: "failed", reason: `terminate failed: ${msg}` };
  }
}

/**
 * Safely terminate a batch of orphaned resources, applying safety rules to each.
 * Processes requests sequentially to avoid overwhelming the provider API.
 */
export async function safeTerminateBatch(
  requests: TerminateRequest[]
): Promise<TerminateResult[]> {
  const results: TerminateResult[] = [];
  for (const req of requests) {
    const result = await safeTerminateOrphan(req);
    results.push(result);
  }
  return results;
}
