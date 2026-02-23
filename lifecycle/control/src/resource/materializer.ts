// resource/materializer.ts - Materialization helpers

import type { FreshnessTier, MaterializeOptions, Materialized } from "@skyrepl/contracts";
import { getProviderTiming } from "@skyrepl/contracts";
import { cacheGet, cacheSet } from "./cache";

// =============================================================================
// Freshness Tier → TTL
// =============================================================================

// Display tier TTL — moderate staleness tolerance for API responses/CLI.
const DISPLAY_TTL_MS = 60_000; // 60s

/**
 * Resolve a FreshnessTier to a TTL in milliseconds.
 *
 * Provider is optional — when supplied, provider-specific overrides apply
 * (e.g., OrbStack's CACHE_INSTANCE_STATE_TTL_MS = 10s vs default 30s).
 */
export function getTtlForTier(tier: FreshnessTier, provider?: string): number {
  switch (tier) {
    case 'decision':
      return provider
        ? getProviderTiming('CACHE_INSTANCE_STATE_TTL_MS', provider)
        : getProviderTiming('CACHE_INSTANCE_STATE_TTL_MS', 'aws');
    case 'display':
      return DISPLAY_TTL_MS;
    case 'loop':
      return 0;
    case 'batch':
      return provider
        ? getProviderTiming('CACHE_CAPACITY_TTL_MS', provider)
        : getProviderTiming('CACHE_CAPACITY_TTL_MS', 'aws');
  }
}

/**
 * Resolve MaterializeOptions into an effective TTL.
 */
export function resolveOptions(opts?: MaterializeOptions, provider?: string): { ttlMs: number; tier: FreshnessTier } {
  if (opts?.forceRefresh) return { ttlMs: 0, tier: 'loop' };
  const tier = opts?.tier ?? 'display';
  return { ttlMs: getTtlForTier(tier, provider), tier };
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Stamp a raw record as materialized. Used by thin (DB-authoritative)
 * materializers where materialized_at = now() trivially.
 */
export function stampMaterialized<T>(record: T): Materialized<T> {
  return { ...record, materialized_at: Date.now() };
}

/**
 * Get-or-materialize pattern with cache integration.
 *
 * 1. If ttlMs > 0, check cache. Return on hit.
 * 2. Call fetcher() to produce fresh value.
 * 3. Cache the result (if ttlMs > 0).
 * 4. Return.
 */
export async function cachedMaterialize<T>(
  cacheKey: string,
  ttlMs: number,
  fetcher: () => Promise<T>,
): Promise<T> {
  if (ttlMs > 0) {
    const cached = cacheGet<T>(cacheKey);
    if (cached !== null) return cached;
  }

  const fresh = await fetcher();

  if (ttlMs > 0) {
    cacheSet(cacheKey, fresh, ttlMs);
  }

  return fresh;
}
