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
 * Resolve MaterializeOptions into an effective TTL and refresh strategy.
 *
 * forceRefresh bypasses the cache READ but keeps the tier's TTL for cache
 * WRITE — the fresh result populates the cache so subsequent reads benefit.
 * This is distinct from tier='loop' (TTL 0, no cache at all).
 */
export function resolveOptions(opts?: MaterializeOptions, provider?: string): { ttlMs: number; tier: FreshnessTier; forceRefresh: boolean } {
  const tier = opts?.tier ?? 'display';
  return { ttlMs: getTtlForTier(tier, provider), tier, forceRefresh: opts?.forceRefresh ?? false };
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
 * 1. If ttlMs > 0 and !forceRefresh, check cache. Return on hit.
 * 2. Call fetcher() to produce fresh value.
 * 3. Cache the result (if ttlMs > 0) — even on forceRefresh, so
 *    subsequent reads at the same tier benefit from the fresh data.
 * 4. Return.
 *
 * forceRefresh skips the cache read but still writes. This is the mechanism
 * for "bypass TTL before acting on a resource" — the caller gets definitive
 * provider state, and the cache is updated for everyone else.
 */
export async function cachedMaterialize<T>(
  cacheKey: string,
  ttlMs: number,
  fetcher: () => Promise<T>,
  forceRefresh?: boolean,
): Promise<T> {
  if (ttlMs > 0 && !forceRefresh) {
    const cached = cacheGet<T>(cacheKey);
    if (cached !== null) return cached;
  }

  const fresh = await fetcher();

  if (ttlMs > 0) {
    cacheSet(cacheKey, fresh, ttlMs);
  }

  return fresh;
}
