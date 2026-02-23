// materialization.ts - Materialization barrier types
//
// The materialization barrier: all resource read paths cross through a
// ResourceMaterializer<T>. Business code operates on Materialized<T>,
// never on raw DB records for reads.

/**
 * Consumer-purpose freshness tiers. The consumer declares what it needs
 * the data for; the materializer selects the appropriate TTL.
 *
 *   decision — tight TTL (warm pool matching, budget enforcement)
 *   display  — moderate TTL (API responses, CLI output)
 *   loop     — always fresh (wait-for-boot, health checks) — TTL 0
 *   batch    — periodic (orphan scanner, reconciliation)
 */
export type FreshnessTier = 'decision' | 'display' | 'loop' | 'batch';

/**
 * Options passed to materializer calls.
 */
export interface MaterializeOptions {
  /** Consumer-purpose freshness tier. Default: 'display'. */
  tier?: FreshnessTier;
  /** Bypass cache read, equivalent to tier='loop'. Default: false. */
  forceRefresh?: boolean;
}

/**
 * Materialized<T> wraps a raw DB record type with materialization metadata
 * and optional enrichment fields. The materialized_at timestamp is the
 * contract: this resource is no more stale than (now - materialized_at).
 *
 * For DB-authoritative resources (runs, allocations, manifests, workflows),
 * materialized_at = now() trivially — the DB IS reality.
 *
 * For observed resources (instances, snapshots), materialized_at = time of
 * last external state fetch.
 */
export type Materialized<T> = T & {
  /** Epoch ms — when this resource was last materialized. */
  materialized_at: number;
};
