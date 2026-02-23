// resource/cache.ts - In-memory TTL cache for materialized resources
//
// Per-process, not shared. Cold on restart (infrequent and correct).
// Keys: '{type}:{id}' — e.g., 'instance:42', 'run:17'.

interface CacheEntry<T> {
  value: T;
  expiresAt: number;
}

let hits = 0;
let misses = 0;

const store = new Map<string, CacheEntry<unknown>>();

export function cacheGet<T>(key: string): T | null {
  const entry = store.get(key);
  if (!entry) {
    misses++;
    return null;
  }
  if (Date.now() > entry.expiresAt) {
    store.delete(key);
    misses++;
    return null;
  }
  hits++;
  return entry.value as T;
}

export function cacheSet<T>(key: string, value: T, ttlMs: number): void {
  if (ttlMs <= 0) return; // loop tier: never cache
  store.set(key, { value, expiresAt: Date.now() + ttlMs });
}

export function cacheInvalidate(key: string): void {
  store.delete(key);
}

export function cacheInvalidateByPrefix(prefix: string): void {
  for (const key of store.keys()) {
    if (key.startsWith(prefix)) {
      store.delete(key);
    }
  }
}

export function cacheClear(): void {
  store.clear();
}

export function cacheStats(): { hits: number; misses: number; size: number } {
  return { hits, misses, size: store.size };
}

/** Reset stats counters (for testing) */
export function cacheResetStats(): void {
  hits = 0;
  misses = 0;
}
