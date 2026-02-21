// rate-limit.ts — Per-token sliding window rate limiting (API-03)

/**
 * In-memory sliding window rate limiter. Tracks request counts per auth token
 * within a configurable window. Returns 429 + Retry-After when exceeded.
 *
 * Design: lifecycle implements the limiter; in managed mode, shell/proxy
 * could add its own layer. This is per-process (not distributed).
 */

export interface RateLimitConfig {
  /** Max requests per window (default: 120) */
  maxRequests: number;
  /** Window size in ms (default: 60_000 = 1 minute) */
  windowMs: number;
}

interface TokenBucket {
  timestamps: number[];
}

const DEFAULT_CONFIG: RateLimitConfig = {
  maxRequests: 120,
  windowMs: 60_000,
};

const buckets = new Map<string, TokenBucket>();

// Periodic cleanup of stale buckets (every 5 minutes)
let cleanupInterval: ReturnType<typeof setInterval> | null = null;

function ensureCleanup(windowMs: number): void {
  if (cleanupInterval) return;
  cleanupInterval = setInterval(() => {
    const cutoff = Date.now() - windowMs * 2;
    for (const [key, bucket] of buckets) {
      if (bucket.timestamps.length === 0 || bucket.timestamps[bucket.timestamps.length - 1] < cutoff) {
        buckets.delete(key);
      }
    }
  }, 300_000);
  // Don't keep process alive for cleanup
  if (typeof cleanupInterval === 'object' && 'unref' in cleanupInterval) {
    cleanupInterval.unref();
  }
}

/**
 * Check rate limit for a token. Returns null if allowed, or a
 * { retryAfter, limit, remaining } object if rate limited.
 */
export function checkRateLimit(
  tokenId: string,
  config: RateLimitConfig = DEFAULT_CONFIG,
): { retryAfter: number; limit: number; remaining: 0 } | null {
  ensureCleanup(config.windowMs);

  const now = Date.now();
  const windowStart = now - config.windowMs;

  let bucket = buckets.get(tokenId);
  if (!bucket) {
    bucket = { timestamps: [] };
    buckets.set(tokenId, bucket);
  }

  // Prune timestamps outside the window
  const firstValid = bucket.timestamps.findIndex(t => t > windowStart);
  if (firstValid > 0) {
    bucket.timestamps = bucket.timestamps.slice(firstValid);
  } else if (firstValid === -1) {
    bucket.timestamps = [];
  }

  if (bucket.timestamps.length >= config.maxRequests) {
    // Rate limited — calculate retry-after from oldest timestamp in window
    const oldestInWindow = bucket.timestamps[0];
    const retryAfter = Math.ceil((oldestInWindow + config.windowMs - now) / 1000);
    return { retryAfter: Math.max(1, retryAfter), limit: config.maxRequests, remaining: 0 };
  }

  // Allow the request
  bucket.timestamps.push(now);
  return null;
}

/**
 * Get current rate limit info for response headers.
 */
export function getRateLimitInfo(
  tokenId: string,
  config: RateLimitConfig = DEFAULT_CONFIG,
): { limit: number; remaining: number; reset: number } {
  const now = Date.now();
  const windowStart = now - config.windowMs;
  const bucket = buckets.get(tokenId);

  if (!bucket) {
    return { limit: config.maxRequests, remaining: config.maxRequests, reset: Math.ceil((now + config.windowMs) / 1000) };
  }

  const validCount = bucket.timestamps.filter(t => t > windowStart).length;
  const remaining = Math.max(0, config.maxRequests - validCount);
  const reset = Math.ceil((now + config.windowMs) / 1000);

  return { limit: config.maxRequests, remaining, reset };
}

/** Reset all rate limit state (for testing). */
export function resetRateLimits(): void {
  buckets.clear();
}
