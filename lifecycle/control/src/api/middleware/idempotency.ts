// api/middleware/idempotency.ts - Idempotency key handling
// Implements §9.7: Idempotency-Key header for safe retries on mutation endpoints.

import { queryOne, execute } from "../../material/db";

const SUCCESS_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours
const CLIENT_ERROR_TTL_MS = 60 * 60 * 1000; // 1 hour

interface CachedResponse {
  key: string;
  endpoint: string;
  params_hash: string;
  response_status: number;
  response_body: string;
  workflow_id: number | null;
  created_at: number;
  expires_at: number;
}

/**
 * Check for an existing idempotency key and return cached response if found.
 *
 * Returns:
 * - { hit: true, status, body } if a cached response exists (caller should return it)
 * - { hit: true, status: 409, body } if key exists with different params (conflict)
 * - { hit: false, paramsHash } if no cached response (caller should proceed normally)
 */
export function checkIdempotencyKey(
  key: string,
  endpoint: string,
  requestBody: string,
): { hit: true; status: number; body: unknown } | { hit: false; paramsHash: string } {
  const paramsHash = new Bun.CryptoHasher("sha256")
    .update(requestBody)
    .digest("hex");

  const existing = queryOne<CachedResponse>(
    "SELECT * FROM idempotency_keys WHERE key = ?",
    [key],
  );

  if (existing) {
    // Check for parameter conflict
    if (existing.params_hash !== paramsHash) {
      return {
        hit: true,
        status: 409,
        body: {
          error: {
            code: "IDEMPOTENCY_CONFLICT",
            message: "Idempotency key already used with different parameters",
            category: "conflict",
          },
        },
      };
    }

    // Check if expired
    if (existing.expires_at < Date.now()) {
      // Expired — clean up and proceed as new request
      execute("DELETE FROM idempotency_keys WHERE key = ?", [key]);
      return { hit: false, paramsHash };
    }

    // Return cached response
    return {
      hit: true,
      status: existing.response_status,
      body: JSON.parse(existing.response_body),
    };
  }

  return { hit: false, paramsHash };
}

/**
 * Store the response for an idempotent request.
 * Call after the handler has produced a response.
 *
 * - 2xx responses: cached for SUCCESS_TTL_MS (24h), or custom ttlMs if provided
 * - 4xx responses: cached for CLIENT_ERROR_TTL_MS (1h)
 * - 5xx responses: NOT cached (allow immediate retry)
 *
 * The optional `ttlMs` parameter overrides the default success TTL. Use this
 * for destructive intents (e.g., terminate, cleanup) where a shorter window
 * allows faster retry after transient failures — see TIMING.DESTRUCTIVE_INTENT_TTL_MS.
 */
export function storeIdempotencyResponse(
  key: string,
  endpoint: string,
  paramsHash: string,
  responseStatus: number,
  responseBody: string,
  workflowId?: number | null,
  ttlMs?: number,
): void {
  const now = Date.now();

  // Don't cache server errors — allow immediate retry
  if (responseStatus >= 500) {
    return;
  }

  const ttl = responseStatus >= 400
    ? CLIENT_ERROR_TTL_MS
    : (ttlMs ?? SUCCESS_TTL_MS);

  execute(
    `INSERT OR REPLACE INTO idempotency_keys
     (key, endpoint, params_hash, response_status, response_body, workflow_id, created_at, expires_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      key,
      endpoint,
      paramsHash,
      responseStatus,
      responseBody,
      workflowId ?? null,
      now,
      now + ttl,
    ],
  );
}

/**
 * Clean up expired idempotency keys.
 * Called periodically by the reconciliation loop.
 */
export function cleanExpiredIdempotencyKeys(): number {
  const now = Date.now();
  execute("DELETE FROM idempotency_keys WHERE expires_at < ?", [now]);
  // Return count would need queryMany — keep it simple for now
  return 0;
}
