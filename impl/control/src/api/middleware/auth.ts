// api/middleware/auth.ts - Agent Authentication Middleware
// Validates bearer tokens for agent endpoints via SHA256 hash comparison.

import crypto from "crypto";
import { getInstanceRecord } from "../../resource/instance";

/**
 * Verify instance auth token from Authorization header or query param.
 * Returns true if the token is valid or if the instance has no token set (backward compatibility).
 */
export function verifyInstanceToken(instanceId: number | string, token: string): boolean {
  const id = typeof instanceId === "string" ? parseInt(instanceId, 10) : instanceId;
  if (isNaN(id)) return false;

  const instance = getInstanceRecord(id);
  if (!instance) return false;

  // Backward compatibility: if no token hash is set, allow any request
  // This ensures existing tests and instances without tokens continue to work
  if (!instance.registration_token_hash) return true;

  const hash = crypto.createHash("sha256").update(token).digest("hex");
  return hash === instance.registration_token_hash;
}

/**
 * Extract token from request: Authorization header or ?token= query param.
 * SSE fallback: EventSource can't set custom headers, so query param is supported.
 */
export function extractToken(request: Request, query?: Record<string, string>): string | null {
  const authHeader = request.headers.get("authorization");
  if (authHeader?.startsWith("Bearer ")) {
    return authHeader.slice(7);
  }
  // SSE fallback: EventSource can't set headers
  if (query?.token) {
    return query.token;
  }
  return null;
}
