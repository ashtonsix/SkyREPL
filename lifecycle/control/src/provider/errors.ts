// provider/errors.ts - Provider Error Taxonomy
// Fully defined error classes

import type { ProviderName } from "./types";

// =============================================================================
// Error Codes & Categories
// =============================================================================

export type ProviderOperationErrorCode =
  | "CAPACITY_ERROR"
  | "AUTH_ERROR"
  | "RATE_LIMIT_ERROR"
  | "SPOT_INTERRUPTED"
  | "QUOTA_EXCEEDED"
  | "INVALID_SPEC"
  | "REGION_UNAVAILABLE"
  | "NETWORK_ERROR"
  | "TIMEOUT_ERROR"
  | "NOT_FOUND"
  | "ALREADY_EXISTS"
  | "INVALID_STATE"
  | "PROVIDER_INTERNAL"
  | "UNSUPPORTED_OPERATION";

export type ProviderOperationErrorCategory =
  | "capacity"
  | "auth"
  | "rate_limit"
  | "validation"
  | "not_found"
  | "conflict"
  | "internal";

// =============================================================================
// Base Error Class
// =============================================================================

export abstract class ProviderOperationError extends Error {
  abstract readonly code: ProviderOperationErrorCode;
  abstract readonly category: ProviderOperationErrorCategory;
  abstract readonly retryable: boolean;
  abstract readonly retry_after_ms?: number;

  constructor(
    message: string,
    public readonly provider: ProviderName,
    public readonly details?: Record<string, unknown>
  ) {
    super(message);
    this.name = this.constructor.name;
  }
}

// =============================================================================
// Specific Error Classes
// =============================================================================

export class CapacityError extends ProviderOperationError {
  readonly code = "CAPACITY_ERROR" as const;
  readonly category = "capacity" as const;
  readonly retryable = true;

  constructor(
    provider: ProviderName,
    public readonly spec: string,
    public readonly region: string,
    public readonly alternativeRegions?: string[],
    public readonly alternativeSpecs?: string[],
    public readonly retry_after_ms?: number
  ) {
    super(
      `Insufficient capacity for ${spec} in ${region}`,
      provider,
      { spec, region, alternativeRegions, alternativeSpecs }
    );
  }
}

export class AuthError extends ProviderOperationError {
  readonly code = "AUTH_ERROR" as const;
  readonly category = "auth" as const;
  readonly retryable = false;
  readonly retry_after_ms = undefined;

  constructor(
    provider: ProviderName,
    public readonly reason: "invalid_credentials" | "expired" | "insufficient_permissions",
    message?: string
  ) {
    super(
      message ?? `Authentication failed: ${reason}`,
      provider,
      { reason }
    );
  }
}

export class RateLimitError extends ProviderOperationError {
  readonly code = "RATE_LIMIT_ERROR" as const;
  readonly category = "rate_limit" as const;
  readonly retryable = true;

  constructor(
    provider: ProviderName,
    public readonly retry_after_ms: number,
    public readonly limitType?: "api" | "resource" | "budget"
  ) {
    super(
      `Rate limited by ${provider}, retry after ${retry_after_ms}ms`,
      provider,
      { retry_after_ms, limitType }
    );
  }
}

export class SpotInterrupted extends ProviderOperationError {
  readonly code = "SPOT_INTERRUPTED" as const;
  readonly category = "capacity" as const;
  readonly retryable = true;

  constructor(
    provider: ProviderName,
    public readonly instanceId: string,
    public readonly interruptionTime: number,
    public readonly warningTimeMs?: number,
    public readonly retry_after_ms?: number
  ) {
    super(
      `Spot instance ${instanceId} interrupted`,
      provider,
      { instanceId, interruptionTime, warningTimeMs }
    );
  }
}

export class QuotaExceededError extends ProviderOperationError {
  readonly code = "QUOTA_EXCEEDED" as const;
  readonly category = "capacity" as const;
  readonly retryable = false;
  readonly retry_after_ms = undefined;

  constructor(
    provider: ProviderName,
    public readonly quotaType: string,
    public readonly currentUsage: number,
    public readonly limit: number
  ) {
    super(
      `Quota exceeded: ${quotaType} (${currentUsage}/${limit})`,
      provider,
      { quotaType, currentUsage, limit }
    );
  }
}

export class InvalidSpecError extends ProviderOperationError {
  readonly code = "INVALID_SPEC" as const;
  readonly category = "validation" as const;
  readonly retryable = false;
  readonly retry_after_ms = undefined;

  constructor(
    provider: ProviderName,
    public readonly spec: string,
    public readonly validSpecs?: string[]
  ) {
    super(
      `Invalid instance spec: ${spec}`,
      provider,
      { spec, validSpecs }
    );
  }
}

export class NotFoundError extends ProviderOperationError {
  readonly code = "NOT_FOUND" as const;
  readonly category = "not_found" as const;
  readonly retryable = false;
  readonly retry_after_ms = undefined;

  constructor(
    provider: ProviderName,
    public readonly resourceType: string,
    public readonly resourceId: string
  ) {
    super(
      `${resourceType} not found: ${resourceId}`,
      provider,
      { resourceType, resourceId }
    );
  }
}

// =============================================================================
// Generic Concrete Error (replaces per-provider error classes)
// =============================================================================

/**
 * Concrete provider error for use by all providers.
 * Replaces structurally identical per-provider classes (AWSError, OrbStackError).
 * Providers pass their name and error details; no subclass needed.
 */
export class ConcreteProviderError extends ProviderOperationError {
  readonly code: ProviderOperationErrorCode;
  readonly category: ProviderOperationErrorCategory;
  readonly retryable: boolean;
  readonly retry_after_ms?: number;

  constructor(
    provider: ProviderName,
    code: ProviderOperationErrorCode,
    message: string,
    options?: {
      retryable?: boolean;
      retry_after_ms?: number;
      details?: Record<string, unknown>;
    }
  ) {
    super(message, provider, options?.details);
    this.code = code;
    this.category = categorizeErrorCode(code);
    this.retryable = options?.retryable ?? false;
    this.retry_after_ms = options?.retry_after_ms;
  }
}

// =============================================================================
// Error Handling Helpers
// =============================================================================

export function mapProviderOperationError(
  provider: ProviderName,
  error: unknown
): ProviderOperationError {
  if (error instanceof ProviderOperationError) return error;

  const message = error instanceof Error ? error.message : String(error);
  return new class extends ProviderOperationError {
    readonly code = "PROVIDER_INTERNAL" as const;
    readonly category = "internal" as const;
    readonly retryable = false;
    readonly retry_after_ms = undefined;
  }(message, provider, { originalError: error });
}

/**
 * Wrap an async provider operation with error mapping.
 * Catches non-ProviderOperationError exceptions and maps them using the
 * provided mapper function (or falls back to mapProviderOperationError).
 *
 * Usage:
 *   await withProviderErrorMapping("aws", async () => { ... }, mapEC2Error);
 */
export async function withProviderErrorMapping<T>(
  provider: ProviderName,
  fn: () => Promise<T>,
  mapper?: (error: unknown) => ProviderOperationError,
): Promise<T> {
  try {
    return await fn();
  } catch (error) {
    if (error instanceof ProviderOperationError) throw error;
    if (mapper) throw mapper(error);
    throw mapProviderOperationError(provider, error);
  }
}

export function categorizeErrorCode(code: ProviderOperationErrorCode): ProviderOperationErrorCategory {
  switch (code) {
    case "CAPACITY_ERROR":
    case "QUOTA_EXCEEDED":
    case "SPOT_INTERRUPTED":
      return "capacity";
    case "AUTH_ERROR":
      return "auth";
    case "RATE_LIMIT_ERROR":
      return "rate_limit";
    case "INVALID_SPEC":
    case "REGION_UNAVAILABLE":
      return "validation";
    case "NOT_FOUND":
      return "not_found";
    case "ALREADY_EXISTS":
    case "INVALID_STATE":
      return "conflict";
    default:
      return "internal";
  }
}

export function shouldRetryWithAlternative(error: ProviderOperationError): boolean {
  return (
    error.code === "CAPACITY_ERROR" ||
    error.code === "SPOT_INTERRUPTED" ||
    error.code === "REGION_UNAVAILABLE"
  );
}

export function shouldRetry(error: ProviderOperationError): boolean {
  return error.retryable && error.category !== "auth";
}
