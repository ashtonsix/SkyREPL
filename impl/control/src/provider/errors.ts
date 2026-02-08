// provider/errors.ts - Provider Error Taxonomy
// Fully defined error classes

import type { ProviderName } from "./types";

// =============================================================================
// Error Codes & Categories
// =============================================================================

export type ProviderErrorCode =
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

export type ProviderErrorCategory =
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

export abstract class ProviderError extends Error {
  abstract readonly code: ProviderErrorCode;
  abstract readonly category: ProviderErrorCategory;
  abstract readonly retryable: boolean;
  abstract readonly retryAfterMs?: number;

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

export class CapacityError extends ProviderError {
  readonly code = "CAPACITY_ERROR" as const;
  readonly category = "capacity" as const;
  readonly retryable = true;

  constructor(
    provider: ProviderName,
    public readonly spec: string,
    public readonly region: string,
    public readonly alternativeRegions?: string[],
    public readonly alternativeSpecs?: string[],
    public readonly retryAfterMs?: number
  ) {
    super(
      `Insufficient capacity for ${spec} in ${region}`,
      provider,
      { spec, region, alternativeRegions, alternativeSpecs }
    );
  }
}

export class AuthError extends ProviderError {
  readonly code = "AUTH_ERROR" as const;
  readonly category = "auth" as const;
  readonly retryable = false;
  readonly retryAfterMs = undefined;

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

export class RateLimitError extends ProviderError {
  readonly code = "RATE_LIMIT_ERROR" as const;
  readonly category = "rate_limit" as const;
  readonly retryable = true;

  constructor(
    provider: ProviderName,
    public readonly retryAfterMs: number,
    public readonly limitType?: "api" | "resource" | "budget"
  ) {
    super(
      `Rate limited by ${provider}, retry after ${retryAfterMs}ms`,
      provider,
      { retryAfterMs, limitType }
    );
  }
}

export class SpotInterrupted extends ProviderError {
  readonly code = "SPOT_INTERRUPTED" as const;
  readonly category = "capacity" as const;
  readonly retryable = true;

  constructor(
    provider: ProviderName,
    public readonly instanceId: string,
    public readonly interruptionTime: number,
    public readonly warningTimeMs?: number,
    public readonly retryAfterMs?: number
  ) {
    super(
      `Spot instance ${instanceId} interrupted`,
      provider,
      { instanceId, interruptionTime, warningTimeMs }
    );
  }
}

export class QuotaExceededError extends ProviderError {
  readonly code = "QUOTA_EXCEEDED" as const;
  readonly category = "capacity" as const;
  readonly retryable = false;
  readonly retryAfterMs = undefined;

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

export class InvalidSpecError extends ProviderError {
  readonly code = "INVALID_SPEC" as const;
  readonly category = "validation" as const;
  readonly retryable = false;
  readonly retryAfterMs = undefined;

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

export class NotFoundError extends ProviderError {
  readonly code = "NOT_FOUND" as const;
  readonly category = "not_found" as const;
  readonly retryable = false;
  readonly retryAfterMs = undefined;

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
// Error Handling Helpers
// =============================================================================

export function mapProviderError(
  provider: ProviderName,
  error: unknown
): ProviderError {
  throw new Error("not implemented");
}

export function shouldRetryWithAlternative(error: ProviderError): boolean {
  throw new Error("not implemented");
}

export function shouldRetry(error: ProviderError): boolean {
  throw new Error("not implemented");
}
