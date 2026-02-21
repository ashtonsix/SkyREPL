// errors.ts - Error Types and Factory Functions

import type { ErrorCategory, WorkflowError, NodeError } from './resources';

// =============================================================================
// BASE ERROR CLASS
// =============================================================================

/** Base error class for all SkyREPL errors */
export class SkyREPLError extends Error {
  readonly code: string;
  readonly category: ErrorCategory;
  readonly details?: Record<string, unknown>;
  readonly retry_after_ms?: number;

  constructor(
    code: string,
    message: string,
    category: ErrorCategory,
    options?: {
      details?: Record<string, unknown>;
      retry_after_ms?: number;
      cause?: unknown;
    },
  ) {
    super(message, { cause: options?.cause });
    this.name = 'SkyREPLError';
    this.code = code;
    this.category = category;
    this.details = options?.details;
    this.retry_after_ms = options?.retry_after_ms;
  }
}

/** Provider-specific error (cloud API failures) */
export class ProviderError extends SkyREPLError {
  readonly provider: string;

  constructor(
    provider: string,
    message: string,
    options?: {
      code?: string;
      details?: Record<string, unknown>;
      retry_after_ms?: number;
      cause?: unknown;
    },
  ) {
    super(options?.code ?? 'PROVIDER_INTERNAL', message, 'provider', options);
    this.name = 'ProviderError';
    this.provider = provider;
  }
}

/** Workflow execution error */
export class WorkflowExecutionError extends SkyREPLError {
  readonly workflowId: number;
  readonly nodeId?: string;

  constructor(
    workflowId: number,
    message: string,
    options?: {
      code?: string;
      category?: ErrorCategory;
      nodeId?: string;
      details?: Record<string, unknown>;
      cause?: unknown;
    },
  ) {
    super(
      options?.code ?? 'WORKFLOW_ERROR',
      message,
      options?.category ?? 'internal',
      options,
    );
    this.name = 'WorkflowExecutionError';
    this.workflowId = workflowId;
    this.nodeId = options?.nodeId;
  }
}

/** Validation error (bad input) */
export class ValidationError extends SkyREPLError {
  constructor(
    message: string,
    options?: {
      code?: string;
      details?: Record<string, unknown>;
      cause?: unknown;
    },
  ) {
    super(options?.code ?? 'INVALID_INPUT', message, 'validation', options);
    this.name = 'ValidationError';
  }
}

/** Resource not found error */
export class NotFoundError extends SkyREPLError {
  constructor(
    resourceType: string,
    resourceId: number | string,
    options?: { cause?: unknown },
  ) {
    super(
      `${resourceType.toUpperCase()}_NOT_FOUND`,
      `${resourceType} ${resourceId} not found`,
      'not_found',
      options,
    );
    this.name = 'NotFoundError';
  }
}

/** State conflict error (invalid state transition, ownership conflict, etc.) */
export class ConflictError extends SkyREPLError {
  constructor(
    message: string,
    options?: {
      code?: string;
      details?: Record<string, unknown>;
      cause?: unknown;
    },
  ) {
    super(
      options?.code ?? 'INVALID_STATE_TRANSITION',
      message,
      'conflict',
      options,
    );
    this.name = 'ConflictError';
  }
}

/** Timeout error */
export class TimeoutError extends SkyREPLError {
  constructor(
    message: string,
    options?: {
      code?: string;
      details?: Record<string, unknown>;
      cause?: unknown;
    },
  ) {
    super(
      options?.code ?? 'OPERATION_TIMEOUT',
      message,
      'timeout',
      options,
    );
    this.name = 'TimeoutError';
  }
}

// =============================================================================
// API ERROR RESPONSE TYPE
// =============================================================================

/** Standardized API error response envelope */
export interface ApiError {
  error: {
    code: string;
    message: string;
    category: ErrorCategory;
    details?: Record<string, unknown>;
    retry_after_ms?: number;
    request_id?: string;
  };
}

// =============================================================================
// ERROR CATEGORY -> HTTP STATUS MAPPING
// =============================================================================

/** Map error category to HTTP status code */
export const ERROR_CATEGORY_HTTP_STATUS: Record<ErrorCategory, number> = {
  validation: 400, // Bad Request
  auth: 401, // Unauthorized
  not_found: 404, // Not Found
  conflict: 409, // Conflict
  rate_limit: 429, // Too Many Requests
  provider: 502, // Bad Gateway (cloud provider error)
  timeout: 504, // Gateway Timeout
  internal: 500, // Internal Server Error
};

/** Get HTTP status code for an error category */
export function httpStatusForCategory(category: ErrorCategory): number {
  return ERROR_CATEGORY_HTTP_STATUS[category] ?? 500;
}

/** Get HTTP status code for a specific error code (handles per-code overrides) */
export function httpStatusForErrorCode(code: string, category: ErrorCategory): number {
  // Auth category: FORBIDDEN is 403, everything else is 401
  if (category === 'auth' && code === 'FORBIDDEN') {
    return 403;
  }
  return ERROR_CATEGORY_HTTP_STATUS[category] ?? 500;
}

// =============================================================================
// ERROR CODES BY CATEGORY
// =============================================================================

/** All error codes grouped by category for documentation/validation */
export const ERROR_CODES_BY_CATEGORY: Record<ErrorCategory, string[]> = {
  validation: ['INVALID_INPUT', 'MISSING_REQUIRED_FIELD', 'INVALID_FORMAT', 'SEAT_LIMIT_EXCEEDED'],
  auth: ['UNAUTHORIZED', 'FORBIDDEN'],
  not_found: [
    'RESOURCE_NOT_FOUND',
    'INSTANCE_NOT_FOUND',
    'RUN_NOT_FOUND',
    'ALLOCATION_NOT_FOUND',
    'WORKFLOW_NOT_FOUND',
    'MANIFEST_NOT_FOUND',
    'OBJECT_NOT_FOUND',
  ],
  conflict: [
    'IDEMPOTENCY_CONFLICT',
    'RESOURCE_ALREADY_EXISTS',
    'DUPLICATE_RESOURCE',
    'OWNERSHIP_CONFLICT',
    'INVALID_STATE_TRANSITION',
  ],
  rate_limit: ['RATE_LIMITED', 'BUDGET_EXCEEDED'],
  provider: ['PROVIDER_INTERNAL', 'CAPACITY_ERROR', 'SPOT_INTERRUPTED'],
  timeout: ['REQUEST_TIMEOUT', 'OPERATION_TIMEOUT'],
  internal: ['INTERNAL_ERROR', 'DATABASE_ERROR'],
};

// =============================================================================
// ERROR FACTORY FUNCTIONS
// =============================================================================

/** Create standardized API error response */
export function createApiError(
  code: string,
  message: string,
  category: ErrorCategory,
  details?: Record<string, unknown>,
  retry_after_ms?: number,
): ApiError {
  return {
    error: { code, message, category, details, retry_after_ms },
  };
}

/** Create a WorkflowError for storage in workflow records */
export function createWorkflowError(
  code: string,
  message: string,
  category: ErrorCategory,
  nodeId?: string,
  details?: Record<string, unknown>,
): WorkflowError {
  return { code, message, category, nodeId, details };
}

/** Create a NodeError for storage in workflow node records */
export function createNodeError(
  code: string,
  message: string,
  category: ErrorCategory,
  retryable: boolean,
  details?: Record<string, unknown>,
): NodeError {
  return { code, message, category, retryable, details };
}

/** Determine error category from an error code string */
export function categoryForCode(code: string): ErrorCategory {
  for (const [category, codes] of Object.entries(ERROR_CODES_BY_CATEGORY)) {
    if (codes.includes(code)) {
      return category as ErrorCategory;
    }
  }
  return 'internal';
}

/** Convert a SkyREPLError to an ApiError response */
export function errorToApiError(err: SkyREPLError): ApiError {
  return createApiError(
    err.code,
    err.message,
    err.category,
    err.details,
    err.retry_after_ms,
  );
}

/** Get HTTP status code for a SkyREPLError */
export function httpStatusForError(err: SkyREPLError): number {
  return httpStatusForErrorCode(err.code, err.category);
}
