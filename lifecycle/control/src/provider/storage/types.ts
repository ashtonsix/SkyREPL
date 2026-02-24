// provider/storage/types.ts - BlobProvider interface
// All blob provider implementations (S3, SQL, MinIO) implement this interface.

export interface UploadOpts {
  contentType?: string;
}

export interface PresignedUrlOpts {
  /** TTL in milliseconds. Default 15min for PUT, 60min for GET */
  ttlMs?: number;
  /** Content type for PUT presigned URLs */
  contentType?: string;
}

// =============================================================================
// Capability Flags (mirrors compute ProviderCapabilities pattern)
// =============================================================================

export interface BlobProviderCapabilities {
  maxObjectBytes: number;
  supportsPresignedUrls: boolean;
  supportsRangeRequests: boolean;
  supportsMultipartUpload: boolean;
  supportsCAS: boolean;
  supportsExpiration: boolean;
}

// =============================================================================
// Lifecycle Hooks (mirrors compute ProviderLifecycleHooks pattern)
// =============================================================================

export interface BlobProviderLifecycleHooks {
  onStartup?(): Promise<void>;
  onShutdown?(): Promise<void>;
  onHealthCheck?(): Promise<{ healthy: boolean; latencyMs: number }>;
}

// =============================================================================
// BlobProvider Interface
// =============================================================================

export interface BlobProvider {
  readonly name: string;
  readonly capabilities: BlobProviderCapabilities;

  upload(key: string, data: Buffer | Uint8Array, opts?: UploadOpts): Promise<void>;
  download(key: string): Promise<Buffer>;
  delete(key: string): Promise<void>;
  exists(key: string): Promise<boolean>;
  generatePresignedUrl(key: string, method: "GET" | "PUT", opts?: PresignedUrlOpts): Promise<string>;
  healthCheck(): Promise<{ healthy: boolean; latencyMs: number }>;
}

export interface BlobProviderConfig {
  providerName: string;
  // Provider-specific config (S3 endpoint/credentials, etc.)
  [key: string]: unknown;
}

// =============================================================================
// BlobProviderError (mirrors compute ConcreteProviderError pattern)
// =============================================================================

export type BlobErrorCode =
  | "BUCKET_NOT_FOUND"
  | "OBJECT_NOT_FOUND"
  | "AUTH_ERROR"
  | "RATE_LIMIT"
  | "TIMEOUT"
  | "QUOTA_EXCEEDED"
  | "PROVIDER_INTERNAL"
  | "UNSUPPORTED_OPERATION";

export class BlobProviderError extends Error {
  constructor(
    message: string,
    public readonly provider: string,
    public readonly code: BlobErrorCode,
    public readonly retryable: boolean,
    public readonly details?: Record<string, unknown>,
  ) {
    super(message);
    this.name = "BlobProviderError";
  }
}

/**
 * Wrap an async blob operation with error mapping.
 * Catches non-BlobProviderError exceptions and maps them.
 */
export async function withBlobErrorMapping<T>(
  provider: string,
  fn: () => Promise<T>,
  mapper?: (error: unknown) => BlobProviderError,
): Promise<T> {
  try {
    return await fn();
  } catch (error) {
    if (error instanceof BlobProviderError) throw error;
    if (mapper) throw mapper(error);
    throw mapBlobError(provider, error);
  }
}

export function mapBlobError(provider: string, error: unknown): BlobProviderError {
  if (error instanceof BlobProviderError) return error;
  const message = error instanceof Error ? error.message : String(error);
  return new BlobProviderError(message, provider, "PROVIDER_INTERNAL", false, { originalError: error });
}

/** Map AWS SDK error codes to BlobProviderError */
export function mapS3Error(provider: string, error: unknown): BlobProviderError {
  const err = error as any;
  const name = err?.name ?? err?.Code ?? "";
  const message = err?.message ?? String(error);

  if (name === "NoSuchBucket") {
    return new BlobProviderError(message, provider, "BUCKET_NOT_FOUND", false);
  }
  if (name === "NoSuchKey" || name === "NotFound" || err?.$metadata?.httpStatusCode === 404) {
    return new BlobProviderError(message, provider, "OBJECT_NOT_FOUND", false);
  }
  if (name === "AccessDenied" || name === "InvalidAccessKeyId" || name === "SignatureDoesNotMatch") {
    return new BlobProviderError(message, provider, "AUTH_ERROR", false);
  }
  if (name === "SlowDown" || name === "Throttling" || err?.$metadata?.httpStatusCode === 429) {
    return new BlobProviderError(message, provider, "RATE_LIMIT", true);
  }
  if (name === "RequestTimeout" || name === "TimeoutError") {
    return new BlobProviderError(message, provider, "TIMEOUT", true);
  }

  // Network errors are retryable
  const retryable = name === "NetworkingError" || name === "ECONNRESET" || name === "ECONNREFUSED";
  return new BlobProviderError(message, provider, "PROVIDER_INTERNAL", retryable, { originalError: error });
}

// =============================================================================
// Constants
// =============================================================================

// SBO threshold: 64KB. Below this, blobs are stored inline in blobs.payload.
// This is NOT configurable -- spec S2.2 mandates it.
export const SBO_THRESHOLD = 65536;

// Presigned URL default TTLs
export const PRESIGNED_PUT_TTL_MS = 15 * 60 * 1000; // 15 min
export const PRESIGNED_GET_TTL_MS = 60 * 60 * 1000; // 60 min
