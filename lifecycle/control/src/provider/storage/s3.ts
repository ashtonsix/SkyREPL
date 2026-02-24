// provider/storage/s3.ts - S3BlobProvider
// S3-compatible blob storage (AWS S3, MinIO, Backblaze B2, DigitalOcean Spaces).

import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  HeadObjectCommand,
  CreateBucketCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import type { BlobProvider, BlobProviderCapabilities, BlobProviderLifecycleHooks, StorageTaskReceipt, StorageHeartbeatExpectations, UploadOpts, PresignedUrlOpts } from "./types";
import { PRESIGNED_PUT_TTL_MS, PRESIGNED_GET_TTL_MS, BlobProviderError, mapS3Error, withBlobErrorMapping } from "./types";

// =============================================================================
// Configuration
// =============================================================================

export interface S3BlobProviderConfig {
  endpoint?: string;
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  forcePathStyle?: boolean;
  /** Separate endpoint for presigned URLs (e.g., external IP for MinIO reachable from VMs) */
  presignedUrlEndpoint?: string;
  /** Inject S3Client for testing. If not provided, creates a real S3Client. */
  _clientFactory?: (opts: { endpoint?: string; region: string; credentials: { accessKeyId: string; secretAccessKey: string }; forcePathStyle: boolean }) => S3Client;
}

// =============================================================================
// S3BlobProvider
// =============================================================================

export class S3BlobProvider implements BlobProvider {
  readonly name: string;
  readonly capabilities: BlobProviderCapabilities = {
    maxObjectBytes: 5 * 1024 * 1024 * 1024, // 5GB single PUT
    supportsPresignedUrls: true,
    supportsRangeRequests: true,
    supportsMultipartUpload: true,
    supportsCAS: false,
    supportsExpiration: true,
  };

  private client: S3Client;
  private presignClient: S3Client | null;
  private bucket: string;

  constructor(name: string, config: S3BlobProviderConfig) {
    this.name = name;
    this.bucket = config.bucket;

    const factory = config._clientFactory ?? ((opts) => new S3Client(opts));

    this.client = factory({
      endpoint: config.endpoint,
      region: config.region,
      credentials: {
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey,
      },
      forcePathStyle: config.forcePathStyle ?? false,
    });

    // If presignedUrlEndpoint differs, create a separate client for URL generation.
    // This enables MinIO's external endpoint rewriting: the internal client talks
    // to localhost, the presign client uses the external IP so URLs are reachable
    // from VMs.
    if (config.presignedUrlEndpoint && config.presignedUrlEndpoint !== config.endpoint) {
      this.presignClient = factory({
        endpoint: config.presignedUrlEndpoint,
        region: config.region,
        credentials: {
          accessKeyId: config.accessKeyId,
          secretAccessKey: config.secretAccessKey,
        },
        forcePathStyle: config.forcePathStyle ?? false,
      });
    } else {
      this.presignClient = null;
    }
  }

  // ---------------------------------------------------------------------------
  // Core CRUD
  // ---------------------------------------------------------------------------

  async upload(key: string, data: Buffer | Uint8Array, opts?: UploadOpts): Promise<void> {
    return withBlobErrorMapping(this.name, async () => {
      await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: key,
          Body: data,
          ContentType: opts?.contentType ?? "application/octet-stream",
        })
      );
    }, (err) => mapS3Error(this.name, err));
  }

  async download(key: string): Promise<Buffer> {
    return withBlobErrorMapping(this.name, async () => {
      const response = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: key,
        })
      );
      if (!response.Body) {
        throw new BlobProviderError(`Empty response body for key: ${key}`, this.name, "PROVIDER_INTERNAL", true);
      }
      const bytes = await response.Body.transformToByteArray();
      return Buffer.from(bytes);
    }, (err) => mapS3Error(this.name, err));
  }

  async delete(key: string): Promise<void> {
    return withBlobErrorMapping(this.name, async () => {
      await this.client.send(
        new DeleteObjectCommand({
          Bucket: this.bucket,
          Key: key,
        })
      );
    }, (err) => mapS3Error(this.name, err));
  }

  async exists(key: string): Promise<boolean> {
    try {
      await this.client.send(
        new HeadObjectCommand({
          Bucket: this.bucket,
          Key: key,
        })
      );
      return true;
    } catch (err: any) {
      if (err.name === "NotFound" || err.$metadata?.httpStatusCode === 404) {
        return false;
      }
      throw mapS3Error(this.name, err);
    }
  }

  // ---------------------------------------------------------------------------
  // Presigned URLs
  // ---------------------------------------------------------------------------

  async generatePresignedUrl(
    key: string,
    method: "GET" | "PUT",
    opts?: PresignedUrlOpts
  ): Promise<string> {
    return withBlobErrorMapping(this.name, async () => {
      const client = this.presignClient ?? this.client;
      const ttlMs = opts?.ttlMs ?? (method === "PUT" ? PRESIGNED_PUT_TTL_MS : PRESIGNED_GET_TTL_MS);
      const expiresIn = Math.ceil(ttlMs / 1000);

      const command =
        method === "GET"
          ? new GetObjectCommand({ Bucket: this.bucket, Key: key })
          : new PutObjectCommand({
              Bucket: this.bucket,
              Key: key,
              ContentType: opts?.contentType ?? "application/octet-stream",
            });

      return getSignedUrl(client, command, { expiresIn });
    }, (err) => mapS3Error(this.name, err));
  }

  // ---------------------------------------------------------------------------
  // Health Check
  // ---------------------------------------------------------------------------

  async healthCheck(): Promise<{ healthy: boolean; latencyMs: number }> {
    const start = Date.now();
    try {
      await this.client.send(
        new HeadObjectCommand({
          Bucket: this.bucket,
          Key: "__healthcheck__",
        })
      );
      // HeadObject on nonexistent key throws 404 -- that's healthy (bucket is reachable)
      return { healthy: true, latencyMs: Date.now() - start };
    } catch (err: any) {
      if (err.name === "NotFound" || err.$metadata?.httpStatusCode === 404) {
        return { healthy: true, latencyMs: Date.now() - start };
      }
      return { healthy: false, latencyMs: Date.now() - start };
    }
  }

  // ---------------------------------------------------------------------------
  // Bucket Management
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // Lifecycle Hooks Factory
  // ---------------------------------------------------------------------------

  /**
   * Create lifecycle hooks for this S3BlobProvider instance.
   * Includes onStartup (bucket ensure), onHealthCheck, and onHeartbeat.
   */
  createHooks(): BlobProviderLifecycleHooks {
    const provider = this;
    return {
      async onStartup() {
        await provider.ensureBucket();
      },
      async onHealthCheck() {
        return provider.healthCheck();
      },
      async onHeartbeat(expectations: StorageHeartbeatExpectations) {
        const receipts: StorageTaskReceipt[] = [];
        for (const task of expectations.tasks) {
          switch (task.type) {
            case 'health_check': {
              try {
                const result = await provider.healthCheck();
                receipts.push({ type: 'health_check', status: result.healthy ? 'completed' : 'failed' });
              } catch (err) {
                receipts.push({ type: 'health_check', status: 'failed', reason: String(err) });
              }
              break;
            }
            case 'blob_gc':
              // Acknowledge — actual GC logic runs in the control plane dispatch loop
              receipts.push({ type: 'blob_gc', status: 'completed' });
              break;
            case 'log_compaction':
              // Acknowledge — actual compaction logic runs in the control plane dispatch loop
              receipts.push({ type: 'log_compaction', status: 'completed' });
              break;
            default:
              receipts.push({ type: task.type, status: 'skipped', reason: `Unknown task type: ${task.type}` });
          }
        }
        return { receipts };
      },
    };
  }

  /** Ensure bucket exists. Called at registration via lifecycle hooks. */
  async ensureBucket(): Promise<void> {
    return withBlobErrorMapping(this.name, async () => {
      try {
        await this.client.send(new CreateBucketCommand({ Bucket: this.bucket }));
      } catch (err: any) {
        // BucketAlreadyOwnedByYou / BucketAlreadyExists are fine
        if (err.name === "BucketAlreadyOwnedByYou" || err.name === "BucketAlreadyExists") {
          return;
        }
        throw err;
      }
    }, (err) => mapS3Error(this.name, err));
  }
}
