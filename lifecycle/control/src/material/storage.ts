// material/storage.ts - Object Storage / Blob Storage

import {
  getDatabase,
  getBlob,
  findOrphanedBlobs,
  deleteBlobBatch,
  deleteObjectBatch,
  queryMany,
} from "./db";
import { TIMING } from "@skyrepl/contracts";
import { getBlobProvider, getDefaultBlobProvider } from "../provider/storage/registry";
import { BlobProviderError, PRESIGNED_PUT_TTL_MS, PRESIGNED_GET_TTL_MS } from "../provider/storage/types";
import { SqlBlobProvider } from "../provider/storage/sql-blob";

// =============================================================================
// Types
// =============================================================================

export interface UploadUrlResult {
  url: string;
  method: string;
  inline: boolean;
}

export interface DownloadUrlResult {
  url: string;
  inline: boolean;
}

// =============================================================================
// Constants
// =============================================================================

export const INLINE_THRESHOLD = 65536; // 64KB
export const LARGE_FILE_THRESHOLD = 104857600; // 100MB

// Storage size thresholds for upgrade encouragement
export const SQL_STORAGE_ADVISORY_BYTES = 1 * 1024 * 1024 * 1024;  // 1GB
export const SQL_STORAGE_STRONG_BYTES   = 5 * 1024 * 1024 * 1024;  // 5GB

// =============================================================================
// SQL Storage Size Cache
// =============================================================================

/** Cached total size of SQL blob storage (bytes). Refreshed during GC. */
let _sqlStorageSizeCache: number = 0;

/**
 * Return the last-known total SQL blob payload size in bytes.
 * The value is refreshed during storageGarbageCollection().
 */
export function getSqlStorageSizeCache(): number {
  return _sqlStorageSizeCache;
}

/**
 * Refresh the SQL storage size cache immediately.
 * Called during GC and at startup.
 */
export function refreshSqlStorageSizeCache(): void {
  const provider = getDefaultBlobProvider();
  if (provider instanceof SqlBlobProvider) {
    _sqlStorageSizeCache = provider.totalSizeBytes();
  }
}

// =============================================================================
// Presigned URLs (async — supports all provider types)
// =============================================================================

export async function generateDownloadUrlAsync(blobId: number): Promise<DownloadUrlResult> {
  const blob = getBlob(blobId);
  if (!blob) throw new Error(`Blob ${blobId} not found`);
  if (blob.payload !== null) {
    return { url: `/v1/blobs/${blobId}/data`, inline: true };
  }
  if (blob.s3_key) {
    const provider = getBlobProvider(blob.tenant_id);
    if (provider.capabilities.supportsPresignedUrls) {
      const url = await provider.generatePresignedUrl(blob.s3_key, "GET", { ttlMs: PRESIGNED_GET_TTL_MS });
      return { url, inline: false };
    }
  }
  return { url: `/v1/blobs/${blobId}/download`, inline: false };
}

export async function generateUploadUrlAsync(
  blobId: number,
  sizeBytes: number,
  contentType?: string,
  reservation?: number
): Promise<UploadUrlResult> {
  const effectiveSize = reservation && reservation > 0 ? reservation : sizeBytes;
  if (effectiveSize < INLINE_THRESHOLD) {
    return { url: `/v1/blobs/${blobId}/data`, method: "PUT", inline: true };
  }
  const blob = getBlob(blobId);
  if (blob?.s3_key) {
    const provider = getBlobProvider(blob.tenant_id);
    if (provider.capabilities.supportsPresignedUrls) {
      const url = await provider.generatePresignedUrl(blob.s3_key, "PUT", {
        ttlMs: PRESIGNED_PUT_TTL_MS,
        contentType,
      });
      return { url, method: "PUT", inline: false };
    }
  }
  return { url: `/v1/blobs/${blobId}/upload`, method: "PUT", inline: false };
}

// =============================================================================
// Inline Storage
// =============================================================================

export function storeInline(blobId: number, data: Buffer): void {
  const db = getDatabase();
  db.prepare("UPDATE blobs SET payload = ?, size_bytes = ? WHERE id = ?")
    .run(data, data.length, blobId);
}

export function getInline(blobId: number): Buffer | null {
  const db = getDatabase();
  const row = db.prepare("SELECT payload FROM blobs WHERE id = ?").get(blobId) as { payload: Uint8Array | null } | null;
  if (!row?.payload) return null;
  return Buffer.from(row.payload);
}

// =============================================================================
// Background: Storage Garbage Collection
// =============================================================================

export async function storageGarbageCollection(): Promise<void> {
  const now = Date.now();
  const batchSize = 500;

  // Sub-task A: Blob GC - delete orphaned blobs unreferenced for grace period
  const graceCutoff = now - TIMING.BLOB_GRACE_PERIOD_MS;
  const orphanedBlobs = findOrphanedBlobs(graceCutoff);

  if (orphanedBlobs.length > 0) {
    // Batch delete: take first 500 blobs (per spec batch size)
    const candidates = orphanedBlobs.slice(0, batchSize);
    const blobBatch: number[] = [];

    for (const blob of candidates) {
      if (blob.s3_key) {
        try {
          await getBlobProvider(blob.tenant_id).delete(blob.s3_key);
          blobBatch.push(blob.id);
        } catch (err) {
          if (err instanceof BlobProviderError && err.retryable) {
            // Transient error — skip this blob, retry next GC cycle
            console.warn(`[gc] Skipping blob ${blob.id} (retryable error: ${err.message})`);
            continue;
          }
          // Non-retryable error — proceed with DB deletion anyway
          blobBatch.push(blob.id);
        }
      } else {
        blobBatch.push(blob.id);
      }
    }

    if (blobBatch.length > 0) {
      deleteBlobBatch(blobBatch);
      console.log(`[gc] Deleted ${blobBatch.length} orphaned blob(s)`);
    }
  }

  // Sub-task B: Object expiry - delete objects with expires_at in the past
  const expiredObjects = queryMany<{ id: number }>(
    "SELECT id FROM objects WHERE expires_at IS NOT NULL AND expires_at < ? LIMIT ?",
    [now, batchSize]
  );

  if (expiredObjects.length > 0) {
    const objectIds = expiredObjects.map(o => o.id);
    deleteObjectBatch(objectIds);
    console.log(`[gc] Expired ${objectIds.length} object(s)`);
  }

  // Sub-task C: Refresh SQL storage size cache and warn if thresholds exceeded
  refreshSqlStorageSizeCache();
  const sqlSize = _sqlStorageSizeCache;
  if (sqlSize >= SQL_STORAGE_STRONG_BYTES) {
    const gb = (sqlSize / (1024 * 1024 * 1024)).toFixed(1);
    console.warn(
      `[storage] WARNING: SQL blob storage at ${gb}GB — strongly recommended to upgrade to S3/MinIO for reliability and performance.`
    );
  } else if (sqlSize >= SQL_STORAGE_ADVISORY_BYTES) {
    const gb = (sqlSize / (1024 * 1024 * 1024)).toFixed(1);
    console.warn(
      `[storage] SQL blob storage at ${gb}GB. Consider upgrading to S3/MinIO for better performance.`
    );
  }
}
