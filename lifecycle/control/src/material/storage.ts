// material/storage.ts - Object Storage / Blob Storage
// Slice 1: Local filesystem implementation

import { existsSync, mkdirSync, writeFileSync, unlinkSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import {
  getDatabase,
  getBlob,
  findBlobByChecksum,
  updateBlobLastReferenced,
  findOrphanedBlobs,
  deleteBlobBatch,
  deleteObjectBatch,
  queryMany,
} from "./db";
import { TIMING } from "@skyrepl/contracts";

// =============================================================================
// Types
// =============================================================================

export interface MultipartInfo {
  uploadId: string;
  parts: { partNumber: number; url: string }[];
}

export interface UploadUrlResult {
  url: string;
  method: string;
  inline: boolean;
  multipart?: MultipartInfo;
}

export interface DownloadUrlResult {
  url: string;
  inline: boolean;
}

export interface FileInfo {
  path: string;
  checksum: string;
}

export interface PreparedFile {
  path: string;
  blobId: number;
  url: string;
}

// =============================================================================
// Constants
// =============================================================================

export const INLINE_THRESHOLD = 65536; // 64KB
export const LARGE_FILE_THRESHOLD = 104857600; // 100MB

const DATA_DIR = process.env.SKYREPL_DATA_DIR || join(homedir(), ".repl", "data");
const BLOB_DIR = join(DATA_DIR, "blobs");

function ensureBlobDir(): void {
  if (!existsSync(BLOB_DIR)) {
    mkdirSync(BLOB_DIR, { recursive: true });
  }
}

function blobPath(checksum: string): string {
  return join(BLOB_DIR, checksum);
}

// =============================================================================
// Presigned URLs
// =============================================================================

export function generateUploadUrl(
  blobId: number,
  sizeBytes: number,
  contentType?: string,
  reservation?: number
): UploadUrlResult {
  const effectiveSize = reservation && reservation > 0 ? reservation : sizeBytes;
  if (effectiveSize <= INLINE_THRESHOLD) {
    return { url: `/v1/blobs/${blobId}/inline`, method: "PUT", inline: true };
  }
  return { url: `/v1/blobs/${blobId}/upload`, method: "PUT", inline: false };
}

export function generateDownloadUrl(blobId: number): DownloadUrlResult {
  const blob = getBlob(blobId);
  if (!blob) throw new Error(`Blob ${blobId} not found`);
  if (blob.payload !== null) {
    return { url: `/v1/blobs/${blobId}/inline`, inline: true };
  }
  return { url: `/v1/blobs/${blobId}/download`, inline: false };
}

export async function generatePresignedUrls(
  checksums: string[],
  method: "GET" | "PUT",
  ttlMs: number
): Promise<string[]> {
  // In local mode, return direct blob URLs
  return checksums.map(checksum => {
    const blob = findBlobByChecksum("run-files", checksum);
    if (!blob) return `/v1/blobs/missing/${checksum}`;
    return method === "GET" ? `/v1/blobs/${blob.id}/download` : `/v1/blobs/${blob.id}/upload`;
  });
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
// S3 Operations
// =============================================================================

export async function uploadToS3(
  blobId: number,
  data: Buffer,
  contentType?: string
): Promise<void> {
  throw new Error("S3 not available in local mode. Use storeInline() or filesystem storage.");
}

export async function downloadFromS3(blobId: number): Promise<Buffer> {
  throw new Error("S3 not available in local mode. Use getInline() or filesystem storage.");
}

export async function deleteFromS3(blobId: number): Promise<void> {
  throw new Error("S3 not available in local mode.");
}

// =============================================================================
// Multipart Uploads
// =============================================================================

export async function generateMultipartUploadUrls(
  blobId: number,
  totalSize: number,
  contentType?: string
): Promise<UploadUrlResult> {
  throw new Error("Multipart upload not supported in local mode");
}

export async function completeMultipartUpload(
  blobId: number,
  uploadId: string,
  parts: { partNumber: number; etag: string }[]
): Promise<void> {
  throw new Error("Multipart upload not supported in local mode");
}

export async function abortMultipartUpload(
  blobId: number,
  uploadId: string
): Promise<void> {
  throw new Error("Multipart upload not supported in local mode");
}

// =============================================================================
// Blob Deduplication
// =============================================================================

export function checkBlobsExist(
  bucket: string,
  checksums: string[]
): { missing: string[]; existing: Record<string, number> } {
  const DEDUPABLE = ["run-files", "artifacts", "snapshots"];
  if (!DEDUPABLE.includes(bucket)) {
    return { missing: checksums, existing: {} };
  }
  const existing: Record<string, number> = {};
  const missing: string[] = [];
  for (const checksum of checksums) {
    const blob = findBlobByChecksum(bucket, checksum);
    if (blob) {
      existing[checksum] = blob.id;
    } else {
      missing.push(checksum);
    }
  }
  return { missing, existing };
}

export function createBlobWithDedup(
  bucket: string,
  checksum: string,
  data: Buffer
): number {
  const DEDUPABLE = ["run-files", "artifacts", "snapshots"];
  if (DEDUPABLE.includes(bucket)) {
    const existing = findBlobByChecksum(bucket, checksum);
    if (existing) {
      updateBlobLastReferenced(existing.id);
      return existing.id;
    }
  }
  // Create new blob record
  const now = Date.now();
  const db = getDatabase();
  const result = db.prepare(
    "INSERT INTO blobs (bucket, checksum, checksum_bytes, size_bytes, payload, created_at, last_referenced_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
  ).run(bucket, checksum, data.length, data.length, data.length <= INLINE_THRESHOLD ? data : null, now, now);
  const blobId = result.lastInsertRowid as number;

  if (data.length > INLINE_THRESHOLD) {
    // Store on filesystem
    ensureBlobDir();
    writeFileSync(blobPath(checksum), data);
  }
  return blobId;
}

// =============================================================================
// File Sync Preparation
// =============================================================================

export function prepareFilesForRun(
  runId: number,
  files: FileInfo[]
): PreparedFile[] {
  const checksums = files.map(f => f.checksum);
  const { existing } = checkBlobsExist("run-files", checksums);
  const result: PreparedFile[] = [];
  for (const file of files) {
    const blobId = existing[file.checksum];
    if (!blobId) throw new Error(`Blob not found for checksum: ${file.checksum}`);
    const { url } = generateDownloadUrl(blobId);
    result.push({ path: file.path, blobId, url });
  }
  return result;
}

// =============================================================================
// Log Streaming
// =============================================================================

export function appendLogData(blobId: number, newData: Buffer, objectId?: number): void {
  const db = getDatabase();
  const now = Date.now();
  db.prepare(
    "UPDATE blobs SET payload = CASE WHEN payload IS NULL THEN ? ELSE payload || ? END, size_bytes = size_bytes + ?, last_referenced_at = ? WHERE id = ?"
  ).run(newData, newData, newData.length, now, blobId);

  // Update objects.updated_at so getLogUpdates can distinguish new vs old data
  if (objectId) {
    db.prepare("UPDATE objects SET updated_at = ? WHERE id = ?").run(now, objectId);
  }
}

export function getLogUpdates(
  runId: number,
  stream: "stdout" | "stderr",
  lastSeen: number
): { payload: Buffer; updatedAt: number }[] {
  const db = getDatabase();
  return db.prepare(`
    SELECT b.payload, o.updated_at as updatedAt
    FROM objects o
    JOIN blobs b ON o.blob_id = b.id
    JOIN object_tags ot_run ON o.id = ot_run.object_id
    JOIN object_tags ot_stream ON o.id = ot_stream.object_id
    WHERE o.type = 'log'
      AND ot_run.tag = ?
      AND ot_stream.tag = ?
      AND o.updated_at > ?
    ORDER BY o.updated_at
  `).all(`run_id:${runId}`, `stream:${stream}`, lastSeen) as { payload: Buffer; updatedAt: number }[];
}

// =============================================================================
// Background Blob GC
// =============================================================================

export async function runBlobGC(): Promise<number> {
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  const orphaned = findOrphanedBlobs(cutoff);
  if (orphaned.length === 0) return 0;
  // Delete filesystem blobs
  for (const blob of orphaned) {
    if (blob.payload === null && blob.checksum) {
      const path = blobPath(blob.checksum);
      try {
        unlinkSync(path);
      } catch {
        // File may not exist
      }
    }
  }
  deleteBlobBatch(orphaned.map(b => b.id));
  return orphaned.length;
}

// =============================================================================
// Manifest Cleanup
// =============================================================================

export function cleanupManifestObjects(manifestId: number): void {
  const db = getDatabase();
  const objectIds = db.prepare(
    "SELECT resource_id FROM manifest_resources WHERE manifest_id = ? AND resource_type = 'object'"
  ).all(manifestId) as { resource_id: string }[];
  if (objectIds.length === 0) return;
  const ids = objectIds.map(r => parseInt(r.resource_id));
  const placeholders = ids.map(() => "?").join(",");
  db.prepare(`DELETE FROM objects WHERE id IN (${placeholders})`).run(...ids);
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
    const blobBatch = orphanedBlobs.slice(0, batchSize).map(b => b.id);
    deleteBlobBatch(blobBatch);
    console.log(`[gc] Deleted ${blobBatch.length} orphaned blob(s)`);
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
}
