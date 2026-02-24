// ─────────────────────────────────────────────────────────────────────────────
// RAW DB LAYER — blobs and objects tables
// No materializer exists for blobs/objects yet. These are used directly
// by the blob storage layer and object tracking layer.
// DB operations below — add new queries here, not at call sites.
// ─────────────────────────────────────────────────────────────────────────────
// db/objects.ts - Blob, StorageObject, and Tag operations

import { ConflictError } from "@skyrepl/contracts";
import { getDatabase, queryOne, queryMany, execute } from "./helpers";

export interface Blob {
  id: number;
  tenant_id: number;
  bucket: string;
  checksum: string;
  checksum_bytes: number | null;
  s3_key: string | null;
  s3_bucket: string | null;
  payload: Buffer | null;
  size_bytes: number;
  created_at: number;
  last_referenced_at: number;
}

export interface StorageObject {
  id: number;
  tenant_id: number;
  type: "snapshot" | "artifact" | "log" | "run_file" | "tailscale_machine" | "feature_installation" | "tailscale_device";
  blob_id: number;
  provider: string | null;
  provider_object_id: string | null;
  metadata_json: string | null;
  expires_at: number | null;
  current_manifest_id: number | null;
  created_at: number;
  accessed_at: number | null;
  updated_at: number | null;
}

// --- Blob Operations ---

export function getBlob(id: number): Blob | null {
  return queryOne<Blob>("SELECT * FROM blobs WHERE id = ?", [id]);
}

export function createBlob(data: Omit<Blob, "id" | "created_at" | "tenant_id">, tenantId: number = 1): Blob {
  const now = Date.now();
  const db = getDatabase();

  const DEDUPABLE_BUCKETS = ["run-files", "artifacts"];

  // For dedupable buckets, check if blob exists
  if (DEDUPABLE_BUCKETS.includes(data.bucket) && data.checksum) {
    const existing = findBlobByChecksum(data.bucket, data.checksum);
    if (existing) {
      // Update last_referenced_at and return existing
      updateBlobLastReferenced(existing.id);
      return existing;
    }
  }

  const stmt = db.prepare(`
    INSERT INTO blobs (tenant_id, bucket, checksum, checksum_bytes, s3_key, s3_bucket, payload, size_bytes, created_at, last_referenced_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const result = stmt.run(
    tenantId,
    data.bucket,
    data.checksum,
    data.checksum_bytes,
    data.s3_key,
    data.s3_bucket,
    data.payload,
    data.size_bytes,
    now,
    data.last_referenced_at
  );

  return getBlob(result.lastInsertRowid as number)!;
}

export function findBlobByChecksum(
  bucket: string,
  checksum: string
): Blob | null {
  return queryOne<Blob>(
    "SELECT * FROM blobs WHERE bucket = ? AND checksum = ? LIMIT 1",
    [bucket, checksum]
  );
}

export function updateBlobLastReferenced(id: number): void {
  const now = Date.now();
  execute("UPDATE blobs SET last_referenced_at = ? WHERE id = ?", [now, id]);
}

export function findOrphanedBlobs(cutoff24hAgo: number): Blob[] {
  return queryMany<Blob>(
    `SELECT b.*
     FROM blobs b
     WHERE b.last_referenced_at < ?
       AND NOT EXISTS (
         SELECT 1 FROM objects o
         WHERE o.blob_id = b.id
       )
       AND NOT EXISTS (
         SELECT 1 FROM log_chunks lc
         WHERE lc.blob_id = b.id
       )`,
    [cutoff24hAgo]
  );
}

export function deleteBlobBatch(blobIds: number[]): void {
  if (blobIds.length === 0) return;

  const db = getDatabase();
  const placeholders = blobIds.map(() => "?").join(",");

  db.prepare(`DELETE FROM blobs WHERE id IN (${placeholders})`).run(...blobIds);
}

export function deleteBlob(id: number): void {
  // Used by GC only - checks no objects or log_chunks reference this blob
  const refs = queryOne<{ count: number }>(
    "SELECT COUNT(*) as count FROM objects WHERE blob_id = ?",
    [id]
  );
  if (refs && refs.count > 0) {
    throw new ConflictError("Cannot delete blob with referencing objects");
  }

  const logRefs = queryOne<{ count: number }>(
    "SELECT COUNT(*) as count FROM log_chunks WHERE blob_id = ?",
    [id]
  );
  if (logRefs && logRefs.count > 0) {
    throw new ConflictError("Cannot delete blob with referencing log chunks");
  }

  execute("DELETE FROM blobs WHERE id = ?", [id]);
}

// --- Object Operations ---

export function getObject(id: number): StorageObject | null {
  return queryOne<StorageObject>("SELECT * FROM objects WHERE id = ?", [id]);
}

export function createObject(
  data: Omit<StorageObject, "id" | "created_at" | "tenant_id">,
  tenantId: number = 1
): StorageObject {
  const now = Date.now();
  const db = getDatabase();

  const stmt = db.prepare(`
    INSERT INTO objects (tenant_id, type, blob_id, provider, provider_object_id, metadata_json, expires_at, current_manifest_id, created_at, accessed_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const result = stmt.run(
    tenantId,
    data.type,
    data.blob_id,
    data.provider,
    data.provider_object_id,
    data.metadata_json,
    data.expires_at,
    data.current_manifest_id,
    now,
    data.accessed_at,
    data.updated_at ?? now
  );

  // Touch blob last_referenced_at
  updateBlobLastReferenced(data.blob_id);

  return getObject(result.lastInsertRowid as number)!;
}

export function addObjectTag(
  objectId: number,
  key: string,
  value: string
): void {
  const tag = `${key}:${value}`;

  execute(
    `INSERT INTO object_tags (object_id, tag)
     VALUES (?, ?)
     ON CONFLICT (object_id, tag) DO NOTHING`,
    [objectId, tag]
  );
}

export function findObjectByTag(
  key: string,
  value: string
): StorageObject | null {
  const tag = `${key}:${value}`;
  return queryOne<StorageObject>(
    `SELECT o.* FROM objects o
     JOIN object_tags t ON o.id = t.object_id
     WHERE t.tag = ?`,
    [tag]
  );
}

export function deleteObject(id: number): void {
  execute("DELETE FROM objects WHERE id = ?", [id]);
}

export function updateObjectTimestamp(objectId: number): void {
  execute("UPDATE objects SET updated_at = ? WHERE id = ?", [Date.now(), objectId]);
}

/** Update object metadata JSON. */
export function updateObjectMetadata(id: number, metadataJson: string): void {
  execute("UPDATE objects SET metadata_json = ?, updated_at = ? WHERE id = ?", [metadataJson, Date.now(), id]);
}

/** Update blob storage key and size after external upload. */
export function updateBlobStorageKey(id: number, s3Key: string, sizeBytes: number): void {
  execute("UPDATE blobs SET s3_key = ?, size_bytes = ? WHERE id = ?", [s3Key, sizeBytes, id]);
}

/** Update blob size after upload (when s3_key already set). */
export function updateBlobSize(id: number, sizeBytes: number): void {
  execute("UPDATE blobs SET size_bytes = ? WHERE id = ?", [sizeBytes, id]);
}

export function deleteObjectBatch(objectIds: number[]): void {
  if (objectIds.length === 0) return;

  const db = getDatabase();
  const placeholders = objectIds.map(() => "?").join(",");

  db.prepare(`DELETE FROM objects WHERE id IN (${placeholders})`).run(...objectIds);
}

/**
 * Batch check which checksums exist in a bucket.
 * Spec MUST (Ch02 §2.2): return distinct lists of existing and missing checksums.
 */
export function checkBlobsExist(
  bucket: string,
  checksums: string[]
): { existing: string[]; missing: string[] } {
  if (checksums.length === 0) return { existing: [], missing: [] };

  const placeholders = checksums.map(() => "?").join(",");
  const rows = queryMany<{ checksum: string }>(
    `SELECT DISTINCT checksum FROM blobs WHERE bucket = ? AND checksum IN (${placeholders})`,
    [bucket, ...checksums]
  );

  const found = new Set(rows.map(r => r.checksum));
  const existing: string[] = [];
  const missing: string[] = [];

  for (const cs of checksums) {
    if (found.has(cs)) {
      existing.push(cs);
    } else {
      missing.push(cs);
    }
  }

  return { existing, missing };
}
