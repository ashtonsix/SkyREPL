// provider/storage/sql-blob.ts - SqlBlobProvider
// -- BLOB-PROVIDER: All blob_payloads access MUST go through this provider.
// -- Do not query blob_payloads directly from outside this module.
// Stores large blob payloads in a separate blob_payloads table (SQLite).
// Default provider for self-managed / single-tenant deployments.

import type { BlobProvider, BlobProviderCapabilities, UploadOpts, PresignedUrlOpts } from "./types";
import { BlobProviderError } from "./types";
import { getDatabase } from "../../material/db";

// =============================================================================
// SqlBlobProvider
// =============================================================================

export class SqlBlobProvider implements BlobProvider {
  readonly name = "sql";
  readonly capabilities: BlobProviderCapabilities = {
    maxObjectBytes: 100 * 1024 * 1024, // 100MB practical limit
    supportsPresignedUrls: false,
    supportsRangeRequests: false,
    supportsMultipartUpload: false,
    supportsCAS: false,
    supportsExpiration: false,
  };

  // ---------------------------------------------------------------------------
  // Core CRUD
  // ---------------------------------------------------------------------------

  async upload(key: string, data: Buffer | Uint8Array, _opts?: UploadOpts): Promise<void> {
    const db = getDatabase();
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    // -- BLOB-PROVIDER: blob_payloads write (SqlBlobProvider.upload)
    db.prepare(
      "INSERT INTO blob_payloads (blob_key, payload, size_bytes, created_at) VALUES (?, ?, ?, ?) ON CONFLICT (blob_key) DO UPDATE SET payload = excluded.payload, size_bytes = excluded.size_bytes"
    ).run(key, buf, buf.length, Date.now());
  }

  async download(key: string): Promise<Buffer> {
    const db = getDatabase();
    // -- BLOB-PROVIDER: blob_payloads read (SqlBlobProvider.download)
    const row = db.prepare("SELECT payload FROM blob_payloads WHERE blob_key = ?").get(key) as
      | { payload: Uint8Array }
      | null;
    if (!row) {
      throw new BlobProviderError(`Blob not found: ${key}`, "sql", "OBJECT_NOT_FOUND", false);
    }
    return Buffer.from(row.payload);
  }

  async delete(key: string): Promise<void> {
    const db = getDatabase();
    // -- BLOB-PROVIDER: blob_payloads delete (SqlBlobProvider.delete)
    db.prepare("DELETE FROM blob_payloads WHERE blob_key = ?").run(key);
  }

  async exists(key: string): Promise<boolean> {
    const db = getDatabase();
    // -- BLOB-PROVIDER: blob_payloads read (SqlBlobProvider.exists)
    const row = db.prepare("SELECT 1 FROM blob_payloads WHERE blob_key = ? LIMIT 1").get(key);
    return row != null;
  }

  // ---------------------------------------------------------------------------
  // Presigned URLs (not supported)
  // ---------------------------------------------------------------------------

  async generatePresignedUrl(
    _key: string,
    _method: "GET" | "PUT",
    _opts?: PresignedUrlOpts
  ): Promise<string> {
    throw new BlobProviderError(
      "SQL provider does not support presigned URLs. Control plane proxies all transfers.",
      "sql",
      "UNSUPPORTED_OPERATION",
      false,
    );
  }

  // ---------------------------------------------------------------------------
  // Health Check
  // ---------------------------------------------------------------------------

  async healthCheck(): Promise<{ healthy: boolean; latencyMs: number }> {
    const start = Date.now();
    try {
      const db = getDatabase();
      db.prepare("SELECT 1").get();
      return { healthy: true, latencyMs: Date.now() - start };
    } catch {
      return { healthy: false, latencyMs: Date.now() - start };
    }
  }

  // ---------------------------------------------------------------------------
  // Metrics
  // ---------------------------------------------------------------------------

  /** Get total size of stored blob payloads (for upgrade encouragement) */
  totalSizeBytes(): number {
    const db = getDatabase();
    // -- BLOB-PROVIDER: blob_payloads read (SqlBlobProvider.totalSizeBytes)
    const row = db.prepare("SELECT COALESCE(SUM(size_bytes), 0) as total FROM blob_payloads").get() as {
      total: number;
    };
    return row.total;
  }
}
