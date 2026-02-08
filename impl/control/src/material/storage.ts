// material/storage.ts - Object Storage / Blob Storage
// Stub: All function bodies throw "not implemented"

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

// =============================================================================
// Presigned URLs
// =============================================================================

export function generateUploadUrl(
  blobId: number,
  sizeBytes: number,
  contentType?: string,
  reservation?: number
): UploadUrlResult {
  throw new Error("not implemented");
}

export function generateDownloadUrl(blobId: number): DownloadUrlResult {
  throw new Error("not implemented");
}

export function generatePresignedUrls(
  checksums: string[],
  method: "GET" | "PUT",
  ttlMs: number
): Promise<string[]> {
  throw new Error("not implemented");
}

// =============================================================================
// Inline Storage
// =============================================================================

export function storeInline(blobId: number, data: Buffer): void {
  throw new Error("not implemented");
}

export function getInline(blobId: number): Buffer | null {
  throw new Error("not implemented");
}

// =============================================================================
// S3 Operations
// =============================================================================

export function uploadToS3(
  blobId: number,
  data: Buffer,
  contentType?: string
): Promise<void> {
  throw new Error("not implemented");
}

export function downloadFromS3(blobId: number): Promise<Buffer> {
  throw new Error("not implemented");
}

export function deleteFromS3(blobId: number): Promise<void> {
  throw new Error("not implemented");
}

// =============================================================================
// Multipart Uploads
// =============================================================================

export function generateMultipartUploadUrls(
  blobId: number,
  totalSize: number,
  contentType?: string
): Promise<UploadUrlResult> {
  throw new Error("not implemented");
}

export function completeMultipartUpload(
  blobId: number,
  uploadId: string,
  parts: { partNumber: number; etag: string }[]
): Promise<void> {
  throw new Error("not implemented");
}

export function abortMultipartUpload(
  blobId: number,
  uploadId: string
): Promise<void> {
  throw new Error("not implemented");
}

// =============================================================================
// Blob Deduplication
// =============================================================================

export function checkBlobsExist(
  bucket: string,
  checksums: string[]
): { missing: string[]; existing: Record<string, number> } {
  throw new Error("not implemented");
}

export function createBlobWithDedup(
  bucket: string,
  checksum: string,
  data: Buffer
): number {
  throw new Error("not implemented");
}

// =============================================================================
// File Sync Preparation
// =============================================================================

export function prepareFilesForRun(
  runId: number,
  files: FileInfo[]
): PreparedFile[] {
  throw new Error("not implemented");
}

// =============================================================================
// Log Streaming
// =============================================================================

export function appendLogData(blobId: number, newData: Buffer): void {
  throw new Error("not implemented");
}

export function getLogUpdates(
  runId: number,
  stream: "stdout" | "stderr",
  lastSeen: number
): { payload: Buffer; updatedAt: number }[] {
  throw new Error("not implemented");
}

// =============================================================================
// Background Blob GC
// =============================================================================

export async function runBlobGC(): Promise<number> {
  throw new Error("not implemented");
}

// =============================================================================
// Manifest Cleanup
// =============================================================================

export function cleanupManifestObjects(manifestId: number): void {
  throw new Error("not implemented");
}
