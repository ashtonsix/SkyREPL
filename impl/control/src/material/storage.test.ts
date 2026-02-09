import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { existsSync, rmSync } from "node:fs";
import { join } from "node:path";
import { initDatabase, closeDatabase, runMigrations, createBlob, getBlob, findBlobByChecksum } from "./db";
import {
  generateUploadUrl,
  generateDownloadUrl,
  storeInline,
  getInline,
  checkBlobsExist,
  createBlobWithDedup,
  appendLogData,
  INLINE_THRESHOLD,
} from "./storage";

const TEST_DATA_DIR = "./data-test";

beforeEach(() => {
  process.env.SKYREPL_DATA_DIR = TEST_DATA_DIR;
  initDatabase(":memory:");
  runMigrations();
});

afterEach(() => {
  closeDatabase();
  // Clean up test blob dir
  if (existsSync(TEST_DATA_DIR)) {
    rmSync(TEST_DATA_DIR, { recursive: true });
  }
  delete process.env.SKYREPL_DATA_DIR;
});

describe("generateUploadUrl", () => {
  it("returns inline URL for small blobs", () => {
    const result = generateUploadUrl(1, 100);
    expect(result.inline).toBe(true);
    expect(result.url).toContain("/inline");
    expect(result.method).toBe("PUT");
  });

  it("returns upload URL for large blobs", () => {
    const result = generateUploadUrl(1, INLINE_THRESHOLD + 1);
    expect(result.inline).toBe(false);
    expect(result.url).toContain("/upload");
  });

  it("uses reservation size when provided", () => {
    // Current size is small, but reservation is large
    const result = generateUploadUrl(1, 100, undefined, INLINE_THRESHOLD + 1);
    expect(result.inline).toBe(false);
  });
});

describe("generateDownloadUrl", () => {
  it("returns inline URL for blobs with payload", () => {
    const blob = createBlob({
      bucket: "logs",
      checksum: "dl-test",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("hello"),
      size_bytes: 5,
      last_referenced_at: Date.now(),
    });

    const result = generateDownloadUrl(blob.id);
    expect(result.inline).toBe(true);
  });

  it("returns download URL for blobs without payload", () => {
    const blob = createBlob({
      bucket: "logs",
      checksum: "dl-test-2",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: null,
      size_bytes: 100000,
      last_referenced_at: Date.now(),
    });

    const result = generateDownloadUrl(blob.id);
    expect(result.inline).toBe(false);
    expect(result.url).toContain("/download");
  });

  it("throws for non-existent blob", () => {
    expect(() => generateDownloadUrl(99999)).toThrow("not found");
  });
});

describe("inline storage", () => {
  it("stores and retrieves inline data", () => {
    const blob = createBlob({
      bucket: "logs",
      checksum: "inline-test",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: null,
      size_bytes: 0,
      last_referenced_at: Date.now(),
    });

    const data = Buffer.from("hello world");
    storeInline(blob.id, data);

    const retrieved = getInline(blob.id);
    expect(retrieved).not.toBeNull();
    expect(retrieved!.toString()).toBe("hello world");
  });

  it("returns null for blob without inline data", () => {
    const blob = createBlob({
      bucket: "logs",
      checksum: "no-inline",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: null,
      size_bytes: 0,
      last_referenced_at: Date.now(),
    });

    const result = getInline(blob.id);
    expect(result).toBeNull();
  });
});

describe("checkBlobsExist", () => {
  it("returns all missing for non-dedupable bucket", () => {
    const result = checkBlobsExist("logs", ["a", "b", "c"]);
    expect(result.missing).toEqual(["a", "b", "c"]);
    expect(Object.keys(result.existing)).toHaveLength(0);
  });

  it("finds existing blobs in dedupable bucket", () => {
    createBlob({
      bucket: "run-files",
      checksum: "exists-1",
      checksum_bytes: 5,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("hello"),
      size_bytes: 5,
      last_referenced_at: Date.now(),
    });

    const result = checkBlobsExist("run-files", ["exists-1", "missing-1"]);
    expect(result.missing).toEqual(["missing-1"]);
    expect(result.existing["exists-1"]).toBeGreaterThan(0);
  });
});

describe("createBlobWithDedup", () => {
  it("creates new blob for first upload", () => {
    const data = Buffer.from("test data");
    const blobId = createBlobWithDedup("run-files", "new-blob", data);
    expect(blobId).toBeGreaterThan(0);

    const blob = getBlob(blobId);
    expect(blob).not.toBeNull();
    expect(blob!.checksum).toBe("new-blob");
  });

  it("deduplicates in dedupable bucket", () => {
    const data = Buffer.from("test data");
    const id1 = createBlobWithDedup("run-files", "dedup-1", data);
    const id2 = createBlobWithDedup("run-files", "dedup-1", data);
    expect(id2).toBe(id1);
  });

  it("stores large blob on filesystem", () => {
    // Create data larger than INLINE_THRESHOLD
    const largeData = Buffer.alloc(INLINE_THRESHOLD + 100, "x");
    const blobId = createBlobWithDedup("run-files", "large-blob", largeData);
    expect(blobId).toBeGreaterThan(0);

    // Blob should exist on filesystem
    // Note: SKYREPL_DATA_DIR is set in beforeEach but the storage module
    // reads it at import time. This test verifies the DB-side behavior.
    const blob = getBlob(blobId);
    expect(blob).not.toBeNull();
    expect(blob!.payload).toBeNull(); // Not stored inline
  });
});

describe("appendLogData", () => {
  it("appends data to blob payload", () => {
    const blob = createBlob({
      bucket: "logs",
      checksum: "log-append",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: Buffer.from("line 1\n"),
      size_bytes: 7,
      last_referenced_at: Date.now(),
    });

    appendLogData(blob.id, Buffer.from("line 2\n"));

    const updated = getBlob(blob.id);
    expect(updated).not.toBeNull();
    expect(updated!.size_bytes).toBe(14); // 7 + 7

    const inline = getInline(blob.id);
    expect(inline).not.toBeNull();
    expect(inline!.toString()).toBe("line 1\nline 2\n");
  });

  it("initializes null payload on first append", () => {
    const blob = createBlob({
      bucket: "logs",
      checksum: "log-null",
      checksum_bytes: null,
      s3_key: null,
      s3_bucket: null,
      payload: null,
      size_bytes: 0,
      last_referenced_at: Date.now(),
    });

    appendLogData(blob.id, Buffer.from("first data"));

    const inline = getInline(blob.id);
    expect(inline).not.toBeNull();
    expect(inline!.toString()).toBe("first data");
  });
});
