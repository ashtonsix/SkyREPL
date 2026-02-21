import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { existsSync, rmSync } from "node:fs";
import { setupTest } from "../../../tests/harness";
import { createBlob, getBlob } from "./db";
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

let cleanup: () => Promise<void>;

beforeEach(() => {
  process.env.SKYREPL_DATA_DIR = TEST_DATA_DIR;
  cleanup = setupTest();
});

afterEach(async () => {
  await cleanup();
  if (existsSync(TEST_DATA_DIR)) {
    rmSync(TEST_DATA_DIR, { recursive: true });
  }
  delete process.env.SKYREPL_DATA_DIR;
});

function makeBlob(overrides: Partial<Parameters<typeof createBlob>[0]> = {}) {
  return createBlob({
    bucket: "logs",
    checksum: `test-${Math.random().toString(36).slice(2, 8)}`,
    checksum_bytes: null,
    s3_key: null,
    s3_bucket: null,
    payload: null,
    size_bytes: 0,
    last_referenced_at: Date.now(),
    ...overrides,
  });
}

describe("Storage", () => {
  test("generateUploadUrl: inline for small, upload for large, respects reservation", () => {
    const small = generateUploadUrl(1, 100);
    expect(small.inline).toBe(true);
    expect(small.url).toContain("/inline");
    expect(small.method).toBe("PUT");

    const large = generateUploadUrl(1, INLINE_THRESHOLD + 1);
    expect(large.inline).toBe(false);
    expect(large.url).toContain("/upload");

    // Reservation size overrides current size
    const reserved = generateUploadUrl(1, 100, undefined, INLINE_THRESHOLD + 1);
    expect(reserved.inline).toBe(false);
  });

  test("generateDownloadUrl: inline for payload blobs, download for others, throws on missing", () => {
    const withPayload = makeBlob({ checksum: "dl-inline", payload: Buffer.from("hello"), size_bytes: 5 });
    const inlineResult = generateDownloadUrl(withPayload.id);
    expect(inlineResult.inline).toBe(true);

    const noPayload = makeBlob({ checksum: "dl-external", size_bytes: 100000 });
    const dlResult = generateDownloadUrl(noPayload.id);
    expect(dlResult.inline).toBe(false);
    expect(dlResult.url).toContain("/download");

    expect(() => generateDownloadUrl(99999)).toThrow("not found");
  });

  test("storeInline/getInline: stores and retrieves, returns null when absent", () => {
    const blob = makeBlob({ checksum: "inline-test" });
    expect(getInline(blob.id)).toBeNull();

    const data = Buffer.from("hello world");
    storeInline(blob.id, data);
    const retrieved = getInline(blob.id);
    expect(retrieved).not.toBeNull();
    expect(retrieved!.toString()).toBe("hello world");
  });

  test("checkBlobsExist: all missing for non-dedupable, finds existing in dedupable", () => {
    const nonDedup = checkBlobsExist("logs", ["a", "b", "c"]);
    expect(nonDedup.missing).toEqual(["a", "b", "c"]);
    expect(Object.keys(nonDedup.existing)).toHaveLength(0);

    makeBlob({ bucket: "run-files", checksum: "exists-1", checksum_bytes: 5, payload: Buffer.from("hello"), size_bytes: 5 });
    const dedup = checkBlobsExist("run-files", ["exists-1", "missing-1"]);
    expect(dedup.missing).toEqual(["missing-1"]);
    expect(dedup.existing["exists-1"]).toBeGreaterThan(0);
  });

  test("createBlobWithDedup: creates, deduplicates, handles large blobs", () => {
    const data = Buffer.from("test data");
    const id1 = createBlobWithDedup("run-files", "new-blob", data);
    expect(id1).toBeGreaterThan(0);
    expect(getBlob(id1)!.checksum).toBe("new-blob");

    // Deduplication
    const id2 = createBlobWithDedup("run-files", "new-blob", data);
    expect(id2).toBe(id1);

    // Large blob stored on filesystem (no inline payload)
    const largeData = Buffer.alloc(INLINE_THRESHOLD + 100, "x");
    const largeId = createBlobWithDedup("run-files", "large-blob", largeData);
    expect(getBlob(largeId)!.payload).toBeNull();
  });

  test("appendLogData: appends to existing, initializes null payload", () => {
    // Append to existing
    const blob1 = makeBlob({ checksum: "log-append", payload: Buffer.from("line 1\n"), size_bytes: 7 });
    appendLogData(blob1.id, Buffer.from("line 2\n"));
    expect(getBlob(blob1.id)!.size_bytes).toBe(14);
    expect(getInline(blob1.id)!.toString()).toBe("line 1\nline 2\n");

    // Initialize null payload
    const blob2 = makeBlob({ checksum: "log-null" });
    appendLogData(blob2.id, Buffer.from("first data"));
    expect(getInline(blob2.id)!.toString()).toBe("first data");
  });
});
