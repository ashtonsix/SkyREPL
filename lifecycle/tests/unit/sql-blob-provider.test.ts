// tests/unit/sql-blob-provider.test.ts - SqlBlobProvider unit tests
// Isolated unit tests for the SQL blob storage provider.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import { SqlBlobProvider } from "../../control/src/provider/storage/sql-blob";
import { BlobProviderError } from "../../control/src/provider/storage/types";

let cleanup: () => Promise<void>;
beforeEach(() => {
  cleanup = setupTest();
});
afterEach(async () => {
  await cleanup();
});

describe("SqlBlobProvider: CRUD operations", () => {
  test("upload, exists, download, delete round-trip", async () => {
    const provider = new SqlBlobProvider();
    const key = "test/round-trip.bin";
    const data = Buffer.from("hello world");

    // Not present initially
    expect(await provider.exists(key)).toBe(false);

    // Upload
    await provider.upload(key, data);
    expect(await provider.exists(key)).toBe(true);

    // Download matches upload
    const downloaded = await provider.download(key);
    expect(downloaded).toEqual(data);

    // Delete
    await provider.delete(key);
    expect(await provider.exists(key)).toBe(false);
  });

  test("proxy upload round-trip: downloaded data is byte-identical to uploaded data", async () => {
    const provider = new SqlBlobProvider();
    const key = "test/proxy-round-trip.bin";
    // Use a Uint8Array (as the proxy path would supply)
    const original = new Uint8Array([0x00, 0x01, 0x02, 0xfe, 0xff, 0x80]);

    await provider.upload(key, original);
    const downloaded = await provider.download(key);

    expect(downloaded.length).toBe(original.length);
    for (let i = 0; i < original.length; i++) {
      expect(downloaded[i]).toBe(original[i]);
    }
  });

  test("upload with upsert: uploading same key twice overwrites", async () => {
    const provider = new SqlBlobProvider();
    const key = "test/upsert.txt";
    const first = Buffer.from("first value");
    const second = Buffer.from("second value, longer content");

    await provider.upload(key, first);
    expect((await provider.download(key)).toString()).toBe("first value");

    await provider.upload(key, second);
    expect((await provider.download(key)).toString()).toBe("second value, longer content");

    // Only one entry exists (no duplicate rows)
    expect(await provider.exists(key)).toBe(true);
  });

  test("download of nonexistent key throws BlobProviderError with code OBJECT_NOT_FOUND", async () => {
    const provider = new SqlBlobProvider();

    let caughtError: unknown;
    try {
      await provider.download("nonexistent/key.bin");
    } catch (err) {
      caughtError = err;
    }
    expect(caughtError).toBeInstanceOf(BlobProviderError);
    expect((caughtError as BlobProviderError).code).toBe("OBJECT_NOT_FOUND");
    expect((caughtError as BlobProviderError).name).toBe("BlobProviderError");
  });
});

describe("SqlBlobProvider: generatePresignedUrl", () => {
  test("throws BlobProviderError with code UNSUPPORTED_OPERATION", async () => {
    const provider = new SqlBlobProvider();

    for (const method of ["GET", "PUT"] as const) {
      let caughtError: unknown;
      try {
        await provider.generatePresignedUrl("any/key", method);
      } catch (err) {
        caughtError = err;
      }
      expect(caughtError).toBeInstanceOf(BlobProviderError);
      expect((caughtError as BlobProviderError).code).toBe("UNSUPPORTED_OPERATION");
      expect((caughtError as BlobProviderError).name).toBe("BlobProviderError");
    }
  });
});

describe("SqlBlobProvider: healthCheck", () => {
  test("returns { healthy: true, latencyMs: <number> } when DB is available", async () => {
    const provider = new SqlBlobProvider();
    const result = await provider.healthCheck();

    expect(result.healthy).toBe(true);
    expect(typeof result.latencyMs).toBe("number");
    expect(result.latencyMs).toBeGreaterThanOrEqual(0);
  });
});

describe("SqlBlobProvider: totalSizeBytes", () => {
  test("returns 0 when no blobs are stored", () => {
    const provider = new SqlBlobProvider();
    expect(provider.totalSizeBytes()).toBe(0);
  });

  test("returns correct sum after multiple uploads", async () => {
    const provider = new SqlBlobProvider();
    const a = Buffer.from("aaaa"); // 4 bytes
    const b = Buffer.from("bbbbbbbbbb"); // 10 bytes
    const c = Buffer.from("ccc"); // 3 bytes

    await provider.upload("size/a", a);
    expect(provider.totalSizeBytes()).toBe(4);

    await provider.upload("size/b", b);
    expect(provider.totalSizeBytes()).toBe(14);

    await provider.upload("size/c", c);
    expect(provider.totalSizeBytes()).toBe(17);
  });

  test("upsert updates size correctly, not double-counts", async () => {
    const provider = new SqlBlobProvider();
    const key = "size/overwrite";

    await provider.upload(key, Buffer.from("123")); // 3 bytes
    expect(provider.totalSizeBytes()).toBe(3);

    await provider.upload(key, Buffer.from("12345678")); // 8 bytes (overwrites)
    expect(provider.totalSizeBytes()).toBe(8);
  });

  test("total decreases after delete", async () => {
    const provider = new SqlBlobProvider();

    await provider.upload("size/x", Buffer.from("xxxxx")); // 5 bytes
    await provider.upload("size/y", Buffer.from("yy")); // 2 bytes
    expect(provider.totalSizeBytes()).toBe(7);

    await provider.delete("size/x");
    expect(provider.totalSizeBytes()).toBe(2);
  });
});
