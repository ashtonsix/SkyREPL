import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../../../tests/harness";
import { createBlob, getBlob } from "./db";
import {
  storeInline,
  getInline,
  INLINE_THRESHOLD,
} from "./storage";

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest();
});

afterEach(async () => {
  await cleanup();
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
  test("storeInline/getInline: stores and retrieves, returns null when absent", () => {
    const blob = makeBlob({ checksum: "inline-test" });
    expect(getInline(blob.id)).toBeNull();

    const data = Buffer.from("hello world");
    storeInline(blob.id, data);
    const retrieved = getInline(blob.id);
    expect(retrieved).not.toBeNull();
    expect(retrieved!.toString()).toBe("hello world");
  });

  test("INLINE_THRESHOLD is 64KB", () => {
    expect(INLINE_THRESHOLD).toBe(65536);
  });
});
