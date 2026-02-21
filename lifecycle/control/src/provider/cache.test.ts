import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { existsSync, rmSync } from "node:fs";
import { join } from "path";
import { mkdtemp } from "fs/promises";
import { tmpdir } from "os";
import { fetchWithDiskCache, clearDiskCache, getCacheDir } from "./cache";

let tempDir: string;

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), "skyrepl-cache-test-"));
  process.env.SKYREPL_CACHE_DIR = tempDir;
});

afterEach(() => {
  delete process.env.SKYREPL_CACHE_DIR;
  if (existsSync(tempDir)) {
    rmSync(tempDir, { recursive: true });
  }
});

describe("fetchWithDiskCache", () => {
  test("fresh fetch writes cache and returns data", async () => {
    let callCount = 0;
    const fetcher = async () => {
      callCount++;
      return { value: 42 };
    };

    const result = await fetchWithDiskCache("test-key", 60_000, fetcher);

    expect(result).toEqual({ value: 42 });
    expect(callCount).toBe(1);

    // Cache file should exist
    const cacheFile = join(tempDir, "test-key.json");
    expect(existsSync(cacheFile)).toBe(true);

    // Cache file should have correct structure
    const raw = await Bun.file(cacheFile).text();
    const parsed = JSON.parse(raw);
    expect(parsed.data).toEqual({ value: 42 });
    expect(typeof parsed.cachedAt).toBe("number");
    expect(parsed.cachedAt).toBeLessThanOrEqual(Date.now());
  });

  test("cached read returns without calling fetcher", async () => {
    let callCount = 0;
    const fetcher = async () => {
      callCount++;
      return { value: 99 };
    };

    // First call — writes cache
    await fetchWithDiskCache("cache-hit-key", 60_000, fetcher);
    expect(callCount).toBe(1);

    // Second call — should hit cache
    const result = await fetchWithDiskCache("cache-hit-key", 60_000, fetcher);
    expect(result).toEqual({ value: 99 });
    expect(callCount).toBe(1);
  });

  test("expired cache re-fetches", async () => {
    let callCount = 0;
    const fetcher = async () => {
      callCount++;
      return { value: callCount };
    };

    // Write a cache entry with a timestamp in the past
    const cacheFile = join(tempDir, "expired-key.json");
    const staleEntry = { data: { value: 0 }, cachedAt: Date.now() - 10_000 };
    await Bun.write(cacheFile, JSON.stringify(staleEntry));

    // TTL of 5s — entry is 10s old, so it's stale
    const result = await fetchWithDiskCache("expired-key", 5_000, fetcher);

    expect(result).toEqual({ value: 1 });
    expect(callCount).toBe(1);
  });

  test("fetcher failure falls back to stale cache with console.warn", async () => {
    // Write a stale cache entry
    const cacheFile = join(tempDir, "stale-fallback.json");
    const staleEntry = { data: { value: "stale-data" }, cachedAt: Date.now() - 100_000 };
    await Bun.write(cacheFile, JSON.stringify(staleEntry));

    const warns: string[] = [];
    const originalWarn = console.warn;
    console.warn = (...args: unknown[]) => warns.push(args.join(" "));

    try {
      const result = await fetchWithDiskCache<{ value: string }>(
        "stale-fallback",
        5_000,
        async () => { throw new Error("network unavailable"); }
      );

      expect(result).toEqual({ value: "stale-data" });
      expect(warns.length).toBe(1);
      expect(warns[0]).toContain("stale-fallback");
      expect(warns[0]).toContain("network unavailable");
    } finally {
      console.warn = originalWarn;
    }
  });

  test("fetcher failure with no cache rethrows", async () => {
    const fetcher = async () => {
      throw new Error("upstream exploded");
    };

    await expect(
      fetchWithDiskCache("no-cache-key", 60_000, fetcher)
    ).rejects.toThrow("upstream exploded");
  });
});

describe("clearDiskCache", () => {
  test("clearDiskCache(key) deletes specific key", async () => {
    // Populate two keys
    await fetchWithDiskCache("key-a", 60_000, async () => "a");
    await fetchWithDiskCache("key-b", 60_000, async () => "b");

    expect(existsSync(join(tempDir, "key-a.json"))).toBe(true);
    expect(existsSync(join(tempDir, "key-b.json"))).toBe(true);

    await clearDiskCache("key-a");

    expect(existsSync(join(tempDir, "key-a.json"))).toBe(false);
    expect(existsSync(join(tempDir, "key-b.json"))).toBe(true);
  });

  test("clearDiskCache(key) is a no-op if key does not exist", async () => {
    // Should not throw
    await expect(clearDiskCache("nonexistent-key")).resolves.toBeUndefined();
  });

  test("clearDiskCache() with no argument deletes all entries", async () => {
    await fetchWithDiskCache("all-a", 60_000, async () => 1);
    await fetchWithDiskCache("all-b", 60_000, async () => 2);
    await fetchWithDiskCache("all-c", 60_000, async () => 3);

    expect(existsSync(join(tempDir, "all-a.json"))).toBe(true);
    expect(existsSync(join(tempDir, "all-b.json"))).toBe(true);
    expect(existsSync(join(tempDir, "all-c.json"))).toBe(true);

    await clearDiskCache();

    expect(existsSync(join(tempDir, "all-a.json"))).toBe(false);
    expect(existsSync(join(tempDir, "all-b.json"))).toBe(false);
    expect(existsSync(join(tempDir, "all-c.json"))).toBe(false);
  });

  test("clearDiskCache() is a no-op when cache dir is empty or absent", async () => {
    // Never wrote anything — cache dir may not exist at all, or may be empty
    await expect(clearDiskCache()).resolves.toBeUndefined();
  });
});
