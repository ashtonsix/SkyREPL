// provider/cache.ts - Disk-backed TTL cache for slow-changing reference data

import { homedir } from "os";
import { mkdir, readFile, writeFile, rename, stat, unlink, readdir, rm } from "fs/promises";
import { join } from "path";

/**
 * Fetch data with a disk-backed TTL cache.
 *
 * Cache location: ~/.repl/_cache/{key}.json
 * Validates TTL on read. Atomic writes (write to .tmp, rename).
 * Falls back to stale cache if fetcher fails (with console.warn).
 *
 * For quick-changing provider state (instance status, spot prices), use the
 * in-memory cache in extensions.ts instead. This is for big, slow-changing
 * reference data only.
 */
export async function fetchWithDiskCache<T>(
  key: string,         // e.g. "ami-catalog-us-east-1"
  ttlMs: number,       // e.g. 86_400_000 (24h)
  fetcher: () => Promise<T>
): Promise<T> {
  const cacheDir = getCacheDir();
  const cachePath = join(cacheDir, `${key}.json`);

  // Try to read a valid (non-stale) cache entry
  const existing = await readCacheEntry<T>(cachePath);
  if (existing !== null && Date.now() - existing.cachedAt < ttlMs) {
    return existing.data;
  }

  // Cache miss or stale — call fetcher
  try {
    const data = await fetcher();
    await writeCacheEntry(cacheDir, cachePath, data);
    return data;
  } catch (err) {
    // Fetcher failed — fall back to stale cache if available
    if (existing !== null) {
      const error = err instanceof Error ? err : new Error(String(err));
      console.warn(`Using stale cache for ${key}: ${error.message}`);
      return existing.data;
    }
    throw err;
  }
}

/**
 * Clear disk cache entries.
 * If key is provided, deletes only that entry. Otherwise deletes all entries.
 */
export async function clearDiskCache(key?: string): Promise<void> {
  const cacheDir = getCacheDir();

  if (key !== undefined) {
    const cachePath = join(cacheDir, `${key}.json`);
    try {
      await unlink(cachePath);
    } catch (err: unknown) {
      // Ignore if the file doesn't exist
      if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
        throw err;
      }
    }
    return;
  }

  // Delete all cache entries
  try {
    const entries = await readdir(cacheDir);
    await Promise.all(
      entries
        .filter((name) => name.endsWith(".json"))
        .map((name) => unlink(join(cacheDir, name)).catch((err: unknown) => {
          if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
        }))
    );
  } catch (err: unknown) {
    // Ignore if cache dir doesn't exist
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
      throw err;
    }
  }
}

// =============================================================================
// Internals
// =============================================================================

interface CacheEntry<T> {
  data: T;
  cachedAt: number;
}

let _cacheDir: string | null = null;

/** Returns the cache directory path, respecting SKYREPL_CACHE_DIR override for tests. */
export function getCacheDir(): string {
  if (process.env.SKYREPL_CACHE_DIR) {
    return process.env.SKYREPL_CACHE_DIR;
  }
  if (_cacheDir === null) {
    _cacheDir = join(homedir(), ".repl", "_cache");
  }
  return _cacheDir;
}

async function readCacheEntry<T>(cachePath: string): Promise<CacheEntry<T> | null> {
  try {
    await stat(cachePath);
    const raw = await readFile(cachePath, "utf8");
    return JSON.parse(raw) as CacheEntry<T>;
  } catch {
    return null;
  }
}

async function writeCacheEntry<T>(cacheDir: string, cachePath: string, data: T): Promise<void> {
  await mkdir(cacheDir, { recursive: true });
  const entry: CacheEntry<T> = { data, cachedAt: Date.now() };
  const tmpPath = `${cachePath}.tmp`;
  await writeFile(tmpPath, JSON.stringify(entry), "utf8");
  await rename(tmpPath, cachePath);
}
