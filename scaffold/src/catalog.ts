// scaffold/src/catalog.ts — Service Catalog
//
// The catalog is the spine of the service fabric. It answers:
//   1. WHERE is service X?  (local reference or remote URL)
//   2. HOW is service X configured?  (DB paths, ports, etc.)
//   3. WHAT can service X provide?  (version, health)
//
// Created once at process startup and passed to each service's init().
// Services discover each other through the catalog, never via direct imports.

import type { Database } from "bun:sqlite";

// =============================================================================
// Types
// =============================================================================

export type ServerName = "control" | "orbital" | "proxy" | "daemon";
export type StorageName = "sqlite" | "duckdb" | "minio" | "kv";
export type ServiceName = ServerName | StorageName;

export type ServiceHandle =
  | { mode: "local"; version: string; ref: unknown }
  | { mode: "remote"; version: string; url: string };

export interface CatalogConfig {
  /** Which services this process manages. Default: all. */
  services: ServiceName[];

  /** SQLite DB file path */
  sqlitePath: string;

  /** HTTP ports per server */
  ports: {
    control: number;
    orbital: number;
    proxy: number;
  };

  /** Version string for this build */
  version: string;
}

// =============================================================================
// KV Cache
// =============================================================================

interface KVEntry {
  value: unknown;
  expiresAt: number; // 0 = no expiry
}

export interface KVCache {
  get<T>(key: string): T | undefined;
  set<T>(key: string, value: T, ttlMs?: number): void;
  delete(key: string): boolean;
  has(key: string): boolean;
  clear(): void;
  /** Number of live (non-expired) entries */
  size(): number;
}

export function createKVCache(): KVCache {
  const store = new Map<string, KVEntry>();

  // Lazy eviction: check expiry on read, periodic sweep on write
  let writesSinceLastSweep = 0;
  const SWEEP_INTERVAL = 100; // sweep every N writes

  function isExpired(entry: KVEntry): boolean {
    return entry.expiresAt > 0 && Date.now() > entry.expiresAt;
  }

  function maybeSweep(): void {
    writesSinceLastSweep++;
    if (writesSinceLastSweep < SWEEP_INTERVAL) return;
    writesSinceLastSweep = 0;
    const now = Date.now();
    for (const [key, entry] of store) {
      if (entry.expiresAt > 0 && now > entry.expiresAt) {
        store.delete(key);
      }
    }
  }

  return {
    get<T>(key: string): T | undefined {
      const entry = store.get(key);
      if (!entry) return undefined;
      if (isExpired(entry)) {
        store.delete(key);
        return undefined;
      }
      return entry.value as T;
    },

    set<T>(key: string, value: T, ttlMs?: number): void {
      const expiresAt = ttlMs && ttlMs > 0 ? Date.now() + ttlMs : 0;
      store.set(key, { value, expiresAt });
      maybeSweep();
    },

    delete(key: string): boolean {
      return store.delete(key);
    },

    has(key: string): boolean {
      const entry = store.get(key);
      if (!entry) return false;
      if (isExpired(entry)) {
        store.delete(key);
        return false;
      }
      return true;
    },

    clear(): void {
      store.clear();
    },

    size(): number {
      // Count only non-expired entries
      let count = 0;
      const now = Date.now();
      for (const entry of store.values()) {
        if (entry.expiresAt === 0 || now <= entry.expiresAt) count++;
      }
      return count;
    },
  };
}

// =============================================================================
// Catalog
// =============================================================================

export interface Catalog {
  // Service discovery
  getService(name: ServiceName): ServiceHandle | undefined;
  registerService(name: ServiceName, handle: ServiceHandle): void;
  hasService(name: ServiceName): boolean;

  // Database handles
  getSQLite(): Database;
  getDuckDB(): any | null; // Use 'any' to avoid import dependency scaffold → lifecycle

  // KV cache (shared across all services)
  getKV(): KVCache;

  // Configuration (frozen at startup)
  getConfig(): Readonly<CatalogConfig>;
}

export function createCatalog(
  config: CatalogConfig,
  sqlite: Database,
  kv: KVCache,
  duckdb?: any | null,
): Catalog {
  const services = new Map<ServiceName, ServiceHandle>();
  // Capture duckdb in closure (may be null/undefined for non-OLAP deployments)
  const duckdbHandle: any | null = duckdb ?? null;

  return {
    getService(name: ServiceName): ServiceHandle | undefined {
      return services.get(name);
    },

    registerService(name: ServiceName, handle: ServiceHandle): void {
      services.set(name, handle);
    },

    hasService(name: ServiceName): boolean {
      return services.has(name);
    },

    getSQLite(): Database {
      return sqlite;
    },

    getDuckDB(): any | null {
      return duckdbHandle;
    },

    getKV(): KVCache {
      return kv;
    },

    getConfig(): Readonly<CatalogConfig> {
      return config;
    },
  };
}

// =============================================================================
// Config Parsing
// =============================================================================

const ALL_SERVICES: ServiceName[] = [
  "control", "orbital", "proxy", "daemon",
  "sqlite", "duckdb", "minio", "kv",
];

export function parseCatalogConfig(): CatalogConfig {
  const servicesEnv = process.env.SKYREPL_SERVICES;
  const services: ServiceName[] = servicesEnv
    ? servicesEnv.split(",").map(s => s.trim()).filter(s => ALL_SERVICES.includes(s as ServiceName)) as ServiceName[]
    : ALL_SERVICES;

  const { homedir } = require("os");
  const { join } = require("path");
  const sqlitePath = process.env.SKYREPL_DB_PATH ?? join(homedir(), ".repl", "skyrepl-control.db");

  return {
    services,
    sqlitePath,
    ports: {
      control: parseInt(process.env.SKYREPL_PORT ?? process.env.PORT ?? "3000", 10),
      orbital: parseInt(process.env.ORBITAL_PORT ?? "3002", 10),
      proxy: parseInt(process.env.SHELL_PORT ?? "3001", 10),
    },
    version: process.env.SKYREPL_VERSION ?? "0.0.1",
  };
}
