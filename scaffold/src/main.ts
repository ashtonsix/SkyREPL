// scaffold/src/main.ts — Unified Service Fabric Entry Point
//
// Single process composing all SkyREPL services. By env configuration
// (SKYREPL_SERVICES) it decides which subset to start. Default: all.
//
// Start via: bun run scaffold/src/main.ts

import { readFileSync, existsSync, mkdirSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import {
  parseCatalogConfig,
  createCatalog,
  createKVCache,
  type Catalog,
  type ServiceName,
} from "./catalog";

// =============================================================================
// Env Loading
// =============================================================================

function loadEnvFile(filePath: string): void {
  if (!existsSync(filePath)) return;
  const content = readFileSync(filePath, "utf-8");
  for (const line of content.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eqIdx = trimmed.indexOf("=");
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    let value = trimmed.slice(eqIdx + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    if (process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

// =============================================================================
// Cross-boundary dynamic import helper
// Casting import() to Promise<any> prevents TypeScript from following the
// module's type graph into sub-projects (avoids TS6059/TS6307 composite errors).
// =============================================================================
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function xImport(path: string): Promise<any> {
  return import(path) as Promise<any>;
}

// =============================================================================
// Service Init
// =============================================================================

type ShutdownFn = () => Promise<void>;
const shutdownFns: ShutdownFn[] = [];

async function initServices(catalog: Catalog): Promise<void> {
  const config = catalog.getConfig();
  const has = (name: ServiceName) => config.services.includes(name);

  // Control plane (must init before others — owns the DB schema)
  if (has("control")) {
    const { initControl } = await xImport("../../lifecycle/control/src/main");
    const { shutdown } = await initControl(catalog);
    shutdownFns.push(shutdown);
    console.log("[scaffold] control plane initialized");
  }

  // Orbital advisory server
  if (has("orbital")) {
    const { initOrbital } = await xImport("../../orbital/impl/src/main");
    const { shutdown } = await initOrbital(catalog);
    shutdownFns.push(shutdown);
    console.log("[scaffold] orbital initialized");
  }

  // Shell proxy
  if (has("proxy")) {
    const { initProxy } = await xImport("../../shell/proxy/src/main");
    const { shutdown } = await initProxy(catalog);
    shutdownFns.push(shutdown);
    console.log("[scaffold] proxy initialized");
  }

  // Shell daemon
  if (has("daemon")) {
    const { initDaemon } = await xImport("../../shell/daemon/index");
    const { shutdown } = await initDaemon(catalog);
    shutdownFns.push(shutdown);
    console.log("[scaffold] daemon initialized");
  }
}

// =============================================================================
// Startup
// =============================================================================

async function startup(): Promise<void> {
  console.log("[scaffold] Starting service fabric...");

  // Load env files (lowest priority — won't overwrite existing env vars)
  const replDir = join(homedir(), ".repl");
  mkdirSync(replDir, { recursive: true });
  loadEnvFile(join(replDir, "control.env"));
  loadEnvFile(join(replDir, "cli.env"));

  // Parse config from env
  const config = parseCatalogConfig();
  console.log(`[scaffold] services: ${config.services.join(", ")}`);
  console.log(`[scaffold] version: ${config.version}`);

  // Init storage engines (scaffold opens DB; control plane owns schema/migrations)
  const { initDatabase } = await xImport("../../lifecycle/control/src/material/db");

  const sqlite = initDatabase(config.sqlitePath);
  console.log("[scaffold] SQLite initialized");

  const kv = createKVCache();
  console.log("[scaffold] KV cache initialized");

  // DuckDB init (OLAP engine)
  let duckdb: any = null;
  if (config.services.includes("duckdb")) {
    try {
      const duckdbPath = join(replDir, "skyrepl-olap.duckdb");
      const { initDuckDB } = await xImport("../../lifecycle/control/src/material/duckdb");
      duckdb = await initDuckDB(duckdbPath, config.sqlitePath);
      console.log("[scaffold] DuckDB initialized");
    } catch (err) {
      console.warn("[scaffold] DuckDB initialization failed (OLAP disabled):", err);
    }
  }

  // Create catalog
  const catalog = createCatalog(config, sqlite, kv, duckdb);

  // Register storage engines in catalog
  catalog.registerService("sqlite", {
    mode: "local",
    version: config.version,
    ref: sqlite,
  });
  catalog.registerService("kv", {
    mode: "local",
    version: config.version,
    ref: kv,
  });
  if (duckdb) {
    catalog.registerService("duckdb", {
      mode: "local",
      version: config.version,
      ref: duckdb,
    });
  }

  // Init all configured services. If a later service fails, clean up earlier ones.
  try {
    await initServices(catalog);
  } catch (err) {
    console.error("[scaffold] service init failed:", err);
    for (let i = shutdownFns.length - 1; i >= 0; i--) {
      try { await shutdownFns[i](); } catch (e) { console.error("[scaffold] cleanup error:", e); }
    }
    try {
      if (duckdb) {
        const { closeDuckDB } = await xImport("../../lifecycle/control/src/material/duckdb");
        await closeDuckDB();
      }
    } catch { /* best-effort */ }
    try {
      const { walCheckpoint, closeDatabase } = await xImport("../../lifecycle/control/src/material/db");
      walCheckpoint();
      closeDatabase();
    } catch { /* best-effort */ }
    throw err; // re-throw to hit startup().catch at bottom
  }

  // Signal handlers
  let isShuttingDown = false;
  const handleSignal = async (signal: string) => {
    if (isShuttingDown) return;
    isShuttingDown = true;
    console.log(`[scaffold] Received ${signal}, shutting down...`);

    // Shutdown services in reverse order
    for (let i = shutdownFns.length - 1; i >= 0; i--) {
      try {
        await shutdownFns[i]();
      } catch (err) {
        console.error("[scaffold] shutdown error:", err);
      }
    }

    // Close DuckDB before SQLite (DuckDB ATTACHes SQLite, must release first)
    try {
      if (duckdb) {
        const { closeDuckDB } = await xImport("../../lifecycle/control/src/material/duckdb");
        await closeDuckDB();
        console.log("[scaffold] DuckDB closed");
      }
    } catch (err) {
      console.warn("[scaffold] DuckDB cleanup error:", err);
    }

    // WAL checkpoint
    try {
      const { walCheckpoint, closeDatabase } = await xImport("../../lifecycle/control/src/material/db");
      walCheckpoint();
      closeDatabase();
      console.log("[scaffold] database closed");
    } catch (err) {
      console.warn("[scaffold] database cleanup error:", err);
    }

    console.log("[scaffold] shutdown complete");
    process.exit(0);
  };

  process.on("SIGINT", () => handleSignal("SIGINT"));
  process.on("SIGTERM", () => handleSignal("SIGTERM"));

  console.log("[scaffold] service fabric ready");
}

// =============================================================================
// Entry Point
// =============================================================================

startup().catch((err) => {
  console.error("[scaffold] Failed to start:", err);
  process.exit(1);
});
