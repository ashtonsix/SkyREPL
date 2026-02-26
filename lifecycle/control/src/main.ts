// main.ts - Control Plane Entry Point

import { createServer } from "./api/routes";
import { registerAgentRoutes, createRealAgentBridge } from "./api/agent";
import { registerWebSocketRoutes, registerSSEWorkflowStream } from "./api/sse-protocol";
import { initDatabase, setDatabase, runMigrations } from "./material/db";
import { createWorkflowEngine, recoverWorkflows, requestEngineShutdown, awaitEngineQuiescence } from "./workflow/engine";
import { registerLaunchRun } from "./intent/launch-run";
import { registerTerminateInstance } from "./intent/terminate-instance";
import { registerCleanupManifest } from "./intent/cleanup-manifest";
import { setAgentBridge } from "./workflow/nodes/start-run";
import { TIMING } from "@skyrepl/contracts";
import { cleanExpiredIdempotencyKeys } from "./api/middleware/idempotency";
import type { WorkflowEngine } from "./workflow/engine";
import type { Catalog } from "../../../scaffold/src/catalog";
import { readFileSync, existsSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import { holdExpiryTask } from "./resource/allocation";

// Moved background task bodies
import { heartbeatTimeoutCheck } from "./resource/instance";
import { manifestCleanupCheck } from "./intent/cleanup-manifest";
import { warmPoolReconciliation } from "./resource/allocation";
import { storageGarbageCollection, refreshSqlStorageSizeCache, getSqlStorageSizeCache, SQL_STORAGE_ADVISORY_BYTES, SQL_STORAGE_STRONG_BYTES } from "./material/storage";
import { periodicFlush, periodicLogCompaction } from "./background/log-streaming";
import { seedApiKey } from "./material/db/tenants";
import { scanAll } from "./orphan/scanner";
import { execute, queryOne } from "./material/db";
import { clearBlobProviderCache, invokeAllStorageHeartbeats } from "./provider/storage/registry";
import { invokeAllHeartbeats, type HeartbeatExpectations } from "./provider/extensions";
import type { StorageHeartbeatExpectations } from "./provider/storage/types";

// =============================================================================
// Module-level State
// =============================================================================

let engine: WorkflowEngine | null = null;
const intervalHandles: ReturnType<typeof setInterval>[] = [];
let httpServer: ReturnType<import("elysia").Elysia["listen"]> | null = null;
let isShuttingDown = false;

// Per-task last-run timestamps for heartbeat dispatch loops
let lastHeartbeatTimeout = 0;
let lastReconciliation = 0;
let lastOrphanScan = 0;
let lastStorageGc = 0;
let lastLogCompaction = 0;

// =============================================================================
// Startup
// =============================================================================

export async function startup(): Promise<void> {
  console.log("[control] Starting control plane...");

  // Load env file (lowest priority — won't overwrite existing env vars)
  loadControlEnvFile();

  initializeDatabase();
  setupWorkflowEngine();
  registerProviders();
  checkSqlStorageAtStartup();

  const port = parseInt(process.env.SKYREPL_PORT ?? process.env.PORT ?? "3000", 10);
  startHttpServer(port);

  startBackgroundTasks();

  // Register signal handlers for graceful shutdown
  process.on("SIGINT", () => {
    console.log("[control] Received SIGINT");
    shutdown();
  });
  process.on("SIGTERM", () => {
    console.log("[control] Received SIGTERM");
    shutdown();
  });

  console.log("[control] Control plane started successfully");
}

// =============================================================================
// Catalog-aware Init (service fabric entry point)
// =============================================================================

export async function initControl(catalog: Catalog): Promise<{ shutdown: () => Promise<void> }> {
  console.log("[control] Starting control plane...");

  // Inject the catalog's already-initialized DB into the module-level singleton
  setDatabase(catalog.getSQLite());
  runMigrations();
  seedApiKey();
  console.log("[control] Database initialized");

  setupWorkflowEngine();
  registerProviders();
  checkSqlStorageAtStartup();

  const port = catalog.getConfig().ports.control;
  const app = createServer({ port, corsOrigins: ["*"], maxBodySize: 10 * 1024 * 1024 });
  registerAgentRoutes(app);
  registerWebSocketRoutes(app);
  registerSSEWorkflowStream(app);
  httpServer = app.listen({ port, idleTimeout: 0 });
  console.log(`[control] Control plane listening on port ${port}`);

  catalog.registerService("control", {
    mode: "local",
    version: catalog.getConfig().version,
    ref: app,
  });

  // If orbital will be co-located, point the orbital client at localhost.
  // Check config (not catalog registration) because orbital inits after control.
  if (catalog.getConfig().services.includes("orbital")) {
    const { setOrbitalUrl } = await import("./provider/orbital");
    setOrbitalUrl(`http://localhost:${catalog.getConfig().ports.orbital}`);
  }

  startBackgroundTasks();

  console.log("[control] Control plane started successfully");

  return {
    shutdown: () => serviceShutdown(),
  };
}

// =============================================================================
// Database Initialization
// =============================================================================

export function initializeDatabase(): void {
  const dbPath = process.env.SKYREPL_DB_PATH ?? join(homedir(), ".repl", "skyrepl-control.db");
  initDatabase(dbPath);
  runMigrations();
  seedApiKey();
  console.log("[control] Database initialized");
}

// =============================================================================
// Workflow Engine Setup
// =============================================================================

export function setupWorkflowEngine(): void {
  engine = createWorkflowEngine();

  // Register the launch-run blueprint and all its node executors
  registerLaunchRun();

  // Register the terminate-instance blueprint and all its node executors
  registerTerminateInstance();

  // Register the cleanup-manifest blueprint and all its node executors
  registerCleanupManifest();

  setAgentBridge(createRealAgentBridge());

  // Recover any workflows that were running when we last crashed
  recoverWorkflows().catch((err) => {
    console.error("[control] Failed to recover workflows:", err);
  });

  console.log("[control] Workflow engine initialized");
}

// =============================================================================
// Provider Registration
// =============================================================================

export function registerProviders(): void {
  // For Slice 1: OrbStack provider is auto-registered via the provider registry
  // (lazy-loaded on first use via providerRegistry in provider/registry.ts)
  // No explicit registration needed — getProvider("orbstack") handles it
  console.log("[control] Providers: orbstack (lazy-loaded via registry)");
  autoConfigureBlobProvider();
}

// =============================================================================
// Auto-configure Blob Provider
// =============================================================================

export function autoConfigureBlobProvider(): void {
  // Skip if the default tenant already has a non-SQL blob provider configured
  const existing = queryOne<{ provider_name: string }>(
    "SELECT provider_name FROM blob_provider_configs WHERE tenant_id = 1",
    []
  );
  if (existing && existing.provider_name !== "sql") {
    console.log(`[control] Blob provider already configured: ${existing.provider_name}`);
    return;
  }

  // Detect MinIO: binary present OR MINIO_ENDPOINT env var set
  const minioBinaryPath = join(homedir(), ".repl", "minio", "minio");
  const minioEnvEndpoint = process.env.MINIO_ENDPOINT;
  const minioAvailable = existsSync(minioBinaryPath) || !!minioEnvEndpoint;

  if (!minioAvailable) {
    console.log("[control] No blob provider configured and MinIO not detected — using SQL blob storage");
    return;
  }

  const endpoint = minioEnvEndpoint ?? "http://127.0.0.1:9000";

  // Determine presignedUrlEndpoint for OrbStack when endpoint is localhost
  let presignedUrlEndpoint: string | undefined;
  const isLocalhost = endpoint.includes("127.0.0.1") || endpoint.includes("localhost");
  if (isLocalhost) {
    if (process.env.MINIO_EXTERNAL_ENDPOINT) {
      presignedUrlEndpoint = process.env.MINIO_EXTERNAL_ENDPOINT;
    } else if (existsSync("/opt/orbstack-guest") || process.env.ORB_VERSION) {
      // Running inside an OrbStack VM — use the standard OrbStack host IP
      presignedUrlEndpoint = "http://198.19.249.1:9000";
    }
  }

  const config: Record<string, unknown> = {
    endpoint,
    region: process.env.MINIO_REGION ?? "us-east-1",
    bucket: process.env.MINIO_BUCKET ?? "skyrepl",
    accessKeyId: process.env.MINIO_ACCESS_KEY ?? "minioadmin",
    secretAccessKey: process.env.MINIO_SECRET_KEY ?? "minioadmin",
    forcePathStyle: true,
  };
  if (presignedUrlEndpoint) {
    config.presignedUrlEndpoint = presignedUrlEndpoint;
  }

  const now = Date.now();
  execute(
    `INSERT INTO blob_provider_configs (tenant_id, provider_name, config_json, created_at, updated_at)
     VALUES (1, 'minio', ?, ?, ?)
     ON CONFLICT (tenant_id) DO UPDATE SET provider_name = 'minio', config_json = excluded.config_json, updated_at = excluded.updated_at`,
    [JSON.stringify(config), now, now]
  );

  clearBlobProviderCache();
  console.log(
    `[control] Auto-configured MinIO blob provider (endpoint=${endpoint}${presignedUrlEndpoint ? `, presignedUrlEndpoint=${presignedUrlEndpoint}` : ""})`
  );
}

// =============================================================================
// SQL Storage Startup Warning
// =============================================================================

export function checkSqlStorageAtStartup(): void {
  refreshSqlStorageSizeCache();
  const bytes = getSqlStorageSizeCache();
  if (bytes >= SQL_STORAGE_STRONG_BYTES) {
    const gb = (bytes / (1024 * 1024 * 1024)).toFixed(1);
    console.warn(
      `[storage] WARNING: SQL blob storage at ${gb}GB — strongly recommended to upgrade to S3/MinIO for reliability and performance.`
    );
  } else if (bytes >= SQL_STORAGE_ADVISORY_BYTES) {
    const gb = (bytes / (1024 * 1024 * 1024)).toFixed(1);
    console.warn(
      `[storage] SQL blob storage at ${gb}GB. Consider upgrading to S3/MinIO for better performance.`
    );
  }
}

// =============================================================================
// HTTP Server
// =============================================================================

export function startHttpServer(port: number): void {
  const app = createServer({ port, corsOrigins: ["*"], maxBodySize: 10 * 1024 * 1024 });

  // Register additional route groups
  registerAgentRoutes(app);
  registerWebSocketRoutes(app);
  registerSSEWorkflowStream(app);

  // CRITICAL: idleTimeout: 0 is required for SSE connections.
  // Without it, Bun kills idle connections after ~8 seconds.
  httpServer = app.listen({ port, idleTimeout: 0 });

  console.log(`[control] Control plane listening on port ${port}`);
}

// =============================================================================
// Background Tasks
// =============================================================================

export function startBackgroundTasks(): void {
  // Warm pool reconciliation: timeout stale CLAIMED, expire old AVAILABLE
  const warmPoolHandle = setInterval(() => {
    warmPoolReconciliation().catch((err) =>
      console.error("[control] warmPoolReconciliation error:", err)
    );
  }, TIMING.WARM_POOL_RECONCILE_INTERVAL_MS);
  intervalHandles.push(warmPoolHandle);

  // Idempotency key cleanup (every 60 minutes)
  const idempotencyHandle = setInterval(() => {
    try { cleanExpiredIdempotencyKeys(); }
    catch (err) { console.error("[control] cleanExpiredIdempotencyKeys error:", err); }
  }, 60 * 60 * 1000);
  intervalHandles.push(idempotencyHandle);

  // Hold expiry check
  const holdHandle = setInterval(() => {
    holdExpiryCheck().catch((err) =>
      console.error("[control] holdExpiryCheck error:", err)
    );
  }, TIMING.HOLD_EXPIRY_CHECK_INTERVAL_MS);
  intervalHandles.push(holdHandle);

  // Manifest cleanup check
  const manifestHandle = setInterval(() => {
    manifestCleanupCheck().catch((err) =>
      console.error("[control] manifestCleanupCheck error:", err)
    );
  }, TIMING.MANIFEST_CLEANUP_INTERVAL_MS);
  intervalHandles.push(manifestHandle);

  // Log chunk periodic flush (every 30s)
  const logFlushHandle = setInterval(() => {
    try { periodicFlush(); }
    catch (err) { console.error("[control] periodicFlush error:", err); }
  }, 30_000);
  intervalHandles.push(logFlushHandle);

  // Compute heartbeat: health check + stale detection + reconciliation + orphan scan
  // Runs at the fastest cadence (HEARTBEAT_CHECK_INTERVAL_MS); slower tasks skip when not due.
  const computeHbHandle = setInterval(() => {
    computeHeartbeatDispatch().catch((err) =>
      console.error("[control] computeHeartbeatDispatch error:", err)
    );
  }, TIMING.HEARTBEAT_CHECK_INTERVAL_MS);
  intervalHandles.push(computeHbHandle);

  // Storage heartbeat: health check + blob GC + log compaction
  // Runs at BLOB_GC_INTERVAL_MS cadence; log compaction skips when not due.
  const storageHbHandle = setInterval(() => {
    storageHeartbeatDispatch().catch((err) =>
      console.error("[control] storageHeartbeatDispatch error:", err)
    );
  }, TIMING.BLOB_GC_INTERVAL_MS);
  intervalHandles.push(storageHbHandle);

  console.log("[control] Background tasks started");
}

export async function holdExpiryCheck(): Promise<void> {
  await holdExpiryTask();
}

export async function reconciliationTask(): Promise<void> {
  const { runReconciliation } = await import("./background/reconciliation");
  await runReconciliation();
}

// =============================================================================
// Compute Heartbeat Dispatch
// =============================================================================

export async function computeHeartbeatDispatch(): Promise<void> {
  const now = Date.now();

  // Always include health_check; add other tasks only when due
  const tasks: HeartbeatExpectations["tasks"] = [
    { type: "health_check", priority: "high" },
  ];

  if (now - lastHeartbeatTimeout >= TIMING.HEARTBEAT_CHECK_INTERVAL_MS) {
    tasks.push({ type: "heartbeat_timeout_scan", priority: "high", lastRun: lastHeartbeatTimeout });
  }
  if (now - lastReconciliation >= TIMING.RECONCILIATION_INTERVAL_MS) {
    tasks.push({ type: "reconcile_instances", priority: "normal", lastRun: lastReconciliation });
  }
  if (now - lastOrphanScan >= TIMING.ORPHAN_SCAN_INTERVAL_MS) {
    tasks.push({ type: "orphan_scan", priority: "low", lastRun: lastOrphanScan });
  }

  const expectations: HeartbeatExpectations = {
    tasks,
    deadline: now + 30_000,
  };

  try {
    await invokeAllHeartbeats(expectations);

    // Run control-plane-side logic for due tasks
    if (tasks.some(t => t.type === "heartbeat_timeout_scan")) {
      await heartbeatTimeoutCheck();
      lastHeartbeatTimeout = Date.now();
    }
    if (tasks.some(t => t.type === "reconcile_instances")) {
      await reconciliationTask();
      lastReconciliation = Date.now();
    }
    if (tasks.some(t => t.type === "orphan_scan")) {
      const scanResults = await scanAll();
      const orphanCount = scanResults.reduce((n, r) => n + r.orphans.filter(o => o.classification !== 'whitelisted').length, 0);
      if (orphanCount > 0) {
        console.warn(`[orphan] Scheduled scan found ${orphanCount} orphaned resource(s) across ${scanResults.length} provider(s)`);
      }
      lastOrphanScan = Date.now();
    }
  } catch (err) {
    console.error("[control] computeHeartbeatDispatch error:", err);
  }
}

// =============================================================================
// Storage Heartbeat Dispatch
// =============================================================================

export async function storageHeartbeatDispatch(): Promise<void> {
  const now = Date.now();

  // Always include health_check; add other tasks only when due
  const tasks: StorageHeartbeatExpectations["tasks"] = [
    { type: "health_check", priority: "high" },
  ];

  if (now - lastStorageGc >= TIMING.BLOB_GC_INTERVAL_MS) {
    tasks.push({ type: "blob_gc", priority: "normal", lastRun: lastStorageGc });
  }
  if (now - lastLogCompaction >= 3600_000) {
    tasks.push({ type: "log_compaction", priority: "low", lastRun: lastLogCompaction });
  }

  const expectations: StorageHeartbeatExpectations = {
    tasks,
    deadline: now + 60_000,
  };

  try {
    await invokeAllStorageHeartbeats(expectations);

    // Run control-plane-side storage tasks for due items
    if (tasks.some(t => t.type === "blob_gc")) {
      await storageGarbageCollection();
      lastStorageGc = Date.now();
    }
    if (tasks.some(t => t.type === "log_compaction")) {
      await periodicLogCompaction();
      lastLogCompaction = Date.now();
    }
  } catch (err) {
    console.error("[control] storageHeartbeatDispatch error:", err);
  }
}

// =============================================================================
// Shutdown
// =============================================================================

/** Service-level shutdown: stop HTTP, drain engine, clear intervals.
 *  Does NOT touch the database — caller (standalone or scaffold) owns DB lifecycle. */
async function serviceShutdown(): Promise<void> {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log("[control] Graceful shutdown initiated...");

  // 1. Stop accepting new requests
  if (httpServer) {
    httpServer.server!.stop(true);
    console.log("[control] HTTP server stopped accepting connections");
  }

  // 2. Clear background task intervals
  for (const handle of intervalHandles) {
    clearInterval(handle);
  }
  intervalHandles.length = 0;

  // 3. Signal engine shutdown and wait for in-flight executeLoops to finish
  requestEngineShutdown();
  await awaitEngineQuiescence(30_000);

  console.log("[control] Service shutdown complete");
}

/** Standalone shutdown: service shutdown + DB cleanup + exit. */
export async function shutdown(): Promise<void> {
  await serviceShutdown();

  try {
    const { walCheckpoint } = await import("./material/db");
    walCheckpoint();
    console.log("[control] WAL checkpoint complete");
  } catch (err) {
    console.warn("[control] WAL checkpoint failed:", err);
  }

  const { closeDatabase } = await import("./material/db");
  closeDatabase();
  console.log("[control] Database closed");

  process.exit(0);
}

// =============================================================================
// Env File
// =============================================================================

function loadControlEnvFile(): void {
  const envPath = join(homedir(), ".repl", "control.env");
  if (!existsSync(envPath)) return;
  const content = readFileSync(envPath, "utf-8");
  for (const line of content.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eqIdx = trimmed.indexOf("=");
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    let value = trimmed.slice(eqIdx + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    if (process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

// =============================================================================
// Entry Point (standalone only — scaffold uses initControl)
// =============================================================================

if (import.meta.main) {
  startup().catch((err) => {
    console.error("Failed to start control plane:", err);
    process.exit(1);
  });
}
