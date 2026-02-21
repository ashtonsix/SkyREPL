// main.ts - Control Plane Entry Point

import { createServer } from "./api/routes";
import { registerAgentRoutes, createRealAgentBridge } from "./api/agent";
import { registerWebSocketRoutes, registerSSEWorkflowStream } from "./api/sse-protocol";
import { initDatabase, runMigrations } from "./material/db";
import { createWorkflowEngine, recoverWorkflows, requestEngineShutdown, awaitEngineQuiescence } from "./workflow/engine";
import { registerLaunchRun } from "./intent/launch-run";
import { registerTerminateInstance } from "./intent/terminate-instance";
import { registerCleanupManifest } from "./intent/cleanup-manifest";
import { setAgentBridge } from "./workflow/nodes/start-run";
import { TIMING } from "@skyrepl/contracts";
import { cleanExpiredIdempotencyKeys } from "./api/middleware/idempotency";
import type { WorkflowEngine } from "./workflow/engine";
import { readFileSync, existsSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import { holdExpiryTask } from "./resource/allocation";

// Moved background task bodies
import { heartbeatTimeoutCheck } from "./resource/instance";
import { manifestCleanupCheck } from "./intent/cleanup-manifest";
import { warmPoolReconciliation } from "./resource/allocation";
import { storageGarbageCollection } from "./material/storage";
import { seedApiKey } from "./material/db/tenants";
import { terminateOrphanedVMs, scanAll } from "./orphan/scanner";

// =============================================================================
// Module-level State
// =============================================================================

let engine: WorkflowEngine | null = null;
const intervalHandles: ReturnType<typeof setInterval>[] = [];
let httpServer: ReturnType<import("elysia").Elysia["listen"]> | null = null;
let isShuttingDown = false;

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
  // Heartbeat timeout: detect stale instances, fail their allocations
  const hbHandle = setInterval(() => {
    heartbeatTimeoutCheck().catch((err) =>
      console.error("[control] heartbeatTimeoutCheck error:", err)
    );
  }, TIMING.HEARTBEAT_CHECK_INTERVAL_MS);
  intervalHandles.push(hbHandle);

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

  // Storage garbage collection
  const gcHandle = setInterval(() => {
    storageGarbageCollection().catch((err) =>
      console.error("[control] storageGarbageCollection error:", err)
    );
  }, TIMING.BLOB_GC_INTERVAL_MS);
  intervalHandles.push(gcHandle);

  // Allocation reconciliation (#LIFE-11): safety net for stuck allocations
  const reconcileHandle = setInterval(() => {
    reconciliationTask().catch((err) =>
      console.error("[control] reconciliationTask error:", err)
    );
  }, TIMING.RECONCILIATION_INTERVAL_MS);
  intervalHandles.push(reconcileHandle);

  // Orphan scanner: periodic scan of all providers for untracked cloud resources
  const orphanScanHandle = setInterval(() => {
    scanAll().catch((err) =>
      console.error("[control] orphan scanAll error:", err)
    );
  }, TIMING.ORPHAN_SCAN_INTERVAL_MS);
  intervalHandles.push(orphanScanHandle);

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
// Shutdown
// =============================================================================

export async function shutdown(): Promise<void> {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log("[control] Graceful shutdown initiated...");

  // 1. Stop accepting new requests (stop the HTTP server)
  // server.stop(true) in Bun closes the listener but lets in-flight finish
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

  // 4. Terminate orphaned VMs
  try {
    await terminateOrphanedVMs();
  } catch (err) {
    console.warn("[control] Error during VM cleanup:", err);
  }

  // 5. WAL checkpoint
  try {
    const { walCheckpoint } = await import("./material/db");
    walCheckpoint();
    console.log("[control] WAL checkpoint complete");
  } catch (err) {
    console.warn("[control] WAL checkpoint failed:", err);
  }

  // 6. Close database
  const { closeDatabase } = await import("./material/db");
  closeDatabase();
  console.log("[control] Database closed");

  console.log("[control] Shutdown complete");
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
// Entry Point
// =============================================================================

startup().catch((err) => {
  console.error("Failed to start control plane:", err);
  process.exit(1);
});
