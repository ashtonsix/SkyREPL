// main.ts - Control Plane Entry Point

import { createServer } from "./api/routes";
import { registerAgentRoutes, createRealAgentBridge } from "./api/agent";
import { registerWebSocketRoutes, registerSSEWorkflowStream } from "./api/sse-protocol";
import { initDatabase, runMigrations, findStaleClaimed, findExpiredAvailable, getInstance, queryOne, queryMany, listExpiredManifests, findOrphanedBlobs, deleteBlobBatch, deleteObjectBatch } from "./material/db";
import { createWorkflowEngine, recoverWorkflows, requestEngineShutdown, awaitEngineQuiescence } from "./workflow/engine";
import { registerLaunchRun } from "./intent/launch-run";
import { setAgentBridge } from "./workflow/nodes/start-run";
import { TIMING } from "@skyrepl/shared";
import { cleanExpiredIdempotencyKeys } from "./api/middleware/idempotency";
import type { WorkflowEngine } from "./workflow/engine";
import { failAllocation } from "./workflow/state-transitions";
import { getProvider } from "./provider/registry";
import { readFileSync, existsSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import { detectStaleHeartbeats } from "./resource/instance";
import { holdExpiryTask } from "./resource/allocation";

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
  console.log("[control] Database initialized");
}

// =============================================================================
// Workflow Engine Setup
// =============================================================================

export function setupWorkflowEngine(): void {
  engine = createWorkflowEngine();

  // Register the launch-run blueprint and all its node executors
  registerLaunchRun();
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

  console.log("[control] Background tasks started");
}

export async function heartbeatTimeoutCheck(): Promise<void> {
  const now = Date.now();

  // 1. Detect all degraded instances (> 2 minutes) — single query, partition locally
  const degradedInstances = detectStaleHeartbeats(TIMING.HEARTBEAT_DEGRADED_MS);

  for (const instance of degradedInstances) {
    const elapsed = now - instance.last_heartbeat;
    const isStale = elapsed >= TIMING.STALE_DETECTION_MS;

    if (isStale) {
      // Find and fail all ACTIVE allocations for this instance
      const activeAllocations = queryMany<{ id: number }>(
        "SELECT id FROM allocations WHERE instance_id = ? AND status = 'ACTIVE'",
        [instance.id]
      );

      let failed = 0;
      for (const alloc of activeAllocations) {
        const result = failAllocation(alloc.id, "ACTIVE");
        if (result.success) failed++;
      }

      if (activeAllocations.length > 0) {
        console.log(
          `[heartbeat] Instance ${instance.id} stale (last heartbeat ${elapsed}ms ago), failed ${failed}/${activeAllocations.length} active allocation(s)`
        );
      }
    } else {
      console.debug(
        `[heartbeat] Instance ${instance.id} degraded (last heartbeat ${elapsed}ms ago)`
      );
    }
  }
}

export async function holdExpiryCheck(): Promise<void> {
  await holdExpiryTask();
}

export async function manifestCleanupCheck(): Promise<void> {
  const now = Date.now();
  const expiredManifests = listExpiredManifests(now);

  for (const manifest of expiredManifests) {
    console.log(`[manifest-cleanup] Manifest ${manifest.id} expired (sealed, past retention)`);
    // TODO(#LIFE-09): Submit cleanup-manifest workflow for each expired manifest
  }
}

export async function reconciliationTask(): Promise<void> {
  // Stub: no-op for Slice 1
}

/**
 * Warm pool background reconciliation:
 * 1. Timeout stale CLAIMED allocations (> CLAIMED_TIMEOUT_MS)
 * 2. Expire old AVAILABLE allocations (> WARM_POOL_EXPIRY_MS)
 *
 * For stale CLAIMED: transition to FAILED (the run will fail and crash recovery handles the rest).
 * For expired AVAILABLE: transition to FAILED and terminate the instance if no other active allocations.
 */
export async function warmPoolReconciliation(): Promise<void> {
  const now = Date.now();

  // 1. Timeout stale CLAIMED allocations
  const claimedCutoff = now - TIMING.CLAIMED_TIMEOUT_MS;
  const staleClaimed = findStaleClaimed(claimedCutoff);
  for (const alloc of staleClaimed) {
    const result = failAllocation(alloc.id, "CLAIMED");
    if (result.success) {
      console.log(`[warm-pool] Timed out stale CLAIMED allocation ${alloc.id} (instance ${alloc.instance_id})`);
    }
  }

  // 2. Expire old AVAILABLE allocations
  const availableCutoff = now - TIMING.WARM_POOL_EXPIRY_MS;
  const expiredAvailable = findExpiredAvailable(availableCutoff);
  for (const alloc of expiredAvailable) {
    const result = failAllocation(alloc.id, "AVAILABLE");
    if (result.success) {
      console.log(`[warm-pool] Expired stale AVAILABLE allocation ${alloc.id} (instance ${alloc.instance_id})`);

      // Check if instance has any remaining non-terminal allocations
      const remaining = queryOne<{ count: number }>(
        "SELECT COUNT(*) as count FROM allocations WHERE instance_id = ? AND status NOT IN ('COMPLETE', 'FAILED')",
        [alloc.instance_id]
      );
      if (remaining && remaining.count === 0) {
        // No active allocations — terminate the instance
        try {
          const instance = getInstance(alloc.instance_id);
          if (instance?.provider_id) {
            const provider = await getProvider(instance.provider as any);
            await provider.terminate(instance.provider_id);
            console.log(`[warm-pool] Terminated instance ${alloc.instance_id} (no active allocations)`);
          }
        } catch (err) {
          console.warn(`[warm-pool] Failed to terminate instance ${alloc.instance_id}:`, err);
        }
      }
    }
  }
}

export async function storageGarbageCollection(): Promise<void> {
  const now = Date.now();

  // Sub-task A: Blob GC - delete orphaned blobs unreferenced for 24h+
  const graceCutoff = now - TIMING.BLOB_GRACE_PERIOD_MS;
  const orphanedBlobs = findOrphanedBlobs(graceCutoff);

  const batchSize = 500;

  if (orphanedBlobs.length > 0) {
    // Batch delete: take first 500 blobs (per spec batch size)
    const blobBatch = orphanedBlobs.slice(0, batchSize).map(b => b.id);
    deleteBlobBatch(blobBatch);
    console.log(`[gc] Deleted ${blobBatch.length} orphaned blob(s)`);
  }

  // Sub-task B: Object expiry - delete objects with expires_at in the past
  const expiredObjects = queryMany<{ id: number }>(
    "SELECT id FROM objects WHERE expires_at IS NOT NULL AND expires_at < ? LIMIT ?",
    [now, batchSize]
  );

  if (expiredObjects.length > 0) {
    const objectIds = expiredObjects.map(o => o.id);
    deleteObjectBatch(objectIds);
    console.log(`[gc] Expired ${objectIds.length} object(s)`);
  }
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

async function terminateOrphanedVMs(): Promise<void> {
  const { getProvider } = await import("./provider/registry");
  try {
    const provider = await getProvider("orbstack");
    const vms = await provider.list();
    if (vms.length === 0) return;

    // Cross-reference with DB to find truly orphaned VMs
    const { queryMany } = await import("./material/db");
    const knownInstances = queryMany(
      "SELECT provider_id FROM instances WHERE provider_id IS NOT NULL AND status != 'terminated'"
    );
    const knownIds = new Set(
      knownInstances.map((r: any) => r.provider_id as string)
    );
    const orphaned = vms.filter((vm) => !knownIds.has(vm.id));

    if (orphaned.length === 0) return;

    console.log(
      `[control] Terminating ${orphaned.length} orphaned VM(s) (${vms.length} total)...`
    );
    for (const vm of orphaned) {
      try {
        await provider.terminate(vm.id);
      } catch (err) {
        console.warn(`[control] Failed to terminate VM ${vm.id}:`, err);
      }
    }
  } catch (err) {
    // Provider not available — skip
    console.warn("[control] Orphan cleanup skipped:", err);
  }
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
