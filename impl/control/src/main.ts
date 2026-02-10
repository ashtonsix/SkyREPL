// main.ts - Control Plane Entry Point

import { createServer } from "./api/routes";
import { registerAgentRoutes, createRealAgentBridge } from "./api/agent";
import { registerWebSocketRoutes, registerSSEWorkflowStream } from "./api/sse-protocol";
import { initDatabase, runMigrations } from "./material/db";
import { createWorkflowEngine, recoverWorkflows } from "./workflow/engine";
import { registerLaunchRun } from "./intent/launch-run";
import { setAgentBridge } from "./workflow/nodes/start-run";
import { TIMING } from "@skyrepl/shared";
import type { WorkflowEngine } from "./workflow/engine";

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
  const dbPath = process.env.SKYREPL_DB_PATH ?? "skyrepl-control.db";
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
  // heartbeatTimeoutCheck: the only one that matters for Slice 1 (still a stub)
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

  // Remaining background tasks: not implemented for Slice 1
  console.log("[control] Background task holdExpiryCheck: not implemented for Slice 1");
  console.log("[control] Background task manifestCleanupCheck: not implemented for Slice 1");
  console.log("[control] Background task orphanScanTask: not implemented for Slice 1");
  console.log("[control] Background task storageGarbageCollection: not implemented for Slice 1");

  console.log("[control] Background tasks started");
}

export async function heartbeatTimeoutCheck(): Promise<void> {
  // Stub for Slice 1: no-op
  console.debug("[control] heartbeatTimeoutCheck: no-op for Slice 1");
}

export async function holdExpiryCheck(): Promise<void> {
  // Stub: no-op for Slice 1
}

export async function manifestCleanupCheck(): Promise<void> {
  // Stub: no-op for Slice 1
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
  const { findStaleClaimed, findExpiredAvailable, getInstance: getInst, queryOne: qo } = await import("./material/db");
  const { failAllocation } = await import("./workflow/state-transitions");

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
      const remaining = qo<{ count: number }>(
        "SELECT COUNT(*) as count FROM allocations WHERE instance_id = ? AND status NOT IN ('COMPLETE', 'FAILED')",
        [alloc.instance_id]
      );
      if (remaining && remaining.count === 0) {
        // No active allocations — terminate the instance
        try {
          const instance = getInst(alloc.instance_id);
          if (instance?.provider_id) {
            const { getProvider: gp } = await import("./provider/registry");
            const provider = await gp(instance.provider as any);
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

export async function orphanScanTask(): Promise<void> {
  // Stub: no-op for Slice 1
}

export async function storageGarbageCollection(): Promise<void> {
  // Stub: no-op for Slice 1
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

  // 3. Wait for in-flight workflows to quiesce (max 30s)
  const { findActiveWorkflows } = await import("./material/db");
  const drainStart = Date.now();
  const DRAIN_TIMEOUT_MS = 30_000;
  while (Date.now() - drainStart < DRAIN_TIMEOUT_MS) {
    const active = findActiveWorkflows();
    if (active.length === 0) break;
    console.log(`[control] Draining ${active.length} active workflow(s)...`);
    await new Promise(r => setTimeout(r, 2000));
  }

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
    console.log(`[control] Terminating ${vms.length} orphaned VM(s)...`);
    for (const vm of vms) {
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
// Entry Point
// =============================================================================

startup().catch((err) => {
  console.error("Failed to start control plane:", err);
  process.exit(1);
});
