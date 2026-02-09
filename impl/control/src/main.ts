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

// =============================================================================
// Startup
// =============================================================================

export async function startup(): Promise<void> {
  console.log("[control] Starting control plane...");

  initializeDatabase();
  setupWorkflowEngine();
  registerProviders();

  const port = parseInt(process.env.SKYREPL_PORT ?? process.env.PORT ?? "8080", 10);
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
  app.listen({ port, idleTimeout: 0 });

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

  // Remaining background tasks: not implemented for Slice 1
  console.log("[control] Background task holdExpiryCheck: not implemented for Slice 1");
  console.log("[control] Background task manifestCleanupCheck: not implemented for Slice 1");
  console.log("[control] Background task reconciliationTask: not implemented for Slice 1");
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
  console.log("[control] Shutting down control plane...");

  // Clear all background task intervals
  for (const handle of intervalHandles) {
    clearInterval(handle);
  }
  intervalHandles.length = 0;

  // For Slice 1: just exit
  process.exit(0);
}

// =============================================================================
// Entry Point
// =============================================================================

startup().catch((err) => {
  console.error("Failed to start control plane:", err);
  process.exit(1);
});
