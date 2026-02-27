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
import type { Catalog } from "@skyrepl/scaffold/src/catalog";
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
import { getDuckDB, flushInboxToDuckDB, closeDuckDB, initDuckDB } from "./material/duckdb";
import { emitAuditEvent } from "./material/db/audit";

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
let lastBudgetCheck = 0;
const BUDGET_CHECK_INTERVAL_MS = 60_000;

// =============================================================================
// Startup
// =============================================================================

export async function startup(): Promise<void> {
  console.log("[control] Starting control plane...");

  // Load env file (lowest priority — won't overwrite existing env vars)
  loadControlEnvFile();

  initializeDatabase();

  // Initialize DuckDB OLAP engine (mirrors scaffold/src/main.ts pattern).
  // Runs after SQLite so initDuckDB() can ATTACH the SQLite DB for cross queries.
  const dbPath = process.env.SKYREPL_DB_PATH ?? join(homedir(), ".repl", "skyrepl-control.db");
  const duckdbPath = process.env.SKYREPL_DUCKDB_PATH ?? join(homedir(), ".repl", "skyrepl-olap.duckdb");
  try {
    await initDuckDB(duckdbPath, dbPath);
    console.log("[control] DuckDB initialized");
  } catch (err) {
    console.warn("[control] DuckDB initialization failed (OLAP/billing disabled):", err);
  }

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
  // OrbStack provider is auto-registered via the provider registry
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

  // Audit inbox → DuckDB flush (every 10s).
  // Conditional: DuckDB may be absent (e.g., disabled via SKYREPL_SERVICES or init failure).
  // In standalone startup(), initDuckDB() runs before startBackgroundTasks(), so this
  // branch is taken whenever DuckDB initialized successfully.
  if (getDuckDB()) {
    const auditFlushHandle = setInterval(() => {
      flushInboxToDuckDB().catch((err) =>
        console.error("[control] flushInboxToDuckDB error:", err)
      );
    }, 10_000);
    intervalHandles.push(auditFlushHandle);
  }

  // skyrepl_infra COGS emitter (daily) — disabled until real COGS tracking is wired up.
  // Enable by setting SKYREPL_ENABLE_COGS_EMITTER=1 once actual cost data is available.
  if (process.env.SKYREPL_ENABLE_COGS_EMITTER === "1") {
    const cogsHandle = setInterval(() => {
      emitCOGSEvents().catch((err) => console.error("[control] COGS emitter error:", err));
    }, 86_400_000); // 24h
    intervalHandles.push(cogsHandle);
  }

  // Billing reconciliation (daily — 4A)
  const billingReconHandle = setInterval(() => {
    billingReconciliationTask().catch((err) =>
      console.error("[control] billingReconciliationTask error:", err)
    );
  }, TIMING.BILLING_RECONCILIATION_INTERVAL_MS);
  intervalHandles.push(billingReconHandle);

  // Billing cycle check (monthly settlement — 4B)
  const billingCycleHandle = setInterval(() => {
    billingCycleTask().catch((err) =>
      console.error("[control] billingCycleTask error:", err)
    );
  }, 3_600_000); // Check hourly
  intervalHandles.push(billingCycleHandle);

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
// COGS Emitter (skyrepl_infra virtual provider)
// =============================================================================

/**
 * Emit daily infrastructure cost events for SkyREPL's own overhead (COGS).
 * Uses dedupe_key to prevent duplicate events for the same day.
 *
 * NOTE: Deferred — real cost breakdown (DNS, compute, storage) not yet tracked.
 * Gated behind SKYREPL_ENABLE_COGS_EMITTER=1 to avoid zero-value event pollution.
 */
async function emitCOGSEvents(): Promise<void> {
  // NOTE: placeholder body — remove early return when real COGS data is available.
  return;

  // Dead code below — kept for reference on intended shape:
  // const dateString = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
  // emitAuditEvent({
  //   event_type: "fee",
  //   tenant_id: 1,
  //   provider: "skyrepl_infra",
  //   source: "cogs_emitter",
  //   is_cost: true,
  //   data: {
  //     category: "dns",
  //     amount_cents: 0,
  //     currency: "USD",
  //   },
  //   dedupe_key: `skyrepl_infra:dns:${dateString}`,
  //   occurred_at: Date.now(),
  // });
}

// =============================================================================
// Billing Reconciliation Task (WL-061-4A)
// =============================================================================

async function billingReconciliationTask(): Promise<void> {
  try {
    const { runBillingReconciliation } = await import("./billing/reconciliation/job");
    const results = await runBillingReconciliation();
    const totalDiscrepancies = results.reduce((n, r) => n + r.discrepancies, 0);
    if (totalDiscrepancies > 0) {
      console.warn(`[billing-recon] Found ${totalDiscrepancies} discrepancies across ${results.length} provider(s)`);
    }
  } catch (err) {
    console.warn("[billing-recon] billingReconciliationTask error:", err);
  }
}

// =============================================================================
// Billing Cycle Task (WL-061-4B §7)
// =============================================================================

/**
 * Lightweight monthly settlement check.
 * Runs hourly; only settles on the 1st of each month.
 * Idempotency: skips settlement if a batch already exists for the current month.
 */
async function billingCycleTask(): Promise<void> {
  const now = new Date();
  // Only run on the 1st of each month
  if (now.getDate() !== 1) return;

  try {
    const { listSettlementBatches, settlePeriod } = await import("./billing/settlement");
    const { listTenants } = await import("./material/db");

    // Calculate previous month's period bounds
    const monthEnd = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0, 0); // Start of current month = end of prev month
    const monthStart = new Date(monthEnd);
    monthStart.setMonth(monthStart.getMonth() - 1); // Start of previous month
    const periodStartMs = monthStart.getTime();
    const periodEndMs = monthEnd.getTime();

    const tenants = listTenants();
    for (const tenant of tenants) {
      // Check if we already settled this period for this tenant
      const existing = listSettlementBatches(tenant.id).find(
        b => b.period_start_ms === periodStartMs && b.period_end_ms === periodEndMs
          && (b.status === "settled" || b.status === "invoiced" || b.status === "paid")
      );
      if (existing) continue;

      try {
        const batch = await settlePeriod(tenant.id, {
          start_ms: periodStartMs,
          end_ms: periodEndMs,
        });
        console.log(
          `[billing-cycle] Settled tenant ${tenant.id} (${tenant.name}): ` +
          `batch ${batch.batch_uuid}, total=${batch.total_cents} cents`
        );

        // Create Stripe invoice if configured
        try {
          const { createStripeInvoice } = await import("./billing/stripe");
          const { getSettlementLineItems } = await import("./billing/settlement");
          const lineItems = await getSettlementLineItems(batch.batch_uuid);
          const result = await createStripeInvoice(batch, lineItems);
          console.log(
            `[billing-cycle] Invoiced tenant ${tenant.id}: ${result.invoice_id}`
          );
        } catch (stripeErr: any) {
          // Stripe not configured or failed — batch stays "settled", retry next tick
          if (!stripeErr?.message?.includes("Stripe not configured")) {
            console.error(`[billing-cycle] Stripe invoice failed for tenant ${tenant.id}:`, stripeErr);
          }
        }
      } catch (err) {
        console.error(`[billing-cycle] Failed to settle tenant ${tenant.id}:`, err);
      }
    }
  } catch (err) {
    console.warn("[billing-cycle] billingCycleTask error:", err);
  }
}

// =============================================================================
// Budget Check Task (WL-061-3B §5)
// =============================================================================

/**
 * Periodic budget check: logs a warning if any tenant is over budget.
 * Runs every 60 seconds, piggybacked on the heartbeat dispatch loop.
 * Future: emit SSE budget_warning events (§6).
 */
async function budgetCheckTask(): Promise<void> {
  // TODO: full implementation with SSE notifications (WL-061-3B §6)
  // For now, just log if any tenant is over budget
  try {
    const { listTenants } = await import("./material/db");
    const { projectBudget } = await import("./billing/budget");
    const tenants = listTenants();
    const now = Date.now();
    const monthStart = new Date();
    monthStart.setDate(1);
    monthStart.setHours(0, 0, 0, 0);

    for (const tenant of tenants) {
      if (tenant.budget_usd == null) continue;
      const projection = await projectBudget({
        tenant_id: tenant.id,
        period_start_ms: monthStart.getTime(),
        period_end_ms: now,
      });
      if (projection.remaining_cents < 0) {
        console.warn(
          `[budget] Tenant ${tenant.id} (${tenant.name}) is over budget: ` +
          `${projection.total_cents} cents spent, budget ${projection.budget_cents} cents`
        );
      }
    }
  } catch (err) {
    console.warn("[budget] budgetCheckTask error:", err);
  }
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

  // Budget check (§5): runs every 60s, piggybacked on the heartbeat loop
  const doBudgetCheck = now - lastBudgetCheck >= BUDGET_CHECK_INTERVAL_MS;

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
    if (doBudgetCheck) {
      await budgetCheckTask();
      lastBudgetCheck = Date.now();
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

/** Service-level shutdown: stop HTTP, drain engine, clear intervals, flush DuckDB.
 *  Does NOT touch SQLite — caller (standalone or scaffold) owns SQLite DB lifecycle. */
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

  // 4. Close DuckDB (flushes remaining inbox rows, releases ATTACH on SQLite).
  // Must happen before SQLite is closed by the caller. closeDuckDB() is idempotent
  // so double-calling from shutdown() or scaffold is harmless.
  try {
    await closeDuckDB();
    console.log("[control] DuckDB closed");
  } catch (err) {
    console.warn("[control] DuckDB close failed:", err);
  }

  console.log("[control] Service shutdown complete");
}

/** Standalone shutdown: service shutdown + SQLite cleanup + exit. */
export async function shutdown(): Promise<void> {
  await serviceShutdown(); // serviceShutdown() closes DuckDB before returning

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
