#!/usr/bin/env bun
// e2e-blob-workload.ts — Full-workload E2E validation of blob provider integration.
// Spins up an in-process control plane, configures MinIO as blob provider,
// then exercises the full agent upload + CLI download round-trip through the API.
//
// Run: cd lifecycle/control && bun src/e2e-blob-workload.ts

import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createHash, randomBytes } from "node:crypto";

import {
  initDatabase,
  closeDatabase,
  runMigrations,
  createInstance,
  createRun,
  createAllocation,
  execute,
  queryOne,
} from "../../control/src/material/db";
import { createServer } from "../../control/src/api/routes";
import { registerAgentRoutes } from "../../control/src/api/agent";
import { registerWebSocketRoutes, registerSSEWorkflowStream } from "../../control/src/api/sse-protocol";
import { clearBlobProviderCache } from "../../control/src/provider/storage/registry";
import { getDatabase } from "../../control/src/material/db/init";
import type { Server } from "bun";

// =============================================================================
// Test state
// =============================================================================

let tmpDir: string;
let server: Server<unknown>;
let baseUrl: string;
let apiKey: string;
let instanceToken: string;
let instanceId: number;
let runId: number;

let passed = 0;
let failed = 0;

function assert(cond: boolean, msg: string) {
  if (cond) { passed++; console.log(`  \u2713 ${msg}`); }
  else { failed++; console.log(`  \u2717 ${msg}`); }
}

function sha256(data: Buffer | Uint8Array): string {
  return createHash("sha256").update(data).digest("hex");
}

// =============================================================================
// Setup
// =============================================================================

async function setup() {
  console.log("\n-- Setup --");
  tmpDir = await mkdtemp(join(tmpdir(), "skyrepl-e2e-blob-"));
  const dbPath = join(tmpDir, "test.db");

  initDatabase(dbPath);
  runMigrations();

  // Seed API key for admin calls
  apiKey = "srk-e2e-test-key-" + randomBytes(16).toString("hex");
  const keyHash = createHash("sha256").update(apiKey).digest("hex");
  const db = getDatabase();
  db.prepare(
    `INSERT OR IGNORE INTO api_keys (key_hash, name, tenant_id, role, permissions, created_at)
     VALUES (?, 'e2e-admin', 1, 'admin', 'all', ?)`
  ).run(keyHash, Date.now());

  // Create the HTTP server
  const app = createServer({ port: 0, corsOrigins: [], maxBodySize: 50_000_000 });
  registerAgentRoutes(app);
  registerWebSocketRoutes(app);
  registerSSEWorkflowStream(app);

  const listener = app.listen({ port: 0 });
  server = (listener as any).server ?? listener;
  const port = (server as any)?.port ?? (listener as any)?.server?.port;
  baseUrl = `http://localhost:${port}`;
  console.log(`  Control plane listening on ${baseUrl}`);

  // Create instance with a known registration token (agent auth)
  instanceToken = "e2e-test-agent-token-" + randomBytes(16).toString("hex");
  const tokenHash = createHash("sha256").update(instanceToken).digest("hex");

  const instance = createInstance({
    provider: "orbstack",
    provider_id: "e2e-test-instance",
    spec: "small",
    region: "local",
    ip: "127.0.0.1",
    workflow_state: "ready",
    workflow_error: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 0,
    spot_request_id: null,
    init_checksum: null,
    registration_token_hash: tokenHash,
    provider_metadata: null,
    last_heartbeat: Date.now(),
  });
  instanceId = instance.id;

  // Create a run + allocation for agent context
  const run = createRun({
    command: "echo test",
    workdir: "/tmp",
    max_duration_ms: 60000,
    workflow_state: "launch-run:running",
    workflow_error: null,
    current_manifest_id: null,
    exit_code: null,
    init_checksum: null,
    create_snapshot: 0,
    spot_interrupted: 0,
    started_at: Date.now(),
    finished_at: null,
  });
  runId = run.id;

  createAllocation({
    instance_id: instanceId,
    run_id: runId,
    status: "ACTIVE",
    current_manifest_id: null,
    user: "test",
    workdir: "/tmp",
    debug_hold_until: null,
    completed_at: null,
  });

  console.log(`  Instance ${instanceId}, Run ${runId}, allocation wired`);
}

// =============================================================================
// Configure MinIO as blob provider
// =============================================================================

async function configureMinioProvider() {
  console.log("\n-- Configure MinIO blob provider --");

  const minioConfig = {
    endpoint: "http://127.0.0.1:9000",
    region: "us-east-1",
    bucket: "skyrepl",
    accessKeyId: "minioadmin",
    secretAccessKey: "minioadmin",
    forcePathStyle: true,
  };

  // Update blob_provider_configs to use MinIO for tenant 1
  execute(
    `UPDATE blob_provider_configs SET provider_name = ?, config_json = ?, updated_at = ? WHERE tenant_id = ?`,
    ["minio", JSON.stringify(minioConfig), Date.now(), 1]
  );

  // Clear cache so the provider is re-read from DB
  clearBlobProviderCache();

  console.log("  Configured tenant 1 to use MinIO");
}

// =============================================================================
// Test: Small blob (inline, <64KB) upload + download via API
// =============================================================================

async function testInlineBlobRoundTrip() {
  console.log("\n== Small Blob (<64KB) Inline Round-Trip ==");

  const data = Buffer.from("Small inline artifact content for E2E test");
  const checksum = sha256(data);

  // Step 1: POST /v1/agent/blobs/upload-url
  const uploadUrlResp = await fetch(`${baseUrl}/v1/agent/blobs/upload-url`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${instanceToken}`,
    },
    body: JSON.stringify({
      checksum,
      size_bytes: data.length,
      run_id: runId,
    }),
  });
  assert(uploadUrlResp.ok, `upload-url returned ${uploadUrlResp.status}`);
  const uploadUrlBody = await uploadUrlResp.json() as any;
  assert(uploadUrlBody.inline === true, `inline = true for small blob (size=${data.length})`);
  assert(typeof uploadUrlBody.blob_id === "number", `got blob_id = ${uploadUrlBody.blob_id}`);
  const blobId = uploadUrlBody.blob_id;

  // Step 2: PUT inline data
  const inlineUrl = uploadUrlBody.url.startsWith("http")
    ? uploadUrlBody.url
    : `${baseUrl}${uploadUrlBody.url}`;
  const putResp = await fetch(inlineUrl, {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${instanceToken}`,
      "Content-Type": "application/octet-stream",
    },
    body: data,
  });
  assert(putResp.ok, `inline PUT returned ${putResp.status}`);

  // Step 3: POST /v1/agent/blobs/confirm
  const confirmResp = await fetch(`${baseUrl}/v1/agent/blobs/confirm`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${instanceToken}`,
    },
    body: JSON.stringify({ blob_id: blobId, checksum }),
  });
  assert(confirmResp.ok, `confirm returned ${confirmResp.status}`);
  const confirmBody = await confirmResp.json() as any;
  assert(confirmBody.confirmed === true, "blob confirmed");

  // Step 4: POST /v1/agent/artifacts (register the artifact)
  const artifactResp = await fetch(`${baseUrl}/v1/agent/artifacts`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${instanceToken}`,
    },
    body: JSON.stringify({
      run_id: runId,
      path: "test/small-artifact.txt",
      checksum,
      size_bytes: data.length,
      blob_id: blobId,
    }),
  });
  assert(artifactResp.ok, `artifacts POST returned ${artifactResp.status}`);

  // Step 5: GET /v1/runs/:id/artifacts (list)
  const listResp = await fetch(`${baseUrl}/v1/runs/${runId}/artifacts`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  assert(listResp.ok, `list artifacts returned ${listResp.status}`);
  const listBody = await listResp.json() as any;
  assert(listBody.data.length >= 1, `found ${listBody.data.length} artifact(s)`);
  const artifact = listBody.data.find((a: any) => a.path === "test/small-artifact.txt");
  assert(artifact !== undefined, "found our small-artifact in the list");

  // Step 6: GET /v1/artifacts/:id/download (inline: should return 200 with data)
  const downloadResp = await fetch(`${baseUrl}/v1/artifacts/${artifact.id}/download`, {
    headers: { Authorization: `Bearer ${apiKey}` },
    redirect: "manual",
  });
  assert(downloadResp.status === 200, `download returned 200 (inline blob)`);
  const downloaded = Buffer.from(await downloadResp.arrayBuffer());
  assert(sha256(downloaded) === checksum, "downloaded content matches checksum");
  assert(downloaded.toString() === data.toString(), "downloaded content matches original");
}

// =============================================================================
// Test: Large blob (external, >=64KB) presigned URL upload + download via API
// =============================================================================

async function testPresignedBlobRoundTrip() {
  console.log("\n== Large Blob (>=64KB) Presigned URL Round-Trip ==");

  const data = randomBytes(100 * 1024); // 100KB
  const checksum = sha256(data);

  // Step 1: POST /v1/agent/blobs/upload-url
  const uploadUrlResp = await fetch(`${baseUrl}/v1/agent/blobs/upload-url`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${instanceToken}`,
    },
    body: JSON.stringify({
      checksum,
      size_bytes: data.length,
      run_id: runId,
    }),
  });
  assert(uploadUrlResp.ok, `upload-url returned ${uploadUrlResp.status}`);
  const uploadUrlBody = await uploadUrlResp.json() as any;
  assert(uploadUrlBody.inline === false, `inline = false for large blob (size=${data.length})`);
  assert(typeof uploadUrlBody.blob_id === "number", `got blob_id = ${uploadUrlBody.blob_id}`);
  const blobId = uploadUrlBody.blob_id;

  // The URL should be a presigned S3 URL (points to MinIO)
  const putUrl = uploadUrlBody.url;
  assert(putUrl.startsWith("http://127.0.0.1:9000"), `presigned PUT URL points to MinIO: ${putUrl.substring(0, 50)}...`);

  // Step 2: PUT directly to presigned URL (agent -> S3, no auth header)
  const putResp = await fetch(putUrl, {
    method: "PUT",
    body: data,
    headers: { "Content-Type": "application/octet-stream" },
  });
  assert(putResp.ok, `presigned PUT succeeded (${putResp.status})`);

  // Step 3: POST /v1/agent/blobs/confirm
  const confirmResp = await fetch(`${baseUrl}/v1/agent/blobs/confirm`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${instanceToken}`,
    },
    body: JSON.stringify({ blob_id: blobId, checksum }),
  });
  assert(confirmResp.ok, `confirm returned ${confirmResp.status}`);
  const confirmBody = await confirmResp.json() as any;
  assert(confirmBody.confirmed === true, "blob confirmed");

  // Step 4: POST /v1/agent/artifacts (register artifact)
  const artifactResp = await fetch(`${baseUrl}/v1/agent/artifacts`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${instanceToken}`,
    },
    body: JSON.stringify({
      run_id: runId,
      path: "test/large-artifact.bin",
      checksum,
      size_bytes: data.length,
      blob_id: blobId,
    }),
  });
  assert(artifactResp.ok, `artifacts POST returned ${artifactResp.status}`);

  // Step 5: GET /v1/artifacts/:id/download (should return 302 redirect to presigned GET URL)
  const listResp = await fetch(`${baseUrl}/v1/runs/${runId}/artifacts`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  const listBody = await listResp.json() as any;
  const artifact = listBody.data.find((a: any) => a.path === "test/large-artifact.bin");
  assert(artifact !== undefined, "found our large-artifact in the list");

  const downloadResp = await fetch(`${baseUrl}/v1/artifacts/${artifact.id}/download`, {
    headers: { Authorization: `Bearer ${apiKey}` },
    redirect: "manual",
  });
  assert(downloadResp.status === 302, `download returned 302 (presigned redirect)`);
  const location = downloadResp.headers.get("Location");
  assert(location !== null && location.startsWith("http://127.0.0.1:9000"), "Location header points to MinIO");

  // Step 6: Follow redirect WITHOUT auth header (simulates CLI behavior)
  const redirectResp = await fetch(location!);
  assert(redirectResp.ok, `presigned GET from MinIO succeeded (${redirectResp.status})`);
  const downloaded = Buffer.from(await redirectResp.arrayBuffer());
  assert(sha256(downloaded) === checksum, "downloaded content matches checksum");
  assert(downloaded.length === data.length, `downloaded size matches: ${downloaded.length} == ${data.length}`);
}

// =============================================================================
// Test: Log streaming (append + download)
// =============================================================================

async function testLogStreaming() {
  console.log("\n== Log Streaming (chunk model) ==");

  // Append log data
  const logLine1 = "Starting job...\n";
  const logLine2 = "Processing data...\n";
  const logLine3 = "Job complete.\n";

  for (const data of [logLine1, logLine2, logLine3]) {
    const resp = await fetch(`${baseUrl}/v1/agent/logs`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${instanceToken}`,
      },
      body: JSON.stringify({
        run_id: runId,
        stream: "stdout",
        data,
        timestamp: Date.now(),
      }),
    });
    assert(resp.ok, `log POST returned ${resp.status}`);
  }

  // Download logs
  const logsResp = await fetch(`${baseUrl}/v1/runs/${runId}/logs?stream=stdout`, {
    headers: {},
  });
  assert(logsResp.ok, `logs GET returned ${logsResp.status}`);
  const logContent = await logsResp.text();
  assert(logContent.includes("Starting job"), "log contains first line");
  assert(logContent.includes("Job complete"), "log contains last line");

  // Download with max_bytes (tail semantics)
  const tailResp = await fetch(`${baseUrl}/v1/runs/${runId}/logs?stream=stdout&max_bytes=20`, {
    headers: {},
  });
  assert(tailResp.ok, `logs GET with max_bytes returned ${tailResp.status}`);
  const tailContent = await tailResp.text();
  assert(tailContent.length <= 20, `max_bytes truncated to ${tailContent.length} bytes`);
}

// =============================================================================
// Test: SQL provider fallback (proxy mode)
// =============================================================================

async function testSqlProviderProxy() {
  console.log("\n== SQL Provider (proxy mode) ==");

  // Switch back to SQL provider
  execute(
    `UPDATE blob_provider_configs SET provider_name = 'sql', config_json = '{}', updated_at = ? WHERE tenant_id = ?`,
    [Date.now(), 1]
  );
  clearBlobProviderCache();

  const data = randomBytes(100 * 1024); // 100KB — above SBO, but SQL provider proxies
  const checksum = sha256(data);

  // Step 1: upload-url should return proxy URL (not presigned)
  const uploadUrlResp = await fetch(`${baseUrl}/v1/agent/blobs/upload-url`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${instanceToken}`,
    },
    body: JSON.stringify({
      checksum,
      size_bytes: data.length,
      run_id: runId,
    }),
  });
  assert(uploadUrlResp.ok, `upload-url returned ${uploadUrlResp.status}`);
  const uploadUrlBody = await uploadUrlResp.json() as any;
  assert(uploadUrlBody.inline === false, "inline = false for large blob");
  const uploadUrl = uploadUrlBody.url;
  assert(uploadUrl.startsWith("/v1/agent/blobs/"), `proxy URL: ${uploadUrl}`);
  const blobId = uploadUrlBody.blob_id;

  // Step 2: PUT to proxy URL (control plane stores in blob_payloads)
  const putResp = await fetch(`${baseUrl}${uploadUrl}`, {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${instanceToken}`,
      "Content-Type": "application/octet-stream",
    },
    body: data,
  });
  assert(putResp.ok, `proxy PUT returned ${putResp.status}`);

  // Step 3: confirm
  const confirmResp = await fetch(`${baseUrl}/v1/agent/blobs/confirm`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${instanceToken}`,
    },
    body: JSON.stringify({ blob_id: blobId, checksum }),
  });
  assert(confirmResp.ok, `confirm returned ${confirmResp.status}`);

  // Step 4: register artifact
  const artifactResp = await fetch(`${baseUrl}/v1/agent/artifacts`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${instanceToken}`,
    },
    body: JSON.stringify({
      run_id: runId,
      path: "test/sql-proxy-artifact.bin",
      checksum,
      size_bytes: data.length,
      blob_id: blobId,
    }),
  });
  assert(artifactResp.ok, `artifacts POST returned ${artifactResp.status}`);

  // Step 5: download — should be 200 (proxy, no redirect)
  const listResp = await fetch(`${baseUrl}/v1/runs/${runId}/artifacts`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  const listBody = await listResp.json() as any;
  const artifact = listBody.data.find((a: any) => a.path === "test/sql-proxy-artifact.bin");
  assert(artifact !== undefined, "found sql-proxy-artifact in list");

  const downloadResp = await fetch(`${baseUrl}/v1/artifacts/${artifact.id}/download`, {
    headers: { Authorization: `Bearer ${apiKey}` },
    redirect: "manual",
  });
  assert(downloadResp.status === 200, `download returned 200 (SQL proxy, no redirect)`);
  const downloaded = Buffer.from(await downloadResp.arrayBuffer());
  assert(sha256(downloaded) === checksum, "downloaded content matches checksum");
  assert(downloaded.length === data.length, `downloaded size matches: ${downloaded.length}`);

  // Restore MinIO provider for subsequent tests
  const minioConfig = {
    endpoint: "http://127.0.0.1:9000",
    region: "us-east-1",
    bucket: "skyrepl",
    accessKeyId: "minioadmin",
    secretAccessKey: "minioadmin",
    forcePathStyle: true,
  };
  execute(
    `UPDATE blob_provider_configs SET provider_name = 'minio', config_json = ?, updated_at = ? WHERE tenant_id = ?`,
    [JSON.stringify(minioConfig), Date.now(), 1]
  );
  clearBlobProviderCache();
}

// =============================================================================
// Test: SBO boundary (exactly 64KB)
// =============================================================================

async function testSBOBoundaryViaAPI() {
  console.log("\n== SBO Boundary (64KB) via API ==");

  // Exactly at threshold (65536) — should go external
  const dataAt = randomBytes(65536);
  const checksumAt = sha256(dataAt);

  const resp = await fetch(`${baseUrl}/v1/agent/blobs/upload-url`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${instanceToken}`,
    },
    body: JSON.stringify({ checksum: checksumAt, size_bytes: 65536, run_id: runId }),
  });
  const body = await resp.json() as any;
  assert(body.inline === false, "65536 bytes -> external (inline=false)");

  // Just below threshold (65535) — should go inline
  const dataBelow = randomBytes(65535);
  const checksumBelow = sha256(dataBelow);

  const respBelow = await fetch(`${baseUrl}/v1/agent/blobs/upload-url`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${instanceToken}`,
    },
    body: JSON.stringify({ checksum: checksumBelow, size_bytes: 65535, run_id: runId }),
  });
  const bodyBelow = await respBelow.json() as any;
  assert(bodyBelow.inline === true, "65535 bytes -> inline (inline=true)");
}

// =============================================================================
// Teardown
// =============================================================================

async function teardown() {
  console.log("\n-- Teardown --");
  try { (server as any).stop(); } catch {}
  try { closeDatabase(); } catch {}
  try { await rm(tmpDir, { recursive: true }); } catch {}
  console.log("  Cleaned up");
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  console.log("\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557");
  console.log("\u2551  E2E Blob Workload Validation                  \u2551");
  console.log("\u2551  Full control-plane + MinIO + SQL round-trip    \u2551");
  console.log("\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d");

  await setup();
  await configureMinioProvider();

  try {
    await testInlineBlobRoundTrip();
    await testPresignedBlobRoundTrip();
    await testLogStreaming();
    await testSqlProviderProxy();
    await testSBOBoundaryViaAPI();
  } catch (err: any) {
    console.error(`\nFATAL: ${err.message}`);
    console.error(err.stack);
    failed++;
  } finally {
    await teardown();
  }

  console.log("\n\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550");
  console.log(`RESULTS: ${passed} passed, ${failed} failed`);
  console.log("\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550");

  if (failed > 0) {
    console.log("\n\u2717 E2E WORKLOAD VALIDATION FAILED");
    process.exit(1);
  } else {
    console.log("\n\u2713 E2E WORKLOAD VALIDATION PASSED");
  }
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
