#!/usr/bin/env bun
// e2e-blob-providers.ts — End-to-end validation of BlobProvider implementations
// Tests S3BlobProvider against MinIO and AWS S3 (real providers, not mocks).
// Run: bun lifecycle/tests/e2e-blob-providers.ts

import { S3BlobProvider, type S3BlobProviderConfig } from "../../control/src/provider/storage/s3";
import { SqlBlobProvider } from "../../control/src/provider/storage/sql-blob";
import { S3Client } from "../../control/node_modules/@aws-sdk/client-s3";
import { createHash, randomBytes } from "node:crypto";

// =============================================================================
// Config
// =============================================================================

const MINIO_CONFIG: S3BlobProviderConfig = {
  endpoint: "http://127.0.0.1:9000",
  region: "us-east-1",
  bucket: "skyrepl-blobs",
  accessKeyId: "minioadmin",
  secretAccessKey: "minioadmin",
  forcePathStyle: true,
};

const AWS_CONFIG: S3BlobProviderConfig = {
  region: "us-east-1",
  bucket: "skyrepl-e2e-test-055b",
  accessKeyId: process.env.AWS_ACCESS_KEY_ID || "",
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "",
  // sessionToken handled via env
};

// =============================================================================
// Test Helpers
// =============================================================================

let passed = 0;
let failed = 0;
let skipped = 0;

function assert(condition: boolean, msg: string): void {
  if (condition) {
    passed++;
    console.log(`  ✓ ${msg}`);
  } else {
    failed++;
    console.log(`  ✗ ${msg}`);
  }
}

async function assertThrows(fn: () => Promise<any>, msg: string): Promise<void> {
  try {
    await fn();
    failed++;
    console.log(`  ✗ ${msg} (expected throw, got success)`);
  } catch {
    passed++;
    console.log(`  ✓ ${msg}`);
  }
}

function sha256(data: Buffer): string {
  return createHash("sha256").update(data).digest("hex");
}

function randomKey(): string {
  return `e2e-test/${Date.now()}-${randomBytes(8).toString("hex")}`;
}

// =============================================================================
// Provider-level tests (direct SDK calls)
// =============================================================================

async function testProviderCRUD(name: string, provider: S3BlobProvider): Promise<void> {
  console.log(`\n── ${name}: CRUD ──`);

  const key = randomKey();
  const data = Buffer.from("Hello, BlobProvider E2E test! 🎉");
  const checksum = sha256(data);

  // Upload
  await provider.upload(key, data);
  assert(true, "upload succeeded");

  // Exists
  const exists = await provider.exists(key);
  assert(exists === true, "exists returns true after upload");

  // Download
  const downloaded = await provider.download(key);
  assert(sha256(downloaded) === checksum, "download matches checksum");
  assert(downloaded.toString() === data.toString(), "download content matches");

  // Delete
  await provider.delete(key);
  assert(true, "delete succeeded");

  // Exists after delete
  const existsAfter = await provider.exists(key);
  assert(existsAfter === false, "exists returns false after delete");
}

async function testProviderPresignedUrls(name: string, provider: S3BlobProvider): Promise<void> {
  console.log(`\n── ${name}: Presigned URLs ──`);

  const key = randomKey();
  const data = Buffer.from("Presigned URL test data — should round-trip through PUT and GET URLs");
  const checksum = sha256(data);

  // Generate presigned PUT URL
  const putUrl = await provider.generatePresignedUrl(key, "PUT", { ttlMs: 300000 });
  assert(putUrl.startsWith("http"), "PUT URL is a valid HTTP URL");
  assert(putUrl.includes(key.replace(/\//g, "%2F")) || putUrl.includes(key), "PUT URL contains the key");

  // PUT directly to presigned URL (simulates what the agent does)
  const putResponse = await fetch(putUrl, {
    method: "PUT",
    body: data,
    headers: { "Content-Type": "application/octet-stream" },
  });
  assert(putResponse.ok, `PUT to presigned URL succeeded (${putResponse.status})`);

  // Verify object exists via SDK
  const exists = await provider.exists(key);
  assert(exists === true, "object exists after presigned PUT");

  // Generate presigned GET URL
  const getUrl = await provider.generatePresignedUrl(key, "GET", { ttlMs: 300000 });
  assert(getUrl.startsWith("http"), "GET URL is a valid HTTP URL");

  // GET from presigned URL (simulates what the CLI does after 302 redirect)
  const getResponse = await fetch(getUrl);
  assert(getResponse.ok, `GET from presigned URL succeeded (${getResponse.status})`);
  const body = Buffer.from(await getResponse.arrayBuffer());
  assert(sha256(body) === checksum, "GET content matches original checksum");
  assert(body.toString() === data.toString(), "GET content matches original data");

  // Cleanup
  await provider.delete(key);
  assert(true, "cleanup: deleted test object");
}

async function testProviderHealthCheck(name: string, provider: S3BlobProvider): Promise<void> {
  console.log(`\n── ${name}: Health Check ──`);

  const health = await provider.healthCheck();
  assert(health.healthy === true, `healthy = true`);
  assert(health.latencyMs >= 0 && health.latencyMs < 10000, `latency = ${health.latencyMs}ms (reasonable)`);
}

async function testProviderLargeBlob(name: string, provider: S3BlobProvider): Promise<void> {
  console.log(`\n── ${name}: Large Blob (100KB) ──`);

  const key = randomKey();
  const data = randomBytes(100 * 1024); // 100KB — above SBO threshold
  const checksum = sha256(data);

  // Upload via presigned PUT
  const putUrl = await provider.generatePresignedUrl(key, "PUT", { ttlMs: 300000 });
  const putResp = await fetch(putUrl, {
    method: "PUT",
    body: data,
    headers: { "Content-Type": "application/octet-stream" },
  });
  assert(putResp.ok, "100KB presigned PUT succeeded");

  // Download via presigned GET
  const getUrl = await provider.generatePresignedUrl(key, "GET", { ttlMs: 300000 });
  const getResp = await fetch(getUrl);
  assert(getResp.ok, "100KB presigned GET succeeded");
  const body = Buffer.from(await getResp.arrayBuffer());
  assert(sha256(body) === checksum, "100KB content matches checksum");
  assert(body.length === data.length, `size matches: ${body.length} == ${data.length}`);

  // Cleanup
  await provider.delete(key);
}

async function testSBOBoundary(name: string, provider: S3BlobProvider): Promise<void> {
  console.log(`\n── ${name}: SBO Boundary (64KB) ──`);

  // Test exactly at the SBO threshold (65536 bytes)
  const keyExact = randomKey();
  const dataExact = randomBytes(65536);
  const checksumExact = sha256(dataExact);

  await provider.upload(keyExact, dataExact);
  const downloaded = await provider.download(keyExact);
  assert(sha256(downloaded) === checksumExact, "65536-byte blob round-trips correctly");
  assert(downloaded.length === 65536, "65536-byte blob size preserved");
  await provider.delete(keyExact);

  // Test just below threshold (65535 bytes)
  const keyBelow = randomKey();
  const dataBelow = randomBytes(65535);
  const checksumBelow = sha256(dataBelow);

  await provider.upload(keyBelow, dataBelow);
  const downloadedBelow = await provider.download(keyBelow);
  assert(sha256(downloadedBelow) === checksumBelow, "65535-byte blob round-trips correctly");
  await provider.delete(keyBelow);

  // Test just above threshold (65537 bytes)
  const keyAbove = randomKey();
  const dataAbove = randomBytes(65537);
  const checksumAbove = sha256(dataAbove);

  await provider.upload(keyAbove, dataAbove);
  const downloadedAbove = await provider.download(keyAbove);
  assert(sha256(downloadedAbove) === checksumAbove, "65537-byte blob round-trips correctly");
  await provider.delete(keyAbove);
}

// =============================================================================
// Main
// =============================================================================

async function main(): Promise<void> {
  console.log("╔════════════════════════════════════════════════╗");
  console.log("║  E2E Blob Provider Validation                 ║");
  console.log("╚════════════════════════════════════════════════╝");

  // ─── MinIO ────────────────────────────────────────────────────
  console.log("\n════════════════════════════════════════════════");
  console.log("PROVIDER: MinIO (local, forcePathStyle=true)");
  console.log("════════════════════════════════════════════════");

  let minioOk = false;
  try {
    const minio = new S3BlobProvider("minio", MINIO_CONFIG);
    const health = await minio.healthCheck();
    if (!health.healthy) {
      console.log("  ⚠ MinIO not healthy — skipping");
    } else {
      minioOk = true;
      await testProviderHealthCheck("MinIO", minio);
      await testProviderCRUD("MinIO", minio);
      await testProviderPresignedUrls("MinIO", minio);
      await testProviderLargeBlob("MinIO", minio);
      await testSBOBoundary("MinIO", minio);
    }
  } catch (err: any) {
    console.log(`  ✗ MinIO setup failed: ${err.message}`);
    failed++;
  }

  // ─── AWS S3 ──────────────────────────────────────────────────
  console.log("\n════════════════════════════════════════════════");
  console.log("PROVIDER: AWS S3 (us-east-1)");
  console.log("════════════════════════════════════════════════");

  let awsOk = false;
  try {
    // Use the default credential chain (supports SSO, env vars, instance profile, etc.)
    const aws = createAwsSsoProvider();
    const health = await aws.healthCheck();
    if (!health.healthy) {
      console.log("  ⚠ AWS S3 not healthy — skipping (run `aws login` or set credentials)");
      skipped += 5;
    } else {
      awsOk = true;
      await testProviderHealthCheck("AWS S3", aws);
      await testProviderCRUD("AWS S3", aws);
      await testProviderPresignedUrls("AWS S3", aws);
      await testProviderLargeBlob("AWS S3", aws);
      await testSBOBoundary("AWS S3", aws);
    }
  } catch (err: any) {
    console.log(`  ✗ AWS S3 setup failed: ${err.message}`);
    failed++;
  }

  // ─── Summary ─────────────────────────────────────────────────
  console.log("\n════════════════════════════════════════════════");
  console.log("SUMMARY");
  console.log("════════════════════════════════════════════════");
  console.log(`  Passed:  ${passed}`);
  console.log(`  Failed:  ${failed}`);
  console.log(`  Skipped: ${skipped}`);
  console.log(`  MinIO:   ${minioOk ? "TESTED" : "SKIPPED"}`);
  console.log(`  AWS S3:  ${awsOk ? "TESTED" : "SKIPPED"}`);

  if (failed > 0) {
    console.log("\n  ✗ E2E VALIDATION FAILED");
    process.exit(1);
  } else {
    console.log("\n  ✓ E2E VALIDATION PASSED");
  }
}

/**
 * Create an S3BlobProvider that uses AWS default credential chain (SSO, env vars,
 * instance profile, etc.) via _clientFactory injection.
 */
function createAwsSsoProvider(): S3BlobProvider {
  return new S3BlobProvider("aws-s3", {
    region: "us-east-1",
    bucket: "skyrepl-e2e-test-055b",
    accessKeyId: "placeholder",
    secretAccessKey: "placeholder",
    _clientFactory: (opts) => new S3Client({ region: opts.region }),
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
