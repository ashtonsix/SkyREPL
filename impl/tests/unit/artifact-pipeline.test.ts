// tests/unit/artifact-pipeline.test.ts - Artifact Pipeline Tests
// Tests for #AGENT-07 (artifact collection/upload) and #SNAP-04 (artifact API/resource)

import { describe, test, expect, beforeEach } from "bun:test";
import {
  initDatabase,
  runMigrations,
  createInstance,
  createRun,
  createAllocation,
  createManifest,
  createBlob,
  createObject,
  addObjectTag,
  addResourceToManifest,
  queryOne,
  queryMany,
  execute,
  getObject,
  type Instance,
  type Run,
  type Allocation,
  type Manifest,
  type StorageObject,
} from "../../control/src/material/db";
import { createBlobWithDedup } from "../../control/src/material/storage";

// =============================================================================
// Test Helpers
// =============================================================================

function createTestInstance(): Instance {
  const now = Date.now();
  return createInstance({
    provider: "orbstack",
    provider_id: `test-vm-${now}-${Math.random().toString(36).slice(2, 8)}`,
    spec: "ubuntu",
    region: "local",
    ip: "10.0.0.1",
    workflow_state: "spawn:complete",
    workflow_error: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 0,
    spot_request_id: null,
    init_checksum: null,
    ssh_port: 22,
    last_heartbeat: now,
    registration_token_hash: null,
    tailscale_ip: null,
    tailscale_status: null,
  });
}

function createTestRun(manifestId: number | null = null): Run {
  return createRun({
    command: "echo hello",
    spec: "ubuntu",
    provider: "orbstack",
    region: "local",
    workdir: "/workspace",
    env_json: "{}",
    init_checksum: null,
    max_duration_ms: 60000,
    hold_duration_ms: 5000,
    create_snapshot: 0,
    spot_requested: 0,
    current_manifest_id: manifestId,
    workflow_state: "launch-run:running",
    workflow_error: null,
    exit_code: null,
    finished_at: null,
    spot_interrupted: 0,
    idempotency_key: null,
  });
}

function createTestAllocation(instanceId: number, runId: number): Allocation {
  return createAllocation({
    instance_id: instanceId,
    run_id: runId,
    status: "ACTIVE",
    claimed_at: Date.now(),
    activated_at: Date.now(),
    completed_at: null,
    failed_at: null,
    failure_reason: null,
    hold_expires_at: null,
    has_ssh_sessions: 0,
  });
}

function storeArtifactObject(
  runId: number,
  path: string,
  content: string,
  manifestId: number | null = null
): { objectId: number; blobId: number } {
  const data = Buffer.from(content, "utf-8");
  const checksum = require("crypto").createHash("sha256").update(data).digest("hex");
  const blobId = createBlobWithDedup("artifacts", checksum, data);
  const now = Date.now();

  const obj = createObject({
    type: "artifact",
    blob_id: blobId,
    provider: null,
    provider_object_id: null,
    metadata_json: JSON.stringify({
      run_id: runId,
      path,
      checksum,
      size_bytes: data.length,
    }),
    expires_at: null,
    current_manifest_id: manifestId,
    accessed_at: null,
    updated_at: now,
  });

  addObjectTag(obj.id, "run_id", String(runId));
  addObjectTag(obj.id, "artifact_path", path);

  if (manifestId) {
    try {
      addResourceToManifest(manifestId, "object", String(obj.id));
    } catch { /* non-fatal */ }
  }

  return { objectId: obj.id, blobId };
}

// =============================================================================
// Tests
// =============================================================================

beforeEach(() => {
  initDatabase(":memory:");
  runMigrations();
});

describe("AGENT-07: Artifact storage", () => {
  test("createBlobWithDedup stores artifact blob", () => {
    const data = Buffer.from("test artifact content");
    const checksum = require("crypto").createHash("sha256").update(data).digest("hex");

    const blobId = createBlobWithDedup("artifacts", checksum, data);
    expect(blobId).toBeGreaterThan(0);

    // Verify blob exists
    const blob = queryOne<{ id: number; payload: Buffer; size_bytes: number }>(
      "SELECT id, payload, size_bytes FROM blobs WHERE id = ?",
      [blobId]
    );
    expect(blob).toBeTruthy();
    expect(blob!.size_bytes).toBe(data.length);
  });

  test("createBlobWithDedup deduplicates same checksum", () => {
    const data = Buffer.from("same content");
    const checksum = require("crypto").createHash("sha256").update(data).digest("hex");

    const blobId1 = createBlobWithDedup("artifacts", checksum, data);
    const blobId2 = createBlobWithDedup("artifacts", checksum, data);

    expect(blobId1).toBe(blobId2);
  });

  test("artifact object is created with correct metadata", () => {
    const run = createTestRun();
    const { objectId } = storeArtifactObject(run.id, "output/result.csv", "col1,col2\n1,2\n");

    const obj = getObject(objectId);
    expect(obj).toBeTruthy();
    expect(obj!.type).toBe("artifact");
    const meta = JSON.parse(obj!.metadata_json!);
    expect(meta.run_id).toBe(run.id);
    expect(meta.path).toBe("output/result.csv");
    expect(meta.size_bytes).toBe(14);
  });

  test("artifact object has correct tags", () => {
    const run = createTestRun();
    const { objectId } = storeArtifactObject(run.id, "model.pt", "model data");

    const tags = queryMany<{ tag: string }>(
      "SELECT tag FROM object_tags WHERE object_id = ?",
      [objectId]
    );
    const tagSet = new Set(tags.map((t) => t.tag));
    expect(tagSet.has(`run_id:${run.id}`)).toBe(true);
    expect(tagSet.has("artifact_path:model.pt")).toBe(true);
  });

  test("artifact wired into manifest", () => {
    // createManifest requires a workflow_id; create a dummy workflow first
    execute(
      `INSERT INTO workflows (type, manifest_id, status, input_json, current_node, started_at, finished_at, output_json, error_json, created_at, updated_at)
       VALUES ('launch-run', NULL, 'running', '{}', NULL, ?, NULL, NULL, NULL, ?, ?)`,
      [Date.now(), Date.now(), Date.now()]
    );
    const wf = queryOne<{ id: number }>("SELECT id FROM workflows ORDER BY id DESC LIMIT 1", []);
    const manifest = createManifest(wf!.id);
    const run = createTestRun(manifest.id);
    const { objectId } = storeArtifactObject(run.id, "output.txt", "hello", manifest.id);

    const resources = queryMany<{ resource_type: string; resource_id: string }>(
      "SELECT resource_type, resource_id FROM manifest_resources WHERE manifest_id = ?",
      [manifest.id]
    );
    expect(resources.length).toBe(1);
    expect(resources[0].resource_type).toBe("object");
    expect(resources[0].resource_id).toBe(String(objectId));
  });

  test("multiple artifacts for same run", () => {
    const run = createTestRun();
    storeArtifactObject(run.id, "output/a.csv", "data a");
    storeArtifactObject(run.id, "output/b.csv", "data b");
    storeArtifactObject(run.id, "model.pt", "model data");

    const artifacts = queryMany<{ id: number }>(
      `SELECT o.id FROM objects o
       JOIN object_tags t ON o.id = t.object_id
       WHERE o.type = 'artifact' AND t.tag = ?`,
      [`run_id:${run.id}`]
    );
    expect(artifacts.length).toBe(3);
  });
});

describe("SNAP-04: Artifact query and download", () => {
  test("list artifacts by run_id via object_tags join", () => {
    const run1 = createTestRun();
    const run2 = createTestRun();

    storeArtifactObject(run1.id, "output/a.csv", "data a");
    storeArtifactObject(run1.id, "output/b.csv", "data b");
    storeArtifactObject(run2.id, "other.txt", "other data");

    // Query matching the route pattern
    const artifacts = queryMany<{
      id: number;
      blob_id: number;
      metadata_json: string | null;
      created_at: number;
    }>(
      `SELECT o.id, o.blob_id, o.metadata_json, o.created_at
       FROM objects o
       JOIN object_tags t ON o.id = t.object_id
       WHERE o.type = 'artifact' AND t.tag = ?
       ORDER BY o.created_at ASC`,
      [`run_id:${run1.id}`]
    );

    expect(artifacts.length).toBe(2);
    const paths = artifacts.map((a) => JSON.parse(a.metadata_json!).path);
    expect(paths).toContain("output/a.csv");
    expect(paths).toContain("output/b.csv");
  });

  test("download artifact blob data by object id", () => {
    const run = createTestRun();
    const content = "col1,col2\nfoo,bar\n";
    const { objectId } = storeArtifactObject(run.id, "result.csv", content);

    // Query matching the download route pattern
    const obj = queryOne<{ id: number; blob_id: number; metadata_json: string | null }>(
      "SELECT id, blob_id, metadata_json FROM objects WHERE id = ? AND type = 'artifact'",
      [objectId]
    );
    expect(obj).toBeTruthy();

    const blob = queryOne<{ id: number; payload: Buffer | null; size_bytes: number }>(
      "SELECT id, payload, size_bytes FROM blobs WHERE id = ?",
      [obj!.blob_id]
    );
    expect(blob).toBeTruthy();
    expect(blob!.payload).toBeTruthy();
    expect(Buffer.from(blob!.payload!).toString("utf-8")).toBe(content);
  });

  test("artifact not found returns null", () => {
    const obj = queryOne<{ id: number }>(
      "SELECT id FROM objects WHERE id = 99999 AND type = 'artifact'",
      [99999]
    );
    expect(obj).toBeNull();
  });

  test("empty run has no artifacts", () => {
    const run = createTestRun();
    const artifacts = queryMany<{ id: number }>(
      `SELECT o.id FROM objects o
       JOIN object_tags t ON o.id = t.object_id
       WHERE o.type = 'artifact' AND t.tag = ?`,
      [`run_id:${run.id}`]
    );
    expect(artifacts.length).toBe(0);
  });

  test("artifact metadata extraction", () => {
    const run = createTestRun();
    const content = "x".repeat(1024);
    const { objectId } = storeArtifactObject(run.id, "data/big-file.bin", content);

    const obj = getObject(objectId);
    const meta = JSON.parse(obj!.metadata_json!);

    expect(meta.path).toBe("data/big-file.bin");
    expect(meta.size_bytes).toBe(1024);
    expect(meta.checksum).toHaveLength(64); // SHA-256 hex
    expect(meta.run_id).toBe(run.id);
  });
});

describe("AGENT-07: Agent-side artifact module", () => {
  test("artifacts.py imports and has collect_and_upload", () => {
    const { spawnSync } = require("child_process");
    const agentDir = `${process.cwd()}/agent`;
    const result = spawnSync("python3", ["-c", `
import sys
sys.path.insert(0, '${agentDir}')
from artifacts import collect_and_upload, MAX_ARTIFACT_SIZE, MAX_ARTIFACTS_PER_RUN
print(f"MAX_SIZE={MAX_ARTIFACT_SIZE}")
print(f"MAX_COUNT={MAX_ARTIFACTS_PER_RUN}")
print("OK")
`], { encoding: "utf-8" });

    expect(result.stdout).toContain("MAX_SIZE=52428800");
    expect(result.stdout).toContain("MAX_COUNT=100");
    expect(result.stdout).toContain("OK");
  });

  test("_find_matching_files with glob patterns", () => {
    const { spawnSync } = require("child_process");
    const { mkdtempSync, writeFileSync, mkdirSync } = require("fs");
    const { join } = require("path");
    const tmpdir = require("os").tmpdir();

    // Create test directory structure
    const workdir = mkdtempSync(join(tmpdir, "skyrepl-artifact-test-"));
    mkdirSync(join(workdir, "output"), { recursive: true });
    writeFileSync(join(workdir, "output", "result.csv"), "data");
    writeFileSync(join(workdir, "output", "log.txt"), "log");
    writeFileSync(join(workdir, "model.pt"), "model");
    writeFileSync(join(workdir, "README.md"), "readme");

    const agentDir = `${process.cwd()}/agent`;
    const result = spawnSync("python3", ["-c", `
import sys, json
sys.path.insert(0, '${agentDir}')
from artifacts import _find_matching_files
files = _find_matching_files('${workdir}', ['output/**', '*.pt'])
print(json.dumps(files))
`], { encoding: "utf-8" });

    const files = JSON.parse(result.stdout.trim());
    expect(files).toContain("model.pt");
    expect(files).toContain("output/result.csv");
    expect(files).toContain("output/log.txt");
    expect(files).not.toContain("README.md");

    // Cleanup
    require("fs").rmSync(workdir, { recursive: true, force: true });
  });

  test("_prepare_artifact computes checksum and base64", () => {
    const { spawnSync } = require("child_process");
    const { mkdtempSync, writeFileSync } = require("fs");
    const { join } = require("path");
    const tmpdir = require("os").tmpdir();

    const workdir = mkdtempSync(join(tmpdir, "skyrepl-artifact-prep-"));
    writeFileSync(join(workdir, "test.txt"), "hello world");

    const agentDir = `${process.cwd()}/agent`;
    const result = spawnSync("python3", ["-c", `
import sys, json
sys.path.insert(0, '${agentDir}')
from artifacts import _prepare_artifact
entry = _prepare_artifact('${workdir}', 'test.txt')
print(json.dumps(entry))
`], { encoding: "utf-8" });

    const entry = JSON.parse(result.stdout.trim());
    expect(entry.path).toBe("test.txt");
    expect(entry.size_bytes).toBe(11);
    expect(entry.checksum).toHaveLength(64);
    // Verify base64 decodes correctly
    const decoded = Buffer.from(entry.content_base64, "base64").toString("utf-8");
    expect(decoded).toBe("hello world");

    require("fs").rmSync(workdir, { recursive: true, force: true });
  });

  test("_prepare_artifact skips files exceeding MAX_ARTIFACT_SIZE", () => {
    const { spawnSync } = require("child_process");
    const agentDir = `${process.cwd()}/agent`;
    const result = spawnSync("python3", ["-c", `
import sys, json
sys.path.insert(0, '${agentDir}')
from artifacts import _prepare_artifact, MAX_ARTIFACT_SIZE
# Test with a non-existent huge file scenario by mocking os.path.getsize
import os
original_getsize = os.path.getsize
os.path.getsize = lambda p: MAX_ARTIFACT_SIZE + 1
result = _prepare_artifact('/tmp', 'fake.bin')
os.path.getsize = original_getsize
print(json.dumps(result))
`], { encoding: "utf-8" });

    expect(result.stdout).toContain("null");
  });
});
