// tests/unit/artifact-pipeline.test.ts - Artifact Pipeline Tests
// Tests for #AGENT-07 (artifact collection/upload) and #SNAP-04 (artifact API/resource)

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import {
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
    workflow_state: "launch-run:provisioning",
    workflow_error: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 0,
    spot_request_id: null,
    init_checksum: null,
    last_heartbeat: now,
    registration_token_hash: null,
  });
}

function createTestRun(manifestId: number | null = null): Run {
  return createRun({
    command: "echo hello",
    workdir: "/workspace",
    init_checksum: null,
    max_duration_ms: 60000,
    create_snapshot: 0,
    current_manifest_id: manifestId,
    workflow_state: "launch-run:running",
    workflow_error: null,
    exit_code: null,
    finished_at: null,
    spot_interrupted: 0,
    started_at: null,
  });
}

function createTestAllocation(instanceId: number, runId: number): Allocation {
  return createAllocation({
    instance_id: instanceId,
    run_id: runId,
    status: "ACTIVE",
    current_manifest_id: null,
    user: "",
    workdir: "/workspace",
    debug_hold_until: null,
    completed_at: null,
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
  const blob = createBlob({
    bucket: "artifacts",
    checksum,
    checksum_bytes: data.length,
    s3_key: null,
    s3_bucket: null,
    payload: data,
    size_bytes: data.length,
    last_referenced_at: Date.now(),
  });
  const blobId = blob.id;
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

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest();
});

afterEach(() => cleanup());

describe("AGENT-07: Artifact storage", () => {
  test("createBlob stores artifact blob", () => {
    const data = Buffer.from("test artifact content");
    const checksum = require("crypto").createHash("sha256").update(data).digest("hex");

    const blob = createBlob({
      bucket: "artifacts",
      checksum,
      checksum_bytes: data.length,
      s3_key: null,
      s3_bucket: null,
      payload: data,
      size_bytes: data.length,
      last_referenced_at: Date.now(),
    });
    expect(blob.id).toBeGreaterThan(0);

    // Verify blob exists
    const row = queryOne<{ id: number; payload: Buffer; size_bytes: number }>(
      "SELECT id, payload, size_bytes FROM blobs WHERE id = ?",
      [blob.id]
    );
    expect(row).toBeTruthy();
    expect(row!.size_bytes).toBe(data.length);
  });

  test("createBlob deduplicates same checksum in artifacts bucket", () => {
    const data = Buffer.from("same content");
    const checksum = require("crypto").createHash("sha256").update(data).digest("hex");

    const blob1 = createBlob({
      bucket: "artifacts",
      checksum,
      checksum_bytes: data.length,
      s3_key: null,
      s3_bucket: null,
      payload: data,
      size_bytes: data.length,
      last_referenced_at: Date.now(),
    });
    const blob2 = createBlob({
      bucket: "artifacts",
      checksum,
      checksum_bytes: data.length,
      s3_key: null,
      s3_bucket: null,
      payload: data,
      size_bytes: data.length,
      last_referenced_at: Date.now(),
    });

    expect(blob1.id).toBe(blob2.id);
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
