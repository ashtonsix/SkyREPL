// tests/unit/resource-manifest.test.ts - Resource Manifest Layer Tests
// Tests for manifest claiming protocol, retention policies, and lifecycle helpers.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import {
  createInstance,
  createManifest,
  addResourceToManifest,
  getManifest,
  getManifestResources,
  listExpiredManifests,
  createWorkflow,
  execute,
  queryMany,
  type Instance,
  type Manifest,
  type Workflow,
  type ManifestResource,
} from "../../control/src/material/db";
import { sealManifest } from "../../control/src/workflow/state-transitions";
import { TIMING } from "@skyrepl/contracts";

// Module under test
import {
  sealManifestWithPolicy,
  claimResourceAtomic,
  claimWarmInstance,
  parentClaimSubworkflowResource,
  isManifestExpired,
} from "../../control/src/resource/manifest";

// =============================================================================
// Test Helpers
// =============================================================================

function createTestInstance(overrides?: Partial<{
  spec: string;
  region: string;
  init_checksum: string | null;
  workflow_state: string;
  last_heartbeat: number;
  provider: string;
}>): Instance {
  const now = Date.now();
  return createInstance({
    provider: overrides?.provider ?? "orbstack",
    provider_id: `test-vm-${now}-${Math.random().toString(36).slice(2, 8)}`,
    spec: overrides?.spec ?? "gpu-small",
    region: overrides?.region ?? "us-east-1",
    ip: "10.0.0.1",
    workflow_state: overrides?.workflow_state ?? "launch-run:provisioning",
    workflow_error: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 0,
    spot_request_id: null,
    init_checksum: overrides?.init_checksum !== undefined ? overrides.init_checksum : null,
    registration_token_hash: null,
    last_heartbeat: overrides?.last_heartbeat ?? now,
  });
}

function createTestWorkflow(overrides?: Partial<{
  type: string;
  manifest_id: number | null;
  status: Workflow["status"];
}>): Workflow {
  return createWorkflow({
    type: overrides?.type ?? "launch-run",
    parent_workflow_id: null,
    depth: 0,
    status: overrides?.status ?? "pending",
    current_node: null,
    input_json: "{}",
    output_json: null,
    error_json: null,
    manifest_id: overrides?.manifest_id ?? null,
    trace_id: null,
    idempotency_key: null,
    timeout_ms: null,
    timeout_at: null,
    started_at: null,
    finished_at: null,
    updated_at: Date.now(),
  });
}

function createTestManifest(workflowId: number): Manifest {
  return createManifest(workflowId);
}

function sealTestManifest(manifestId: number, expiresAt?: number): void {
  const result = sealManifest(manifestId, expiresAt ?? Date.now() + 3_600_000);
  if (!result.success) {
    throw new Error(`Failed to seal manifest ${manifestId}: ${result.reason}`);
  }
}

// =============================================================================
// Setup
// =============================================================================

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest();
});

afterEach(() => cleanup());

// =============================================================================
// sealManifestWithPolicy
// =============================================================================

describe("manifest: sealManifestWithPolicy", () => {
  test("launch-run gets 30 minute retention", () => {
    const workflow = createTestWorkflow({ type: "launch-run" });
    const manifest = createTestManifest(workflow.id);

    const beforeSeal = Date.now();
    const result = sealManifestWithPolicy(manifest.id, "launch-run");
    const afterSeal = Date.now();

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.status).toBe("SEALED");
      const expectedMin = beforeSeal + 30 * 60_000;
      const expectedMax = afterSeal + 30 * 60_000;
      expect(result.data.expires_at).toBeGreaterThanOrEqual(expectedMin);
      expect(result.data.expires_at).toBeLessThanOrEqual(expectedMax);
    }
  });

  test("create-snapshot gets ~1 year retention (indefinite)", () => {
    const workflow = createTestWorkflow({ type: "create-snapshot" });
    const manifest = createTestManifest(workflow.id);

    const beforeSeal = Date.now();
    const result = sealManifestWithPolicy(manifest.id, "create-snapshot");

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.status).toBe("SEALED");
      // Should be ~1 year from now
      const oneYear = 365 * 86_400_000;
      expect(result.data.expires_at).toBeGreaterThanOrEqual(
        beforeSeal + oneYear - 1000
      );
    }
  });

  test("terminate-instance gets 5 minute retention", () => {
    const workflow = createTestWorkflow({ type: "terminate-instance" });
    const manifest = createTestManifest(workflow.id);

    const beforeSeal = Date.now();
    const result = sealManifestWithPolicy(manifest.id, "terminate-instance");

    expect(result.success).toBe(true);
    if (result.success) {
      const expectedMin = beforeSeal + 5 * 60_000;
      expect(result.data.expires_at).toBeGreaterThanOrEqual(expectedMin - 100);
    }
  });

  test("cleanup-manifest gets immediate expiry", () => {
    const workflow = createTestWorkflow({ type: "cleanup-manifest" });
    const manifest = createTestManifest(workflow.id);

    const beforeSeal = Date.now();
    const result = sealManifestWithPolicy(manifest.id, "cleanup-manifest");

    expect(result.success).toBe(true);
    if (result.success) {
      // expires_at should be approximately now (within 1 second)
      expect(result.data.expires_at).toBeGreaterThanOrEqual(beforeSeal);
      expect(result.data.expires_at).toBeLessThanOrEqual(beforeSeal + 1000);
    }
  });

  test("unknown workflow type gets default retention", () => {
    const workflow = createTestWorkflow({ type: "unknown-workflow" });
    const manifest = createTestManifest(workflow.id);

    const beforeSeal = Date.now();
    const result = sealManifestWithPolicy(manifest.id, "unknown-workflow");

    expect(result.success).toBe(true);
    if (result.success) {
      const expectedMin = beforeSeal + TIMING.DEFAULT_MANIFEST_RETENTION_MS;
      const expectedMax =
        beforeSeal + TIMING.DEFAULT_MANIFEST_RETENTION_MS + 1000;
      expect(result.data.expires_at).toBeGreaterThanOrEqual(expectedMin - 100);
      expect(result.data.expires_at).toBeLessThanOrEqual(expectedMax);
    }
  });

  test("fails for already sealed manifest", () => {
    const workflow = createTestWorkflow();
    const manifest = createTestManifest(workflow.id);
    sealTestManifest(manifest.id);

    const result = sealManifestWithPolicy(manifest.id, "launch-run");
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.reason).toBe("WRONG_STATE");
    }
  });
});

// =============================================================================
// claimResourceAtomic
// =============================================================================

describe("manifest: claimResourceAtomic", () => {
  test("successfully transfers resource between manifests", () => {
    const workflow1 = createTestWorkflow();
    const workflow2 = createTestWorkflow();
    const sourceManifest = createTestManifest(workflow1.id);
    const targetManifest = createTestManifest(workflow2.id);
    const instance = createTestInstance();

    addResourceToManifest(sourceManifest.id, "instance", String(instance.id), {
      cleanupPriority: 50,
    });
    sealTestManifest(sourceManifest.id);

    const result = claimResourceAtomic(
      targetManifest.id,
      "instance",
      String(instance.id)
    );

    expect(result).toBe(true);

    // Verify resource is now in target manifest
    const targetResources = getManifestResources(targetManifest.id);
    expect(targetResources.length).toBe(1);
    expect(targetResources[0].resource_type).toBe("instance");
    expect(targetResources[0].resource_id).toBe(String(instance.id));
    expect(targetResources[0].owner_type).toBe("workflow");
    expect(targetResources[0].owner_id).toBe(targetManifest.id);

    // Verify resource is removed from source manifest
    const sourceResources = getManifestResources(sourceManifest.id);
    expect(sourceResources.length).toBe(0);
  });

  test("fails if resource not found in any SEALED manifest", () => {
    const workflow = createTestWorkflow();
    const targetManifest = createTestManifest(workflow.id);

    const result = claimResourceAtomic(
      targetManifest.id,
      "instance",
      "99999"
    );

    expect(result).toBe(false);
  });

  test("fails if target manifest is not DRAFT", () => {
    const workflow1 = createTestWorkflow();
    const workflow2 = createTestWorkflow();
    const sourceManifest = createTestManifest(workflow1.id);
    const targetManifest = createTestManifest(workflow2.id);
    const instance = createTestInstance();

    addResourceToManifest(sourceManifest.id, "instance", String(instance.id));
    sealTestManifest(sourceManifest.id);
    sealTestManifest(targetManifest.id);

    const result = claimResourceAtomic(
      targetManifest.id,
      "instance",
      String(instance.id)
    );

    expect(result).toBe(false);

    // Resource should still be in source manifest
    const sourceResources = getManifestResources(sourceManifest.id);
    expect(sourceResources.length).toBe(1);
  });

  test("fails if resource is in DRAFT manifest (not sealed)", () => {
    const workflow1 = createTestWorkflow();
    const workflow2 = createTestWorkflow();
    const sourceManifest = createTestManifest(workflow1.id);
    const targetManifest = createTestManifest(workflow2.id);
    const instance = createTestInstance();

    // Add to source but do NOT seal it
    addResourceToManifest(sourceManifest.id, "instance", String(instance.id));

    const result = claimResourceAtomic(
      targetManifest.id,
      "instance",
      String(instance.id)
    );

    expect(result).toBe(false);
  });

  test("preserves cleanup_priority during transfer", () => {
    const workflow1 = createTestWorkflow();
    const workflow2 = createTestWorkflow();
    const sourceManifest = createTestManifest(workflow1.id);
    const targetManifest = createTestManifest(workflow2.id);
    const instance = createTestInstance();

    addResourceToManifest(sourceManifest.id, "instance", String(instance.id), {
      cleanupPriority: 90,
    });
    sealTestManifest(sourceManifest.id);

    claimResourceAtomic(targetManifest.id, "instance", String(instance.id));

    const targetResources = getManifestResources(targetManifest.id);
    expect(targetResources[0].cleanup_priority).toBe(90);
  });
});

// =============================================================================
// claimWarmInstance
// =============================================================================

describe("manifest: claimWarmInstance", () => {
  test("finds and claims matching instance", () => {
    const workflow1 = createTestWorkflow();
    const workflow2 = createTestWorkflow();
    const sourceManifest = createTestManifest(workflow1.id);
    const targetManifest = createTestManifest(workflow2.id);
    const instance = createTestInstance({ spec: "gpu-small" });

    addResourceToManifest(sourceManifest.id, "instance", String(instance.id));
    sealTestManifest(sourceManifest.id);

    const result = claimWarmInstance(targetManifest.id, { spec: "gpu-small" });

    expect(result.success).toBe(true);
    expect(result.instanceId).toBe(instance.id);

    // Verify the resource moved
    const targetResources = getManifestResources(targetManifest.id);
    expect(targetResources.length).toBe(1);
    expect(targetResources[0].resource_id).toBe(String(instance.id));
  });

  test("prefers checksum match", () => {
    const workflow1 = createTestWorkflow();
    const workflow2 = createTestWorkflow();
    const workflow3 = createTestWorkflow();
    const sourceManifest1 = createTestManifest(workflow1.id);
    const sourceManifest2 = createTestManifest(workflow2.id);
    const targetManifest = createTestManifest(workflow3.id);

    const vanillaInstance = createTestInstance({
      spec: "gpu-small",
      init_checksum: null,
    });
    const matchingInstance = createTestInstance({
      spec: "gpu-small",
      init_checksum: "abc123",
    });

    addResourceToManifest(
      sourceManifest1.id,
      "instance",
      String(vanillaInstance.id)
    );
    sealTestManifest(sourceManifest1.id);

    addResourceToManifest(
      sourceManifest2.id,
      "instance",
      String(matchingInstance.id)
    );
    sealTestManifest(sourceManifest2.id);

    const result = claimWarmInstance(targetManifest.id, {
      spec: "gpu-small",
      initChecksum: "abc123",
    });

    expect(result.success).toBe(true);
    expect(result.instanceId).toBe(matchingInstance.id);
  });

  test("returns false when no match", () => {
    const workflow = createTestWorkflow();
    const targetManifest = createTestManifest(workflow.id);

    const result = claimWarmInstance(targetManifest.id, {
      spec: "gpu-large",
    });

    expect(result.success).toBe(false);
    expect(result.instanceId).toBeUndefined();
  });

  test("excludes specified instance IDs", () => {
    const workflow1 = createTestWorkflow();
    const workflow2 = createTestWorkflow();
    const workflow3 = createTestWorkflow();
    const sourceManifest1 = createTestManifest(workflow1.id);
    const sourceManifest2 = createTestManifest(workflow2.id);
    const targetManifest = createTestManifest(workflow3.id);

    const instance1 = createTestInstance({ spec: "gpu-small" });
    const instance2 = createTestInstance({ spec: "gpu-small" });

    addResourceToManifest(
      sourceManifest1.id,
      "instance",
      String(instance1.id)
    );
    sealTestManifest(sourceManifest1.id);

    addResourceToManifest(
      sourceManifest2.id,
      "instance",
      String(instance2.id)
    );
    sealTestManifest(sourceManifest2.id);

    const result = claimWarmInstance(targetManifest.id, {
      spec: "gpu-small",
      excludeInstanceIds: [instance1.id],
    });

    expect(result.success).toBe(true);
    expect(result.instanceId).toBe(instance2.id);
  });

  test("filters by region", () => {
    const workflow1 = createTestWorkflow();
    const workflow2 = createTestWorkflow();
    const workflow3 = createTestWorkflow();
    const sourceManifest1 = createTestManifest(workflow1.id);
    const sourceManifest2 = createTestManifest(workflow2.id);
    const targetManifest = createTestManifest(workflow3.id);

    const usInstance = createTestInstance({
      spec: "gpu-small",
      region: "us-east-1",
    });
    const euInstance = createTestInstance({
      spec: "gpu-small",
      region: "eu-west-1",
    });

    addResourceToManifest(
      sourceManifest1.id,
      "instance",
      String(usInstance.id)
    );
    sealTestManifest(sourceManifest1.id);

    addResourceToManifest(
      sourceManifest2.id,
      "instance",
      String(euInstance.id)
    );
    sealTestManifest(sourceManifest2.id);

    const result = claimWarmInstance(targetManifest.id, {
      spec: "gpu-small",
      region: "eu-west-1",
    });

    expect(result.success).toBe(true);
    expect(result.instanceId).toBe(euInstance.id);
  });

  test("does not claim instance in non-complete workflow state", () => {
    const workflow1 = createTestWorkflow();
    const workflow2 = createTestWorkflow();
    const sourceManifest = createTestManifest(workflow1.id);
    const targetManifest = createTestManifest(workflow2.id);

    const instance = createTestInstance({
      spec: "gpu-small",
      workflow_state: "spawn:pending",
    });

    addResourceToManifest(sourceManifest.id, "instance", String(instance.id));
    sealTestManifest(sourceManifest.id);

    const result = claimWarmInstance(targetManifest.id, { spec: "gpu-small" });

    expect(result.success).toBe(false);
  });
});

// =============================================================================
// parentClaimSubworkflowResource
// =============================================================================

describe("manifest: parentClaimSubworkflowResource", () => {
  test("claims from child's sealed manifest", () => {
    // Create child workflow with manifest
    const childWorkflow = createTestWorkflow();
    const childManifest = createTestManifest(childWorkflow.id);
    const instance = createTestInstance();

    addResourceToManifest(childManifest.id, "instance", String(instance.id));
    sealTestManifest(childManifest.id);

    // Update workflow to reference its manifest
    execute("UPDATE workflows SET manifest_id = ? WHERE id = ?", [
      childManifest.id,
      childWorkflow.id,
    ]);

    // Create parent workflow with manifest
    const parentWorkflow = createTestWorkflow();
    const parentManifest = createTestManifest(parentWorkflow.id);

    const result = parentClaimSubworkflowResource(
      parentManifest.id,
      childWorkflow.id,
      "instance"
    );

    expect(result.success).toBe(true);
    expect(result.resourceId).toBe(String(instance.id));

    // Verify resource moved to parent
    const parentResources = getManifestResources(parentManifest.id);
    expect(parentResources.length).toBe(1);
    expect(parentResources[0].owner_type).toBe("workflow");
  });

  test("fails if child manifest not sealed", () => {
    const childWorkflow = createTestWorkflow();
    const childManifest = createTestManifest(childWorkflow.id);
    const instance = createTestInstance();

    addResourceToManifest(childManifest.id, "instance", String(instance.id));
    // Do NOT seal

    execute("UPDATE workflows SET manifest_id = ? WHERE id = ?", [
      childManifest.id,
      childWorkflow.id,
    ]);

    const parentWorkflow = createTestWorkflow();
    const parentManifest = createTestManifest(parentWorkflow.id);

    const result = parentClaimSubworkflowResource(
      parentManifest.id,
      childWorkflow.id,
      "instance"
    );

    expect(result.success).toBe(false);
  });

  test("fails if child workflow has no manifest", () => {
    const childWorkflow = createTestWorkflow({ manifest_id: null });
    const parentWorkflow = createTestWorkflow();
    const parentManifest = createTestManifest(parentWorkflow.id);

    const result = parentClaimSubworkflowResource(
      parentManifest.id,
      childWorkflow.id,
      "instance"
    );

    expect(result.success).toBe(false);
  });

  test("fails if no matching resource type in child manifest", () => {
    const childWorkflow = createTestWorkflow();
    const childManifest = createTestManifest(childWorkflow.id);
    const instance = createTestInstance();

    addResourceToManifest(childManifest.id, "instance", String(instance.id));
    sealTestManifest(childManifest.id);

    execute("UPDATE workflows SET manifest_id = ? WHERE id = ?", [
      childManifest.id,
      childWorkflow.id,
    ]);

    const parentWorkflow = createTestWorkflow();
    const parentManifest = createTestManifest(parentWorkflow.id);

    // Request a "run" type, but only "instance" exists
    const result = parentClaimSubworkflowResource(
      parentManifest.id,
      childWorkflow.id,
      "run"
    );

    expect(result.success).toBe(false);
  });
});

// =============================================================================
// isManifestExpired
// =============================================================================

describe("manifest: isManifestExpired", () => {
  test("returns true for expired SEALED manifest", () => {
    const workflow = createTestWorkflow();
    const manifest = createTestManifest(workflow.id);

    // Seal with an already-past expiry
    sealTestManifest(manifest.id, Date.now() - 1000);

    expect(isManifestExpired(manifest.id)).toBe(true);
  });

  test("returns false for non-expired SEALED manifest", () => {
    const workflow = createTestWorkflow();
    const manifest = createTestManifest(workflow.id);

    sealTestManifest(manifest.id, Date.now() + 3_600_000);

    expect(isManifestExpired(manifest.id)).toBe(false);
  });

  test("returns false for DRAFT manifest", () => {
    const workflow = createTestWorkflow();
    const manifest = createTestManifest(workflow.id);

    // Do not seal — remains DRAFT
    expect(isManifestExpired(manifest.id)).toBe(false);
  });

  test("returns false for non-existent manifest", () => {
    expect(isManifestExpired(99999)).toBe(false);
  });

  test("returns false when manifest has unclaimed resources (owner_type='manifest')", () => {
    const workflow = createTestWorkflow();
    const manifest = createTestManifest(workflow.id);
    const instance = createTestInstance();

    // Add resource, seal, then manually revert owner_type to 'manifest'
    // to test the safety-net check. Normally sealing marks resources as
    // 'released' (Step 9), but this tests the edge case where a resource
    // somehow retains owner_type='manifest'.
    addResourceToManifest(manifest.id, "instance", String(instance.id));
    sealTestManifest(manifest.id, Date.now() - 1000);

    // Revert owner_type to simulate pre-Step-9 state or data corruption
    execute(
      `UPDATE manifest_resources SET owner_type = 'manifest'
       WHERE manifest_id = ? AND resource_type = 'instance'`,
      [manifest.id]
    );

    // Despite being past expires_at, should NOT be expired because of unclaimed resources
    expect(isManifestExpired(manifest.id)).toBe(false);
  });

  test("returns true when all resources are claimed (owner_type='workflow') and time expired", () => {
    const workflow = createTestWorkflow();
    const manifest = createTestManifest(workflow.id);
    const instance = createTestInstance();

    // Add resource with owner_type='workflow' (claimed by a workflow)
    addResourceToManifest(manifest.id, "instance", String(instance.id), {
      ownerType: "workflow",
      ownerId: workflow.id,
    });

    // Seal with an already-past expiry
    sealTestManifest(manifest.id, Date.now() - 1000);

    // Should be expired: time is past and no unclaimed resources
    expect(isManifestExpired(manifest.id)).toBe(true);
  });

  test("returns true when manifest has no resources and time expired", () => {
    const workflow = createTestWorkflow();
    const manifest = createTestManifest(workflow.id);

    // No resources added — seal with past expiry
    sealTestManifest(manifest.id, Date.now() - 1000);

    expect(isManifestExpired(manifest.id)).toBe(true);
  });

  test("Step 9: sealing marks resources as released, unblocking expiry", () => {
    const workflow = createTestWorkflow();
    const manifest = createTestManifest(workflow.id);
    const instance = createTestInstance();

    // Add resource (owner_type defaults to 'manifest')
    addResourceToManifest(manifest.id, "instance", String(instance.id));

    // Verify resource is owner_type='manifest' before seal
    const before = getManifestResources(manifest.id);
    expect(before[0].owner_type).toBe("manifest");

    // Seal with an already-past expiry
    sealTestManifest(manifest.id, Date.now() - 1000);

    // After sealing, owner_type should be 'released'
    const after = getManifestResources(manifest.id);
    expect(after[0].owner_type).toBe("released");

    // Manifest should now be expired (released resources don't block)
    expect(isManifestExpired(manifest.id)).toBe(true);
  });
});

// =============================================================================
// listExpiredManifests (SD-G1-03)
// =============================================================================

describe("manifest: listExpiredManifests", () => {
  test("returns manifests with no unclaimed resources", () => {
    const workflow = createTestWorkflow();
    const manifest = createTestManifest(workflow.id);

    // Seal with past expiry, no resources
    sealTestManifest(manifest.id, Date.now() - 1000);

    const expired = listExpiredManifests(Date.now());
    expect(expired.length).toBe(1);
    expect(expired[0].id).toBe(manifest.id);
  });

  test("excludes manifests with unclaimed resources (owner_type='manifest')", () => {
    const workflow = createTestWorkflow();
    const manifest = createTestManifest(workflow.id);
    const instance = createTestInstance();

    // Add resource, seal, then manually revert owner_type to 'manifest'
    // to test the safety-net check (sealing now marks them as 'released').
    addResourceToManifest(manifest.id, "instance", String(instance.id));
    sealTestManifest(manifest.id, Date.now() - 1000);

    execute(
      `UPDATE manifest_resources SET owner_type = 'manifest'
       WHERE manifest_id = ? AND resource_type = 'instance'`,
      [manifest.id]
    );

    const expired = listExpiredManifests(Date.now());
    expect(expired.length).toBe(0);
  });

  test("includes manifests where all resources are claimed (owner_type='workflow')", () => {
    const workflow = createTestWorkflow();
    const manifest = createTestManifest(workflow.id);
    const instance = createTestInstance();

    // Add resource claimed by a workflow
    addResourceToManifest(manifest.id, "instance", String(instance.id), {
      ownerType: "workflow",
      ownerId: workflow.id,
    });

    // Seal with past expiry
    sealTestManifest(manifest.id, Date.now() - 1000);

    const expired = listExpiredManifests(Date.now());
    expect(expired.length).toBe(1);
    expect(expired[0].id).toBe(manifest.id);
  });

  test("excludes non-expired manifests", () => {
    const workflow = createTestWorkflow();
    const manifest = createTestManifest(workflow.id);

    // Seal with future expiry
    sealTestManifest(manifest.id, Date.now() + 3_600_000);

    const expired = listExpiredManifests(Date.now());
    expect(expired.length).toBe(0);
  });

  test("Step 9: sealed manifest with released resources appears in expired list", () => {
    const workflow = createTestWorkflow();
    const manifest = createTestManifest(workflow.id);
    const instance = createTestInstance();

    // Add resource (owner_type='manifest'), then seal (marks as 'released')
    addResourceToManifest(manifest.id, "instance", String(instance.id));
    sealTestManifest(manifest.id, Date.now() - 1000);

    // Verify sealing changed owner_type to 'released'
    const resources = getManifestResources(manifest.id);
    expect(resources[0].owner_type).toBe("released");

    // Should appear in expired list (released resources don't block)
    const expired = listExpiredManifests(Date.now());
    expect(expired.length).toBe(1);
    expect(expired[0].id).toBe(manifest.id);
  });
});

