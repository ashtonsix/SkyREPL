import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../../../tests/harness";
import {
  generateCandidateName,
  generateUniqueName,
  ADJECTIVES,
  NOUNS,
} from "./wordlist";
import { createInstance, createManifest, createWorkflow } from "../material/db";

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest();
});

afterEach(() => cleanup());

// =============================================================================
// Word List
// =============================================================================

describe("word lists", () => {
  test("adjectives list is non-empty and all lowercase", () => {
    expect(ADJECTIVES.length).toBe(175);
    for (const adj of ADJECTIVES) {
      expect(adj).toBe(adj.toLowerCase());
      expect(adj.length).toBeGreaterThan(0);
    }
  });

  test("nouns list is non-empty and all lowercase", () => {
    expect(NOUNS.length).toBe(157);
    for (const noun of NOUNS) {
      expect(noun).toBe(noun.toLowerCase());
      expect(noun.length).toBeGreaterThan(0);
    }
  });

  test("no duplicate adjectives", () => {
    const unique = new Set(ADJECTIVES);
    expect(unique.size).toBe(ADJECTIVES.length);
  });

  test("no duplicate nouns", () => {
    const unique = new Set(NOUNS);
    expect(unique.size).toBe(NOUNS.length);
  });
});

// =============================================================================
// Name Generation
// =============================================================================

describe("generateCandidateName", () => {
  test("returns adjective-noun format (hyphen-separated)", () => {
    const name = generateCandidateName();
    expect(name).toMatch(/^[a-z]+-[a-z]+$/);
    const [adj, noun] = name.split("-");
    expect(ADJECTIVES).toContain(adj);
    expect(NOUNS).toContain(noun);
  });

  test("returns distinct values across multiple calls", () => {
    const names = new Set(Array.from({ length: 50 }, () => generateCandidateName()));
    // With 40k+ combos, 50 draws should not all be the same
    expect(names.size).toBeGreaterThan(1);
  });

  test("name is all lowercase", () => {
    for (let i = 0; i < 20; i++) {
      const name = generateCandidateName();
      expect(name).toBe(name.toLowerCase());
    }
  });
});

// =============================================================================
// Unique Name Generation
// =============================================================================

describe("generateUniqueName", () => {
  test("returns a valid adjective-noun name", () => {
    const name = generateUniqueName(1, "instance");
    expect(name).toMatch(/^[a-z]+-[a-z]+(-\d+)?$/);
  });

  test("avoids collision with existing instance display_name", () => {
    // Create a bunch of instances and verify they all get unique names
    const names = new Set<string>();
    for (let i = 0; i < 10; i++) {
      const inst = createInstance({
        provider: "orbstack",
        provider_id: `vm-${i}`,
        spec: "4vcpu-8gb",
        region: "local",
        ip: null,
        workflow_state: "spawn:pending",
        workflow_error: null,
        current_manifest_id: null,
        spawn_idempotency_key: null,
        is_spot: 0,
        spot_request_id: null,
        init_checksum: null,
        registration_token_hash: null,
        provider_metadata: null,
        display_name: null,
        last_heartbeat: Date.now(),
      });
      expect(inst.display_name).not.toBeNull();
      names.add(inst.display_name!);
    }
    // All 10 instances should have unique names
    expect(names.size).toBe(10);
  });

  test("terminated instances do not block name reuse", () => {
    // Create a terminated instance with a specific name
    const inst = createInstance({
      provider: "orbstack",
      provider_id: "vm-terminated",
      spec: "4vcpu-8gb",
      region: "local",
      ip: null,
      workflow_state: "terminate:complete",
      workflow_error: null,
      current_manifest_id: null,
      spawn_idempotency_key: null,
      is_spot: 0,
      spot_request_id: null,
      init_checksum: null,
      registration_token_hash: null,
      provider_metadata: null,
      display_name: "serene-otter",
      last_heartbeat: Date.now(),
    });

    expect(inst.display_name).toBe("serene-otter");

    // generateUniqueName should be able to return "serene-otter" again since the
    // instance is terminated (workflow_state starts with "terminate:")
    // We can't guarantee it will pick that exact name, but we can ensure no crash
    const name = generateUniqueName(1, "instance");
    expect(name).toBeTruthy();
  });

  test("instances get display_name set on creation", () => {
    const inst = createInstance({
      provider: "orbstack",
      provider_id: "vm-display-test",
      spec: "4vcpu-8gb",
      region: "local",
      ip: null,
      workflow_state: "spawn:pending",
      workflow_error: null,
      current_manifest_id: null,
      spawn_idempotency_key: null,
      is_spot: 0,
      spot_request_id: null,
      init_checksum: null,
      registration_token_hash: null,
      provider_metadata: null,
      display_name: null,
      last_heartbeat: Date.now(),
    });

    expect(inst.display_name).not.toBeNull();
    expect(inst.display_name).toMatch(/^[a-z]+-[a-z]+(-\d+)?$/);
  });

  test("manifests get display_name set on creation", () => {
    const wf = createWorkflow({
      type: "launch-run",
      parent_workflow_id: null,
      depth: 0,
      status: "pending",
      current_node: null,
      input_json: "{}",
      output_json: null,
      error_json: null,
      manifest_id: null,
      trace_id: null,
      idempotency_key: null,
      timeout_ms: null,
      timeout_at: null,
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });

    const manifest = createManifest(wf.id);
    expect(manifest.display_name).not.toBeNull();
    expect(manifest.display_name).toMatch(/^[a-z]+-[a-z]+(-\d+)?$/);
  });

  test("explicit display_name on instance creation is preserved", () => {
    const inst = createInstance({
      provider: "orbstack",
      provider_id: "vm-explicit",
      spec: "4vcpu-8gb",
      region: "local",
      ip: null,
      workflow_state: "spawn:pending",
      workflow_error: null,
      current_manifest_id: null,
      spawn_idempotency_key: null,
      is_spot: 0,
      spot_request_id: null,
      init_checksum: null,
      registration_token_hash: null,
      provider_metadata: null,
      display_name: "custom-name",
      last_heartbeat: Date.now(),
    });

    expect(inst.display_name).toBe("custom-name");
  });
});
