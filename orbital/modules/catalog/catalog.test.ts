// orbital/modules/catalog/catalog.test.ts - Catalog resolution, scoring, and data integrity tests
//
// These tests exercise orbital internals (resolve, scoring, data catalogs).
// Lifecycle-side integration (validateSpec, 503 fallback) lives in
// lifecycle/tests/unit/catalog-resolve.test.ts.

import { describe, test, expect } from "bun:test";
import { resolveSpec, listInstances } from "./resolve";
import { scoreSpecs } from "./scoring";
import { ALL_INSTANCES } from "./data/instances";
import { ALL_REGIONS } from "./data/regions";
import { ALL_GPUS, GPU_ALIASES } from "./data/gpus";

// =============================================================================
// Catalog Data Integrity
// =============================================================================

describe("Catalog data integrity", () => {
  test("all instances have required fields", () => {
    for (const inst of ALL_INSTANCES) {
      expect(inst.provider).toBeTruthy();
      expect(inst.spec).toBeTruthy();
      expect(typeof inst.vcpus).toBe("number");
      expect(typeof inst.memoryGib).toBe("number");
      expect(typeof inst.gpuCount).toBe("number");
      expect(["amd64", "arm64"]).toContain(inst.arch);
      expect(typeof inst.hasGpu).toBe("boolean");
      expect(typeof inst.hasSpot).toBe("boolean");
      expect(typeof inst.hasLifecycle).toBe("boolean");
      expect(Array.isArray(inst.regions)).toBe(true);
      expect(inst.regions.length).toBeGreaterThan(0);
    }
  });

  test("all GPU instances have GPU model", () => {
    const gpuInstances = ALL_INSTANCES.filter(i => i.hasGpu);
    expect(gpuInstances.length).toBeGreaterThan(0);
    for (const inst of gpuInstances) {
      expect(inst.gpuModel).toBeTruthy();
      expect(inst.gpuCount).toBeGreaterThan(0);
      expect(inst.gpuMemoryGib).toBeGreaterThan(0);
    }
  });

  test("all providers are represented", () => {
    const providers = new Set(ALL_INSTANCES.map(i => i.provider));
    expect(providers.has("aws")).toBe(true);
    expect(providers.has("lambda")).toBe(true);
    expect(providers.has("digitalocean")).toBe(true);
    expect(providers.has("orbstack")).toBe(true);
  });

  test("all regions have required fields", () => {
    for (const region of ALL_REGIONS) {
      expect(region.provider).toBeTruthy();
      expect(region.region).toBeTruthy();
      expect(region.displayName).toBeTruthy();
      expect(region.country).toBeTruthy();
      expect(typeof region.latitude).toBe("number");
      expect(typeof region.longitude).toBe("number");
    }
  });

  test("GPU families have valid data", () => {
    for (const gpu of ALL_GPUS) {
      expect(gpu.model).toBeTruthy();
      expect(gpu.family).toBeTruthy();
      expect(gpu.generation).toBeGreaterThan(0);
      expect(gpu.memoryGib).toBeGreaterThan(0);
      expect(gpu.fp16Tflops).toBeGreaterThan(0);
      expect(Array.isArray(gpu.alternatives)).toBe(true);
    }
  });

  test("GPU aliases map to valid models", () => {
    const validModels = new Set(ALL_GPUS.map(g => g.model));
    for (const [alias, model] of Object.entries(GPU_ALIASES)) {
      expect(validModels.has(model)).toBe(true);
    }
  });
});

// =============================================================================
// Spec Resolution
// =============================================================================

describe("resolveSpec", () => {
  test("exact match: AWS instance type", () => {
    const results = resolveSpec({ action: "resolve-spec", spec: "p4d.24xlarge" });
    expect(results.length).toBe(1);
    expect(results[0]!.provider).toBe("aws");
    expect(results[0]!.spec).toBe("p4d.24xlarge");
    expect(results[0]!.matchType).toBe("exact");
    expect(results[0]!.confidence).toBe(100);
  });

  test("exact match: Lambda instance type", () => {
    const results = resolveSpec({ action: "resolve-spec", spec: "gpu_1x_a100" });
    expect(results.length).toBe(1);
    expect(results[0]!.provider).toBe("lambda");
    expect(results[0]!.matchType).toBe("exact");
  });

  test("exact match: DigitalOcean size slug", () => {
    const results = resolveSpec({ action: "resolve-spec", spec: "s-1vcpu-1gb" });
    expect(results.length).toBe(1);
    expect(results[0]!.provider).toBe("digitalocean");
    expect(results[0]!.matchType).toBe("exact");
  });

  test("exact match: OrbStack spec", () => {
    const results = resolveSpec({ action: "resolve-spec", spec: "ubuntu:noble:arm64" });
    expect(results.length).toBe(1);
    expect(results[0]!.provider).toBe("orbstack");
    expect(results[0]!.matchType).toBe("exact");
  });

  test("GPU name resolution: a100", () => {
    const results = resolveSpec({ action: "resolve-spec", spec: "a100" });
    expect(results.length).toBeGreaterThan(0);
    for (const r of results) {
      expect(r.instance.gpuModel).toBe("A100");
      expect(r.matchType).toBe("gpu_name");
    }
  });

  test("GPU name resolution: h100", () => {
    const results = resolveSpec({ action: "resolve-spec", spec: "h100" });
    expect(results.length).toBeGreaterThan(0);
    for (const r of results) {
      expect(r.instance.gpuModel).toBe("H100");
    }
  });

  test("GPU name resolution: case-insensitive", () => {
    const lower = resolveSpec({ action: "resolve-spec", spec: "a100" });
    const upper = resolveSpec({ action: "resolve-spec", spec: "A100" });
    expect(lower.length).toBeGreaterThan(0);
    expect(upper.length).toBe(lower.length);
    const lowerSpecs = lower.map(r => `${r.provider}:${r.spec}`).sort();
    const upperSpecs = upper.map(r => `${r.provider}:${r.spec}`).sort();
    expect(upperSpecs).toEqual(lowerSpecs);
  });

  test("structured spec: gpu:h100:1", () => {
    const results = resolveSpec({ action: "resolve-spec", spec: "gpu:h100:1" });
    expect(results.length).toBeGreaterThan(0);
    for (const r of results) {
      expect(r.instance.gpuModel).toBe("H100");
      expect(r.instance.gpuCount).toBe(1);
      expect(r.matchType).toBe("parse_spec");
    }
  });

  test("structured spec: gpu:a100:8", () => {
    const results = resolveSpec({ action: "resolve-spec", spec: "gpu:a100:8" });
    expect(results.length).toBeGreaterThan(0);
    for (const r of results) {
      expect(r.instance.gpuModel).toBe("A100");
      expect(r.instance.gpuCount).toBe(8);
    }
  });

  test("provider filter narrows results", () => {
    const all = resolveSpec({ action: "resolve-spec", spec: "a100" });
    const awsOnly = resolveSpec({ action: "resolve-spec", spec: "a100", provider: "aws" });
    expect(awsOnly.length).toBeLessThan(all.length);
    for (const r of awsOnly) {
      expect(r.provider).toBe("aws");
    }
  });

  test("no match returns empty array", () => {
    const results = resolveSpec({ action: "resolve-spec", spec: "nonexistent-spec-xyz" });
    expect(results.length).toBe(0);
  });

  test("empty spec returns empty array", () => {
    const results = resolveSpec({ action: "resolve-spec" });
    expect(results.length).toBe(0);
  });

  test("fuzzy match: substring", () => {
    const results = resolveSpec({ action: "resolve-spec", spec: "h100x1" });
    expect(results.length).toBeGreaterThan(0);
    expect(results[0]!.matchType).toBe("fuzzy");
  });
});

// =============================================================================
// List Operations
// =============================================================================

describe("listInstances", () => {
  test("list all instances", () => {
    const instances = listInstances({ action: "list-instances" });
    expect(instances.length).toBe(ALL_INSTANCES.length);
  });

  test("filter by provider", () => {
    const aws = listInstances({ action: "list-instances", provider: "aws" });
    expect(aws.length).toBeGreaterThan(0);
    for (const inst of aws) {
      expect(inst.provider).toBe("aws");
    }
  });

  test("filter by GPU", () => {
    const gpu = listInstances({ action: "list-instances", filters: { hasGpu: true } });
    expect(gpu.length).toBeGreaterThan(0);
    for (const inst of gpu) {
      expect(inst.hasGpu).toBe(true);
    }
  });

  test("filter by arch", () => {
    const arm = listInstances({ action: "list-instances", filters: { arch: "arm64" } });
    expect(arm.length).toBeGreaterThan(0);
    for (const inst of arm) {
      expect(inst.arch).toBe("arm64");
    }
  });

  test("filter by min GPU count", () => {
    const multi = listInstances({ action: "list-instances", filters: { minGpuCount: 4 } });
    expect(multi.length).toBeGreaterThan(0);
    for (const inst of multi) {
      expect(inst.gpuCount).toBeGreaterThanOrEqual(4);
    }
  });

  test("combined filters", () => {
    const results = listInstances({
      action: "list-instances",
      provider: "lambda",
      filters: { hasGpu: true, minGpuCount: 1 },
    });
    expect(results.length).toBeGreaterThan(0);
    for (const inst of results) {
      expect(inst.provider).toBe("lambda");
      expect(inst.hasGpu).toBe(true);
      expect(inst.gpuCount).toBeGreaterThanOrEqual(1);
    }
  });
});

// =============================================================================
// Property-based: every known instance type resolves via exact match
// =============================================================================

describe("property: exact resolution for all known specs", () => {
  test("every instance spec in the catalog resolves to itself via exact match", () => {
    for (const inst of ALL_INSTANCES) {
      const results = resolveSpec({ action: "resolve-spec", spec: inst.spec });
      expect(results.length).toBeGreaterThanOrEqual(1);
      const exactMatch = results.find(r => r.spec === inst.spec && r.provider === inst.provider);
      expect(exactMatch).toBeTruthy();
      expect(exactMatch!.matchType).toBe("exact");
      expect(exactMatch!.confidence).toBe(100);
    }
  });
});

// =============================================================================
// Scoring (D6)
// =============================================================================

describe("scoring interface", () => {
  test("scoreSpecs returns scored results with all fields", () => {
    const resolved = resolveSpec({ action: "resolve-spec", spec: "a100" });
    expect(resolved.length).toBeGreaterThan(0);
    const scored = scoreSpecs(resolved);
    expect(scored.length).toBe(resolved.length);
    for (const s of scored) {
      expect(typeof s.score).toBe("number");
      expect(typeof s.priceScore).toBe("number");
      expect(typeof s.matchScore).toBe("number");
      expect(typeof s.capabilityScore).toBe("number");
      expect(typeof s.diversityContribution).toBe("number");
      expect(Array.isArray(s.constraintViolations)).toBe(true);
      expect(typeof s.reason).toBe("string");
    }
  });

  test("scoreSpecs respects maxHourlyBudget constraint", () => {
    const resolved = resolveSpec({ action: "resolve-spec", spec: "a100" });
    const scored = scoreSpecs(resolved, { maxHourlyBudget: 2.00 });
    for (const s of scored) {
      if (s.resolved.instance.onDemandHourly !== null && s.resolved.instance.onDemandHourly > 2.00) {
        expect(s.priceScore).toBe(0);
      }
    }
  });

  test("constraint violations are populated", () => {
    const resolved = resolveSpec({ action: "resolve-spec", spec: "t3.micro" });
    const scored = scoreSpecs(resolved, { requireGpu: true });
    for (const s of scored) {
      if (!s.resolved.instance.hasGpu) {
        expect(s.constraintViolations.length).toBeGreaterThan(0);
        expect(s.constraintViolations).toContain("requires GPU");
      }
    }
  });
});
