// orbital/modules/catalog/resolve.ts - Spec resolution logic

import type { InstanceEntry, ResolvedSpec, CatalogRequest } from "./types";
import { ALL_INSTANCES } from "./data/instances";
import { ALL_GPUS, GPU_ALIASES } from "./data/gpus";

// =============================================================================
// Resolution Strategy: exact → GPU name → parseSpec → fuzzy
// =============================================================================

export function resolveSpec(request: CatalogRequest): ResolvedSpec[] {
  const spec = request.spec;
  if (!spec) return [];

  const candidates = filterInstances(ALL_INSTANCES, request);

  // 1. Exact match: spec matches a provider-native instance type
  const exact = candidates.filter(i => i.spec === spec);
  if (exact.length > 0) {
    return exact.map(i => ({
      provider: i.provider,
      spec: i.spec,
      instance: i,
      matchType: "exact" as const,
      confidence: 100,
    }));
  }

  // 2. GPU name match: spec is a GPU model (e.g. "a100", "h100")
  const gpuResults = resolveByGpuName(spec, candidates);
  if (gpuResults.length > 0) return gpuResults;

  // 3. parseSpec format: "distro:version:arch" or "gpu:model:count"
  const parseResults = resolveByParseSpec(spec, candidates);
  if (parseResults.length > 0) return parseResults;

  // 4. Fuzzy: substring match on spec
  const fuzzy = candidates.filter(i =>
    i.spec.toLowerCase().includes(spec.toLowerCase()) ||
    spec.toLowerCase().includes(i.spec.toLowerCase())
  );
  if (fuzzy.length > 0) {
    return fuzzy.map(i => ({
      provider: i.provider,
      spec: i.spec,
      instance: i,
      matchType: "fuzzy" as const,
      confidence: 40,
    }));
  }

  return [];
}

// =============================================================================
// GPU Name Resolution
// =============================================================================

function resolveByGpuName(spec: string, candidates: InstanceEntry[]): ResolvedSpec[] {
  // Normalize underscores to hyphens so "a100_sxm" matches "a100-sxm"
  const lower = spec.toLowerCase().trim().replace(/_/g, "-");

  // Check aliases
  const canonical = GPU_ALIASES[lower];
  if (!canonical) {
    // Try as raw model name
    const gpu = ALL_GPUS.find(g => g.model.toLowerCase() === lower);
    if (!gpu) return [];
    return matchByGpu(gpu.model, candidates);
  }
  return matchByGpu(canonical, candidates);
}

function matchByGpu(model: string, candidates: InstanceEntry[]): ResolvedSpec[] {
  const matches = candidates.filter(i => i.gpuModel === model && i.gpuCount > 0);
  return matches.map(i => ({
    provider: i.provider,
    spec: i.spec,
    instance: i,
    matchType: "gpu_name" as const,
    confidence: 80,
  }));
}

// =============================================================================
// Parse Spec Resolution (structured spec strings)
// =============================================================================

function resolveByParseSpec(spec: string, candidates: InstanceEntry[]): ResolvedSpec[] {
  const parts = spec.split(":");

  // "gpu:model:count" format
  if (parts[0] === "gpu" && parts.length >= 2) {
    const model = parts[1]!;
    const countStr = parts[2];
    const count = countStr ? parseInt(countStr, 10) : undefined;

    // Invalid count (e.g. "gpu:a100:abc") → reject immediately
    if (count !== undefined && isNaN(count)) {
      return [];
    }

    const canonical = GPU_ALIASES[model.toLowerCase().replace(/_/g, "-")] ?? model.toUpperCase();
    let matches = candidates.filter(i => i.gpuModel === canonical);
    if (count !== undefined) {
      matches = matches.filter(i => i.gpuCount === count);
    }

    return matches.map(i => ({
      provider: i.provider,
      spec: i.spec,
      instance: i,
      matchType: "parse_spec" as const,
      confidence: 90,
    }));
  }

  // "distro:version:arch" format (OrbStack-style)
  if (parts.length >= 2 && parts[0] !== "gpu") {
    const distro = parts[0]!;
    const version = parts[1];
    const arch = parts[2];

    const specStr = [distro, version, arch].filter(Boolean).join(":");
    const matches = candidates.filter(i => i.spec === specStr);
    if (matches.length > 0) {
      return matches.map(i => ({
        provider: i.provider,
        spec: i.spec,
        instance: i,
        matchType: "parse_spec" as const,
        confidence: 95,
      }));
    }
  }

  return [];
}

// =============================================================================
// Filtering
// =============================================================================

function filterInstances(instances: InstanceEntry[], request: CatalogRequest): InstanceEntry[] {
  let result = instances;
  const f = request.filters;

  if (request.provider) {
    result = result.filter(i => i.provider === request.provider);
  }
  if (request.region) {
    result = result.filter(i => i.regions.includes(request.region!));
  }
  if (f) {
    if (f.minGpuCount !== undefined) result = result.filter(i => i.gpuCount >= f.minGpuCount!);
    if (f.minMemoryGib !== undefined) result = result.filter(i => i.memoryGib >= f.minMemoryGib!);
    if (f.minVcpus !== undefined) result = result.filter(i => i.vcpus >= f.minVcpus!);
    if (f.arch) result = result.filter(i => i.arch === f.arch);
    if (f.hasGpu !== undefined) result = result.filter(i => i.hasGpu === f.hasGpu);
    if (f.hasSpot !== undefined) result = result.filter(i => i.hasSpot === f.hasSpot);
    if (f.providers?.length) result = result.filter(i => f.providers!.includes(i.provider));
  }

  return result;
}

// =============================================================================
// List Operations
// =============================================================================

export function listInstances(request: CatalogRequest): InstanceEntry[] {
  return filterInstances(ALL_INSTANCES, request);
}
