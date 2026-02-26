// orbital/modules/catalog/types.ts - Catalog type definitions

// =============================================================================
// Instance Catalog
// =============================================================================

export interface InstanceEntry {
  provider: string;
  spec: string;            // provider-native instance type (e.g. "p4d.24xlarge")
  vcpus: number;
  memoryGib: number;
  storageGib: number;
  gpuCount: number;
  gpuModel: string | null; // e.g. "A100", "H100"
  gpuMemoryGib: number;
  arch: "amd64" | "arm64";
  // Capability flags (C2 structural capability)
  hasGpu: boolean;
  hasSpot: boolean;
  hasLifecycle: boolean;   // supports full lifecycle (snapshots, volumes)
  hasNvlink: boolean;
  // Pricing (populated by live pricing heartbeat, null = unknown)
  onDemandHourly: number | null;
  spotHourly: number | null;
  // Region availability
  regions: string[];
}

// =============================================================================
// Region Catalog
// =============================================================================

export interface RegionEntry {
  provider: string;
  region: string;          // provider-native region code (e.g. "us-east-1")
  displayName: string;     // human-readable (e.g. "US East (Virginia)")
  country: string;         // ISO 3166-1 alpha-2 (e.g. "US")
  city: string;            // closest city
  continent: string;       // "NA", "EU", "AP", "SA", "AF", "OC"
  latitude: number;
  longitude: number;
}

// =============================================================================
// GPU Family Catalog
// =============================================================================

export interface GpuFamily {
  model: string;           // e.g. "A100"
  family: string;          // e.g. "Ampere"
  generation: number;      // e.g. 8 (compute capability 8.0)
  memoryGib: number;       // e.g. 80
  fp16Tflops: number;      // half-precision throughput
  alternatives: string[];  // equivalent or similar models
}

// =============================================================================
// Module I/O
// =============================================================================

export interface CatalogRequest {
  action: "resolve-spec" | "list-instances" | "list-regions" | "list-gpus" | "get-pricing";
  spec?: string;
  provider?: string;
  region?: string;
  gpuModel?: string;
  filters?: {
    minGpuCount?: number;
    minMemoryGib?: number;
    minVcpus?: number;
    arch?: "amd64" | "arm64";
    hasGpu?: boolean;
    hasSpot?: boolean;
    providers?: string[];
  };
}

export interface ResolvedSpec {
  provider: string;
  spec: string;
  instance: InstanceEntry;
  matchType: "exact" | "gpu_name" | "parse_spec" | "fuzzy";
  confidence: number;       // 0-100
}

export interface CatalogResponse {
  ok: boolean;
  action: string;
  results?: ResolvedSpec[];
  instances?: InstanceEntry[];
  regions?: RegionEntry[];
  gpus?: GpuFamily[];
  pricing?: { provider: string; spec: string; onDemandHourly: number | null; spotHourly: number | null }[];
  error?: string;
}

// =============================================================================
// Scoring (D6 foundations)
// =============================================================================

export interface ScoredSpec {
  resolved: ResolvedSpec;
  score: number;            // 0-100 composite score
  priceScore: number;       // 0-100 lower cost = higher score
  matchScore: number;       // 0-100 better match = higher score
  capabilityScore: number;  // 0-100 more capabilities = higher score
  diversityContribution: number;    // 0-100 contribution to result diversity (stub: 0)
  constraintViolations: string[];   // list of violated constraints (stub: [])
  reason: string;
}

/**
 * User constraints for spec resolution and scoring.
 *
 * Request-scoped: passed per API call, not stored per-tenant. Per-tenant
 * persistence is deferred to #ADVISORY-05 (user equivalence history).
 *
 * Flat fields are the current working interface. The tiered structure
 * (hard/soft/delegations) from the equivalence-classes design note will
 * replace these as scoring matures. See docs/equivalence-classes.txt.
 */
export interface UserConstraints {
  // ─── Flat interface (current) ────────────────────────────────────
  preferredProviders?: string[];
  preferredRegions?: string[];
  maxHourlyBudget?: number;
  requireGpu?: boolean;
  requireSpot?: boolean;
  requireLifecycle?: boolean;
  minGpuMemoryGib?: number;
  minGpuCount?: number;
  arch?: "amd64" | "arm64";

  // ─── Tiered interface (forward-looking, not yet consumed) ────────
  hard?: {
    providers?: string[];
    regions?: string[];
    countries?: string[];
    minGpuMemoryGib?: number;
  };
  soft?: {
    preferSpot?: boolean;
    maxPricePerHour?: number;
    preferProviders?: string[];
  };
  delegations?: {
    regionEquivalent?: boolean;
    spotFallbackToOnDemand?: boolean;
  };
}
