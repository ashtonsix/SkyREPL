// provider/orbital.ts - Orbital advisory client for lifecycle integration
//
// When co-located (service fabric): calls orbital's HTTP server on localhost.
// When remote: calls ORBITAL_URL.
// On 503/timeout/connection refused: returns null so callers fall through
// to provider-native passthrough.

const ORBITAL_URL = process.env.ORBITAL_URL ?? 'http://localhost:3002';

// Read lazily so test preloads (preload-quiet.ts) can set the env var before first use.
function getOrbitalTimeoutMs(): number {
  return process.env.ORBITAL_TIMEOUT_MS ? parseInt(process.env.ORBITAL_TIMEOUT_MS, 10) : 3000;
}

export interface OrbitalResolvedSpec {
  provider: string;
  spec: string;
  instance: {
    provider: string;
    spec: string;
    vcpus: number;
    memoryGib: number;
    gpuCount: number;
    gpuModel: string | null;
    onDemandHourly: number | null;
    spotHourly: number | null;
    regions: string[];
  };
  matchType: "exact" | "gpu_name" | "parse_spec" | "fuzzy";
  confidence: number;
}

export interface OrbitalScoredSpec {
  resolved: OrbitalResolvedSpec;
  score: number;
  priceScore: number;
  matchScore: number;
  capabilityScore: number;
  reason: string;
}

export interface OrbitalResolveResult {
  ok: boolean;
  results: OrbitalResolvedSpec[];
  scored?: OrbitalScoredSpec[];
  error?: string;
}

export interface OrbitalAlternativeResult {
  ok: boolean;
  action: string;
  alternatives: OrbitalResolvedSpec[];
  scored?: OrbitalScoredSpec[];
  next?: OrbitalResolvedSpec | null;
  reasoning?: string;
}

// =============================================================================
// Catalog-aware URL resolution
// =============================================================================

/** Overridable orbital URL — set by initControl when co-located. */
let _orbitalUrl: string = ORBITAL_URL;

/** Called by control plane init when orbital is co-located in the service fabric. */
export function setOrbitalUrl(url: string): void {
  _orbitalUrl = url;
}

// =============================================================================
// API calls
// =============================================================================

/**
 * Call orbital retry-alternative endpoint to discover alternative specs/regions
 * after a CAPACITY_ERROR, REGION_UNAVAILABLE, or QUOTA_EXCEEDED failure.
 * Returns null if orbital is unavailable (503, timeout, connection refused).
 */
export async function resolveAlternativeViaOrbital(
  originalSpec: string,
  failedProvider: string,
  failedSpec: string,
): Promise<OrbitalAlternativeResult | null> {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), getOrbitalTimeoutMs());

    const response = await fetch(`${_orbitalUrl}/v1/advisory/retry-alternative`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ original_spec: originalSpec, failed_provider: failedProvider, failed_spec: failedSpec }),
      signal: controller.signal,
    });

    clearTimeout(timeout);

    if (response.status === 503) return null;
    if (!response.ok) return null;

    return await response.json() as OrbitalAlternativeResult;
  } catch {
    // Connection refused, timeout, or network error — orbital is down
    return null;
  }
}

/**
 * Call orbital resolve-spec endpoint.
 * Returns null if orbital is unavailable (503, timeout, connection refused).
 * Callers should fall through to existing provider.spawn() with raw spec.
 */
export async function resolveSpecViaOrbital(
  spec: string,
  provider?: string,
  region?: string,
): Promise<OrbitalResolveResult | null> {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), getOrbitalTimeoutMs());

    const response = await fetch(`${_orbitalUrl}/v1/advisory/resolve-spec`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ spec, provider, region }),
      signal: controller.signal,
    });

    clearTimeout(timeout);

    if (response.status === 503) return null;
    if (!response.ok) return null;

    return await response.json() as OrbitalResolveResult;
  } catch {
    // Connection refused, timeout, or network error — orbital is down
    return null;
  }
}
