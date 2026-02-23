// provider/compute/lambda.ts - Lambda Labs GPU Provider
//
// Production implementation of the Provider interface for Lambda Labs.
// Uses the Lambda Cloud REST API v1 via fetch() directly — no SDK dependency.
//
// Lambda is a minimal provider: GPU instances only, no snapshots, no spot.
// Bootstrap is delivered as a shell script (SSH-executed), not cloud-init.
// Capacity is pre-checked via /instance-types before every spawn attempt.

import type {
  Provider,
  ProviderInstance,
  ProviderSnapshot,
  ProviderCapabilities,
  ProviderInstanceStatus,
  SpawnOptions,
  BootstrapConfig,
  BootstrapScript,
  ListFilter,
  LambdaInstance,
} from "../types";
import { PROVIDER_CAPABILITIES } from "../types";
import {
  ProviderOperationError,
  ConcreteProviderError,
  withProviderErrorMapping,
} from "../errors";
import { assembleShellBootstrap } from "../bootstrap/shell";
import { formatResourceName } from "../../material/control-id";
import type { ProviderLifecycleHooks, TaskReceipt, HeartbeatExpectations } from "../extensions";

// =============================================================================
// Constants
// =============================================================================

const LAMBDA_API_BASE = "https://cloud.lambdalabs.com/api/v1";
const LAMBDA_PRICING_CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

// =============================================================================
// Raw API Types (internal)
// =============================================================================

interface LambdaRawInstance {
  id: string;
  name: string | null;
  ip: string | null;
  private_ip: string | null;
  status: string;
  ssh_key_names: string[];
  region: {
    name: string;
    description: string;
  };
  instance_type: {
    name: string;
    description: string;
    gpu_description: string;
    price_cents_per_hour: number;
    specs: {
      vcpus: number;
      memory_gib: number;
      storage_gib: number;
      gpus: number;
    };
  };
  hostname: string | null;
  jupyter_token: string | null;
  jupyter_url: string | null;
  tags: string[];
  file_system_names: string[];
}

interface LambdaInstanceTypeData {
  instance_type: {
    name: string;
    description: string;
    gpu_description: string;
    price_cents_per_hour: number;
    specs: {
      vcpus: number;
      memory_gib: number;
      storage_gib: number;
      gpus: number;
    };
  };
  regions_with_capacity_available: Array<{
    name: string;
    description: string;
  }>;
}

// =============================================================================
// Status Mapping
// =============================================================================

/**
 * Map Lambda Labs instance status to ProviderInstanceStatus.
 *
 *   active      → running     (instance is running and healthy)
 *   booting     → starting    (instance is initializing)
 *   unhealthy   → error       (instance has degraded health)
 *   terminated  → terminated  (instance is destroyed)
 *   terminating → terminating (intermediate death state, like EC2)
 *   preempted   → terminated  (Lambda system preemption — treat as terminated)
 *   (default)   → pending     (any unknown status is treated as in-progress)
 */
export function mapLambdaStatus(s: string): ProviderInstanceStatus {
  switch (s) {
    case "active":      return "running";
    case "booting":     return "starting";
    case "unhealthy":   return "error";
    case "terminated":  return "terminated";
    case "terminating": return "terminating";
    case "preempted":   return "terminated";
    default:            return "pending";
  }
}

// =============================================================================
// Error Handling
// =============================================================================

/**
 * Map Lambda HTTP error responses to provider error taxonomy.
 *
 * Lambda returns JSON bodies with `error.code` and `error.message` fields
 * on non-2xx responses — NOT wrapped in `data`.
 */
export function mapLambdaError(
  httpStatus: number,
  body: { error?: { code?: string; message?: string } } | null,
): ConcreteProviderError {
  const message = body?.error?.message ?? `HTTP ${httpStatus}`;
  const code = body?.error?.code ?? "";

  switch (httpStatus) {
    case 401:
      return new ConcreteProviderError("lambda", "AUTH_ERROR", message);
    case 403:
      return new ConcreteProviderError("lambda", "AUTH_ERROR", message);
    case 404:
      return new ConcreteProviderError("lambda", "NOT_FOUND", message);
    case 429:
      return new ConcreteProviderError("lambda", "RATE_LIMIT_ERROR", message, {
        retryable: true,
        retry_after_ms: 10_000,
      });
    case 400:
      if (code === "instance-operations/launch/insufficient-capacity") {
        return new ConcreteProviderError("lambda", "CAPACITY_ERROR", message, {
          retryable: true,
          retry_after_ms: 30_000,
        });
      }
      if (code === "global/quota-exceeded") {
        return new ConcreteProviderError("lambda", "QUOTA_EXCEEDED", message);
      }
      return new ConcreteProviderError("lambda", "PROVIDER_INTERNAL", message);
    default:
      if (httpStatus >= 500) {
        return new ConcreteProviderError("lambda", "PROVIDER_INTERNAL", message, {
          retryable: true,
          retry_after_ms: 5_000,
        });
      }
      return new ConcreteProviderError("lambda", "PROVIDER_INTERNAL", message);
  }
}

/**
 * Generic Lambda API request helper.
 *
 * Attaches Bearer auth, parses JSON, throws mapped errors on non-2xx.
 * Returns parsed response body.
 */
export async function lambdaRequest<T>(
  token: string,
  method: string,
  path: string,
  body?: unknown,
  fetchImpl: typeof fetch = globalThis.fetch,
): Promise<T> {
  const url = `${LAMBDA_API_BASE}${path}`;
  const headers: Record<string, string> = {
    "Authorization": `Bearer ${token}`,
    "Content-Type": "application/json",
    "Accept": "application/json",
  };

  const response = await fetchImpl(url, {
    method,
    headers,
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  });

  let responseBody: any;
  try {
    responseBody = await response.json();
  } catch {
    responseBody = null;
  }

  if (!response.ok) {
    throw mapLambdaError(response.status, responseBody);
  }

  return responseBody as T;
}

// =============================================================================
// Provider Config
// =============================================================================

export interface LambdaLabsProviderConfig {
  token: string;
  defaultRegion: string;
  /** Lambda uses SSH key NAME (not numeric ID) */
  sshKeyName: string;
  /** For tests only: inject a custom fetch implementation. */
  _fetchImpl?: typeof fetch;
}

// =============================================================================
// Pricing Cache
// =============================================================================

interface PricingCacheEntry {
  rates: Record<string, number>;
  availableRegions: Record<string, string[]>;
  expiresAt: number;
}

// Keyed by a single global key (null → "global") since pricing is account-wide.
const pricingCache = new Map<string, PricingCacheEntry>();
const PRICING_CACHE_KEY = "global";

// =============================================================================
// Lambda Labs Provider
// =============================================================================

export class LambdaLabsProvider implements Provider<LambdaInstance> {
  readonly name = "lambda" as const;
  readonly capabilities: ProviderCapabilities = PROVIDER_CAPABILITIES.lambda;

  private token: string;
  private config: LambdaLabsProviderConfig;
  private fetchImpl: typeof fetch;

  constructor(config: LambdaLabsProviderConfig) {
    this.token = config.token;
    this.config = config;
    this.fetchImpl = config._fetchImpl ?? globalThis.fetch;
  }

  // ─── Private: API helper ──────────────────────────────────────────────────

  private request<T>(method: string, path: string, body?: unknown): Promise<T> {
    return lambdaRequest<T>(this.token, method, path, body, this.fetchImpl);
  }

  // ─── Core: spawn ──────────────────────────────────────────────────────────

  async spawn(options: SpawnOptions): Promise<LambdaInstance> {
    // Guard: Lambda has no spot instances
    if (options.spot) {
      throw new ConcreteProviderError(
        "lambda",
        "UNSUPPORTED_OPERATION",
        "Lambda Labs does not support spot instances",
      );
    }

    const instanceType = options.instanceType;
    if (!instanceType) {
      throw new ConcreteProviderError(
        "lambda",
        "INVALID_SPEC",
        "Lambda Labs requires instanceType to be specified (e.g. 'gpu_1x_a100')",
      );
    }

    // Pre-check capacity and find an available region
    const region = await this.findAvailableRegion(instanceType, options.region);
    if (!region) {
      throw new ConcreteProviderError(
        "lambda",
        "CAPACITY_ERROR",
        `No capacity available for ${instanceType} in any region`,
        { retryable: true, retry_after_ms: 60_000 },
      );
    }

    // Generate shell-format bootstrap
    const bootstrap = this.generateBootstrap(options.bootstrap);

    // Hostname: must match ^[a-z0-9][0-9a-z-]{0,62}$
    const rawName = formatResourceName(options.controlId, options.manifestId, options.instanceId);
    // formatResourceName produces "repl-<cid>-<mid>-<iid>" which is valid per the pattern
    const hostname = rawName;

    return withProviderErrorMapping("lambda", async () => {
      const launchResult = await this.request<{ data: { instance_ids: string[] } }>(
        "POST",
        "/instance-operations/launch",
        {
          region_name: region,
          instance_type_name: instanceType,
          ssh_key_names: [this.config.sshKeyName],
          name: rawName,
          hostname,
          user_data: bootstrap.content,
        },
      );

      const instanceIds = launchResult?.data?.instance_ids;
      if (!instanceIds || instanceIds.length === 0) {
        throw new ConcreteProviderError(
          "lambda",
          "PROVIDER_INTERNAL",
          "Launch returned no instance IDs",
        );
      }

      const providerId = instanceIds[0]!;

      // Fetch the full instance object immediately after launch
      const instance = await this.get(providerId, region);
      if (!instance) {
        // Instance was just created — return a minimal projected record
        return {
          id: providerId,
          status: "starting" as const,
          spec: options.spec,
          region,
          ip: null,
          privateIp: null,
          createdAt: Date.now(),
          isSpot: false,
          instanceTypeName: instanceType,
          metadata: { instanceType },
        } satisfies LambdaInstance;
      }

      return instance;
    });
  }

  // ─── Core: terminate ──────────────────────────────────────────────────────

  async terminate(providerId: string, _region?: string): Promise<void> {
    try {
      await this.request<{ data: { terminated_instances: LambdaRawInstance[] } }>(
        "POST",
        "/instance-operations/terminate",
        { instance_ids: [providerId] },
      );
    } catch (err) {
      // Idempotent: swallow NOT_FOUND so terminate is safe to call twice
      if (err instanceof ProviderOperationError && err.code === "NOT_FOUND") {
        return;
      }
      throw err;
    }
  }

  // ─── Core: list ───────────────────────────────────────────────────────────

  async list(filter?: ListFilter): Promise<LambdaInstance[]> {
    return withProviderErrorMapping("lambda", async () => {
      // Lambda returns all instances at once — no pagination needed
      const result = await this.request<{ data: LambdaRawInstance[] }>("GET", "/instances");
      let instances = (result.data ?? []).map((raw) => this.projectInstance(raw));

      if (filter?.spec) {
        instances = instances.filter((i) => i.spec === filter.spec);
      }

      if (filter?.status) {
        instances = instances.filter((i) => filter.status!.includes(i.status));
      }

      if (filter?.region) {
        instances = instances.filter((i) => i.region === filter.region);
      }

      if (!filter?.includeTerminated) {
        instances = instances.filter((i) => i.status !== "terminated");
      }

      if (filter?.limit) {
        instances = instances.slice(0, filter.limit);
      }

      return instances;
    });
  }

  // ─── Core: get ────────────────────────────────────────────────────────────

  async get(providerId: string, _region?: string): Promise<LambdaInstance | null> {
    try {
      const result = await this.request<{ data: LambdaRawInstance }>(
        "GET",
        `/instances/${providerId}`,
      );
      if (!result?.data) return null;
      return this.projectInstance(result.data);
    } catch (err) {
      if (err instanceof ProviderOperationError && err.code === "NOT_FOUND") {
        return null;
      }
      throw err;
    }
  }

  // ─── Core: generateBootstrap ──────────────────────────────────────────────

  generateBootstrap(config: BootstrapConfig): BootstrapScript {
    return assembleShellBootstrap(config);
  }

  // ─── Public: listAvailableSpecs ───────────────────────────────────────────

  /**
   * Fetch available instance types with their hourly rates and available regions.
   *
   * Results are cached for 7 days (Lambda pricing is stable).
   */
  async listAvailableSpecs(): Promise<{ name: string; hourlyRate: number; availableRegions: string[] }[]> {
    const cached = pricingCache.get(PRICING_CACHE_KEY);
    if (cached && Date.now() < cached.expiresAt) {
      return Object.keys(cached.rates).map((name) => ({
        name,
        hourlyRate: cached.rates[name]!,
        availableRegions: cached.availableRegions[name] ?? [],
      }));
    }

    return withProviderErrorMapping("lambda", async () => {
      const result = await this.request<{ data: Record<string, LambdaInstanceTypeData> }>(
        "GET",
        "/instance-types",
      );

      const rates: Record<string, number> = {};
      const availableRegions: Record<string, string[]> = {};

      for (const [, entry] of Object.entries(result.data ?? {})) {
        const name = entry.instance_type.name;
        // Convert cents/hour to dollars/hour
        rates[name] = entry.instance_type.price_cents_per_hour / 100;
        availableRegions[name] = entry.regions_with_capacity_available.map((r) => r.name);
      }

      pricingCache.set(PRICING_CACHE_KEY, {
        rates,
        availableRegions,
        expiresAt: Date.now() + LAMBDA_PRICING_CACHE_TTL_MS,
      });

      return Object.keys(rates).map((name) => ({
        name,
        hourlyRate: rates[name]!,
        availableRegions: availableRegions[name] ?? [],
      }));
    });
  }

  /**
   * Get the hourly rate (in USD) for a specific instance type.
   * Returns 0 if the spec is not found.
   */
  async getHourlyRate(spec: string): Promise<number> {
    const specs = await this.listAvailableSpecs();
    return specs.find((s) => s.name === spec)?.hourlyRate ?? 0;
  }

  // ─── Private: projectInstance ─────────────────────────────────────────────

  private projectInstance(raw: LambdaRawInstance): LambdaInstance {
    const specs = raw.instance_type?.specs;
    const instanceTypeName = raw.instance_type?.name ?? "";

    return {
      id: raw.id,
      status: mapLambdaStatus(raw.status),
      // Lambda has no spec tag — use instance_type name as the spec identifier
      spec: instanceTypeName,
      region: raw.region?.name ?? "",
      ip: raw.ip ?? null,
      privateIp: raw.private_ip ?? null,
      createdAt: Date.now(), // Lambda API does not return a created_at timestamp
      isSpot: false, // Lambda has no spot instances
      // Lambda-specific enrichment from instance_type data
      instanceTypeName,
      gpuCount: specs?.gpus,
      gpuDescription: raw.instance_type?.gpu_description,
      vcpus: specs?.vcpus,
      memoryGib: specs?.memory_gib,
      storageGib: specs?.storage_gib,
      priceCentsPerHour: raw.instance_type?.price_cents_per_hour,
      hostname: raw.hostname ?? undefined,
      metadata: {
        name: raw.name,
        sshKeyNames: raw.ssh_key_names,
        jupyterToken: raw.jupyter_token,
        jupyterUrl: raw.jupyter_url,
        tags: raw.tags,
        fileSystemNames: raw.file_system_names,
        instanceTypeDescription: raw.instance_type?.description,
        regionDescription: raw.region?.description,
      },
    };
  }

  // ─── Private: findAvailableRegion ─────────────────────────────────────────

  /**
   * Perform a fresh /instance-types fetch to find a region with available
   * capacity for the given spec. Prefers defaultRegion if it has capacity.
   *
   * Returns null if no region has capacity for this spec.
   */
  async findAvailableRegion(spec: string, preferredRegion?: string): Promise<string | null> {
    // Always fetch fresh for capacity checks — do not use the 7-day pricing cache
    const result = await this.request<{ data: Record<string, LambdaInstanceTypeData> }>(
      "GET",
      "/instance-types",
    );

    // Also opportunistically refresh the pricing cache
    const rates: Record<string, number> = {};
    const availableRegions: Record<string, string[]> = {};

    for (const [, entry] of Object.entries(result.data ?? {})) {
      const name = entry.instance_type.name;
      rates[name] = entry.instance_type.price_cents_per_hour / 100;
      availableRegions[name] = entry.regions_with_capacity_available.map((r) => r.name);
    }

    pricingCache.set(PRICING_CACHE_KEY, {
      rates,
      availableRegions,
      expiresAt: Date.now() + LAMBDA_PRICING_CACHE_TTL_MS,
    });

    const regions = availableRegions[spec] ?? [];
    if (regions.length === 0) return null;

    // Prefer the explicitly requested region, then the config default
    const preferred = preferredRegion ?? this.config.defaultRegion;
    if (regions.includes(preferred)) return preferred;

    // Fall back to first available region
    return regions[0] ?? null;
  }
}

// =============================================================================
// Lifecycle Hooks Factory
// =============================================================================

export function createLambdaHooks(provider: LambdaLabsProvider): ProviderLifecycleHooks {
  return {
    async onStartup() {
      // Verify key by fetching /instances — lightweight auth check
      await (provider as any).request("GET", "/instances");
    },
    async onShutdown() {
      // No-op
    },
    async onHeartbeat(expectations: HeartbeatExpectations) {
      const receipts: TaskReceipt[] = [];

      for (const task of expectations.tasks) {
        switch (task.type) {
          case "health_check": {
            const start = Date.now();
            try {
              await (provider as any).request("GET", "/instances");
              receipts.push({
                type: "health_check",
                status: "completed",
                result: { latencyMs: Date.now() - start },
              });
            } catch (err) {
              receipts.push({
                type: "health_check",
                status: "failed",
                result: { error: err instanceof Error ? err.message : String(err) },
              });
            }
            break;
          }
          case "reconcile":
            receipts.push({
              type: "reconcile",
              status: "skipped",
              reason: "DB access not yet available in provider layer",
            });
            break;
          case "cache_refresh":
            // Refresh the pricing cache
            try {
              await provider.listAvailableSpecs();
              receipts.push({
                type: "cache_refresh",
                status: "completed",
                result: { note: "Lambda pricing cache refreshed" },
              });
            } catch (err) {
              receipts.push({
                type: "cache_refresh",
                status: "failed",
                result: { error: err instanceof Error ? err.message : String(err) },
              });
            }
            break;
          case "pool_maintenance":
            receipts.push({
              type: "pool_maintenance",
              status: "skipped",
              reason: "Warm pool not supported on Lambda Labs",
            });
            break;
          default:
            receipts.push({
              type: task.type,
              status: "skipped",
              reason: `Unknown task type: ${task.type}`,
            });
        }
      }

      return { receipts };
    },
  };
}

// =============================================================================
// Default Export
// =============================================================================

const token = process.env.LAMBDA_API_KEY ?? "";
const defaultRegion = process.env.LAMBDA_REGION ?? "us-east-1";
const sshKeyName = process.env.LAMBDA_SSH_KEY_NAME ?? "";

export default new LambdaLabsProvider({
  token,
  defaultRegion,
  sshKeyName,
});
