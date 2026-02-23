// provider/compute/digitalocean.ts - DigitalOcean Droplet Provider (#PROV-054C)
//
// Production implementation of the Provider interface for DigitalOcean.
// Uses the DO v2 REST API via fetch() directly — no SDK dependency.
//
// WL-054C deliverable: flat REST API, 4-status model, cloud-init bootstrap,
// snapshot support, tag-based filtering.

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
  CreateSnapshotOptions,
  SnapshotRequest,
  SnapshotStatus,
  DigitalOceanInstance,
  DigitalOceanSnapshot,
} from "../types";
import { PROVIDER_CAPABILITIES, parseSpec } from "../types";
import {
  ProviderOperationError,
  ConcreteProviderError,
  withProviderErrorMapping,
} from "../errors";
import { assembleCloudInit } from "../bootstrap/cloud-init";
import { formatResourceName } from "../../material/control-id";
import type { ProviderLifecycleHooks, TaskReceipt, HeartbeatExpectations } from "../extensions";

// =============================================================================
// DO Status Mapping
// =============================================================================

/**
 * Map DigitalOcean droplet status to ProviderInstanceStatus.
 *
 * DO has a flat 4-status model:
 *   new     → pending    (provisioning in progress)
 *   active  → running    (droplet is running, public IP assigned)
 *   off     → stopped    (powered off, still billed for storage)
 *   archive → terminated (destroyed, may linger briefly in list)
 */
export function mapDOStatus(status: string): ProviderInstanceStatus {
  switch (status) {
    case "new":     return "pending";
    case "active":  return "running";
    case "off":     return "stopped";
    case "archive": return "terminated";
    default:        return "error";
  }
}

// =============================================================================
// DO Error Handling
// =============================================================================

const DO_API_BASE = "https://api.digitalocean.com";

/**
 * Map DO HTTP error responses to provider error taxonomy.
 *
 * DO returns JSON bodies with `id` (error code string) and `message` fields
 * on non-2xx responses.
 */
export function mapDOError(
  httpStatus: number,
  body: { id?: string; message?: string } | null
): ConcreteProviderError {
  const message = body?.message ?? `HTTP ${httpStatus}`;

  switch (httpStatus) {
    case 401:
      return new ConcreteProviderError("digitalocean", "AUTH_ERROR", message);
    case 403:
      return new ConcreteProviderError("digitalocean", "AUTH_ERROR", message);
    case 404:
      return new ConcreteProviderError("digitalocean", "NOT_FOUND", message);
    case 422:
      return new ConcreteProviderError("digitalocean", "INVALID_SPEC", message);
    case 429:
      return new ConcreteProviderError("digitalocean", "RATE_LIMIT_ERROR", message, {
        retryable: true,
        retry_after_ms: 10_000,
      });
    default:
      if (httpStatus >= 500) {
        return new ConcreteProviderError("digitalocean", "PROVIDER_INTERNAL", message, {
          retryable: true,
          retry_after_ms: 5_000,
        });
      }
      return new ConcreteProviderError("digitalocean", "PROVIDER_INTERNAL", message);
  }
}

/**
 * Generic DO API request helper.
 *
 * Attaches Authorization header, parses JSON, throws mapped errors on non-2xx.
 * Returns parsed response body. DELETE returns null for 204 No Content.
 */
export async function doRequest<T>(
  token: string,
  method: string,
  path: string,
  body?: unknown,
  fetchImpl: typeof fetch = globalThis.fetch,
): Promise<T> {
  const url = `${DO_API_BASE}${path}`;
  const headers: Record<string, string> = {
    "Authorization": `Bearer ${token}`,
    "Content-Type": "application/json",
  };

  const response = await fetchImpl(url, {
    method,
    headers,
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  });

  // 204 No Content (DELETE success)
  if (response.status === 204) {
    return null as T;
  }

  let responseBody: any;
  try {
    responseBody = await response.json();
  } catch {
    responseBody = null;
  }

  if (!response.ok) {
    throw mapDOError(response.status, responseBody);
  }

  return responseBody as T;
}

// =============================================================================
// DO Provider Config
// =============================================================================

export interface DigitalOceanProviderConfig {
  token: string;
  defaultRegion: string;
  defaultSizeSlug?: string;
  defaultSshKeyIds?: number[];
  defaultVpcUuid?: string;
  /** For tests only: inject a custom fetch implementation. */
  _fetchImpl?: typeof fetch;
}

// =============================================================================
// Size Mapping
// =============================================================================

/**
 * Default droplet size slug. DO is x64-only — no ARM droplets.
 * instanceType from SpawnOptions overrides this.
 */
const DEFAULT_SIZE_SLUG = "s-1vcpu-1gb";

/**
 * Ubuntu image slug mapping.
 * DO uses slug-format image identifiers.
 */
const UBUNTU_IMAGE_SLUGS: Record<string, string> = {
  noble: "ubuntu-24-04-x64",
  jammy: "ubuntu-22-04-x64",
  focal: "ubuntu-20-04-x64",
};

// =============================================================================
// DigitalOcean Provider
// =============================================================================

export class DigitalOceanProvider
  implements Provider<DigitalOceanInstance, DigitalOceanSnapshot>
{
  readonly name = "digitalocean" as const;
  readonly capabilities: ProviderCapabilities = PROVIDER_CAPABILITIES.digitalocean;

  private token: string;
  private config: DigitalOceanProviderConfig;
  private fetchImpl: typeof fetch;

  constructor(config: DigitalOceanProviderConfig) {
    this.token = config.token;
    this.config = config;
    this.fetchImpl = config._fetchImpl ?? globalThis.fetch;
  }

  // ─── Private: API helper ──────────────────────────────────────────────────

  private request<T>(method: string, path: string, body?: unknown): Promise<T> {
    return doRequest<T>(this.token, method, path, body, this.fetchImpl);
  }

  // ─── Core: spawn ──────────────────────────────────────────────────────────

  async spawn(options: SpawnOptions): Promise<DigitalOceanInstance> {
    const region = options.region ?? this.config.defaultRegion;
    const { version, arch } = parseSpec(options.spec);

    // Resolve image: snapshot ID or Ubuntu slug
    const image: string | number = options.snapshotId
      ? (isNaN(Number(options.snapshotId)) ? options.snapshotId : Number(options.snapshotId))
      : (UBUNTU_IMAGE_SLUGS[version] ?? "ubuntu-24-04-x64");

    // Resolve size (DO is x64-only — no arch-based size selection needed)
    const size = options.instanceType
      ?? this.config.defaultSizeSlug
      ?? DEFAULT_SIZE_SLUG;

    // Generate cloud-init user-data
    const bootstrap = this.generateBootstrap(options.bootstrap);

    // Build tags — DO tags are flat strings, encode structured data as tag values
    const mPart = options.manifestId === null ? "none" : String(options.manifestId);
    const tags = [
      "skyrepl",
      `skyrepl-spec:${options.spec}`,
      `skyrepl-id:${options.instanceId}`,
      `skyrepl-key:spawn:${options.controlId}-${mPart}-${options.instanceId}`,
    ];

    // Build droplet name from naming convention
    const name = formatResourceName(options.controlId, options.manifestId, options.instanceId);

    const createBody: Record<string, unknown> = {
      name,
      region,
      size,
      image,
      ssh_keys: this.config.defaultSshKeyIds ?? [],
      user_data: bootstrap.content, // DO accepts plain UTF-8, no base64
      tags,
      ipv6: true,
    };

    if (this.config.defaultVpcUuid) {
      createBody.vpc_uuid = this.config.defaultVpcUuid;
    }
    if (options.networkConfig?.vpcId) {
      createBody.vpc_uuid = options.networkConfig.vpcId;
    }

    return withProviderErrorMapping("digitalocean", async () => {
      const result = await this.request<{ droplet: any }>(
        "POST",
        "/v2/droplets",
        createBody,
      );

      if (!result?.droplet) {
        throw new ConcreteProviderError(
          "digitalocean",
          "PROVIDER_INTERNAL",
          "Create droplet returned no droplet object",
        );
      }

      return this.mapToDropletInstance(result.droplet, options.spec);
    });
  }

  // ─── Core: terminate ──────────────────────────────────────────────────────

  async terminate(providerId: string, _region?: string): Promise<void> {
    try {
      await this.request<null>("DELETE", `/v2/droplets/${providerId}`);
    } catch (err) {
      // Idempotent: swallow NOT_FOUND so terminate is safe to call twice
      if (err instanceof ProviderOperationError && err.code === "NOT_FOUND") {
        return;
      }
      throw err;
    }
  }

  // ─── Core: list ───────────────────────────────────────────────────────────

  async list(filter?: ListFilter): Promise<DigitalOceanInstance[]> {
    return withProviderErrorMapping("digitalocean", async () => {
      const droplets: any[] = [];
      let page = 1;
      const perPage = 100;

      // Paginate through all skyrepl-tagged droplets
      while (true) {
        const result = await this.request<{ droplets: any[]; meta: { total: number }; links?: any }>(
          "GET",
          `/v2/droplets?tag_name=skyrepl&per_page=${perPage}&page=${page}`,
        );

        droplets.push(...(result.droplets ?? []));

        // Check if there are more pages
        if (
          !result.links?.pages?.next ||
          (result.droplets?.length ?? 0) < perPage
        ) {
          break;
        }
        page++;
      }

      // Map and apply client-side filters
      let instances = droplets.map((d) => this.mapToDropletInstance(d));

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

  async get(providerId: string, _region?: string): Promise<DigitalOceanInstance | null> {
    try {
      const result = await this.request<{ droplet: any }>(
        "GET",
        `/v2/droplets/${providerId}`,
      );
      if (!result?.droplet) return null;
      return this.mapToDropletInstance(result.droplet);
    } catch (err) {
      if (err instanceof ProviderOperationError && err.code === "NOT_FOUND") {
        return null;
      }
      throw err;
    }
  }

  // ─── Core: generateBootstrap ──────────────────────────────────────────────

  generateBootstrap(config: BootstrapConfig): BootstrapScript {
    return assembleCloudInit(config);
  }

  // ─── Optional: createSnapshot ─────────────────────────────────────────────

  async createSnapshot(
    providerId: string,
    options: CreateSnapshotOptions,
    _region?: string,
  ): Promise<SnapshotRequest> {
    return withProviderErrorMapping("digitalocean", async () => {
      const result = await this.request<{ action: any }>(
        "POST",
        `/v2/droplets/${providerId}/actions`,
        { type: "snapshot", name: options.name },
      );

      const action = result?.action;
      if (!action) {
        throw new ConcreteProviderError(
          "digitalocean",
          "PROVIDER_INTERNAL",
          "Snapshot action returned no action object",
        );
      }

      return {
        requestId: `${providerId}:${action.id}`,
        status: { status: "creating" as const },
      };
    });
  }

  // ─── Optional: getSnapshotStatus ──────────────────────────────────────────

  async getSnapshotStatus(requestId: string, _region?: string): Promise<SnapshotStatus> {
    // requestId format: "dropletId:actionId"
    const [dropletId, actionId] = requestId.split(":");
    if (!dropletId || !actionId) {
      return { status: "failed", error: `Invalid requestId format: ${requestId}` };
    }

    return withProviderErrorMapping("digitalocean", async () => {
      const result = await this.request<{ action: any }>(
        "GET",
        `/v2/droplets/${dropletId}/actions/${actionId}`,
      );

      const action = result?.action;
      if (!action) {
        return { status: "failed" as const, error: "Action not found" };
      }

      switch (action.status) {
        case "in-progress":
          return { status: "creating" as const, progress: 50 };
        case "completed": {
          // Try to find the snapshot created by this action
          const snapshotId = action.resource_id
            ? String(action.resource_id)
            : undefined;
          return {
            status: "available" as const,
            providerSnapshotId: snapshotId,
          };
        }
        case "errored":
          return { status: "failed" as const, error: "Snapshot action failed" };
        default:
          return { status: "pending" as const };
      }
    });
  }

  // ─── Optional: deleteSnapshot ─────────────────────────────────────────────

  async deleteSnapshot(providerSnapshotId: string, _region?: string): Promise<void> {
    try {
      await this.request<null>("DELETE", `/v2/snapshots/${providerSnapshotId}`);
    } catch (err) {
      // Idempotent: swallow NOT_FOUND
      if (err instanceof ProviderOperationError && err.code === "NOT_FOUND") {
        return;
      }
      throw err;
    }
  }

  // ─── Optional: getSnapshotByName ──────────────────────────────────────────

  async getSnapshotByName(name: string, _region?: string): Promise<DigitalOceanSnapshot | null> {
    return withProviderErrorMapping("digitalocean", async () => {
      const result = await this.request<{ snapshots: any[] }>(
        "GET",
        `/v2/snapshots?resource_type=droplet&per_page=100`,
      );

      const snap = (result.snapshots ?? []).find((s: any) => s.name === name);
      if (!snap) return null;

      return this.mapToSnapshot(snap);
    });
  }

  // ─── Private: verifyAccount ───────────────────────────────────────────────

  async verifyAccount(): Promise<{ email: string; dropletLimit: number; status: string }> {
    const result = await this.request<{ account: any }>("GET", "/v2/account");
    return {
      email: result.account?.email ?? "",
      dropletLimit: result.account?.droplet_limit ?? 0,
      status: result.account?.status ?? "unknown",
    };
  }

  // ─── Private: mapToDropletInstance ─────────────────────────────────────────

  private mapToDropletInstance(droplet: any, originalSpec?: string): DigitalOceanInstance {
    const specTag = (droplet.tags ?? [])
      .find((t: string) => t.startsWith("skyrepl-spec:"))
      ?.slice("skyrepl-spec:".length);

    const v4Networks = droplet.networks?.v4 ?? [];
    const publicIp = v4Networks.find((n: any) => n.type === "public")?.ip_address ?? null;
    const privateIp = v4Networks.find((n: any) => n.type === "private")?.ip_address ?? null;

    return {
      id: String(droplet.id),
      status: mapDOStatus(droplet.status),
      spec: originalSpec ?? specTag ?? droplet.size_slug ?? "",
      region: droplet.region?.slug ?? "",
      ip: publicIp,
      privateIp,
      createdAt: droplet.created_at ? Date.parse(droplet.created_at) : Date.now(),
      isSpot: false, // DO has no spot
      metadata: {
        sizeSlug: droplet.size_slug,
        imageSlug: droplet.image?.slug,
        vpcUuid: droplet.vpc_uuid,
        tags: droplet.tags,
      },
      // DO-specific fields
      dropletId: droplet.id,
      sizeSlug: droplet.size_slug ?? "",
      imageSlug: droplet.image?.slug,
      vpcUuid: droplet.vpc_uuid,
      tags: droplet.tags ?? [],
    };
  }

  // ─── Private: mapToSnapshot ───────────────────────────────────────────────

  private mapToSnapshot(snap: any): DigitalOceanSnapshot {
    return {
      id: String(snap.id),
      name: snap.name ?? "",
      status: "available",
      createdAt: snap.created_at ? Date.parse(snap.created_at) : Date.now(),
      sizeBytes: (snap.size_gigabytes ?? 0) * 1024 * 1024 * 1024,
      // DO-specific fields
      snapshotId: snap.id,
      resourceType: "droplet",
      regions: snap.regions ?? [],
      minDiskSize: snap.min_disk_size ?? 0,
    };
  }
}

// =============================================================================
// Lifecycle Hooks Factory
// =============================================================================

export function createDigitalOceanHooks(provider: DigitalOceanProvider): ProviderLifecycleHooks {
  return {
    async onStartup() {
      await provider.verifyAccount();
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
              await provider.verifyAccount();
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

const token = process.env.DIGITALOCEAN_TOKEN ?? "";
const defaultRegion = process.env.DO_DEFAULT_REGION ?? "nyc3";
const defaultSizeSlug = process.env.DO_DEFAULT_SIZE ?? undefined;

// Parse SSH key IDs from comma-separated env var
const sshKeyIds = process.env.DO_SSH_KEY_IDS
  ? process.env.DO_SSH_KEY_IDS.split(",").map(Number).filter((n) => !isNaN(n))
  : undefined;

const defaultVpcUuid = process.env.DO_VPC_UUID ?? undefined;

export default new DigitalOceanProvider({
  token,
  defaultRegion,
  defaultSizeSlug,
  defaultSshKeyIds: sshKeyIds,
  defaultVpcUuid,
});
