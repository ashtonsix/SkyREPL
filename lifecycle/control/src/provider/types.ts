// provider/types.ts - Provider Interface & Type Definitions
// Fully defined types (no stubs needed for type-only files)

import type { ProviderName } from "@skyrepl/contracts";

// Re-export ProviderName for convenience
export type { ProviderName };

// =============================================================================
// Provider Interface
// =============================================================================

export interface Provider<
  TInstance extends ProviderInstance = ProviderInstance,
  TSnapshot extends ProviderSnapshot = ProviderSnapshot
> {
  readonly name: ProviderName;
  readonly capabilities: ProviderCapabilities;

  // Core methods (required)
  spawn(options: SpawnOptions): Promise<TInstance>;
  terminate(providerId: string, region?: string): Promise<void>;
  list(filter?: ListFilter): Promise<TInstance[]>;
  get(providerId: string, region?: string): Promise<TInstance | null>;
  generateBootstrap(config: BootstrapConfig): BootstrapScript;

  // Optional methods (gated by capabilities)
  createSnapshot?(providerId: string, options: CreateSnapshotOptions, region?: string): Promise<SnapshotRequest>;
  getSnapshotStatus?(requestId: string, region?: string): Promise<SnapshotStatus>;
  deleteSnapshot?(providerSnapshotId: string, region?: string): Promise<void>;
  getSnapshotByName?(name: string, region?: string): Promise<TSnapshot | null>;
  requestSpotInstance?(options: SpotOptions): Promise<SpotRequest>;
  cancelSpotRequest?(requestId: string): Promise<void>;
  getSpotPrices?(spec: string, regions?: string[]): Promise<SpotPriceInfo[]>;
}

// =============================================================================
// Capability Flags
// =============================================================================

export interface ProviderCapabilities {
  snapshots: boolean;
  spot: boolean;
  gpu: boolean;
  multiRegion: boolean;
  persistentVolumes: boolean;
  warmVolumes: boolean;
  hibernation: boolean;
  costExplorer: boolean;
  tailscaleNative: boolean;
  idempotentSpawn: boolean;
  customNetworking: boolean;
}

export const PROVIDER_CAPABILITIES: Record<ProviderName, ProviderCapabilities> = {
  aws: {
    snapshots: true,
    spot: true,
    gpu: true,
    multiRegion: true,
    persistentVolumes: true,
    warmVolumes: true,
    hibernation: true,
    costExplorer: true,
    tailscaleNative: false,
    idempotentSpawn: true,
    customNetworking: true,
  },
  lambda: {
    snapshots: false,
    spot: false,
    gpu: true,
    multiRegion: true,
    persistentVolumes: false,
    warmVolumes: false,
    hibernation: false,
    costExplorer: false,
    tailscaleNative: false,
    idempotentSpawn: false,
    customNetworking: false,
  },
  runpod: {
    snapshots: true,
    spot: true,
    gpu: true,
    multiRegion: true,
    persistentVolumes: true,
    warmVolumes: false,
    hibernation: false,
    costExplorer: false,
    tailscaleNative: false,
    idempotentSpawn: false,
    customNetworking: false,
  },
  orbstack: {
    snapshots: true,
    spot: false,
    gpu: false,
    multiRegion: false,
    persistentVolumes: false,
    warmVolumes: false,
    hibernation: false,
    costExplorer: false,
    tailscaleNative: true,
    idempotentSpawn: true,
    customNetworking: false,
  },
  digitalocean: {
    snapshots: true,
    spot: false,
    gpu: true,
    multiRegion: true,
    persistentVolumes: true,
    warmVolumes: false,
    hibernation: false,
    costExplorer: false,
    tailscaleNative: false,
    idempotentSpawn: false,
    customNetworking: true,
  },
  gcp: {
    snapshots: true,
    spot: true,
    gpu: true,
    multiRegion: true,
    persistentVolumes: true,
    warmVolumes: false,
    hibernation: false,
    costExplorer: true,
    tailscaleNative: false,
    idempotentSpawn: true,
    customNetworking: true,
  },
};

// =============================================================================
// Spawn Options & Result
// =============================================================================

export interface SpawnOptions {
  spec: string;
  region?: string;
  instanceType?: string;
  spot?: boolean;
  maxSpotPrice?: number;
  initChecksum?: string;
  tags?: Record<string, string>;
  controlId: string;
  manifestId: number | null;
  instanceId: number;
  bootstrap: BootstrapConfig;
  snapshotId?: string;
  volumeId?: string;
  networkConfig?: NetworkConfig;
}

export interface ProviderInstance {
  id: string;
  status: ProviderInstanceStatus;
  spec: string;
  region?: string;
  ip?: string | null;
  privateIp?: string | null;
  createdAt: number;
  isSpot: boolean;
  spotRequestId?: string;
  metadata?: Record<string, unknown>;
}

export type ProviderInstanceStatus =
  | "pending"
  | "starting"
  | "running"
  | "stopping"
  | "stopped"
  | "terminating"
  | "terminated"
  | "error";

// =============================================================================
// List Filter
// =============================================================================

export interface ListFilter {
  status?: ProviderInstanceStatus[];
  spec?: string;
  region?: string;
  tags?: Record<string, string>;
  limit?: number;
  includeTerminated?: boolean;
}

// =============================================================================
// Bootstrap Configuration
// =============================================================================

export interface BootstrapConfig {
  agentUrl: string;
  controlPlaneUrl: string;
  registrationToken: string;
  initScript?: string;
  initChecksum?: string;
  features?: BootstrapFeature[];
  environment?: Record<string, string>;
}

export interface BootstrapFeature {
  name: "tailscale" | "nvidia-driver" | "cuda" | "docker";
  version?: string;
  config?: Record<string, unknown>;
}

export interface BootstrapScript {
  content: string;
  format: "cloud-init" | "shell" | "ignition";
  checksum: string;
}

// =============================================================================
// Network Configuration
// =============================================================================

export interface NetworkConfig {
  subnetId?: string;
  securityGroupIds?: string[];
  vpcId?: string;
}

// =============================================================================
// Snapshot Types
// =============================================================================

export interface CreateSnapshotOptions {
  name: string;
  description?: string;
  tags?: Record<string, string>;
  includeMemory?: boolean;
}

export interface SnapshotRequest {
  requestId: string;
  status: SnapshotStatus;
  estimatedCompletionAt?: number;
}

export interface SnapshotStatus {
  status: "pending" | "creating" | "available" | "failed" | "deleting";
  progress?: number;
  providerSnapshotId?: string;
  error?: string;
  sizeBytes?: number;
}

export interface ProviderSnapshot {
  id: string;
  name: string;
  status: "available" | "pending" | "failed";
  createdAt: number;
  sizeBytes?: number;
  spec?: string;
}

// =============================================================================
// Spot Types
// =============================================================================

export interface SpotOptions extends SpawnOptions {
  maxSpotPrice: number;
  interruptionBehavior?: "terminate" | "stop" | "hibernate";
  requestType?: "one-time" | "persistent";
}

export interface SpotRequest {
  requestId: string;
  status: "pending" | "active" | "fulfilled" | "cancelled" | "failed";
  instanceId?: string;
  spotPrice?: number;
}

export interface SpotPriceInfo {
  spec: string;
  region: string;
  availabilityZone?: string;
  price: number;
  timestamp: number;
  savingsPercent?: number;
}

// =============================================================================
// Provider-Specific Types
// =============================================================================

export interface OrbStackInstance extends ProviderInstance {
  vmName: string;
  arch: "amd64" | "arm64";
  distro: string;
  distroVersion: string;
  cpuCores: number;
  memoryMb: number;
  diskGb: number;
  hostMountPath?: string;
  useRosetta?: boolean;
}

export interface DigitalOceanInstance extends ProviderInstance {
  dropletId: number;
  sizeSlug: string;
  imageSlug?: string;
  vpcUuid?: string;
  tags: string[];
}

export interface DigitalOceanSnapshot extends ProviderSnapshot {
  snapshotId: number;
  resourceType: "droplet";
  regions: string[];
  minDiskSize: number;
}

// =============================================================================
// Feature Provider Interface
// =============================================================================

// ─── Design Note ─────────────────────────────────────────────────────────────
// FeatureProvider is the interface for "attach to running instance" features:
// Tailscale, WandB, VS Code Server, etc. Normative per spec §8.4.
//
// First planned implementation: #PROV-05 (Tailscale feature provider),
// which depends on #WF-02 (done).
//
// The feature provider registry was pruned in 031B (D6) — it was an empty
// registry with no implementations. Rebuild it alongside #PROV-05.
// ─────────────────────────────────────────────────────────────────────────────
export interface FeatureProvider<TConfig = unknown, TState = unknown> {
  name: string;
  capabilities: FeatureCapabilities;
  attach(instanceId: string, config: TConfig): Promise<TState>;
  detach(instanceId: string): Promise<void>;
  status(instanceId: string): Promise<TState | null>;
  reconcile?(context: FeatureReconcileContext): Promise<ReconcileResult>;
  onInstanceHeartbeat?(instanceId: string): Promise<void>;
}

export interface FeatureCapabilities {
  requiresBootstrap: boolean;
  supportsHotAttach: boolean;
  supportsDetach: boolean;
  crossProvider: boolean;
}

export interface FeatureReconcileContext {
  expectedAttachments: Array<{
    instanceId: string;
    config: unknown;
  }>;
  actualState: Map<string, unknown>;
}

export interface ReconcileResult {
  orphans: string[];
  missing: string[];
}

// =============================================================================
// Provider Utilities
// =============================================================================

/**
 * Parse a spec string ("distro:version:arch") and return the components.
 *
 * Accepts:
 *   "ubuntu:noble:arm64"  → { distro: "ubuntu", version: "noble", arch: "arm64" }
 *   "ubuntu:noble:amd64"  → { distro: "ubuntu", version: "noble", arch: "amd64" }
 *   "ubuntu:jammy:arm64"  → ...
 */
export function parseSpec(spec: string): { distro: string; version: string; arch: string } {
  const parts = spec.split(":");
  return {
    distro: parts[0] || "ubuntu",
    version: parts[1] || "noble",
    arch: parts[2] || "arm64",
  };
}

// =============================================================================
// Provider Category Interfaces
// =============================================================================

// ─── Design Note ─────────────────────────────────────────────────────────────
// Type-only definitions for future storage and tunnel provider categories.
// No runtime registry exists — category registry was pruned in 031B (D6)
// because the taxonomy design is unresolved (#BL-22).
//
// StorageProvider: future S3/MinIO integration for blob storage.
// TunnelProvider: future Tailscale Funnel / Cloudflared / ngrok (#PROV-05).
//
// When implementing, create a new registry module rather than re-adding to
// registry.ts — keep compute provider registration separate.
// ─────────────────────────────────────────────────────────────────────────────

export type ProviderCategory = 'compute' | 'storage' | 'tunnel';

export interface StorageProviderCapabilities {
  maxObjectSizeBytes: number;
  supportsRangeRequests: boolean;
  supportsMultipartUpload: boolean;
  supportsCAS: boolean; // Content-Addressable Storage (blob dedup)
  supportsExpiration: boolean;
}

export interface StorageProvider {
  readonly name: string;
  readonly category: 'storage';
  readonly capabilities: StorageProviderCapabilities;

  // Core CRUD
  putObject(key: string, data: Buffer | Uint8Array, metadata?: Record<string, string>): Promise<void>;
  getObject(key: string): Promise<{ data: Buffer; metadata: Record<string, string> } | null>;
  deleteObject(key: string): Promise<void>;
  listObjects(prefix: string, options?: { limit?: number; cursor?: string }): Promise<{
    objects: Array<{ key: string; sizeBytes: number; updatedAt: number }>;
    nextCursor?: string;
  }>;

  // Optional CAS operations (gated by capabilities.supportsCAS)
  putObjectCAS?(data: Buffer | Uint8Array): Promise<{ key: string; deduplicated: boolean }>;

  // Health
  healthCheck(): Promise<{ healthy: boolean; latencyMs: number }>;
}

export interface TunnelProviderCapabilities {
  supportsCustomDomains: boolean;
  supportsTLS: boolean;
  maxConcurrentTunnels: number;
  supportsWebSocket: boolean;
}

export interface TunnelProvider {
  readonly name: string;
  readonly category: 'tunnel';
  readonly capabilities: TunnelProviderCapabilities;

  // Core lifecycle
  expose(config: TunnelConfig): Promise<TunnelInfo>;
  revoke(tunnelId: string): Promise<void>;
  status(tunnelId: string): Promise<TunnelStatus>;

  // Listing
  list(): Promise<TunnelInfo[]>;

  // Health
  healthCheck(): Promise<{ healthy: boolean; latencyMs: number }>;
}

export interface TunnelConfig {
  localPort: number;
  localHost?: string; // default: localhost
  protocol?: 'http' | 'https' | 'tcp';
  subdomain?: string;
}

export interface TunnelInfo {
  tunnelId: string;
  publicUrl: string;
  localPort: number;
  protocol: string;
  createdAt: number;
  status: TunnelStatus;
}

export type TunnelStatus = 'active' | 'starting' | 'stopped' | 'error';

// =============================================================================
// Provider Workflow Contracts
// =============================================================================

// ─── Design Note ─────────────────────────────────────────────────────────────
// These type definitions describe the contract interface between providers and
// the workflow layer. No runtime registry exists yet — it will be created by
// the #WF2 (Workflow Hardening) epic when workflow nodes need contract lookup.
//
// Key consumer: spawn-instance workflow node (two-phase spawn protocol).
// See 031B_WORKLOG §4 for the two-phase spawn protocol blueprint.
// ─────────────────────────────────────────────────────────────────────────────

export interface ProviderWorkflowConstraints {
  maxDurationMs: number;
  requiredOutputFields: string[];
  inputSchema?: Record<string, unknown>;
  emittableResources: string[];
  canSpawnSubworkflows: boolean;
  maxSubworkflowDepth: number;
}

export interface ProviderWorkflowContract {
  type: string;
  provider: ProviderName;
  description: string;
  constraints: ProviderWorkflowConstraints;
  validateInput(input: unknown): ValidationResult;
  compensate(context: CompensationContext): Promise<void>;
}

export interface ValidationResult {
  valid: boolean;
  errors?: string[];
}

export interface CompensationContext {
  workflowId: number;
  nodeId: string;
  input: unknown;
  output?: unknown;
}

export interface SpawnInstanceContract extends ProviderWorkflowContract {
  type: "spawn-instance";

  input: {
    spec: string;
    region?: string;
    spot?: boolean;
    bootstrap: BootstrapConfig;
  };

  output: {
    instanceId: string;      // Our internal ID
    providerId: string;      // Provider's instance ID
    ip: string | null;       // Public IP (may be null initially)
    status: string;
  };
}

export interface TerminateInstanceContract extends ProviderWorkflowContract {
  type: "terminate-instance";

  input: {
    providerId: string;
    gracePeriodMs?: number;
  };

  output: {
    terminated: boolean;
    finalStatus: string;
  };
}

export interface CreateSnapshotContract extends ProviderWorkflowContract {
  type: "create-snapshot";

  input: {
    providerId: string;
    name: string;
    spec: string;
    initChecksum?: string;
  };

  output: {
    snapshotId: string;
    providerSnapshotId: string;
    sizeBytes?: number;
  };
}
