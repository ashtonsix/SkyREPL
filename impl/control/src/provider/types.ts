// provider/types.ts - Provider Interface & Type Definitions
// Fully defined types (no stubs needed for type-only files)

import type { ProviderName } from "@skyrepl/shared";

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
  terminate(providerId: string): Promise<void>;
  list(filter?: ListFilter): Promise<TInstance[]>;
  get(providerId: string): Promise<TInstance | null>;
  generateBootstrap(config: BootstrapConfig): BootstrapScript;

  // Optional methods (gated by capabilities)
  createSnapshot?(providerId: string, options: CreateSnapshotOptions): Promise<SnapshotRequest>;
  getSnapshotStatus?(requestId: string): Promise<SnapshotStatus>;
  deleteSnapshot?(providerSnapshotId: string): Promise<void>;
  getSnapshotByName?(name: string): Promise<TSnapshot | null>;
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
};

// =============================================================================
// Spawn Options & Result
// =============================================================================

export interface SpawnOptions {
  spec: string;
  region?: string;
  spot?: boolean;
  maxSpotPrice?: number;
  initChecksum?: string;
  tags?: Record<string, string>;
  bootstrap: BootstrapConfig;
  snapshotId?: string;
  volumeId?: string;
  networkConfig?: NetworkConfig;
  clientRequestId?: string;
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

// =============================================================================
// Feature Provider Interface
// =============================================================================

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
// Category Interfaces Re-exports
// =============================================================================

export type {
  ProviderCategory,
  StorageProvider,
  StorageProviderCapabilities,
  TunnelProvider,
  TunnelProviderCapabilities,
  TunnelConfig,
  TunnelInfo,
  TunnelStatus,
} from "./categories";
