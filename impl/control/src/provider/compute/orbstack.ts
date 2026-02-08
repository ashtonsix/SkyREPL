// provider/compute/orbstack.ts - OrbStack Provider Implementation
// Stub: All function bodies throw "not implemented"

import type {
  Provider,
  OrbStackInstance,
  SpawnOptions,
  BootstrapConfig,
  BootstrapScript,
  ListFilter,
  CreateSnapshotOptions,
  SnapshotRequest,
  SnapshotStatus,
  ProviderSnapshot,
  ProviderCapabilities,
} from "../types";

// =============================================================================
// OrbStack Provider
// =============================================================================

export class OrbStackProvider implements Provider<OrbStackInstance> {
  readonly name = "orbstack" as const;

  readonly capabilities: ProviderCapabilities = {
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
  };

  async spawn(options: SpawnOptions): Promise<OrbStackInstance> {
    throw new Error("not implemented");
  }

  async terminate(providerId: string): Promise<void> {
    throw new Error("not implemented");
  }

  async list(filter?: ListFilter): Promise<OrbStackInstance[]> {
    throw new Error("not implemented");
  }

  async get(providerId: string): Promise<OrbStackInstance | null> {
    throw new Error("not implemented");
  }

  generateBootstrap(config: BootstrapConfig): BootstrapScript {
    throw new Error("not implemented");
  }

  async createSnapshot(
    providerId: string,
    options: CreateSnapshotOptions
  ): Promise<SnapshotRequest> {
    throw new Error("not implemented");
  }

  async getSnapshotStatus(requestId: string): Promise<SnapshotStatus> {
    throw new Error("not implemented");
  }

  async deleteSnapshot(providerSnapshotId: string): Promise<void> {
    throw new Error("not implemented");
  }

  async getSnapshotByName(name: string): Promise<ProviderSnapshot | null> {
    throw new Error("not implemented");
  }
}

export default new OrbStackProvider();
