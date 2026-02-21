// helpers/mock-provider.ts - In-process provider that starts SimulatedAgents
//
// Implements the Provider interface. Instead of spawning a real VM, it starts
// a SimulatedAgent that connects to the control plane via HTTP/SSE. This lets
// us test the full workflow end-to-end without VMs, in <5 seconds.

import type {
  Provider,
  ProviderInstance,
  ProviderCapabilities,
  SpawnOptions,
  BootstrapConfig,
  BootstrapScript,
  ListFilter,
  CreateSnapshotOptions,
  SnapshotRequest,
  SnapshotStatus,
  ProviderSnapshot,
} from "../../../control/src/provider/types";
import { SimulatedAgent, type AgentBehavior } from "./simulated-agent";

// =============================================================================
// Mock Instance
// =============================================================================

interface MockInstance extends ProviderInstance {
  agent: SimulatedAgent;
}

// =============================================================================
// Mock Provider
// =============================================================================

export class MockProvider implements Provider {
  readonly name = "mock" as any;
  readonly capabilities: ProviderCapabilities = {
    snapshots: false,
    spot: false,
    gpu: false,
    multiRegion: false,
    persistentVolumes: false,
    warmVolumes: false,
    hibernation: false,
    costExplorer: false,
    tailscaleNative: false,
    idempotentSpawn: true,
    customNetworking: false,
  };

  private instances = new Map<string, MockInstance>();
  private nextId = 1;

  /** Configure behavior for the next agent that gets spawned. */
  nextBehavior: AgentBehavior | undefined;

  async spawn(options: SpawnOptions): Promise<MockInstance> {
    const id = `mock-${this.nextId++}`;
    const instanceId = options.bootstrap?.environment?.SKYREPL_INSTANCE_ID ?? "0";
    const token = options.bootstrap?.registrationToken ?? "";
    const controlPlaneUrl = options.bootstrap?.controlPlaneUrl ?? "";

    const agent = new SimulatedAgent({
      baseUrl: controlPlaneUrl,
      instanceId,
      authToken: token,
      behavior: this.nextBehavior,
    });
    this.nextBehavior = undefined;

    const instance: MockInstance = {
      id,
      status: "running",
      spec: options.spec,
      ip: "127.0.0.1",
      createdAt: Date.now(),
      isSpot: false,
      agent,
    };

    this.instances.set(id, instance);

    // Connect agent after a microtask delay (mimics brief boot time,
    // ensures spawn-instance DB writes complete before agent connects)
    setTimeout(() => agent.connect(), 10);

    return instance;
  }

  async terminate(providerId: string): Promise<void> {
    const instance = this.instances.get(providerId);
    if (instance) {
      instance.agent.disconnect();
      instance.status = "terminated";
      this.instances.delete(providerId);
    }
  }

  async list(_filter?: ListFilter): Promise<MockInstance[]> {
    return Array.from(this.instances.values());
  }

  async get(providerId: string): Promise<MockInstance | null> {
    return this.instances.get(providerId) ?? null;
  }

  generateBootstrap(_config: BootstrapConfig): BootstrapScript {
    return { content: "#!/bin/bash\n# mock", format: "shell", checksum: "mock" };
  }

  /** Disconnect all agents and clear state. Call in afterAll/afterEach. */
  cleanup(): void {
    for (const instance of this.instances.values()) {
      instance.agent.disconnect();
    }
    this.instances.clear();
    this.nextBehavior = undefined;
  }
}
