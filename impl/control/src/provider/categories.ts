// provider/categories.ts - Provider Category Interfaces
// Design only: no implementations in Slice 2.

// =============================================================================
// Provider Category Enum
// =============================================================================

export type ProviderCategory = 'compute' | 'storage' | 'tunnel';

// =============================================================================
// Storage Provider Interface
// =============================================================================

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

// =============================================================================
// Tunnel Provider Interface
// =============================================================================

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
