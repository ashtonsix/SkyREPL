// provider/feature/tailscale-api.ts - Tailscale HTTP API Client
//
// Thin wrapper around the Tailscale v2 REST API. Not a FeatureProvider —
// just the API transport layer consumed by the tailscale-ensure endpoint.
//
// Config: TAILSCALE_API_KEY + TAILSCALE_TAILNET env vars (or ~/.repl/control.env).
// If TAILSCALE_API_KEY is absent, all methods throw TailscaleConfigError.

// =============================================================================
// Types
// =============================================================================

export interface TailscaleDevice {
  id: string;
  hostname: string;
  name: string;
  /** Tailscale IPs (100.x.x.x range) */
  addresses: string[];
  os: string;
  online: boolean;
  lastSeen: string;
  authorized: boolean;
}

export interface CreateAuthKeyOptions {
  ephemeral?: boolean;
  preauthorized?: boolean;
  tags?: string[];
  description?: string;
}

export interface AuthKeyResult {
  key: string;
  id: string;
  expires: string;
}

// =============================================================================
// Error
// =============================================================================

export class TailscaleConfigError extends Error {
  readonly code = "TAILSCALE_NOT_CONFIGURED";
  readonly category = "configuration";
  constructor(message: string) {
    super(message);
    this.name = "TailscaleConfigError";
  }
}

export class TailscaleApiError extends Error {
  readonly code = "TAILSCALE_API_ERROR";
  readonly category = "external";
  constructor(
    message: string,
    public readonly status: number,
    public readonly body?: string
  ) {
    super(message);
    this.name = "TailscaleApiError";
  }
}

// =============================================================================
// Retry helper
// =============================================================================

const RETRY_DELAYS_MS = [1000, 2000, 4000];

// maxAttempts=3: one initial attempt + two retries, delays 1s then 2s between them
async function retryFetch(
  url: string,
  options: RequestInit,
  maxAttempts = 3
): Promise<Response> {
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    let resp: Response;
    try {
      resp = await fetch(url, options);
    } catch (err) {
      // Network-level failure — not retryable (connection refused, DNS, etc.)
      throw err;
    }

    const isRetryable = resp.status === 429 || resp.status >= 500;
    const hasNextAttempt = attempt + 1 < maxAttempts;
    if (!isRetryable || !hasNextAttempt) {
      return resp;
    }

    const delay = RETRY_DELAYS_MS[attempt] ?? RETRY_DELAYS_MS[RETRY_DELAYS_MS.length - 1];
    await new Promise((resolve) => setTimeout(resolve, delay));
  }

  // Should be unreachable
  throw new Error("retryFetch: exhausted attempts");
}

// =============================================================================
// Client
// =============================================================================

export class TailscaleApiClient {
  private apiKey: string;
  private tailnet: string;
  private baseUrl: string;

  constructor(config: { apiKey: string; tailnet: string }) {
    this.apiKey = config.apiKey;
    this.tailnet = config.tailnet;
    this.baseUrl = `https://api.tailscale.com/api/v2`;
  }

  private get authHeader(): string {
    // Tailscale API keys use HTTP Basic with key as the password, empty username
    return `Basic ${btoa(`:${this.apiKey}`)}`;
  }

  private async request<T>(
    method: string,
    path: string,
    body?: unknown
  ): Promise<T> {
    const url = `${this.baseUrl}${path}`;
    const resp = await retryFetch(url, {
      method,
      headers: {
        Authorization: this.authHeader,
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });

    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      throw new TailscaleApiError(
        `Tailscale API ${method} ${path} failed: ${resp.status} ${resp.statusText}`,
        resp.status,
        text
      );
    }

    if (resp.status === 204) {
      return undefined as T;
    }

    return resp.json() as Promise<T>;
  }

  /**
   * Create an ephemeral, pre-approved auth key for a new instance.
   * The key is single-use and short-lived (default: ephemeral + preauthorized).
   */
  async createAuthKey(options: CreateAuthKeyOptions = {}): Promise<AuthKeyResult> {
    const {
      ephemeral = true,
      preauthorized = true,
      tags = [],
      description,
    } = options;

    const capabilities: Record<string, unknown> = {
      devices: {
        create: {
          reusable: false,
          ephemeral,
          preauthorized,
          tags: tags.length > 0 ? tags : undefined,
        },
      },
    };

    const payload: Record<string, unknown> = {
      capabilities,
      expirySeconds: 300, // 5-minute window is enough for install
    };
    if (description) {
      payload.description = description;
    }

    const raw = await this.request<{
      key: string;
      id: string;
      expires: string;
    }>("POST", `/tailnet/${encodeURIComponent(this.tailnet)}/keys`, payload);

    return { key: raw.key, id: raw.id, expires: raw.expires };
  }

  /** List all devices in the tailnet */
  async listDevices(): Promise<TailscaleDevice[]> {
    const raw = await this.request<{ devices: RawDevice[] }>(
      "GET",
      `/tailnet/${encodeURIComponent(this.tailnet)}/devices`
    );
    return (raw.devices ?? []).map(normalizeDevice);
  }

  /** Get a specific device by its Tailscale node ID. Returns null if not found. */
  async getDevice(deviceId: string): Promise<TailscaleDevice | null> {
    try {
      const raw = await this.request<RawDevice>(
        "GET",
        `/device/${encodeURIComponent(deviceId)}`
      );
      return normalizeDevice(raw);
    } catch (err) {
      if (err instanceof TailscaleApiError && err.status === 404) {
        return null;
      }
      throw err;
    }
  }

  /** Remove a device from the tailnet */
  async deleteDevice(deviceId: string): Promise<void> {
    await this.request<void>(
      "DELETE",
      `/device/${encodeURIComponent(deviceId)}`
    );
  }
}

// =============================================================================
// Raw API shape → normalized TailscaleDevice
// =============================================================================

interface RawDevice {
  id: string;
  hostname: string;
  name: string;
  addresses?: string[];
  os?: string;
  online?: boolean;
  lastSeen?: string;
  authorized?: boolean;
}

function normalizeDevice(raw: RawDevice): TailscaleDevice {
  return {
    id: raw.id,
    hostname: raw.hostname ?? "",
    name: raw.name ?? "",
    addresses: raw.addresses ?? [],
    os: raw.os ?? "",
    online: raw.online ?? false,
    lastSeen: raw.lastSeen ?? "",
    authorized: raw.authorized ?? false,
  };
}

// =============================================================================
// Singleton factory — reads env / control.env
// =============================================================================

let _client: TailscaleApiClient | null = null;
let _clientError: string | null = null;

/**
 * Get the configured TailscaleApiClient singleton.
 * Reads TAILSCALE_API_KEY and TAILSCALE_TAILNET from process.env.
 * Throws TailscaleConfigError if either var is missing.
 */
export function getTailscaleClient(): TailscaleApiClient {
  if (_clientError) {
    throw new TailscaleConfigError(_clientError);
  }
  if (_client) {
    return _client;
  }

  const apiKey = process.env.TAILSCALE_API_KEY;
  const tailnet = process.env.TAILSCALE_TAILNET;

  if (!apiKey || !tailnet) {
    const missing = [
      !apiKey && "TAILSCALE_API_KEY",
      !tailnet && "TAILSCALE_TAILNET",
    ]
      .filter(Boolean)
      .join(", ");
    _clientError = `Tailscale not configured: missing env vars: ${missing}`;
    throw new TailscaleConfigError(_clientError);
  }

  _client = new TailscaleApiClient({ apiKey, tailnet });
  return _client;
}

/**
 * Reset the cached singleton (used in tests).
 */
export function _resetTailscaleClient(): void {
  _client = null;
  _clientError = null;
}
