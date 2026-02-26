import { Elysia } from 'elysia';
import type { Catalog } from "../../../scaffold/src/catalog";

// Tenant routing mode: 'single' (all requests → one lifecycle) or 'managed'
// (API key → tenant lookup → per-tenant lifecycle). Single-machine deployments
// use 'single'. Managed mode is for multi-tenant SaaS deployment.

/**
 * Resolve the lifecycle URL for a given request.
 * In single mode, always returns lifecycleUrl.
 * In managed mode, would lookup tenant from API key and route accordingly.
 */
function resolveLifecycleUrl(request: Request, lifecycleUrl: string): string {
  // Managed mode: API key → tenant → lifecycle URL
  // For now, single control plane with tenant-partitioned DB.
  // Future: per-tenant control plane instances.
  return lifecycleUrl;
}

export async function initProxy(catalog: Catalog): Promise<{ shutdown: () => Promise<void> }> {
  const config = catalog.getConfig();
  const proxyPort = config.ports.proxy;

  const lifecycleUrl = catalog.hasService('control')
    ? `http://localhost:${config.ports.control}`
    : process.env.LIFECYCLE_URL || `http://localhost:${config.ports.control}`;

  const app = new Elysia()
    .all('/*', async ({ request }) => {
      const target = new URL(resolveLifecycleUrl(request, lifecycleUrl));
      const url = new URL(request.url);
      url.host = target.host;
      url.port = target.port;
      url.protocol = target.protocol;

      // Forward the request, preserving headers (including Authorization)
      const resp = await fetch(new Request(url.toString(), request));
      return new Response(resp.body, {
        status: resp.status,
        headers: resp.headers,
      });
    })
    .listen({ port: proxyPort, idleTimeout: 0 });

  console.log(`Shell proxy listening on :${proxyPort} → ${lifecycleUrl}`);

  catalog.registerService('proxy', { mode: 'local', version: config.version, ref: app });

  return {
    shutdown: async () => {
      await app.stop();
    },
  };
}

if (import.meta.main) {
  const LIFECYCLE_URL = process.env.LIFECYCLE_URL || 'http://localhost:3000';
  const SHELL_PORT = process.env.SHELL_PORT || 3001;
  const ROUTING_MODE = process.env.ROUTING_MODE || 'single';

  const app = new Elysia()
    .all('/*', async ({ request }) => {
      const lifecycleUrl = ROUTING_MODE === 'single' ? LIFECYCLE_URL : LIFECYCLE_URL;
      const target = new URL(lifecycleUrl);
      const url = new URL(request.url);
      url.host = target.host;
      url.port = target.port;
      url.protocol = target.protocol;

      const resp = await fetch(new Request(url.toString(), request));
      return new Response(resp.body, {
        status: resp.status,
        headers: resp.headers,
      });
    })
    .listen({ port: Number(SHELL_PORT), idleTimeout: 0 });

  console.log(`Shell proxy listening on :${SHELL_PORT} → ${LIFECYCLE_URL} (${ROUTING_MODE} mode)`);
}
