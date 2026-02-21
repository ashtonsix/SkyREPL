import { Elysia } from 'elysia';

const LIFECYCLE_URL = process.env.LIFECYCLE_URL || 'http://localhost:3000';
const SHELL_PORT = process.env.SHELL_PORT || 3001;

// Tenant routing mode: 'single' (all requests → one lifecycle) or 'managed'
// (API key → tenant lookup → per-tenant lifecycle). Single-machine deployments
// use 'single'. Managed mode is for multi-tenant SaaS deployment.
const ROUTING_MODE = process.env.ROUTING_MODE || 'single';

/**
 * Resolve the lifecycle URL for a given request.
 * In single mode, always returns LIFECYCLE_URL.
 * In managed mode, would lookup tenant from API key and route accordingly.
 */
function resolveLifecycleUrl(request: Request): string {
  if (ROUTING_MODE === 'single') {
    return LIFECYCLE_URL;
  }

  // Managed mode: API key → tenant → lifecycle URL
  // For now, single control plane with tenant-partitioned DB.
  // Future: per-tenant control plane instances.
  return LIFECYCLE_URL;
}

const app = new Elysia()
  .all('/*', async ({ request }) => {
    const lifecycleUrl = resolveLifecycleUrl(request);
    const target = new URL(lifecycleUrl);
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
  .listen({ port: Number(SHELL_PORT), idleTimeout: 0 });

console.log(`Shell proxy listening on :${SHELL_PORT} → ${LIFECYCLE_URL} (${ROUTING_MODE} mode)`);
