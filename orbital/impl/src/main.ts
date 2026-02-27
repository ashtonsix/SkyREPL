import { Elysia, t } from 'elysia';
import { resolve } from 'path';
import type { Catalog } from '../../../scaffold/src/catalog';
import { resolveSpec } from '../../modules/catalog/resolve';
import { scoreSpecs } from '../../modules/catalog/scoring';
import type { CatalogRequest, CatalogResponse, ScoredSpec } from '../../modules/catalog/types';

const ORBITAL_PORT = process.env.ORBITAL_PORT || 3002;

// =============================================================================
// Module Dispatch
// =============================================================================

const CATALOG_MODULE = resolve(import.meta.dir, '../../modules/catalog/main.ts');

/**
 * Invoke an orbital module as a subprocess.
 * Spawn-per-invocation: reads stdin JSON, writes stdout JSON, exit code 0/non-zero.
 */
async function invokeModule<TInput, TOutput>(
  modulePath: string,
  input: TInput,
  timeoutMs = 5000,
): Promise<{ ok: boolean; data: TOutput | null; error?: string }> {
  try {
    const proc = Bun.spawn(['bun', 'run', modulePath], {
      stdin: new Response(JSON.stringify(input)).body!,
      stdout: 'pipe',
      stderr: 'pipe',
    });

    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      proc.kill();
    }, timeoutMs);

    const [stdout, stderr] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
    ]);
    const exitCode = await proc.exited;
    clearTimeout(timer);

    if (timedOut) {
      return { ok: false, data: null, error: `Module timed out after ${timeoutMs}ms` };
    }

    if (exitCode !== 0) {
      return { ok: false, data: null, error: stderr.trim() || `Module exited with code ${exitCode}` };
    }

    const parsed = JSON.parse(stdout) as TOutput;
    return { ok: true, data: parsed };
  } catch (err) {
    return { ok: false, data: null, error: err instanceof Error ? err.message : String(err) };
  }
}

// =============================================================================
// Elysia App
// =============================================================================

function buildApp() {
  return new Elysia()
    // ─── Resolve Spec (D2) — direct import, scored ─────────────────────
    .post('/v1/advisory/resolve-spec', ({ body, set }) => {
      const request: CatalogRequest = {
        action: 'resolve-spec',
        spec: body.spec,
        provider: body.provider || undefined,
        region: body.region || undefined,
        filters: body.filters as CatalogRequest['filters'] || undefined,
      };

      const resolved = resolveSpec(request);
      const scored = scoreSpecs(resolved);

      return {
        ok: true,
        action: 'resolve-spec',
        results: resolved,
        scored,
      };
    }, {
      body: t.Object({
        spec: t.String({ maxLength: 200 }),
        provider: t.Optional(t.String()),
        region: t.Optional(t.String()),
        filters: t.Optional(t.Object({
          minGpuCount: t.Optional(t.Number()),
          minMemoryGib: t.Optional(t.Number()),
          minVcpus: t.Optional(t.Number()),
          arch: t.Optional(t.Union([t.Literal('amd64'), t.Literal('arm64')])),
          hasGpu: t.Optional(t.Boolean()),
          hasSpot: t.Optional(t.Boolean()),
          providers: t.Optional(t.Array(t.String())),
        })),
      }),
    })

    // ─── Retry Alternative (D6) — direct import, scored, region-filtered ─
    .post('/v1/advisory/retry-alternative', ({ body, set }) => {
      const request: CatalogRequest = {
        action: 'resolve-spec',
        spec: body.original_spec,
        filters: body.filters as CatalogRequest['filters'] || undefined,
      };

      const resolved = resolveSpec(request);

      // Filter out the failed provider+region combination (D6: provider, D14: region)
      const filtered = resolved.filter(r =>
        !(r.provider === body.failed_provider &&
          (r.spec === body.failed_spec ||
           (body.failed_region && r.instance.regions.length > 0 &&
            r.instance.regions.every(reg => reg === body.failed_region))))
      );

      const scored = scoreSpecs(filtered);
      const next = scored.length > 0 ? scored[0]! : null;
      const reasoning = next
        ? `Alternative: ${next.resolved.provider}/${next.resolved.spec} (score ${next.score}, ${next.reason})`
        : 'No alternatives available after excluding failed provider/region';

      return {
        ok: true,
        action: 'retry-alternative',
        alternatives: filtered,
        scored,
        next: next ? next.resolved : null,
        reasoning,
      };
    }, {
      body: t.Object({
        original_spec: t.String(),
        failed_provider: t.String(),
        failed_spec: t.String(),
        failed_region: t.Optional(t.String()),
        filters: t.Optional(t.Object({
          minGpuCount: t.Optional(t.Number()),
          minMemoryGib: t.Optional(t.Number()),
          minVcpus: t.Optional(t.Number()),
          arch: t.Optional(t.Union([t.Literal('amd64'), t.Literal('arm64')])),
          hasGpu: t.Optional(t.Boolean()),
          hasSpot: t.Optional(t.Boolean()),
          providers: t.Optional(t.Array(t.String())),
        })),
      }),
    })

    .get('/v1/advisory/health', () => ({ status: 'ok' }));
}

// =============================================================================
// Fabric Init
// =============================================================================

export async function initOrbital(
  catalog: Catalog,
// eslint-disable-next-line @typescript-eslint/no-explicit-any
): Promise<{ shutdown: () => Promise<void>; app: any }> {
  const port = catalog.getConfig().ports.orbital;
  const app = buildApp().listen({ port, idleTimeout: 0 });

  catalog.registerService('orbital', {
    mode: 'local',
    version: catalog.getConfig().version,
    ref: { app, invokeModule },
  });

  return {
    app,
    shutdown: async () => {
      await app.stop();
    },
  };
}

// =============================================================================
// Standalone Entry
// =============================================================================

if (import.meta.main) {
  const app = buildApp().listen({ port: Number(ORBITAL_PORT), idleTimeout: 0 });
  console.log(`Orbital advisory API listening on :${ORBITAL_PORT}`);
}
