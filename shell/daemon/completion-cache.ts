// daemon/completion-cache.ts — Completion cache push for autocomplete Track A
//
// Queries the control plane for allocations, runs, and instances, then writes
// a structured cache file to ~/.repl/completion-cache.json for the CLI's
// --list-completions handler to consume.
//
// All errors are swallowed — same pattern as triggerSSHRegen(). The cache is
// written atomically via tmp + rename.

import { existsSync, readFileSync, writeFileSync, renameSync } from 'fs';
import { join } from 'path';
import { homedir } from 'os';

const HOME = homedir();
const REPL_DIR = join(HOME, '.repl');
const CACHE_FILE = join(REPL_DIR, 'completion-cache.json');
const CACHE_TMP_FILE = join(REPL_DIR, 'completion-cache.tmp.json');

// =============================================================================
// Env helpers (mirrors daemon/index.ts — copied so this module is self-contained
// and can be loaded after env is already in process.env)
// =============================================================================

function getControlPlaneUrl(): string {
  return process.env.SKYREPL_CONTROL_PLANE_URL ?? 'http://localhost:3000';
}

function getApiKey(): string | null {
  const keyFile = join(REPL_DIR, 'api-key');
  if (!existsSync(keyFile)) return null;
  return readFileSync(keyFile, 'utf-8').trim().split('\n')[0].trim() || null;
}

// =============================================================================
// Cache shape
// =============================================================================

interface CompletionCache {
  version: number;
  updated_at: string; // ISO 8601
  allocations: Array<{
    slug: string;
    status: string;
    run_slug: string | null;
    command: string | null;
    provider: string | null;
    created_at: number;
    debug_hold_until: number | null;
  }>;
  runs: Array<{
    slug: string;
    state: string;
    command: string;
    exit_code: number | null;
    created_at: number;
  }>;
  instances: Array<{
    slug: string;
    provider: string;
    spec: string;
    region: string;
    state: string;
    ip: string | null;
    created_at: number;
  }>;
  profiles: string[];
  providers: string[];
}

// =============================================================================
// Main export
// =============================================================================

export async function triggerCompletionCacheUpdate(): Promise<void> {
  try {
    // Dynamic imports so we pick up env after it's been loaded by the daemon
    const { ApiClient } = await import('../cli/src/client');
    const { idToSlug } = await import('@skyrepl/contracts');
    const { findProjectConfig } = await import('../cli/src/project');

    const client = new ApiClient(getControlPlaneUrl(), getApiKey());

    // Fetch resource lists — non-fatal if control plane is down
    let allocations: CompletionCache['allocations'] = [];
    let runs: CompletionCache['runs'] = [];
    let instances: CompletionCache['instances'] = [];
    let providers: string[] = [];

    try {
      const [allocResult, runResult, instanceResult] = await Promise.all([
        client.listAllocations(),
        client.listRuns(),
        client.listInstances(),
      ]);

      allocations = (allocResult.data ?? []).map((a: any) => ({
        slug: idToSlug(a.id),
        status: a.status ?? '',
        run_slug: a.run_id != null ? idToSlug(a.run_id) : null,
        command: null,
        provider: a.instance_provider ?? null,
        created_at: a.created_at ?? 0,
        debug_hold_until: a.debug_hold_until ?? null,
      }));

      runs = (runResult.data ?? []).map((r: any) => ({
        slug: idToSlug(r.id),
        state: r.workflow_state ?? '',
        command: r.command ?? '',
        exit_code: r.exit_code ?? null,
        created_at: r.created_at ?? 0,
      }));

      instances = (instanceResult.data ?? []).map((i: any) => ({
        slug: idToSlug(i.id),
        provider: i.provider ?? '',
        spec: i.spec ?? '',
        region: i.region ?? '',
        state: i.workflow_state ?? '',
        ip: i.ip ?? null,
        created_at: i.created_at ?? 0,
      }));

      providers = [...new Set(instances.map((i) => i.provider).filter(Boolean))];
    } catch {
      // Control plane unreachable — continue with empty resource lists
      // so that local-only data (profiles) still populates the cache
    }

    // --- Profiles (from nearest repl.toml, null-safe) ---
    let profiles: string[] = [];
    try {
      const project = findProjectConfig();
      if (project) {
        profiles = Object.keys(project.profiles);
      }
    } catch {
      // repl.toml missing or unparseable — not fatal
    }

    // --- Version: increment from existing cache if present ---
    let version = 1;
    if (existsSync(CACHE_FILE)) {
      try {
        const existing = JSON.parse(readFileSync(CACHE_FILE, 'utf-8')) as { version?: number };
        if (typeof existing.version === 'number') {
          version = existing.version + 1;
        }
      } catch {
        // Corrupt cache — start fresh at 1
      }
    }

    const cache: CompletionCache = {
      version,
      updated_at: new Date().toISOString(),
      allocations,
      runs,
      instances,
      profiles,
      providers,
    };

    // Atomic write: tmp file then rename
    writeFileSync(CACHE_TMP_FILE, JSON.stringify(cache, null, 2) + '\n', { mode: 0o600 });
    renameSync(CACHE_TMP_FILE, CACHE_FILE);
  } catch {
    // Control plane unreachable or any other error — not fatal, swallow silently
  }
}
