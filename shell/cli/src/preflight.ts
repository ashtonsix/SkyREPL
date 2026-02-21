// preflight.ts — Pre-launch checks for `repl run` (CLI-03)
//
// Delegates to GET /v1/preflight on the server, which aggregates orphan counts,
// budget checks, credential expiry, and stale allocations in one round-trip.
// Falls back to a local health check if the server endpoint is unreachable.

import { ApiClient } from './client';
import { getOutputMode } from './config';

export interface PreflightWarning {
  level: 'info' | 'warn' | 'error';
  message: string;
}

/**
 * Run preflight checks before launching a run.
 * Returns warnings. Errors are blocking (caller should abort launch).
 */
export async function runPreflight(
  client: ApiClient,
  params?: {
    operation?: 'launch-run' | 'terminate-instance' | 'create-snapshot';
    spec?: string;
    provider?: string;
    region?: string;
  }
): Promise<PreflightWarning[]> {
  const warnings: PreflightWarning[] = [];

  // 1. Check control plane health (fast fail before any other check)
  try {
    await client.healthCheck();
  } catch {
    warnings.push({ level: 'error', message: 'Control plane is not reachable' });
    return warnings; // Can't do other checks
  }

  // 2. Call the server-side preflight endpoint (aggregates all checks)
  try {
    const result = await client.getPreflight({
      operation: params?.operation ?? 'launch-run',
      spec: params?.spec,
      provider: params?.provider,
      region: params?.region,
    });

    // Map server errors → local 'error' level
    for (const err of result.errors ?? []) {
      warnings.push({ level: 'error', message: err.message });
    }

    // Map server warnings → local severity mapping
    for (const w of result.warnings) {
      const level: PreflightWarning['level'] = w.severity === 'critical' ? 'error'
        : w.severity === 'warning' ? 'warn'
        : 'info';
      warnings.push({ level, message: w.message });
    }
  } catch {
    // Server preflight unavailable — non-blocking, continue with launch.
    // The health check above already confirmed the server is up, so this is
    // most likely a transient error or a server version that predates the endpoint.
  }

  return warnings;
}

/**
 * Display preflight warnings to the user.
 * Returns true if launch should proceed, false if an error-level warning blocks it.
 */
export function displayWarnings(warnings: PreflightWarning[]): boolean {
  if (warnings.length === 0) return true;

  if (getOutputMode() === 'quiet') {
    // Quiet mode: only show errors
    const errors = warnings.filter(w => w.level === 'error');
    for (const e of errors) {
      console.error(e.message);
    }
    return errors.length === 0;
  }

  for (const w of warnings) {
    const prefix = w.level === 'error' ? 'ERROR' : w.level === 'warn' ? 'WARN' : 'INFO';
    if (w.level === 'error') {
      console.error(`[${prefix}] ${w.message}`);
    } else {
      console.log(`[${prefix}] ${w.message}`);
    }
  }

  return !warnings.some(w => w.level === 'error');
}
