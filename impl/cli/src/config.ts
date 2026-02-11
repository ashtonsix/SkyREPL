import { readFileSync, existsSync } from 'fs';
import { homedir } from 'os';
import { join } from 'path';

/**
 * Load a KEY=VALUE env file into process.env (lowest priority — won't overwrite existing vars).
 * Blank lines and lines starting with # are ignored. No shell expansion.
 */
export function loadEnvFile(filePath: string): void {
  if (!existsSync(filePath)) return;
  const content = readFileSync(filePath, 'utf-8');
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    let value = trimmed.slice(eqIdx + 1).trim();
    // Strip optional quotes
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    // Don't overwrite existing env vars
    if (process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

/** Path to the ~/.repl directory. */
export const REPL_DIR = join(homedir(), '.repl');

/**
 * Get control plane URL from environment or config file.
 *
 * Resolution order:
 * 1. SKYREPL_CONTROL_PLANE_URL environment variable (includes cli.env, loaded at startup)
 * 2. Default: http://localhost:3000
 */
export function getControlPlaneUrl(): string {
  if (process.env.SKYREPL_CONTROL_PLANE_URL) {
    return process.env.SKYREPL_CONTROL_PLANE_URL;
  }
  return 'http://localhost:3000';
}

/** Check if an error is a connection-refused error (works with both Bun and Node). */
export function isConnectionRefused(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const code = (err as any).code;
  return code === 'ConnectionRefused' || code === 'ECONNREFUSED' ||
    err.message.includes('ECONNREFUSED') || err.message.includes('Unable to connect');
}

/** Print a table with padded columns. */
export function printTable(headers: string[], rows: string[][], widths: number[]): void {
  const fmt = (row: string[]) => row.map((v, i) => v.padEnd(widths[i])).join('  ');
  console.log(fmt(headers));
  console.log(widths.map(w => '-'.repeat(w)).join('  '));
  for (const row of rows) {
    console.log(fmt(row));
  }
}

/** Map internal workflow_state to a short display string. */
export function displayState(state: string): string {
  if (state.includes('finalized')) return 'completed';
  if (state.includes('executing') || state.includes('syncing')) return 'running';
  if (state.includes('spawning')) return 'spawning';
  if (state.includes('compensat')) return 'cancelling';
  if (state.includes('failed')) return 'failed';
  if (state.includes('pending')) return 'pending';
  if (state.includes('complete')) return 'completed';
  return state;
}
